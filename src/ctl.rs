//! `openab set/get` IPC over Unix domain socket.
//!
//! Architecture (like consul/vault):
//! - `openab run` spawns a UnixListener at a well-known path.
//! - `openab set key value` connects, sends a JSON request, reads the response.
//!
//! Phase 1 supported keys:
//! - `thread.name` — rename the current Discord/Slack thread

use crate::adapter::{ChannelRef, ChatAdapter};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

/// Default socket path. Overridable via `OPENAB_SOCK` env var.
pub fn socket_path() -> PathBuf {
    std::env::var("OPENAB_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/openab.sock"))
}

// ─── Protocol ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub action: Action,
    pub key: String,
    pub value: Option<String>,
    /// Target thread/channel ID — daemon uses this to route to the correct adapter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Set,
    Get,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

// ─── Server (runs inside `openab run`) ──────────────────────────────────────

/// Handler trait — `openab run` provides the concrete implementation that
/// can access Discord/Slack adapters.
#[async_trait::async_trait]
pub trait CtlHandler: Send + Sync + 'static {
    async fn handle_set(&self, thread_id: Option<&str>, key: &str, value: &str) -> Response;
    async fn handle_get(&self, thread_id: Option<&str>, key: &str) -> Response;
}

/// Start the control socket server. Call this from `openab run` startup.
/// Returns a JoinHandle; abort it on shutdown.
pub fn spawn_server(
    handler: std::sync::Arc<dyn CtlHandler>,
) -> tokio::task::JoinHandle<()> {
    spawn_server_at(socket_path(), handler)
}

/// Start the control socket server at a specific path.
pub fn spawn_server_at(
    path: PathBuf,
    handler: std::sync::Arc<dyn CtlHandler>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Remove stale socket file
        let _ = std::fs::remove_file(&path);
        let listener = match UnixListener::bind(&path) {
            Ok(l) => l,
            Err(e) => {
                error!(path = %path.display(), error = %e, "failed to bind control socket");
                return;
            }
        };
        // Restrict socket to owner only (defense-in-depth for shared hosts).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        info!(path = %path.display(), "control socket listening");

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!(error = %e, "control socket accept error");
                    continue;
                }
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(stream, &*handler).await {
                    debug!(error = %e, "control socket connection error");
                }
            });
        }
    })
}

async fn handle_conn(
    stream: UnixStream,
    handler: &dyn CtlHandler,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    if let Some(line) = lines.next_line().await? {
        let req: Request = serde_json::from_str(&line)?;
        let resp = match req.action {
            Action::Set => {
                let val = req.value.as_deref().unwrap_or("");
                handler.handle_set(req.thread_id.as_deref(), &req.key, val).await
            }
            Action::Get => handler.handle_get(req.thread_id.as_deref(), &req.key).await,
        };
        let mut buf = serde_json::to_vec(&resp)?;
        buf.push(b'\n');
        writer.write_all(&buf).await?;
    }
    Ok(())
}

// ─── Client (used by `openab set/get` subcommands) ──────────────────────────

/// Thread registry: maps thread_id → platform name.
/// Shared between the message dispatcher (writes) and the ctl handler (reads).
pub type ThreadRegistry = Arc<tokio::sync::RwLock<std::collections::HashMap<String, String>>>;

/// Create an empty thread registry.
pub fn new_registry() -> ThreadRegistry {
    Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Register a thread→platform mapping. Called by adapters on message dispatch.
pub async fn register_thread(registry: &ThreadRegistry, thread_id: &str, platform: &str) {
    registry.write().await.insert(thread_id.to_string(), platform.to_string());
}

/// Concrete handler for `openab run` — dispatches to platform adapters.
pub struct RuntimeHandler {
    /// Registered adapters by platform name.
    adapters: std::collections::HashMap<String, Arc<dyn ChatAdapter>>,
    /// thread_id → platform mapping. Populated by `openab run` when it dispatches messages.
    registry: Arc<tokio::sync::RwLock<std::collections::HashMap<String, String>>>,
    shard: Arc<std::sync::OnceLock<serenity::gateway::ShardMessenger>>,
}

impl RuntimeHandler {
    pub fn new(
        adapters: std::collections::HashMap<String, Arc<dyn ChatAdapter>>,
        registry: Arc<tokio::sync::RwLock<std::collections::HashMap<String, String>>>,
        shard: Arc<std::sync::OnceLock<serenity::gateway::ShardMessenger>>,
    ) -> Self {
        Self { adapters, registry, shard }
    }

    /// Resolve which adapter to use for a given thread_id.
    async fn resolve(&self, thread_id: Option<&str>) -> Option<(Arc<dyn ChatAdapter>, String)> {
        let tid = thread_id?;
        let platform = self.registry.read().await.get(tid).cloned()?;
        let adapter = self.adapters.get(&platform)?.clone();
        Some((adapter, tid.to_string()))
    }
}

#[async_trait::async_trait]
impl CtlHandler for RuntimeHandler {
    async fn handle_set(&self, thread_id: Option<&str>, key: &str, value: &str) -> Response {
        match key {
            "thread.name" => {
                let Some((adapter, tid)) = self.resolve(thread_id).await else {
                    return Response {
                        ok: false,
                        message: "unknown thread (use --thread or register via message dispatch)".into(),
                        value: None,
                    };
                };
                let channel = ChannelRef {
                    platform: String::new(),
                    channel_id: tid,
                    thread_id: None,
                    parent_id: None,
                    origin_event_id: None,
                };
                match adapter.rename_thread(&channel, value).await {
                    Ok(()) => Response {
                        ok: true,
                        message: format!("thread renamed to: {value}"),
                        value: None,
                    },
                    Err(e) => Response {
                        ok: false,
                        message: format!("rename failed: {e}"),
                        value: None,
                    },
                }
            }
            "thread.archived" => {
                let Some((adapter, tid)) = self.resolve(thread_id).await else {
                    return Response {
                        ok: false,
                        message: "unknown thread (use --thread or register via message dispatch)".into(),
                        value: None,
                    };
                };
                let archived = match value {
                    "true" | "1" | "yes" => true,
                    "false" | "0" | "no" => false,
                    _ => {
                        return Response {
                            ok: false,
                            message: format!("invalid value: {value} (expected true/false)"),
                            value: None,
                        };
                    }
                };
                let channel = ChannelRef {
                    platform: String::new(),
                    channel_id: tid,
                    thread_id: None,
                    parent_id: None,
                    origin_event_id: None,
                };
                match adapter.archive_thread(&channel, archived).await {
                    Ok(()) => Response {
                        ok: true,
                        message: format!(
                            "thread {}",
                            if archived { "archived" } else { "unarchived" }
                        ),
                        value: None,
                    },
                    Err(e) => Response {
                        ok: false,
                        message: format!("archive failed: {e}"),
                        value: None,
                    },
                }
            }
            "agent.status" => {
                let Some(shard) = self.shard.get() else {
                    return Response {
                        ok: false,
                        message: "agent.status only supported on Discord".into(),
                        value: None,
                    };
                };
                use serenity::gateway::ActivityData;
                use serenity::model::user::OnlineStatus;
                let activity = if value.is_empty() {
                    None
                } else {
                    Some(ActivityData::custom(value))
                };
                shard.set_presence(activity, OnlineStatus::Online);
                Response {
                    ok: true,
                    message: if value.is_empty() {
                        "status cleared".into()
                    } else {
                        format!("status set to: {value}")
                    },
                    value: None,
                }
            }
            _ => Response {
                ok: false,
                message: format!("unknown key: {key}"),
                value: None,
            },
        }
    }

    async fn handle_get(&self, _thread_id: Option<&str>, key: &str) -> Response {
        match key {
            "thread.name" | "thread.archived" | "agent.status" => Response {
                ok: false,
                message: format!("{key} get not yet supported"),
                value: None,
            },
            _ => Response {
                ok: false,
                message: format!("unknown key: {key}"),
                value: None,
            },
        }
    }
}

pub async fn send_request(req: &Request) -> anyhow::Result<Response> {
    send_request_to(&socket_path(), req).await
}

/// Send a request to a specific socket path.
pub async fn send_request_to(path: &PathBuf, req: &Request) -> anyhow::Result<Response> {
    let stream = UnixStream::connect(&path).await.map_err(|e| {
        anyhow::anyhow!(
            "cannot connect to openab at {}: {} (is `openab run` running?)",
            path.display(),
            e
        )
    })?;
    let (reader, mut writer) = stream.into_split();
    let mut buf = serde_json::to_vec(req)?;
    buf.push(b'\n');
    writer.write_all(&buf).await?;
    writer.shutdown().await?;

    let mut lines = BufReader::new(reader).lines();
    let line = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow::anyhow!("no response from openab"))?;
    let resp: Response = serde_json::from_str(&line)?;
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_serialization() {
        let req = Request {
            action: Action::Set,
            key: "thread.name".into(),
            value: Some("hello".into()),
            thread_id: Some("123".into()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, Action::Set);
        assert_eq!(parsed.key, "thread.name");
        assert_eq!(parsed.value.as_deref(), Some("hello"));
        assert_eq!(parsed.thread_id.as_deref(), Some("123"));
    }

    #[tokio::test]
    async fn server_client_roundtrip() {
        struct MockHandler;
        #[async_trait::async_trait]
        impl CtlHandler for MockHandler {
            async fn handle_set(&self, thread_id: Option<&str>, key: &str, value: &str) -> Response {
                Response {
                    ok: true,
                    message: format!("{key} = {value} (thread: {})", thread_id.unwrap_or("none")),
                    value: None,
                }
            }
            async fn handle_get(&self, _thread_id: Option<&str>, key: &str) -> Response {
                Response {
                    ok: true,
                    message: String::new(),
                    value: Some(format!("val-of-{key}")),
                }
            }
        }

        // Use a temp path to avoid conflicts
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");

        let handler = std::sync::Arc::new(MockHandler);
        let server = spawn_server_at(sock.clone(), handler);
        // Give server a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Test set
        let resp = send_request_to(&sock, &Request {
            action: Action::Set,
            key: "thread.name".into(),
            value: Some("hello world".into()),
            thread_id: Some("999".into()),
        })
        .await
        .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.message, "thread.name = hello world (thread: 999)");

        // Test get
        let resp = send_request_to(&sock, &Request {
            action: Action::Get,
            key: "thread.name".into(),
            value: None,
            thread_id: None,
        })
        .await
        .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.value.as_deref(), Some("val-of-thread.name"));

        server.abort();
    }
}
