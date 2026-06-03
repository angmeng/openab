use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use tokio::process::Command;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    id: Option<u64>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcNotification {
    jsonrpc: &'static str,
    method: String,
    params: Value,
}

/// Persisted session→conversation mapping stored in ~/.openab/agy-acp/sessions.json
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SessionStore {
    sessions: HashMap<String, StoredSession>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSession {
    conversation_id: Option<String>,
    #[serde(default)]
    prev_line_count: usize,
    /// Hash of the last line at prev_line_count boundary; detects same-length rewrites.
    #[serde(default)]
    prev_last_line_hash: u64,
}

struct Session {
    conversation_id: Option<String>,
    /// Number of lines already delivered to the caller; used for delta extraction.
    prev_line_count: usize,
    /// Hash of the last line at prev_line_count boundary.
    prev_last_line_hash: u64,
}

struct Adapter {
    sessions: HashMap<String, Session>,
    working_dir: String,
    conversations_dir: PathBuf,
    state_file: PathBuf,
}

impl Adapter {
    fn new() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let state_dir = PathBuf::from(&home).join(".openab/agy-acp");
        Self {
            sessions: HashMap::new(),
            working_dir: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/tmp".to_string()),
            conversations_dir: PathBuf::from(&home).join(".gemini/antigravity-cli/conversations"),
            state_file: state_dir.join("sessions.json"),
        }
    }

    /// Acquire exclusive lock on a dedicated lock file for read-write mutual exclusion.
    fn lock_state_file(&self) -> Option<fs::File> {
        if let Some(parent) = self.state_file.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let lock_path = self.state_file.with_extension("lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .ok()?;
        lock_file.lock_exclusive().ok()?;
        Some(lock_file)
    }

    /// Load persisted session store (caller must hold lock).
    fn load_store_inner(&self) -> SessionStore {
        let Some(file) = fs::File::open(&self.state_file).ok() else {
            return SessionStore::default();
        };
        serde_json::from_reader(&file).unwrap_or_default()
    }

    /// Load persisted session store with lock.
    fn load_store(&self) -> SessionStore {
        let _lock = self.lock_state_file();
        self.load_store_inner()
    }

    /// Try to restore conversation_id, prev_line_count, and prev_last_line_hash from persisted state.
    fn restore_session(&self, session_id: &str) -> Option<(String, usize, u64)> {
        let store = self.load_store();
        store.sessions.get(session_id).and_then(|s| {
            s.conversation_id
                .clone()
                .map(|cid| (cid, s.prev_line_count, s.prev_last_line_hash))
        })
    }

    /// Persist a session binding (read-modify-write under single lock).
    fn persist_session(&self, session_id: &str, conversation_id: Option<&str>, prev_line_count: usize, prev_last_line_hash: u64) {
        let Some(_lock) = self.lock_state_file() else {
            return;
        };
        let mut store = self.load_store_inner();
        store.sessions.insert(
            session_id.to_string(),
            StoredSession {
                conversation_id: conversation_id.map(String::from),
                prev_line_count,
                prev_last_line_hash,
            },
        );
        let tmp = self.state_file.with_extension("tmp");
        if let Ok(file) = fs::File::create(&tmp) {
            if serde_json::to_writer_pretty(&file, &store).is_ok() {
                let _ = fs::rename(&tmp, &self.state_file);
            }
        }
    }

    fn conversation_snapshot(&self) -> HashSet<String> {
        let Ok(entries) = fs::read_dir(&self.conversations_dir) else {
            return HashSet::new();
        };
        entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let path = e.path();
                if path.extension().map(|x| x == "pb").unwrap_or(false) {
                    path.file_stem().map(|s| s.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    fn new_conversation_id(&self, before: &HashSet<String>) -> Option<String> {
        let after = self.conversation_snapshot();
        let mut created: Vec<_> = after.difference(before).collect();
        if created.is_empty() {
            return None;
        }
        if created.len() > 1 {
            eprintln!(
                "[agy-acp] WARN: multiple new agy conversation files appeared; \
                 refusing to bind"
            );
            return None;
        }
        Some(created.remove(0).clone())
    }

    fn line_hash(line: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        line.hash(&mut hasher);
        hasher.finish()
    }

    fn extract_delta(full_text: &str, prev_line_count: usize, prev_last_line_hash: u64, conversation_bound: bool) -> (String, usize, u64) {
        let lines: Vec<&str> = full_text.lines().collect();
        let total = lines.len();
        let last_hash = lines.last().map(|l| Self::line_hash(l)).unwrap_or(0);
        if !conversation_bound || prev_line_count == 0 {
            return (full_text.to_string(), total, last_hash);
        }
        if total < prev_line_count {
            eprintln!(
                "[agy-acp] WARN: agy stdout has fewer lines than expected ({total} < {prev_line_count}); \
                 sending full output and resetting delta baseline"
            );
            return (full_text.to_string(), total, last_hash);
        }
        // Verify the boundary line hash to detect same-length rewrites
        if total == prev_line_count {
            if prev_last_line_hash != 0 && last_hash != prev_last_line_hash {
                eprintln!(
                    "[agy-acp] WARN: agy stdout has same line count but different content; \
                     sending full output"
                );
                return (full_text.to_string(), total, last_hash);
            }
            // Same count, same hash — genuinely no new content
            return (String::new(), total, last_hash);
        }
        // Verify that the line at prev boundary still matches
        if prev_line_count > 0 && prev_last_line_hash != 0 {
            let boundary_hash = Self::line_hash(lines[prev_line_count - 1]);
            if boundary_hash != prev_last_line_hash {
                eprintln!(
                    "[agy-acp] WARN: agy stdout content changed at boundary; \
                     sending full output and resetting delta baseline"
                );
                return (full_text.to_string(), total, last_hash);
            }
        }
        let delta = lines[prev_line_count..].join("\n");
        (delta, total, last_hash)
    }

    fn evict_if_needed(&mut self) {
        const MAX_SESSIONS: usize = 64;
        while self.sessions.len() >= MAX_SESSIONS {
            if let Some(key) = self.sessions.keys().next().cloned() {
                self.sessions.remove(&key);
            }
        }
    }

    fn restore_session_state(&mut self, session_id: &str) -> bool {
        let Some((conversation_id, prev_line_count, prev_last_line_hash)) = self.restore_session(session_id) else {
            return false;
        };
        // Evict only after confirming the restore target exists
        if !self.sessions.contains_key(session_id) {
            self.evict_if_needed();
        }
        self.sessions.insert(
            session_id.to_string(),
            Session {
                conversation_id: Some(conversation_id),
                prev_line_count,
                prev_last_line_hash,
            },
        );
        true
    }

    fn handle_initialize(&self, id: u64) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({
                "protocolVersion": 1,
                "agentInfo": { "name": "agy", "version": env!("CARGO_PKG_VERSION") },
                "agentCapabilities": { "streaming": true, "loadSession": true },
            })),
            error: None,
        }
    }

    fn handle_session_new(&mut self, id: u64) -> JsonRpcResponse {
        let session_id = Uuid::new_v4().to_string();
        self.evict_if_needed();
        let conversation_id = None;
        self.sessions.insert(
            session_id.clone(),
            Session {
                conversation_id,
                prev_line_count: 0,
                prev_last_line_hash: 0,
            },
        );
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "sessionId": session_id })),
            error: None,
        }
    }

    fn handle_session_load(&mut self, id: u64, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if session_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId"})),
            };
        }

        if self.restore_session_state(session_id) {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(json!({ "sessionId": session_id })),
                error: None,
            };
        }

        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(json!({
                "code": -32000,
                "message": format!("unknown sessionId: {session_id}"),
            })),
        }
    }

    async fn handle_session_prompt(&mut self, id: u64, params: &Value) -> Vec<String> {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Restore evicted session from state file if needed
        if !session_id.is_empty() && !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        let prompt_text = params
            .get("prompt")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        let clean_prompt = prompt_text.trim();

        // Take snapshot before spawning agy if we need to bind a conversation
        let snapshot = if self
            .sessions
            .get(session_id)
            .map(|s| s.conversation_id.is_none())
            .unwrap_or(false)
        {
            Some(self.conversation_snapshot())
        } else {
            None
        };

        // Build args
        let mut args: Vec<String> = Vec::new();
        args.push("--add-dir".to_string());
        args.push(self.working_dir.clone());
        if let Ok(extra) = std::env::var("AGY_EXTRA_ARGS") {
            args.extend(extra.split_whitespace().map(String::from));
        }
        if let Some(session) = self.sessions.get(session_id) {
            if let Some(conv_id) = &session.conversation_id {
                args.push("--conversation".to_string());
                args.push(conv_id.clone());
            }
        }
        args.push("-p".to_string());
        args.push(clean_prompt.to_string());

        let result = Command::new("agy")
            .args(&args)
            .current_dir(&self.working_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await;

        let mut output_lines = Vec::new();

        match result {
            Ok(output) => {
                // Log stderr if non-empty
                let stderr_text = String::from_utf8_lossy(&output.stderr);
                if !stderr_text.is_empty() {
                    eprintln!("[agy-acp] agy stderr: {}", stderr_text.trim_end());
                }

                if !output.status.success() {
                    eprintln!("[agy-acp] WARN: agy exited with status: {}", output.status);
                    if output.stdout.is_empty() {
                        let msg = if stderr_text.is_empty() {
                            format!("agy exited with status: {}", output.status)
                        } else {
                            format!("agy failed: {}", stderr_text.trim_end())
                        };
                        let resp = JsonRpcResponse {
                            jsonrpc: "2.0",
                            id,
                            result: None,
                            error: Some(json!({"code":-32000,"message":msg})),
                        };
                        output_lines.push(serde_json::to_string(&resp).unwrap());
                        return output_lines;
                    }
                }

                let full_text = String::from_utf8_lossy(&output.stdout).to_string();

                let prev_line_count = self
                    .sessions
                    .get(session_id)
                    .map(|s| s.prev_line_count)
                    .unwrap_or(0);
                let prev_last_line_hash = self
                    .sessions
                    .get(session_id)
                    .map(|s| s.prev_last_line_hash)
                    .unwrap_or(0);
                let conversation_bound = self
                    .sessions
                    .get(session_id)
                    .map(|s| s.conversation_id.is_some())
                    .unwrap_or(false);
                let (new_text, total_lines, last_hash) = Self::extract_delta(&full_text, prev_line_count, prev_last_line_hash, conversation_bound);

                // Bind conversation from snapshot diff
                let conv_id = snapshot
                    .as_ref()
                    .and_then(|before| self.new_conversation_id(before));

                let mut should_persist = false;
                if let Some(session) = self.sessions.get_mut(session_id) {
                    let newly_bound = session.conversation_id.is_none() && conv_id.is_some();
                    if session.conversation_id.is_none() {
                        session.conversation_id = conv_id.clone();
                    }
                    if session.conversation_id.is_some() {
                        session.prev_line_count = total_lines;
                        session.prev_last_line_hash = last_hash;
                        should_persist = true;
                    } else {
                        session.prev_line_count = 0;
                        session.prev_last_line_hash = 0;
                        eprintln!(
                            "[agy-acp] WARN: could not bind conversation ID; \
                             running in single-turn mode"
                        );
                    }
                    let _ = newly_bound; // persist regardless — line count changed
                }
                if should_persist {
                    let cid = self.sessions.get(session_id).and_then(|s| s.conversation_id.clone());
                    self.persist_session(session_id, cid.as_deref(), total_lines, last_hash);
                }

                let notification = serde_json::to_string(&JsonRpcNotification {
                    jsonrpc: "2.0",
                    method: "session/update".to_string(),
                    params: json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": new_text },
                        },
                    }),
                })
                .unwrap();
                output_lines.push(notification);
                let resp = JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: Some(json!({ "stopReason": "end_turn" })),
                    error: None,
                };
                output_lines.push(serde_json::to_string(&resp).unwrap());
            }
            Err(e) => {
                let resp = JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(json!({"code":-32000,"message":format!("failed to run agy: {e}")})),
                };
                output_lines.push(serde_json::to_string(&resp).unwrap());
            }
        }
        output_lines
    }
}

#[tokio::main]
async fn main() {
    let mut adapter = Adapter::new();

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(l) if !l.trim().is_empty() => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
                _ => {}
            }
        }
    });

    let mut stdout = io::stdout();

    while let Some(line) = rx.recv().await {
        let req: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let id = match req.id {
            Some(id) => id,
            None => continue,
        };

        let output = match req.method.as_deref() {
            Some("initialize") => {
                vec![serde_json::to_string(&adapter.handle_initialize(id)).unwrap()]
            }
            Some("session/new") => {
                vec![serde_json::to_string(&adapter.handle_session_new(id)).unwrap()]
            }
            Some("session/load") => {
                let params = req.params.unwrap_or(json!({}));
                vec![serde_json::to_string(&adapter.handle_session_load(id, &params)).unwrap()]
            }
            Some("session/prompt") => {
                let params = req.params.unwrap_or(json!({}));
                adapter.handle_session_prompt(id, &params).await
            }
            Some("session/cancel") => {
                let r = JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: Some(json!({})),
                    error: None,
                };
                vec![serde_json::to_string(&r).unwrap()]
            }
            Some(method) => {
                let r = JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(
                        json!({"code":-32601,"message":format!("method not found: {method}")}),
                    ),
                };
                vec![serde_json::to_string(&r).unwrap()]
            }
            None => continue,
        };

        for line in output {
            let _ = writeln!(stdout, "{}", line);
        }
        let _ = stdout.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_delta_returns_full_text_when_unbound() {
        let (result, count, _hash) = Adapter::extract_delta("old\nnew", 1, 0, false);
        assert_eq!(result, "old\nnew");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_extract_delta_skips_previous_lines_when_bound() {
        let (result, count, _hash) =
            Adapter::extract_delta("first response\nsecond response", 1, 0, true);
        assert_eq!(result, "second response");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_extract_delta_returns_full_when_fewer_lines_than_expected() {
        let (result, count, _hash) = Adapter::extract_delta("short", 5, 0, true);
        assert_eq!(result, "short");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_extract_delta_preserves_indentation() {
        let (result, count, _hash) = Adapter::extract_delta("hello\n  indented code", 1, 0, true);
        assert_eq!(result, "  indented code");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_extract_delta_detects_same_length_rewrite() {
        let original = "line one\nline two";
        let (_result, _count, hash) = Adapter::extract_delta(original, 0, 0, false);
        // Now simulate same line count but different content
        let rewritten = "line one\nline CHANGED";
        let (result, count, _new_hash) = Adapter::extract_delta(rewritten, 2, hash, true);
        // Should detect mismatch and send full output
        assert_eq!(result, rewritten);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_extract_delta_boundary_hash_mismatch_sends_full() {
        let original = "aaa\nbbb\nccc";
        // Simulate: prev_line_count=2, hash of line "bbb" (the boundary)
        let boundary_hash = Adapter::line_hash("bbb");
        // Now content changed at boundary
        let modified = "aaa\nXXX\nccc\nnew line";
        let (result, count, _hash) = Adapter::extract_delta(modified, 2, boundary_hash, true);
        // Should detect boundary mismatch and send full
        assert_eq!(result, modified);
        assert_eq!(count, 4);
    }

    #[test]
    fn test_initialize_advertises_load_session_support() {
        let adapter = Adapter::new();
        let response = adapter.handle_initialize(1);
        assert_eq!(
            response
                .result
                .as_ref()
                .and_then(|r| r.get("agentCapabilities"))
                .and_then(|c| c.get("loadSession"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    #[ignore] // filesystem I/O — run with CHI_INTEG=1
    fn test_session_load_restores_persisted_session() {
        let root = std::env::temp_dir().join(format!("agy-acp-load-{}", Uuid::new_v4()));
        let _ = fs::create_dir_all(&root);

        let mut adapter = Adapter {
            sessions: HashMap::new(),
            working_dir: root.to_string_lossy().to_string(),
            conversations_dir: root.join("conversations"),
            state_file: root.join("sessions.json"),
        };
        adapter.persist_session("sess-1", Some("conv-abc"), 10, 12345);

        let response = adapter.handle_session_load(7, &json!({"sessionId": "sess-1"}));
        assert!(response.error.is_none());
        assert_eq!(
            adapter
                .sessions
                .get("sess-1")
                .and_then(|s| s.conversation_id.as_deref()),
            Some("conv-abc")
        );
        assert_eq!(
            adapter
                .sessions
                .get("sess-1")
                .map(|s| s.prev_line_count),
            Some(10)
        );
        assert_eq!(
            adapter
                .sessions
                .get("sess-1")
                .map(|s| s.prev_last_line_hash),
            Some(12345)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[ignore] // filesystem I/O — run with CHI_INTEG=1
    fn test_session_load_rejects_unknown_session() {
        let root = std::env::temp_dir().join(format!("agy-acp-missing-{}", Uuid::new_v4()));
        let _ = fs::create_dir_all(&root);

        let mut adapter = Adapter {
            sessions: HashMap::new(),
            working_dir: root.to_string_lossy().to_string(),
            conversations_dir: root.join("conversations"),
            state_file: root.join("sessions.json"),
        };

        let response = adapter.handle_session_load(9, &json!({"sessionId": "missing"}));
        assert!(response.result.is_none());
        assert_eq!(
            response
                .error
                .as_ref()
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str()),
            Some("unknown sessionId: missing")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[ignore] // filesystem I/O — run with CHI_INTEG=1
    fn test_new_conversation_id_returns_none_when_multiple_files() {
        let root = std::env::temp_dir().join(format!("agy-acp-multi-{}", Uuid::new_v4()));
        let conv_dir = root.join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();

        let adapter = Adapter {
            sessions: HashMap::new(),
            working_dir: root.to_string_lossy().to_string(),
            conversations_dir: conv_dir.clone(),
            state_file: root.join("sessions.json"),
        };

        let before = adapter.conversation_snapshot();
        fs::write(conv_dir.join("a.pb"), b"").unwrap();
        fs::write(conv_dir.join("b.pb"), b"").unwrap();

        assert_eq!(adapter.new_conversation_id(&before), None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[ignore] // filesystem I/O — run with CHI_INTEG=1
    fn test_snapshot_diff_binds_single_new_conversation() {
        let root = std::env::temp_dir().join(format!("agy-acp-snap-{}", Uuid::new_v4()));
        let conv_dir = root.join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        fs::write(conv_dir.join("existing.pb"), b"old").unwrap();

        let adapter = Adapter {
            sessions: HashMap::new(),
            working_dir: root.to_string_lossy().to_string(),
            conversations_dir: conv_dir.clone(),
            state_file: root.join("sessions.json"),
        };

        let before = adapter.conversation_snapshot();
        fs::write(conv_dir.join("new-conv.pb"), b"new").unwrap();

        assert_eq!(
            adapter.new_conversation_id(&before),
            Some("new-conv".to_string())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[ignore] // filesystem I/O — run with CHI_INTEG=1
    fn test_persist_and_restore_session_binding() {
        let root = std::env::temp_dir().join(format!("agy-acp-state-{}", Uuid::new_v4()));
        let _ = fs::create_dir_all(&root);

        let adapter = Adapter {
            sessions: HashMap::new(),
            working_dir: root.to_string_lossy().to_string(),
            conversations_dir: root.join("conversations"),
            state_file: root.join("sessions.json"),
        };

        adapter.persist_session("sess-1", Some("conv-abc"), 7, 99999);
        let restored = adapter.restore_session("sess-1");
        assert_eq!(restored, Some(("conv-abc".to_string(), 7, 99999)));

        let missing = adapter.restore_session("sess-unknown");
        assert_eq!(missing, None);

        let _ = fs::remove_dir_all(root);
    }
}
