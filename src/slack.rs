use crate::acp::ContentBlock;
use crate::adapter::{ChannelRef, ChatAdapter, MessageRef, SenderContext};
use crate::bot_turns::{BotTurnTracker, TurnAction, TurnSeverity};
use crate::config::{AllowBots, AllowUsers, SttConfig};
use crate::media;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite;
use tracing::{debug, error, info, warn};

/// Marker syntax for outbound file attachments in agent text output.
/// Tifa writes `<<openab-send-file /abs/path/to/file>>` in her response, OpenAB
/// intercepts before posting and uploads the file via Slack's files API.
///
/// IMPORTANT: do NOT use colons in this marker (`:something:` would collide
/// with Slack's emoji shortcode parser and get rendered as a gray-box
/// placeholder even before our interceptor runs).
const FILE_SEND_MARKER_PREFIX: &str = "<<openab-send-file ";
const FILE_SEND_MARKER_SUFFIX: &str = ">>";
/// Sanity cap so an agent typo doesn't try to upload /Users/jazlim or similar.
/// Slack's own per-file limit is 1 GB by default; we cap at 100 MB.
const FILE_SEND_MAX_BYTES: u64 = 100 * 1024 * 1024;

/// Marker syntax for owner-triggered Slack channel creation. DM-only: only
/// honored when the agent's reply is going to a DM channel (id starts with 'D'),
/// so channel/relay chatter can never trigger it.
/// `<<openab-create-channel name=<slug> [private] [topic="…"] [invite=@U…,@U…]>>`
const CREATE_CHANNEL_MARKER_PREFIX: &str = "<<openab-create-channel ";
const CREATE_CHANNEL_MARKER_SUFFIX: &str = ">>";

const SLACK_API: &str = "https://slack.com/api";

/// Map Unicode emoji to Slack short names for reactions API.
/// Only covers the default `[reactions.emojis]` set. Custom emoji configured
/// outside this map will fall back to `grey_question`.
fn unicode_to_slack_emoji(unicode: &str) -> &str {
    match unicode {
        "👀" => "eyes",
        "🤔" => "thinking_face",
        "🔥" => "fire",
        "👨\u{200d}💻" => "technologist",
        "⚡" => "zap",
        "🆗" => "ok",
        "😱" => "scream",
        "🚫" => "no_entry_sign",
        "😊" => "blush",
        "😎" => "sunglasses",
        "🫡" => "saluting_face",
        "🤓" => "nerd_face",
        "😏" => "smirk",
        "✌\u{fe0f}" => "v",
        "💪" => "muscle",
        "🦾" => "mechanical_arm",
        "🥱" => "yawning_face",
        "😨" => "fearful",
        "✅" => "white_check_mark",
        "❌" => "x",
        "🔧" => "wrench",
        "🎤" => "microphone",
        _ => "grey_question",
    }
}

// --- SlackAdapter: implements ChatAdapter for Slack ---

/// TTL for cached user display names (5 minutes).
const USER_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Maximum entries in the participation cache before eviction.
const PARTICIPATION_CACHE_MAX: usize = 1000;

/// Maximum entries in the streams map before eviction (safety net for
/// aborted turns that begin a stream but never reach stream_finish).
const STREAM_CACHE_MAX: usize = 1024;

#[derive(Default)]
struct StreamEntry {
    active: bool,
    degraded_buf: String,
}

pub struct SlackAdapter {
    client: reqwest::Client,
    bot_token: String,
    bot_user_id: tokio::sync::OnceCell<String>,
    user_cache: tokio::sync::Mutex<HashMap<String, (String, tokio::time::Instant)>>,
    /// Cache: Bot ID (B...) → Bot User ID (U...) for trusted_bot_ids matching.
    bot_id_cache: tokio::sync::Mutex<HashMap<String, String>>,
    /// Positive-only cache: thread_ts → cached_at for threads where bot has participated.
    participated_threads: tokio::sync::Mutex<HashMap<String, tokio::time::Instant>>,
    /// Positive-only cache: thread_ts → cached_at for threads where other bots have posted.
    /// Like participation, a thread becoming multi-bot is irreversible (bot messages don't disappear).
    multibot_threads: tokio::sync::Mutex<HashMap<String, tokio::time::Instant>>,
    /// TTL for participation cache entries (matches session_ttl_hours from config).
    session_ttl: std::time::Duration,
    /// Config `[slack].streaming`. When false, streaming (typewriter) edits are
    /// disabled outright — every reply is sent once via chat.postMessage. (B)
    /// Previously the SlackAdapter ignored this flag entirely; the only gate was
    /// the runtime `!other_bot_present` check, so `streaming = false` was dead.
    streaming: bool,
    /// Trusted peer-bot user IDs (from `[slack].trusted_bot_ids`). Used to decide
    /// whether a channel is a multi-bot context where streaming must be off — see
    /// `use_streaming`. (A) A streamed reply reaches peer bots only as
    /// `message_changed` events, which every bot's handler skips, so any @mention
    /// of a peer bot in a streamed message never triggers it.
    trusted_bot_ids: HashSet<String>,
    /// Dedup set for file uploads — keyed on `{channel}|{thread_ts}|{path}`.
    /// Streaming edit_message can fire repeatedly during a single agent turn,
    /// each potentially containing the file-send marker. Without dedup we'd
    /// re-upload the same file dozens of times. Cleared once per session restart.
    file_upload_cache: tokio::sync::Mutex<HashSet<String>>,
    /// Channel allowlist (from `[slack].allowed_channels`), shared mutable so a
    /// channel the bot CREATES (via the `<<openab-create-channel>>` marker) can
    /// be added at runtime — otherwise the bot is deaf in its own ticket channels
    /// until the next restart (observed 2026-06-04: @mention in a freshly-created
    /// `at-2043-…` channel was dropped at the gate). The event loop reads a
    /// snapshot per message; `create_channel_in_slack` inserts after a successful
    /// create. `allow_all_channels` (separate flag) still bypasses this entirely.
    allowed_channels: Arc<tokio::sync::RwLock<HashSet<String>>>,
    /// Path to the on-disk config file, so runtime allowlist additions (a channel
    /// the bot is invited into, or creates) can be PERSISTED — otherwise they're
    /// lost on restart. None when the config came from a URL (can't write back).
    config_path: Option<std::path::PathBuf>,
    /// Assistant mode: stream via chat.startStream + assistant.threads.setStatus.
    assistant_mode: bool,
    /// streaming message ts → state. active=false = degraded (post+edit fallback).
    /// Lifecycle: stream_begin inserts, stream_finish removes; insert_stream
    /// bounds the map (STREAM_CACHE_MAX) as a safety net against aborted turns.
    streams: tokio::sync::Mutex<HashMap<String, StreamEntry>>,
}

impl SlackAdapter {
    pub fn new(
        bot_token: String,
        session_ttl: std::time::Duration,
        _allow_bot_messages: AllowBots,
        streaming: bool,
        trusted_bot_ids: HashSet<String>,
        allowed_channels: HashSet<String>,
        config_path: Option<std::path::PathBuf>,
        assistant_mode: bool,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            bot_token,
            bot_user_id: tokio::sync::OnceCell::new(),
            user_cache: tokio::sync::Mutex::new(HashMap::new()),
            bot_id_cache: tokio::sync::Mutex::new(HashMap::new()),
            participated_threads: tokio::sync::Mutex::new(HashMap::new()),
            multibot_threads: tokio::sync::Mutex::new(HashMap::new()),
            session_ttl,
            streaming,
            trusted_bot_ids,
            file_upload_cache: tokio::sync::Mutex::new(HashSet::new()),
            allowed_channels: Arc::new(tokio::sync::RwLock::new(allowed_channels)),
            config_path,
            assistant_mode,
            streams: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Add a channel to the runtime allowlist AND persist it to the on-disk
    /// config so it survives a restart. Idempotent. The runtime add is the part
    /// that matters for *this* process (the gate reads it next message);
    /// persistence is best-effort — if the config write fails (URL config, perms,
    /// unexpected format) we log and keep the runtime add, never block listening.
    /// `reason` is for the log line ("created" | "invited").
    async fn allow_channel_now(&self, channel_id: &str, reason: &str) {
        let newly_added = {
            let mut allowed = self.allowed_channels.write().await;
            allowed.insert(channel_id.to_string())
        };
        if newly_added {
            info!(channel_id, reason, "slack: added channel to runtime allowlist");
        }
        if let Some(path) = &self.config_path {
            match crate::config::persist_allowed_channel(path, channel_id) {
                Ok(true) => info!(channel_id, "slack: persisted channel to config allowlist"),
                Ok(false) => {} // already in config
                Err(e) => warn!(
                    channel_id, error = %e,
                    "slack: could not persist channel to config (runtime add still active; \
                     add it to [slack].allowed_channels by hand to survive restart)"
                ),
            }
        }
    }

    /// Returns the bot token for use in API calls outside the adapter.
    pub fn bot_token(&self) -> &str {
        &self.bot_token
    }

    /// Eagerly record that another bot has posted in a thread. Called from the
    /// event loop when a bot message arrives, so multibot detection doesn't
    /// depend on fetching thread history. Idempotent.
    async fn note_other_bot_in_thread(&self, thread_ts: &str) {
        let mut cache = self.multibot_threads.lock().await;
        cache
            .entry(thread_ts.to_string())
            .or_insert_with(tokio::time::Instant::now);
        enforce_cache_bounds(&mut cache, self.session_ttl);
    }


    /// Insert a stream entry, bounding the map so aborted turns (begin without a
    /// matching finish) can't leak unboundedly. Normal lifecycle: stream_begin
    /// inserts, stream_finish removes.
    async fn insert_stream(&self, ts: String, entry: StreamEntry) {
        let mut map = self.streams.lock().await;
        if map.len() >= STREAM_CACHE_MAX {
            // Only evict inactive (degraded/stale) streams to avoid cutting off
            // active streams mid-turn. If no inactive entries exist, fall through
            // and allow the map to grow slightly beyond the soft cap.
            let evict: Vec<String> = map
                .iter()
                .filter(|(_, e)| !e.active)
                .map(|(k, _)| k.clone())
                .collect();
            for k in evict {
                map.remove(&k);
            }
        }
        map.insert(ts, entry);
    }

    /// Accumulate a delta into a degraded stream's buffer and return the new
    /// cumulative text. Returns None if no (degraded) stream entry exists for
    /// `ts` — never resurrects a removed/absent stream. No network I/O.
    async fn accumulate_degraded(&self, ts: &str, delta: &str) -> Option<String> {
        let mut map = self.streams.lock().await;
        let entry = map.get_mut(ts)?;
        entry.degraded_buf.push_str(delta);
        Some(entry.degraded_buf.clone())
    }

    /// Get the bot's own Slack user ID (cached after first call).
    async fn get_bot_user_id(&self) -> Option<&str> {
        self.bot_user_id
            .get_or_try_init(|| async {
                let resp = self
                    .api_post("auth.test", serde_json::json!({}))
                    .await
                    .map_err(|e| anyhow!("auth.test failed: {e}"))?;
                resp["user_id"]
                    .as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| anyhow!("no user_id in auth.test response"))
            })
            .await
            .inspect_err(|e| warn!(error = %e, "bot user ID unavailable; mention detection may suppress bot messages under Mentions mode"))
            .ok()
            .map(|s| s.as_str())
    }

    async fn api_post(&self, method: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        let resp = self
            .client
            .post(format!("{SLACK_API}/{method}"))
            .header("Authorization", format!("Bearer {}", self.bot_token))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await?;

        let json: serde_json::Value = resp.json().await?;
        if json["ok"].as_bool() != Some(true) {
            let err = json["error"].as_str().unwrap_or("unknown error");
            return Err(anyhow!("Slack API {method}: {err}"));
        }
        Ok(json)
    }

    /// Call a Slack API method using GET with query parameters.
    /// Required for read methods like conversations.replies that don't accept JSON body.
    async fn api_get(&self, method: &str, params: &[(&str, &str)]) -> Result<serde_json::Value> {
        let resp = self
            .client
            .get(format!("{SLACK_API}/{method}"))
            .header("Authorization", format!("Bearer {}", self.bot_token))
            .query(params)
            .send()
            .await?;

        let json: serde_json::Value = resp.json().await?;
        if json["ok"].as_bool() != Some(true) {
            let err = json["error"].as_str().unwrap_or("unknown error");
            return Err(anyhow!("Slack API {method}: {err}"));
        }
        Ok(json)
    }

    /// Resolve a Slack user ID to display name via users.info API.
    /// Results are cached for 5 minutes to avoid hitting Slack rate limits.
    async fn resolve_user_name(&self, user_id: &str) -> Option<String> {
        // Check cache first
        {
            let cache = self.user_cache.lock().await;
            if let Some((name, ts)) = cache.get(user_id) {
                if ts.elapsed() < USER_CACHE_TTL {
                    return Some(name.clone());
                }
            }
        }

        let resp = self
            .api_post("users.info", serde_json::json!({ "user": user_id }))
            .await
            .ok()?;
        let user = resp.get("user")?;
        let profile = user.get("profile")?;
        let display = profile
            .get("display_name")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let real = profile
            .get("real_name")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let name = user.get("name").and_then(|v| v.as_str());
        let resolved = display.or(real).or(name)?.to_string();

        // Cache the result
        self.user_cache.lock().await.insert(
            user_id.to_string(),
            (resolved.clone(), tokio::time::Instant::now()),
        );

        Some(resolved)
    }

    /// Resolve a Bot ID (B...) to Bot User ID (U...) via bots.info API.
    /// Cached permanently (bot IDs don't change).
    async fn resolve_bot_user_id(&self, bot_id: &str) -> Option<String> {
        if bot_id.is_empty() {
            return None;
        }

        {
            let cache = self.bot_id_cache.lock().await;
            if let Some(user_id) = cache.get(bot_id) {
                return Some(user_id.clone());
            }
        }

        let resp = self
            .api_post("bots.info", serde_json::json!({ "bot": bot_id }))
            .await
            .inspect_err(|e| {
                warn!(
                    bot_id,
                    error = %e,
                    "failed to resolve Slack bot ID via bots.info"
                )
            })
            .ok()?;
        let user_id = resp.get("bot")?.get("user_id")?.as_str()?.to_string();

        self.bot_id_cache
            .lock()
            .await
            .insert(bot_id.to_string(), user_id.clone());

        Some(user_id)
    }

    async fn trusted_bot_ids_contains(
        &self,
        trusted_bot_ids: &HashSet<String>,
        event_bot_id: &str,
    ) -> bool {
        if trusted_bot_ids.is_empty() {
            return true;
        }
        if bot_id_matches_trusted(trusted_bot_ids, event_bot_id, None) {
            return true;
        }
        let resolved = self.resolve_bot_user_id(event_bot_id).await;
        bot_id_matches_trusted(trusted_bot_ids, event_bot_id, resolved.as_deref())
    }

    /// Check whether the bot has participated in a Slack thread and whether
    /// other bots have also posted in it.
    /// Returns `(involved, other_bot_present)`.
    /// Involved = parent message @mentions the bot OR any message in thread is from the bot.
    /// Fail-closed: returns `(false, false)` on API error (consistent with Discord's approach).
    /// Caches positive results only — both states are irreversible.
    async fn bot_participated_in_thread(&self, channel: &str, thread_ts: &str) -> (bool, bool) {
        let cached_involved = {
            let cache = self.participated_threads.lock().await;
            cache
                .get(thread_ts)
                .is_some_and(|ts| ts.elapsed() < self.session_ttl)
        };
        let cached_multibot = {
            let cache = self.multibot_threads.lock().await;
            cache
                .get(thread_ts)
                .is_some_and(|ts| ts.elapsed() < self.session_ttl)
        };

        // Eager multibot detection from message events populates the cache
        // before this runs. When already involved and cached, skip the fetch.
        if cached_involved {
            return (true, cached_multibot);
        }

        let bot_id = match self.get_bot_user_id().await {
            Some(id) => id,
            None => {
                warn!("cannot resolve bot user ID, rejecting (fail-closed)");
                return (false, false);
            }
        };

        let resp = self
            .api_get(
                "conversations.replies",
                &[
                    ("channel", channel),
                    ("ts", thread_ts),
                    ("limit", "200"),
                    ("inclusive", "true"),
                ],
            )
            .await;

        let json = match resp {
            Ok(json) => json,
            Err(e) => {
                warn!(channel, thread_ts, error = %e, "failed to fetch thread replies, rejecting (fail-closed)");
                return (false, false);
            }
        };
        let Some(messages) = json["messages"].as_array() else {
            return (false, false);
        };

        let parent_mentions_bot = messages
            .first()
            .and_then(|m| m["text"].as_str())
            .is_some_and(|text| text_mentions_uid(text, bot_id));

        let bot_posted = messages.iter().any(|m| m["user"].as_str() == Some(bot_id));

        let involved = parent_mentions_bot || bot_posted;
        let other_bot_present = cached_multibot
            || messages.iter().any(|m| {
                let is_bot_msg =
                    m["bot_id"].is_string() || m["subtype"].as_str() == Some("bot_message");
                is_bot_msg && m["user"].as_str() != Some(bot_id)
            });

        if involved {
            self.cache_participation(thread_ts).await;
        }
        if other_bot_present && !cached_multibot {
            self.note_other_bot_in_thread(thread_ts).await;
        }

        (involved, other_bot_present)
    }

    /// Fetch a thread's messages and render them as a plain-text transcript for
    /// agent context. Reuses the same `conversations.replies` read path as
    /// `bot_participated_in_thread` (line ~328) — the token already has the
    /// scope. Returns `None` on API error or empty thread (fail-soft: missing
    /// context degrades the reply, it shouldn't drop the turn). The trigger
    /// message itself is excluded — it arrives via the normal prompt, so
    /// including it here would duplicate it. 2026-06-03: added for 2a (thread
    /// summarise), since OpenAB exposes no agent-callable read tool.
    async fn fetch_thread_context(&self, channel: &str, thread_ts: &str, trigger_ts: &str) -> Option<String> {
        let json = match self
            .api_get(
                "conversations.replies",
                &[
                    ("channel", channel),
                    ("ts", thread_ts),
                    ("limit", "200"),
                    ("inclusive", "true"),
                ],
            )
            .await
        {
            Ok(json) => json,
            Err(e) => {
                warn!(channel, thread_ts, error = %e, "fetch_thread_context: conversations.replies failed, skipping context (fail-soft)");
                return None;
            }
        };

        let messages = json["messages"].as_array()?;
        let mut lines: Vec<String> = Vec::new();
        for m in messages {
            let msg_ts = m["ts"].as_str().unwrap_or("");
            // Skip the trigger message — it's already in the prompt.
            if msg_ts == trigger_ts {
                continue;
            }
            let text = m["text"].as_str().unwrap_or("").trim();
            if text.is_empty() {
                continue;
            }
            // Strip leaked process-narration preambles from re-injected history
            // so the model can't imitate its own (or a peer bot's) earlier leak.
            // This is the second half of the fix in adapter.rs — see that fn's
            // doc + 2026-06-16-tifa-meta-preamble-leak-rootcause.md.
            let text = crate::adapter::strip_meta_preamble(text);
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            let who = m["user"]
                .as_str()
                .or_else(|| m["username"].as_str())
                .unwrap_or("unknown");
            lines.push(format!("<@{who}>: {text}"));
        }

        if lines.is_empty() {
            return None;
        }

        Some(format!(
            "[Thread context — earlier messages in this Slack thread, oldest first. \
             Provided so you can read/summarise the thread; not a new instruction.]\n{}",
            lines.join("\n")
        ))
    }

    /// Insert a positive participation entry, enforcing cache bounds.
    async fn cache_participation(&self, thread_ts: &str) {
        let mut cache = self.participated_threads.lock().await;
        cache.insert(thread_ts.to_string(), tokio::time::Instant::now());
        enforce_cache_bounds(&mut cache, self.session_ttl);
    }

    /// Post a plain text message — the original `send_message` path, extracted
    /// so the marker-aware path can reuse it.
    async fn send_plain_text(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        let mrkdwn = markdown_to_mrkdwn(content);
        let mut body = serde_json::json!({
            "channel": channel.channel_id,
            "text": mrkdwn,
        });
        if let Some(thread_ts) = &channel.thread_id {
            body["thread_ts"] = serde_json::Value::String(thread_ts.clone());
        }
        let resp = self.api_post("chat.postMessage", body).await?;
        let ts = resp["ts"]
            .as_str()
            .ok_or_else(|| anyhow!("no ts in chat.postMessage response"))?;
        Ok(MessageRef {
            channel: ChannelRef {
                platform: "slack".into(),
                channel_id: channel.channel_id.clone(),
                thread_id: channel.thread_id.clone(),
                parent_id: None,
                origin_event_id: None,
            },
            message_id: ts.to_string(),
        })
    }

    /// Upload a file from disk and share it into the given channel/thread.
    ///
    /// Implements Slack's 3-step modern upload API:
    ///   1. `files.getUploadURLExternal` → get an upload_url + file_id
    ///   2. POST raw bytes to that upload_url (multipart/form-data, field name `file`)
    ///   3. `files.completeUploadExternal` → publish into the channel
    ///
    /// Returns a MessageRef pointing at the share message ts. Failures bubble up
    /// with the Slack error code in the message; caller is responsible for
    /// surfacing to the user.
    ///
    /// Required Slack bot scopes: `files:write`. The file path must be readable
    /// by the OpenAB process (host or container — whichever filesystem the
    /// bridge runs in).
    async fn send_file_to_slack(&self, channel: &ChannelRef, path: &str) -> Result<MessageRef> {
        use tokio::io::AsyncReadExt;

        // --- Validate the path ---
        let path_buf = std::path::PathBuf::from(path);
        if !path_buf.is_file() {
            return Err(anyhow!("not a regular file: {path}"));
        }
        let metadata = tokio::fs::metadata(&path_buf).await?;
        let size = metadata.len();
        if size == 0 {
            return Err(anyhow!("file is empty: {path}"));
        }
        if size > FILE_SEND_MAX_BYTES {
            return Err(anyhow!(
                "file too large ({size} bytes > {FILE_SEND_MAX_BYTES} cap): {path}"
            ));
        }
        let filename = path_buf
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("could not derive filename from path: {path}"))?
            .to_string();

        debug!(path = %path, filename = %filename, size, "slack: starting file upload");

        // --- Step 1: getUploadURLExternal (form-encoded GET, per Slack docs) ---
        let step1_resp = self
            .client
            .get(format!("{SLACK_API}/files.getUploadURLExternal"))
            .header("Authorization", format!("Bearer {}", self.bot_token))
            .query(&[
                ("filename", filename.as_str()),
                ("length", size.to_string().as_str()),
            ])
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if step1_resp["ok"].as_bool() != Some(true) {
            let err = step1_resp["error"]
                .as_str()
                .unwrap_or("unknown getUploadURLExternal error");
            return Err(anyhow!("files.getUploadURLExternal: {err}"));
        }
        let upload_url = step1_resp["upload_url"]
            .as_str()
            .ok_or_else(|| anyhow!("no upload_url in getUploadURLExternal response"))?
            .to_string();
        let file_id = step1_resp["file_id"]
            .as_str()
            .ok_or_else(|| anyhow!("no file_id in getUploadURLExternal response"))?
            .to_string();

        // --- Step 2: PUT raw bytes to the signed URL ---
        let mut file_bytes = Vec::with_capacity(size as usize);
        tokio::fs::File::open(&path_buf)
            .await?
            .read_to_end(&mut file_bytes)
            .await?;

        let step2_resp = self
            .client
            .post(&upload_url)
            .body(file_bytes)
            .send()
            .await?;

        if !step2_resp.status().is_success() {
            let status = step2_resp.status();
            let body = step2_resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "upload PUT failed: HTTP {status} — body: {}",
                body.chars().take(200).collect::<String>()
            ));
        }

        // --- Step 3: completeUploadExternal — actually publish into the channel ---
        let mut complete_body = serde_json::json!({
            "files": [{ "id": file_id, "title": filename }],
            "channel_id": channel.channel_id,
        });
        if let Some(thread_ts) = &channel.thread_id {
            complete_body["thread_ts"] = serde_json::Value::String(thread_ts.clone());
        }

        let step3_resp = self.api_post("files.completeUploadExternal", complete_body).await?;

        // Slack returns the file metadata; we want the share's message timestamp.
        // It's available under `files[0].shares.public.<channel>[0].ts` or
        // `files[0].shares.private.<channel>[0].ts`. Probe both.
        let ts = extract_share_message_ts(&step3_resp, &channel.channel_id).unwrap_or_else(|| {
            // No ts surfaced — use file_id as a stand-in. MessageRef is mostly
            // used for reactions and edits, neither of which makes sense for a
            // file share. So file_id is a reasonable degenerate identity.
            file_id.clone()
        });

        info!(
            file_id = %file_id,
            filename = %filename,
            size,
            channel = %channel.channel_id,
            "slack: file upload complete"
        );

        Ok(MessageRef {
            channel: ChannelRef {
                platform: "slack".into(),
                channel_id: channel.channel_id.clone(),
                thread_id: channel.thread_id.clone(),
                parent_id: None,
                origin_event_id: None,
            },
            message_id: ts,
        })
    }

    /// Create a Slack channel, then optionally set its topic and invite users.
    ///
    /// Required bot scopes: `channels:manage` (public) or `groups:write`
    /// (private); those also cover setTopic + invite. Returns the new channel id.
    /// `api_post` already turns a non-`ok` Slack response into an `Err` carrying
    /// the Slack error code (e.g. `name_taken`, `missing_scope`), so the caller's
    /// error arm surfaces it to the owner.
    async fn create_channel_in_slack(&self, spec: &CreateChannelSpec) -> Result<String> {
        let create_resp = self
            .api_post(
                "conversations.create",
                serde_json::json!({
                    "name": spec.name,
                    "is_private": spec.is_private,
                }),
            )
            .await?;

        let channel_id = create_resp["channel"]["id"]
            .as_str()
            .ok_or_else(|| anyhow!("conversations.create returned no channel id"))?
            .to_string();

        info!(
            channel_id = %channel_id,
            name = %spec.name,
            is_private = spec.is_private,
            "slack: channel created"
        );

        // Self-heal the allowlist: a channel the bot just created must be
        // listenable immediately, or @mentions in it are dropped at the gate
        // until the next restart (2026-06-04 fix). Runtime add + persist to
        // config so it also survives a restart. `allow_all_channels` makes the
        // gate a no-op, so this only matters when the allowlist is enforced —
        // adding unconditionally is harmless.
        self.allow_channel_now(&channel_id, "created").await;

        if let Some(topic) = &spec.topic {
            if let Err(e) = self
                .api_post(
                    "conversations.setTopic",
                    serde_json::json!({ "channel": channel_id, "topic": topic }),
                )
                .await
            {
                warn!(channel_id = %channel_id, error = %e, "slack: setTopic failed (channel still created)");
            }
        }

        if !spec.invite.is_empty() {
            if let Err(e) = self
                .api_post(
                    "conversations.invite",
                    serde_json::json!({ "channel": channel_id, "users": spec.invite.join(",") }),
                )
                .await
            {
                warn!(channel_id = %channel_id, error = %e, "slack: invite failed (channel still created)");
            }
        }

        Ok(channel_id)
    }
}

/// Parse outbound text for file-send markers `<<openab-send-file PATH>>`.
/// Returns `Some((text_without_markers, paths))` if at least one marker found,
/// `None` if no marker line is present (fast-path).
///
/// **Line-anchored** (2026-05-26): the marker must occupy a line on its own
/// (after trimming whitespace). Inline occurrences inside running text are
/// preserved as literal — this prevents the agent from self-triggering when
/// quoting its own source code or documentation that mentions the marker.
///
/// Lines containing other content alongside a marker are left untouched (no
/// partial extraction); the agent must put the marker on its own line for it
/// to fire. This trades a slightly stricter calling convention for immunity
/// from natural-language and code-quote collisions.
fn extract_file_send_markers(content: &str) -> Option<(String, Vec<String>)> {
    if !content.contains(FILE_SEND_MARKER_PREFIX) {
        return None;
    }

    let mut paths: Vec<String> = Vec::new();
    let mut kept_lines: Vec<&str> = Vec::new();

    for line in content.split('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with(FILE_SEND_MARKER_PREFIX) && trimmed.ends_with(FILE_SEND_MARKER_SUFFIX)
        {
            // Verified marker line: extract path between prefix and suffix.
            let inner = &trimmed[FILE_SEND_MARKER_PREFIX.len()
                ..trimmed.len() - FILE_SEND_MARKER_SUFFIX.len()];
            let path = inner.trim();
            if !path.is_empty() {
                paths.push(path.to_string());
                // Skip this line entirely from output.
                continue;
            }
            // Empty path inside otherwise-valid marker — drop the line too
            // (don't leak the empty `<<openab-send-file >>` to the user).
            continue;
        }
        // Not a marker line — preserve verbatim. Inline `<<openab-send-file ...>>`
        // text is intentionally kept as literal content.
        kept_lines.push(line);
    }

    if paths.is_empty() {
        return None;
    }
    Some((kept_lines.join("\n"), paths))
}

/// Spec parsed from an `<<openab-create-channel …>>` marker.
struct CreateChannelSpec {
    name: String,
    is_private: bool,
    topic: Option<String>,
    invite: Vec<String>,
}

/// Parse outbound text for a single owner-triggered channel-create marker.
/// Line-anchored like the file-send marker (must occupy its own trimmed line).
/// Returns `Some((text_without_marker, spec))` for the FIRST valid marker line;
/// later marker lines are left as literal text. `None` if no marker present or
/// the marker has no usable `name=`.
fn extract_create_channel_marker(content: &str) -> Option<(String, CreateChannelSpec)> {
    if !content.contains(CREATE_CHANNEL_MARKER_PREFIX) {
        return None;
    }

    let mut spec: Option<CreateChannelSpec> = None;
    let mut kept_lines: Vec<&str> = Vec::new();

    for line in content.split('\n') {
        let trimmed = line.trim();
        if spec.is_none()
            && trimmed.starts_with(CREATE_CHANNEL_MARKER_PREFIX)
            && trimmed.ends_with(CREATE_CHANNEL_MARKER_SUFFIX)
        {
            let inner = trimmed[CREATE_CHANNEL_MARKER_PREFIX.len()
                ..trimmed.len() - CREATE_CHANNEL_MARKER_SUFFIX.len()]
                .trim();
            if let Some(parsed) = parse_create_channel_args(inner) {
                spec = Some(parsed);
                continue; // strip the marker line from output
            }
            // invalid marker (no name) — drop the line so we don't leak it
            continue;
        }
        kept_lines.push(line);
    }

    spec.map(|s| (kept_lines.join("\n"), s))
}

/// Parse the inner args of a create-channel marker. Grammar:
///   name=<slug>            (required)
///   private                (optional bare flag)
///   topic="<free text>"    (optional, quoted — may contain spaces)
///   invite=@U1,@U2,...     (optional, comma-separated Slack user IDs)
fn parse_create_channel_args(inner: &str) -> Option<CreateChannelSpec> {
    // Pull out topic="..." first (it may contain spaces), then parse the rest
    // as whitespace-separated tokens.
    let mut rest = inner.to_string();
    let mut topic: Option<String> = None;
    if let Some(start) = rest.find("topic=\"") {
        let after = start + "topic=\"".len();
        if let Some(end_rel) = rest[after..].find('"') {
            let t = rest[after..after + end_rel].trim().to_string();
            if !t.is_empty() {
                topic = Some(t);
            }
            let end = after + end_rel + 1;
            rest.replace_range(start..end, " ");
        }
    }

    let mut name: Option<String> = None;
    let mut is_private = false;
    let mut invite: Vec<String> = Vec::new();

    for tok in rest.split_whitespace() {
        if tok == "private" {
            is_private = true;
        } else if let Some(v) = tok.strip_prefix("name=") {
            let slug = normalize_channel_name(v);
            if !slug.is_empty() {
                name = Some(slug);
            }
        } else if let Some(v) = tok.strip_prefix("invite=") {
            for id in v.split(',') {
                let id = id.trim().trim_start_matches('@');
                if !id.is_empty() {
                    invite.push(id.to_string());
                }
            }
        }
    }

    name.map(|name| CreateChannelSpec {
        name,
        is_private,
        topic,
        invite,
    })
}

/// Normalize a requested channel name to Slack's rules: lowercase, only
/// `a-z0-9` plus hyphen/underscore, spaces/`.`/`/`→hyphen, ≤80 chars.
fn normalize_channel_name(raw: &str) -> String {
    let mut out = String::new();
    for c in raw.trim().to_lowercase().chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            out.push(c);
        } else if c == ' ' || c == '.' || c == '/' {
            out.push('-');
        }
        // drop anything else
    }
    out.trim_matches('-').chars().take(80).collect()
}

/// Pull the share message timestamp out of a `files.completeUploadExternal`
/// response, probing both `public` and `private` share entries for the channel.
/// Returns None if the response shape doesn't match (Slack may change this).
fn extract_share_message_ts(resp: &serde_json::Value, channel_id: &str) -> Option<String> {
    let files = resp.get("files")?.as_array()?;
    let first = files.first()?;
    let shares = first.get("shares")?;
    for visibility in ["public", "private"] {
        if let Some(share_map) = shares.get(visibility) {
            if let Some(channel_shares) = share_map.get(channel_id) {
                if let Some(first_share) = channel_shares.as_array().and_then(|a| a.first()) {
                    if let Some(ts) = first_share.get("ts").and_then(|v| v.as_str()) {
                        return Some(ts.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Shared eviction policy for positive-only caches.
/// First drops expired entries; if still over, drops the oldest half.
fn enforce_cache_bounds(
    cache: &mut HashMap<String, tokio::time::Instant>,
    ttl: std::time::Duration,
) {
    if cache.len() <= PARTICIPATION_CACHE_MAX {
        return;
    }
    cache.retain(|_, ts| ts.elapsed() < ttl);
    if cache.len() > PARTICIPATION_CACHE_MAX {
        let mut entries: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by_key(|(_, ts)| *ts);
        let evict_count = entries.len() / 2;
        for (key, _) in entries.into_iter().take(evict_count) {
            cache.remove(&key);
        }
    }
}

#[async_trait]
impl ChatAdapter for SlackAdapter {
    fn platform(&self) -> &'static str {
        "slack"
    }

    fn message_limit(&self) -> usize {
        // Match the Block Kit `markdown` block cap (12k) minus headroom. Messages
        // are sent as markdown blocks, so the old 4000 mrkdwn-era limit would
        // split long replies (and Markdown tables) across messages needlessly —
        // a mid-table split renders as raw pipes. 11_900 keeps typical tables in
        // one block and cuts message-spam on long replies.
        MARKDOWN_BLOCK_LIMIT
    }

    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        // Owner-triggered channel creation (DM-only). Runs before the file-send
        // check so the marker line is stripped regardless of outcome.
        if let Some((residual, spec)) = extract_create_channel_marker(content) {
            if !channel.channel_id.starts_with('D') {
                // Not a DM — channel creation is owner-DM-only. Strip the marker,
                // forward any residual text, and note it was ignored.
                let trimmed = residual.trim();
                let notice = if trimmed.is_empty() {
                    "⚠️ channel creation is owner-DM-only; ignored here.".to_string()
                } else {
                    format!("{trimmed}\n\n⚠️ (channel-create marker ignored: DM-only)")
                };
                return self.send_plain_text(channel, &notice).await;
            }
            let confirmation = match self.create_channel_in_slack(&spec).await {
                Ok(cid) => {
                    let mut parts = vec![format!("✅ Created <#{cid}|{}>", spec.name)];
                    if spec.topic.is_some() {
                        parts.push("topic set".to_string());
                    }
                    if !spec.invite.is_empty() {
                        parts.push(format!("invited {}", spec.invite.len()));
                    }
                    parts.join(" · ")
                }
                Err(e) => format!("⚠️ Failed to create channel `{}`: {}", spec.name, e),
            };
            let residual = residual.trim();
            let body = if residual.is_empty() {
                confirmation
            } else {
                format!("{residual}\n\n{confirmation}")
            };
            return self.send_plain_text(channel, &body).await;
        }

        // Scan for file-send markers <<openab:send-file:PATH>>. If found, intercept:
        // upload each file via Slack's files API, then send the remaining text (with
        // markers stripped). Returns the MessageRef of the last action performed.
        if let Some((stripped_text, file_paths)) = extract_file_send_markers(content) {
            let mut last_msg: Option<MessageRef> = None;

            // Post the residual text first (if non-empty), so the file appears AFTER
            // any caption. Matches the natural reading order users expect.
            let trimmed = stripped_text.trim();
            if !trimmed.is_empty() {
                last_msg = Some(self.send_plain_text(channel, trimmed).await?);
            }

            // Upload each file. Failure on one shouldn't block the rest — log & continue.
            for path in &file_paths {
                match self.send_file_to_slack(channel, path).await {
                    Ok(msg_ref) => {
                        info!(path = %path, "slack: file uploaded");
                        last_msg = Some(msg_ref);
                    }
                    Err(e) => {
                        error!(path = %path, error = %e, "slack: file upload failed");
                        // Surface the failure to the user so they know it didn't go through.
                        let err_text = format!(
                            "⚠️ Failed to send file `{}`: {}\n(See OpenAB logs for details.)",
                            path, e
                        );
                        last_msg = Some(self.send_plain_text(channel, &err_text).await?);
                    }
                }
            }

            // If we somehow had only markers and no surviving text, return a stub.
            return last_msg.ok_or_else(|| anyhow!("no message sent (empty after marker strip)"));
        }

        // Standard path — no markers. Use upstream's Block Kit `markdown` body
        // (renders native tables / headings), with graceful text-only fallback.
        let thread_ts = channel.thread_id.as_deref();
        let body = build_post_message_body(&channel.channel_id, thread_ts, content);
        let resp = match self.api_post("chat.postMessage", body).await {
            Ok(r) => r,
            // Graceful degradation: if the `blocks` payload is rejected (workspace
            // lacks the markdown block, or content exceeds the cumulative block
            // cap), retry text-only so the message still lands (mrkdwn fallback)
            // instead of failing outright.
            Err(e) if is_block_payload_rejected(&e) => {
                warn!(error = %e, "markdown block rejected; retrying chat.postMessage text-only");
                let fallback = build_post_message_text_only(&channel.channel_id, thread_ts, content);
                self.api_post("chat.postMessage", fallback).await?
            }
            Err(e) => return Err(e),
        };
        let ts = resp["ts"]
            .as_str()
            .ok_or_else(|| anyhow!("no ts in chat.postMessage response"))?;
        Ok(MessageRef {
            channel: ChannelRef {
                platform: "slack".into(),
                channel_id: channel.channel_id.clone(),
                thread_id: channel.thread_id.clone(),
                parent_id: None,
                origin_event_id: None,
            },
            message_id: ts.to_string(),
        })
    }

    async fn create_thread(
        &self,
        channel: &ChannelRef,
        trigger_msg: &MessageRef,
        _title: &str,
    ) -> Result<ChannelRef> {
        // Slack threads are implicit — posting with thread_ts creates/continues a thread.
        Ok(ChannelRef {
            platform: "slack".into(),
            channel_id: channel.channel_id.clone(),
            thread_id: Some(trigger_msg.message_id.clone()),
            parent_id: None,
            origin_event_id: None,
        })
    }

    async fn add_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()> {
        let name = unicode_to_slack_emoji(emoji);
        match self
            .api_post(
                "reactions.add",
                serde_json::json!({
                    "channel": msg.channel.channel_id,
                    "timestamp": msg.message_id,
                    "name": name,
                }),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("already_reacted") => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn remove_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()> {
        let name = unicode_to_slack_emoji(emoji);
        match self
            .api_post(
                "reactions.remove",
                serde_json::json!({
                    "channel": msg.channel.channel_id,
                    "timestamp": msg.message_id,
                    "name": name,
                }),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("no_reaction") => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn edit_message(&self, msg: &MessageRef, content: &str) -> Result<()> {
        // Marker handling for the streaming path: OpenAB streams the final agent
        // response via repeated edit_message calls against a placeholder. If the
        // text contains file-send markers, we strip them from the edit (so the
        // placeholder shows clean text) and trigger uploads as separate follow-up
        // messages in the same channel/thread.
        //
        // Idempotency: we only want to upload each file ONCE per session, not on
        // every streaming edit. We track this via the file_upload_cache keyed on
        // (channel_id, thread_ts, path). The cache lives on the adapter struct.
        if let Some((stripped_text, file_paths)) = extract_file_send_markers(content) {
            // Edit the placeholder to the clean text (without markers).
            let stripped_mrkdwn = markdown_to_mrkdwn(&stripped_text);
            self.api_post(
                "chat.update",
                serde_json::json!({
                    "channel": msg.channel.channel_id,
                    "ts": msg.message_id,
                    "text": stripped_mrkdwn,
                }),
            )
            .await?;

            // Upload each file as a follow-up. Dedup via the cache so we don't
            // re-upload on each streaming edit chunk.
            //
            // Race-free check-and-claim: insert into cache FIRST under the lock,
            // then upload. If insertion returns false (key already present),
            // another edit_message call already claimed the slot — skip.
            // The previous version did check-then-act with the lock released
            // between check and insert, allowing duplicate uploads when two
            // streaming edits fired within the upload latency window.
            for path in &file_paths {
                let cache_key = format!(
                    "{}|{}|{}",
                    msg.channel.channel_id,
                    msg.channel.thread_id.as_deref().unwrap_or(""),
                    path
                );
                let claimed = {
                    let mut cache = self.file_upload_cache.lock().await;
                    cache.insert(cache_key.clone()) // returns true if newly inserted
                };
                if !claimed {
                    debug!(path = %path, "skipping duplicate file upload (already claimed)");
                    continue;
                }
                match self.send_file_to_slack(&msg.channel, path).await {
                    Ok(_) => {
                        info!(path = %path, "slack: file uploaded via edit_message");
                    }
                    Err(e) => {
                        error!(path = %path, error = %e, "slack: file upload failed");
                        // Roll back the claim so a future retry of the SAME path
                        // (e.g. user re-asks after fixing the path) isn't blocked.
                        let mut cache = self.file_upload_cache.lock().await;
                        cache.remove(&cache_key);
                        let err_text = format!(
                            "⚠️ Failed to send file `{}`: {}\n(See OpenAB logs for details.)",
                            path, e
                        );
                        let _ = self.send_plain_text(&msg.channel, &err_text).await;
                    }
                }
            }
            return Ok(());
        }

        // Final plain edit — use upstream's Block Kit `markdown` body (native
        // tables), with graceful text-only fallback.
        let body = build_update_body(&msg.channel.channel_id, &msg.message_id, content);
        match self.api_post("chat.update", body).await {
            Ok(_) => Ok(()),
            // See send_message: degrade to text-only if the blocks payload is rejected.
            Err(e) if is_block_payload_rejected(&e) => {
                warn!(error = %e, "markdown block rejected; retrying chat.update text-only");
                let fallback =
                    build_update_text_only(&msg.channel.channel_id, &msg.message_id, content);
                self.api_post("chat.update", fallback).await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn use_streaming(&self, other_bot_present: bool) -> bool {
        // (B) Config master switch: `[slack].streaming = false` disables it outright.
        if !self.streaming {
            return false;
        }
        // (A) If this deployment has any trusted peer bots configured, it's a
        // multi-bot setup. Don't stream — a streamed reply reaches peer bots
        // only as `message_changed` events (which every bot's handler skips),
        // so any @mention of a peer bot in the reply would never trigger it.
        // This closes the race the trait doc admits: `other_bot_present` can be
        // false when a peer bot is addressed before it has posted in the thread
        // (e.g. "@Tifa say hi to Nyx" — Nyx hasn't spoken yet). Keying off the
        // configured trusted-bot set is race-free; the cost is that a pure-DM
        // bot that happens to have trusted_bot_ids set also won't stream, which
        // is acceptable (streaming is a nicety, peer-bot delivery is correctness).
        if !self.trusted_bot_ids.is_empty() {
            return false;
        }
        // Single-bot deployment: stream unless a peer bot is already in-thread.
        !other_bot_present
    }

    fn renders_native_tables(&self) -> bool {
        true
    }

    fn uses_assistant_status(&self) -> bool {
        self.assistant_mode
    }

    fn uses_native_streaming(&self, other_bot_present: bool) -> bool {
        // Native (assistant_mode) streaming must honor the SAME gates as
        // use_streaming, or it bypasses the send-once finalization that
        // post_tool_only / suppress_send depend on:
        //   (B) [slack].streaming = false master switch, and
        //   (A) trusted_bot_ids (multi-bot) — a streamed reply reaches peer
        //       bots only as message_changed events their handlers skip.
        // assistant_mode defaults to true upstream, so without these guards a
        // fork deployment with streaming=false (e.g. Tifa) would still
        // native-stream and skip post_tool_only. See use_streaming above.
        if !self.streaming || !self.trusted_bot_ids.is_empty() {
            return false;
        }
        let native = self.assistant_mode && !other_bot_present;
        debug!(
            assistant_mode = self.assistant_mode,
            other_bot_present,
            native,
            "slack assistant_mode decision (per turn)"
        );
        native
    }

    async fn stream_begin(
        &self,
        channel: &ChannelRef,
        recipient: Option<(String, String)>,
    ) -> Result<MessageRef> {
        let thread_ts = channel.thread_id.clone().unwrap_or_default();
        // recipient is bound to this turn (captured at message arrival, carried on
        // BufferedMessage) — no shared thread cache, so no cross-turn race.
        let make_ref = |ts: String| MessageRef {
            channel: ChannelRef {
                platform: "slack".into(),
                channel_id: channel.channel_id.clone(),
                thread_id: channel.thread_id.clone(),
                parent_id: None,
                origin_event_id: None,
            },
            message_id: ts,
        };

        if let Some((user_id, team_id)) = recipient {
            let body = build_start_stream_body(&channel.channel_id, &thread_ts, &user_id, &team_id);
            match self.api_post("chat.startStream", body).await {
                Ok(resp) => {
                    if let Some(ts) = resp["ts"].as_str() {
                        self.insert_stream(
                            ts.to_string(),
                            StreamEntry { active: true, degraded_buf: String::new() },
                        )
                        .await;
                        return Ok(make_ref(ts.to_string()));
                    }
                    error!("chat.startStream ok but no ts; falling back to post+edit");
                }
                Err(e) => {
                    error!(error = %e, "chat.startStream failed; falling back to post+edit for this turn");
                }
            }
        } else {
            // Expected for bot-authored turns (no recipient bound) and non-user
            // triggers, so warn! rather than error! to avoid on-call noise.
            warn!(thread_ts, "no recipient for turn; falling back to post+edit");
        }

        // Degraded fallback: plain placeholder via send_message; mark inactive.
        let msg = self.send_message(channel, "…").await?;
        self.insert_stream(
            msg.message_id.clone(),
            StreamEntry { active: false, degraded_buf: String::new() },
        )
        .await;
        Ok(msg)
    }

    async fn stream_append(&self, msg: &MessageRef, delta: &str) -> Result<()> {
        let ts = &msg.message_id;
        let active = {
            let map = self.streams.lock().await;
            map.get(ts).map(|e| e.active).unwrap_or(false)
        };
        if active {
            let body = build_append_stream_body(&msg.channel.channel_id, ts, delta);
            if let Err(e) = self.api_post("chat.appendStream", body).await {
                warn!(error = %e, "chat.appendStream failed (cosmetic; final replace will correct)");
            }
        } else if let Some(cumulative) = self.accumulate_degraded(ts, delta).await {
            let _ = self.edit_message(msg, &cumulative).await; // cosmetic mid-stream
        }
        Ok(())
    }

    async fn stream_finish(&self, msg: &MessageRef, final_content: &str) -> Result<()> {
        let ts = &msg.message_id;
        let active = {
            let map = self.streams.lock().await;
            map.get(ts).map(|e| e.active).unwrap_or(false)
        };
        if active {
            // Close the native stream WITHOUT re-sending content. The reply was
            // already streamed live via chat.appendStream; stopStream's
            // `markdown_text` *appends* (it does not replace), so passing the full
            // content here duplicates the whole reply (#1055). Close only, then
            // replace with the finalized content via chat.update below.
            let close = serde_json::json!({ "channel": msg.channel.channel_id, "ts": ts });
            if let Err(e) = self.api_post("chat.stopStream", close).await {
                warn!(error = %e, "chat.stopStream(close) failed; continuing to final replace");
            }
        }
        // Replace with the finalized content (Block Kit markdown). For the active
        // path this overwrites the streamed preview with a single clean copy
        // (rich rendering + native tables); for the degraded path it is the final
        // post+edit update. chat.update replaces, so there is no duplication.
        if let Err(e) = self.edit_message(msg, final_content).await {
            if active {
                // The native stream already delivered the reply (chat.appendStream),
                // and stopStream left it in place. Do NOT postMessage a fallback
                // here — that would post a duplicate copy. Keep the streamed
                // content as the final message.
                warn!(error = %e, "final chat.update failed; keeping streamed content (no duplicate post)");
            } else {
                // Degraded path: no streamed content exists (post+edit placeholder),
                // so post the final as a new message to avoid losing the reply.
                warn!(error = %e, "final chat.update failed; trying postMessage");
                if let Err(e2) = self.send_message(&msg.channel, final_content).await {
                    error!(error = %e2, "final postMessage also failed; reply may be incomplete");
                }
            }
        }
        self.streams.lock().await.remove(ts);
        Ok(())
    }

    async fn set_status(&self, channel: &ChannelRef, status: &str) -> Result<()> {
        let thread_ts = channel.thread_id.clone().unwrap_or_default();
        let body = build_set_status_body(&channel.channel_id, &thread_ts, status);
        if let Err(e) = self.api_post("assistant.threads.setStatus", body).await {
            warn!(error = %e, status, "assistant.threads.setStatus failed (cosmetic)");
        }
        Ok(())
    }
}

// --- Socket Mode event loop ---

/// Hard cap on consecutive bot messages in a thread. Prevents runaway loops.
const MAX_CONSECUTIVE_BOT_TURNS: usize = 1000;

/// Run the Slack adapter using Socket Mode (persistent WebSocket, no public URL needed).
/// Reconnects automatically on disconnect.
#[allow(clippy::too_many_arguments)]
pub async fn run_slack_adapter(
    adapter: Arc<SlackAdapter>,
    app_token: String,
    allow_all_channels: bool,
    allow_all_users: bool,
    allowed_users: HashSet<String>,
    allow_bot_messages: AllowBots,
    trusted_bot_ids: HashSet<String>,
    allow_user_messages: AllowUsers,
    max_bot_turns: u32,
    stt_config: SttConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    dispatcher: Arc<crate::dispatch::Dispatcher>,
) -> Result<()> {
    let bot_token = adapter.bot_token().to_string();
    let bot_turns = Arc::new(tokio::sync::Mutex::new(BotTurnTracker::new(max_bot_turns)));

    loop {
        // Check for shutdown before (re)connecting
        if *shutdown_rx.borrow() {
            info!("Slack adapter shutting down");
            return Ok(());
        }

        // Bound the HTTP call. reqwest has no default timeout, so a hung TCP
        // connect (e.g. waking from laptop sleep onto a not-yet-ready network)
        // would block here forever and the reconnect loop would never retry —
        // process alive, last log line "connecting…", no "connected", silent.
        let ws_url = match tokio::time::timeout(
            std::time::Duration::from_secs(20),
            get_socket_mode_url(&app_token),
        ).await {
            Ok(Ok(url)) => url,
            Ok(Err(e)) => {
                error!("failed to get Socket Mode URL: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            Err(_) => {
                warn!("get_socket_mode_url timed out after 20s — retrying");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        info!(url = %ws_url, "connecting to Slack Socket Mode");

        // Bound the WebSocket handshake for the same reason — connect_async
        // can otherwise hang indefinitely on a half-up network.
        match tokio::time::timeout(
            std::time::Duration::from_secs(20),
            tokio_tungstenite::connect_async(&ws_url),
        ).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Slack Socket Mode connected");
                let (mut write, mut read) = ws_stream.split();

                loop {
                    tokio::select! {
                        // Wrap read.next() in a read deadline. Slack Socket Mode
                        // sends a server ping every ~30-45s, so 70s of total
                        // silence means the connection has gone half-open (peer
                        // stopped sending without a FIN/RST — no read Err fires,
                        // so without this the loop would block here forever and
                        // never reconnect). On timeout, break to the existing
                        // `reconnect in 5s` path below.
                        timed = tokio::time::timeout(
                            std::time::Duration::from_secs(70),
                            read.next(),
                        ) => {
                            let msg_result = match timed {
                                Ok(m) => m,
                                Err(_) => {
                                    warn!("Slack Socket Mode idle >70s (no server ping) — assuming half-open, reconnecting");
                                    break;
                                }
                            };
                            let Some(msg_result) = msg_result else { break };
                            match msg_result {
                                Ok(tungstenite::Message::Text(text)) => {
                                    let envelope: serde_json::Value =
                                        match serde_json::from_str(&text) {
                                            Ok(v) => v,
                                            Err(_) => continue,
                                        };

                                    // Acknowledge the envelope immediately
                                    if let Some(envelope_id) = envelope["envelope_id"].as_str() {
                                        let ack = serde_json::json!({"envelope_id": envelope_id});
                                        let _ = write
                                            .send(tungstenite::Message::Text(ack.to_string()))
                                            .await;
                                    }

                                    // Slash commands and interactive block_actions aren't
                                    // handled on Slack: slash commands are blocked by Slack
                                    // in thread composers, and the channel-level delivery
                                    // lacks the thread_ts needed to route to a session.
                                    // Ack only; ignore payload.
                                    match envelope["type"].as_str() {
                                        Some("slash_commands") | Some("interactive") => {
                                            debug!(
                                                envelope_type = envelope["type"].as_str().unwrap_or(""),
                                                "ignoring Slack envelope type (not supported on this adapter)"
                                            );
                                            continue;
                                        }
                                        _ => {}
                                    }

                                    // Route events
                                    if envelope["type"].as_str() == Some("events_api") {
                                        let event = &envelope["payload"]["event"];
                                        let event_type = event["type"].as_str().unwrap_or("");
                                        match event_type {
                                            "app_mention" => {
                                                // Apply bot gating for app_mention events (same rules as message events)
                                                let is_bot = event["bot_id"].is_string()
                                                    || event["subtype"].as_str() == Some("bot_message");
                                                if is_bot {
                                                    match allow_bot_messages {
                                                        AllowBots::Off => { continue; }
                                                        AllowBots::Mentions | AllowBots::All => {
                                                            if !trusted_bot_ids.is_empty() {
                                                                let event_bot_id = event["bot_id"].as_str().unwrap_or("");
                                                                let is_trusted = adapter
                                                                    .trusted_bot_ids_contains(&trusted_bot_ids, event_bot_id)
                                                                    .await;
                                                                if !is_trusted {
                                                                    debug!(event_bot_id, "bot not in trusted_bot_ids, ignoring app_mention");
                                                                    continue;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                let event = event.clone();
                                                let adapter = adapter.clone();
                                                let bot_token = bot_token.clone();
                                                // Snapshot the (runtime-mutable) allowlist per message. Cheap —
                                                // a handful of channel IDs — and avoids holding the lock across
                                                // handle_message's awaits. Picks up channels the bot just created.
                                                let allowed_channels =
                                                    adapter.allowed_channels.read().await.clone();
                                                let allowed_users = allowed_users.clone();
                                                let stt_config = stt_config.clone();
                                                let dispatcher = dispatcher.clone();
                                                let team_id = envelope["payload"]["team_id"]
                                                    .as_str()
                                                    .unwrap_or("")
                                                    .to_string();
                                                tokio::spawn(async move {
                                                    handle_message(
                                                        &event,
                                                        &team_id,
                                                        &adapter,
                                                        &bot_token,
                                                        allow_all_channels,
                                                        allow_all_users,
                                                        &allowed_channels,
                                                        &allowed_users,
                                                        &stt_config,
                                                        &dispatcher,
                                                    )
                                                    .await;
                                                });
                                            }
                                            "message" => {
                                                let channel_id = event["channel"].as_str().unwrap_or("");
                                                let has_thread = event["thread_ts"].is_string();
                                                let is_bot = event["bot_id"].is_string()
                                                    || event["subtype"].as_str() == Some("bot_message");
                                                let subtype = event["subtype"].as_str().unwrap_or("");
                                                let msg_text = event["text"].as_str().unwrap_or("");
                                                let bot_uid_opt = adapter.get_bot_user_id().await.map(|s| s.to_string());
                                                let mentions_bot = bot_uid_opt
                                                    .as_ref()
                                                    .is_some_and(|bot_uid| text_mentions_uid(msg_text, bot_uid));
                                                let is_dm = channel_id.starts_with('D');
                                                let event_user_id = event["user"].as_str();
                                                let is_own_bot_msg = is_bot
                                                    && bot_uid_opt.as_deref().is_some()
                                                    && event_user_id == bot_uid_opt.as_deref();

                                                debug!(
                                                    channel_id,
                                                    has_thread,
                                                    is_bot,
                                                    is_dm,
                                                    subtype,
                                                    mentions_bot,
                                                    text = msg_text,
                                                    "message event received"
                                                );

                                                // Bot invited into an existing channel: Slack emits a
                                                // `channel_join` message whose `user` is the joiner. When that's
                                                // the bot itself, self-heal the allowlist (runtime + persist) so
                                                // the bot can hear that channel without a manual config edit +
                                                // restart (2026-06-04 — symmetric with the create-channel path).
                                                // Still falls through to skip_subtype below (no agent dispatch for
                                                // a join notice).
                                                if subtype == "channel_join"
                                                    && bot_uid_opt.as_deref().is_some()
                                                    && event_user_id == bot_uid_opt.as_deref()
                                                {
                                                    adapter.allow_channel_now(channel_id, "invited").await;
                                                }

                                                // Skip non-message subtypes
                                                let skip_subtype = matches!(subtype,
                                                    "message_changed" | "message_deleted" |
                                                    "channel_join" | "channel_leave" |
                                                    "channel_topic" | "channel_purpose"
                                                );
                                                if skip_subtype { continue; }

                                                // --- Eager multibot detection ---
                                                // Runs before self-check and bot gating so we always detect
                                                // other bots even when allow_bot_messages=Off filters them out.
                                                // Matches Discord #481 ordering.
                                                if is_bot && !is_own_bot_msg {
                                                    if let Some(thread_ts) = event["thread_ts"].as_str() {
                                                        adapter.note_other_bot_in_thread(thread_ts).await;
                                                    }
                                                }

                                                // --- Bot turn tracking ---
                                                // Runs before self-check so ALL bot messages (including own)
                                                // count toward the per-thread limit. Matches Discord #483.
                                                // Keyed on thread_ts when in a thread, else channel:ts.
                                                // Non-thread messages get a unique key per message, so the
                                                // counter never accumulates — intentional, because bot-to-bot
                                                // loops only happen inside threads.
                                                let turn_key = if let Some(thread_ts) = event["thread_ts"].as_str() {
                                                    thread_ts.to_string()
                                                } else {
                                                    format!("{}:{}", channel_id, event["ts"].as_str().unwrap_or(""))
                                                };
                                                {
                                                    let mut tracker = bot_turns.lock().await;
                                                    if is_bot {
                                                        match tracker.classify_bot_message(&turn_key) {
                                                            TurnAction::Continue => {}
                                                            TurnAction::SilentStop => continue,
                                                            TurnAction::WarnAndStop { severity, turns, user_message } => {
                                                                match severity {
                                                                    TurnSeverity::Hard => warn!(channel_id, turns, "hard bot turn limit reached"),
                                                                    TurnSeverity::Soft => info!(channel_id, turns, max = max_bot_turns, "soft bot turn limit reached"),
                                                                }
                                                                let channel_allowed = allow_all_channels
                                                                    || adapter.allowed_channels.read().await.contains(channel_id);
                                                                if !is_own_bot_msg && channel_allowed {
                                                                    let warn_channel = ChannelRef {
                                                                        platform: "slack".into(),
                                                                        channel_id: channel_id.to_string(),
                                                                        thread_id: event["thread_ts"].as_str().map(|s| s.to_string()),
                                                                        parent_id: None,
                                                                        origin_event_id: None,
                                                                    };
                                                                    let _ = adapter.send_message(&warn_channel, &user_message).await;
                                                                }
                                                                continue;
                                                            }
                                                        }
                                                    } else if is_plain_user_message(subtype, msg_text) {
                                                        tracker.on_human_message(&turn_key);
                                                    }
                                                }

                                                // Ignore own bot messages (after counting toward turns)
                                                if is_own_bot_msg { continue; }

                                                // Skip messages that @mention the bot — app_mention handles those.
                                                // EXCEPT bot-authored mentions: Slack never emits an app_mention
                                                // event for a mention made by another bot/app, so deferring would
                                                // drop a peer bot's @mention entirely (it never arrives via
                                                // app_mention). Also except DMs, where app_mention doesn't fire.
                                                // Let both fall through to the bot/user gating below.
                                                if mentions_bot && !is_dm && !is_bot { continue; }

                                                // --- Bot message gating ---
                                                if is_bot {
                                                    let event_bot_id = event["bot_id"].as_str().unwrap_or("");
                                                    match allow_bot_messages {
                                                        AllowBots::Off => { continue; }
                                                        AllowBots::Mentions => {
                                                            if !mentions_bot { continue; }
                                                        }
                                                        AllowBots::All => {
                                                            // Loop protection: count consecutive bot msgs (fail-closed)
                                                            if let Some(thread_ts) = event["thread_ts"].as_str() {
                                                                let cap = MAX_CONSECUTIVE_BOT_TURNS;
                                                                let limit_str = std::cmp::min(cap + 1, 1000).to_string();
                                                                match adapter.api_get(
                                                                    "conversations.replies",
                                                                    &[
                                                                        ("channel", channel_id),
                                                                        ("ts", thread_ts),
                                                                        ("limit", &limit_str),
                                                                        ("inclusive", "true"),
                                                                    ],
                                                                ).await {
                                                                    Ok(resp) => {
                                                                        if let Some(msgs) = resp["messages"].as_array() {
                                                                            let consecutive = msgs.iter().rev()
                                                                                .take_while(|m| {
                                                                                    m["bot_id"].is_string()
                                                                                        || m["subtype"].as_str() == Some("bot_message")
                                                                                })
                                                                                .count();
                                                                            if consecutive >= cap {
                                                                                warn!(channel_id, cap, "bot turn cap reached, ignoring");
                                                                                continue;
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        warn!(channel_id, thread_ts, error = %e, "failed to fetch thread for bot loop check, rejecting (fail-closed)");
                                                                        continue;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    // Check trusted_bot_ids
                                                    if !trusted_bot_ids.is_empty() {
                                                        let is_trusted = adapter
                                                            .trusted_bot_ids_contains(&trusted_bot_ids, event_bot_id)
                                                            .await;
                                                        if !is_trusted {
                                                            debug!(event_bot_id, "bot not in trusted_bot_ids, ignoring");
                                                            continue;
                                                        }
                                                    }
                                                    // Bot messages must be in a thread (no top-level bot processing)
                                                    if !has_thread { continue; }
                                                }

                                                // --- User message gating ---
                                                if !is_bot {
                                                    if is_dm {
                                                        // DM: implicit mention — always process
                                                    } else {
                                                        match allow_user_messages {
                                                            AllowUsers::Mentions => {
                                                                if !mentions_bot { continue; }
                                                            }
                                                            AllowUsers::Involved => {
                                                                if !has_thread {
                                                                    continue;
                                                                }
                                                                let thread_ts = event["thread_ts"].as_str().unwrap_or("");
                                                                let (involved, _) = adapter
                                                                    .bot_participated_in_thread(channel_id, thread_ts)
                                                                    .await;
                                                                if !involved {
                                                                    debug!(channel_id, thread_ts, "bot not involved in thread, ignoring");
                                                                    continue;
                                                                }
                                                            }
                                                            AllowUsers::MultibotMentions => {
                                                                if !has_thread {
                                                                    continue;
                                                                }
                                                                let thread_ts = event["thread_ts"].as_str().unwrap_or("");
                                                                let (involved, other_bot) = adapter
                                                                    .bot_participated_in_thread(channel_id, thread_ts)
                                                                    .await;
                                                                if !involved {
                                                                    debug!(channel_id, thread_ts, "bot not involved in thread, ignoring");
                                                                    continue;
                                                                }
                                                                // In multi-bot threads, require @mention — mirrors
                                                                // Discord's `should_process_user_message`. In practice
                                                                // mention-bearing message events are already deduped
                                                                // earlier (app_mention handles the @-path), so this
                                                                // branch rarely sees `mentions_bot == true`, but keep
                                                                // the explicit check so the logic is self-consistent
                                                                // and survives changes to the earlier dedup.
                                                                if other_bot && !mentions_bot {
                                                                    debug!(channel_id, thread_ts, "multi-bot thread without @mention, ignoring");
                                                                    continue;
                                                                }
                                                            }
                                                            AllowUsers::OwnerOrMentions => {
                                                                if !has_thread {
                                                                    continue;
                                                                }
                                                                let thread_ts = event["thread_ts"].as_str().unwrap_or("");
                                                                let (involved, _) = adapter
                                                                    .bot_participated_in_thread(channel_id, thread_ts)
                                                                    .await;
                                                                if !involved {
                                                                    debug!(channel_id, thread_ts, "bot not involved in thread, ignoring");
                                                                    continue;
                                                                }
                                                                // Only the owner (allowed_users) gets a tag-free reply
                                                                // in a shared thread; everyone else must @mention.
                                                                // Keeps owner conversation frictionless while other
                                                                // humans in the thread can't pull the bot in unasked.
                                                                let is_owner = event_user_id
                                                                    .is_some_and(|u| allowed_users.contains(u));
                                                                if !is_owner && !mentions_bot {
                                                                    debug!(channel_id, thread_ts, "non-owner without @mention, ignoring");
                                                                    continue;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }

                                                // Dispatch to handle_message (per-thread serialization comes
                                                // from Dispatcher consumer task in batched mode and from
                                                // pool.with_connection in per-message mode).
                                                let team_id = envelope["payload"]["team_id"]
                                                    .as_str()
                                                    .unwrap_or("")
                                                    .to_string();
                                                let event = event.clone();
                                                let adapter = adapter.clone();
                                                let bot_token = bot_token.clone();
                                                // Snapshot the (runtime-mutable) allowlist per message. Cheap —
                                                // a handful of channel IDs — and avoids holding the lock across
                                                // handle_message's awaits. Picks up channels the bot just created.
                                                let allowed_channels =
                                                    adapter.allowed_channels.read().await.clone();
                                                let allowed_users = allowed_users.clone();
                                                let stt_config = stt_config.clone();
                                                let dispatcher = dispatcher.clone();
                                                tokio::spawn(async move {
                                                    handle_message(
                                                        &event,
                                                        &team_id,
                                                        &adapter,
                                                        &bot_token,
                                                        allow_all_channels,
                                                        allow_all_users,
                                                        &allowed_channels,
                                                        &allowed_users,
                                                        &stt_config,
                                                        &dispatcher,
                                                    )
                                                    .await;
                                                });
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Ok(tungstenite::Message::Ping(data)) => {
                                    let _ = write.send(tungstenite::Message::Pong(data)).await;
                                }
                                Ok(tungstenite::Message::Close(_)) => {
                                    warn!("Slack Socket Mode connection closed by server");
                                    break;
                                }
                                Err(e) => {
                                    error!("Socket Mode read error: {e}");
                                    break;
                                }
                                _ => {}
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            info!("Slack adapter received shutdown signal");
                            let _ = write.send(tungstenite::Message::Close(None)).await;
                            return Ok(());
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                error!("failed to connect to Slack Socket Mode: {e}");
            }
            Err(_) => {
                warn!("connect_async timed out after 20s — retrying");
            }
        }

        warn!("reconnecting to Slack Socket Mode in 5s...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Call apps.connections.open to get a WebSocket URL for Socket Mode.
async fn get_socket_mode_url(app_token: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{SLACK_API}/apps.connections.open"))
        .header("Authorization", format!("Bearer {app_token}"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await?;
    let json: serde_json::Value = resp.json().await?;
    if json["ok"].as_bool() != Some(true) {
        let err = json["error"].as_str().unwrap_or("unknown");
        return Err(anyhow!("apps.connections.open: {err}"));
    }
    json["url"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("no url in apps.connections.open response"))
}

#[allow(clippy::too_many_arguments)]
async fn handle_message(
    event: &serde_json::Value,
    team_id: &str,
    adapter: &Arc<SlackAdapter>,
    bot_token: &str,
    allow_all_channels: bool,
    allow_all_users: bool,
    allowed_channels: &HashSet<String>,
    allowed_users: &HashSet<String>,
    stt_config: &SttConfig,
    dispatcher: &Arc<crate::dispatch::Dispatcher>,
) {
    let channel_id = match event["channel"].as_str() {
        Some(ch) => ch.to_string(),
        None => return,
    };
    // Bot messages may lack "user" field — fall back to "bot_id" as sender identifier
    let user_id = match event["user"].as_str().or_else(|| event["bot_id"].as_str()) {
        Some(u) => u.to_string(),
        None => return,
    };
    let is_bot_msg =
        event["bot_id"].is_string() || event["subtype"].as_str() == Some("bot_message");
    let text = match event["text"].as_str() {
        Some(t) => t.to_string(),
        None => return,
    };
    let ts = match event["ts"].as_str() {
        Some(ts) => ts.to_string(),
        None => return,
    };
    let thread_ts = event["thread_ts"].as_str().map(|s| s.to_string());

    // Check allowed channels. DMs (channel id starts with 'D') are exempt:
    // they're inherently 1:1 and already gated by `allowed_users` below, so
    // they must not require the DM channel to appear in `allowed_channels`.
    // Without this exemption, setting `allowed_channels` to lock down a shared
    // channel silently drops every DM (regression from the Atlas team-bots
    // patch, 2026-05-28).
    let is_dm = channel_id.starts_with('D');
    if !is_dm && !allow_all_channels && !allowed_channels.contains(&channel_id) {
        return;
    }

    // Check allowed users — skip for bot messages (they go through trusted_bot_ids instead)
    if !is_bot_msg && !allow_all_users && !allowed_users.contains(&user_id) {
        // 2026-06-03 (洺哥): silently ignore denied users — no 🚫 reaction. The
        // reaction marked non-allowlisted senders in shared channels, but it
        // surfaces the bot's presence to people it won't talk to, which reads as
        // rude. Log-only denial is quieter; the user simply gets no response.
        tracing::info!(user_id, "denied Slack user, ignoring (no reaction)");
        return;
    }

    // Capture the native-streaming recipient for THIS turn, now that the sender has
    // passed the channel + user allow-list checks above (so denied/unauthorized
    // senders are never recorded). It rides on the per-turn BufferedMessage to
    // stream_begin — no shared thread cache, no cross-turn race. Real users only:
    // bot IDs (B...) are rejected by chat.startStream's recipient_user_id, and an
    // empty team_id would silently degrade, so we surface that.
    let stream_recipient = if is_bot_msg {
        None
    } else {
        if team_id.is_empty() {
            warn!("empty team_id; chat.startStream will degrade to post+edit");
        }
        Some((user_id.clone(), team_id.to_string()))
    };

    // Resolve mentions: strip only this bot's own trigger mention so the LLM
    // can still @-mention other users in its reply.
    let bot_id = adapter.get_bot_user_id().await;
    let prompt = resolve_slack_mentions(&text, bot_id);

    // Process file attachments (images, audio)
    let files = event["files"].as_array();
    let has_files = files.is_some_and(|f| !f.is_empty());

    if prompt.is_empty() && !has_files {
        return;
    }

    // Caps mirror Discord's text-file attachment flow (PR #291) so both
    // adapters apply the same limits: 5 files or 1 MB of text per message.
    const TEXT_TOTAL_CAP: u64 = 1024 * 1024;
    const TEXT_FILE_COUNT_CAP: u32 = 5;

    let mut extra_blocks = Vec::new();

    // 2a (2026-06-03): if this message is in a thread, fetch the earlier thread
    // messages and prepend them as context so the agent can read/summarise the
    // thread. OpenAB exposes no agent-callable read tool (mcpServers: [] in the
    // ACP handshake), so always-on injection is how thread content reaches the
    // agent. Fail-soft: on API error the helper returns None and we proceed
    // without thread context rather than dropping the turn.
    if let Some(thread_ts) = thread_ts.as_deref() {
        if let Some(ctx) = adapter.fetch_thread_context(&channel_id, thread_ts, &ts).await {
            extra_blocks.push(ContentBlock::Text { text: ctx });
        }
    }

    let mut echo_entries: Vec<crate::stt::EchoEntry> = Vec::new();
    let mut text_file_bytes: u64 = 0;
    let mut text_file_count: u32 = 0;
    let mut failed_image_files: Vec<String> = Vec::new();

    if let Some(files) = files {
        for file in files {
            let mimetype_raw = file["mimetype"].as_str().unwrap_or("");
            let mimetype = strip_mime_params(mimetype_raw);
            let filename = file["name"].as_str().unwrap_or("file");
            let size = file["size"].as_u64().unwrap_or(0);
            // Slack private files require Bearer token to download
            let url = slack_file_download_url(file);

            if url.is_empty() {
                continue;
            }

            if media::is_audio_mime(mimetype) {
                if stt_config.enabled {
                    match media::download_and_transcribe(
                        url,
                        filename,
                        mimetype,
                        size,
                        stt_config,
                        Some(bot_token),
                    )
                    .await
                    {
                        Some(transcript) => {
                            debug!(
                                filename,
                                chars = transcript.len(),
                                "voice transcript injected"
                            );
                            extra_blocks.insert(
                                0,
                                ContentBlock::Text {
                                    text: format!("[Voice message transcript]: {transcript}"),
                                },
                            );
                            echo_entries.push(crate::stt::EchoEntry::Success(transcript));
                        }
                        None => {
                            warn!(filename, "STT failed for voice attachment");
                            echo_entries.push(crate::stt::EchoEntry::Failed);
                        }
                    }
                } else {
                    debug!(filename, "skipping audio attachment (STT disabled)");
                    let msg_ref = MessageRef {
                        channel: ChannelRef {
                            platform: "slack".into(),
                            channel_id: channel_id.clone(),
                            thread_id: thread_ts.clone(),
                            parent_id: None,
                            origin_event_id: None,
                        },
                        message_id: ts.clone(),
                    };
                    let _ = adapter.add_reaction(&msg_ref, "🎤").await;
                }
            } else if media::is_text_file(filename, Some(mimetype)) {
                if text_file_count >= TEXT_FILE_COUNT_CAP {
                    debug!(
                        filename,
                        count = text_file_count,
                        "text file count cap reached, skipping"
                    );
                    continue;
                }
                // Pre-check with Slack-reported size as a fast path when the
                // field is populated. Slack can report `size == 0` for
                // externally-backed files, so this is advisory only — the
                // authoritative cap check happens after download using
                // `actual_bytes`.
                if size > 0 && text_file_bytes + size > TEXT_TOTAL_CAP {
                    debug!(
                        filename,
                        total = text_file_bytes,
                        "text attachments total exceeds 1MB cap, skipping remaining"
                    );
                    continue;
                }
                if let Some((block, actual_bytes)) =
                    media::download_and_read_text_file(url, filename, size, Some(bot_token)).await
                {
                    if text_file_bytes + actual_bytes > TEXT_TOTAL_CAP {
                        debug!(
                            filename,
                            running = text_file_bytes,
                            actual = actual_bytes,
                            "text attachments total exceeds 1MB cap after download, dropping file",
                        );
                        continue;
                    }
                    text_file_bytes += actual_bytes;
                    text_file_count += 1;
                    debug!(filename, "adding text file attachment");
                    extra_blocks.push(block);
                }
            } else if mimetype.starts_with("image/") {
                match media::download_and_encode_image(
                    url,
                    Some(mimetype),
                    filename,
                    size,
                    Some(bot_token),
                )
                .await
                {
                    Ok(block) => {
                        debug!(filename, "adding image attachment");
                        extra_blocks.push(block);
                    }
                    // mimetype claimed image/* but media disagreed; nothing usable.
                    Err(media::MediaFetchError::NotAnImage) => {}
                    Err(media::MediaFetchError::SizeExceeded { actual, limit }) => {
                        warn!(filename, actual, limit, "image exceeds size limit");
                        failed_image_files.push(filename.to_string());
                    }
                    Err(
                        media::MediaFetchError::UnsupportedResponseType { .. }
                        | media::MediaFetchError::InvalidImageBody { .. },
                    ) => {
                        warn!(
                            filename,
                            "image validation failed; server may have returned non-image content"
                        );
                        failed_image_files.push(filename.to_string());
                    }
                    Err(media::MediaFetchError::ProcessingFailed(ref e)) => {
                        warn!(filename, error = %e, "image post-processing failed");
                        failed_image_files.push(filename.to_string());
                    }
                    Err(media::MediaFetchError::HttpStatus(status))
                        if status.is_client_error() =>
                    {
                        warn!(filename, %status, "image download denied");
                        failed_image_files.push(filename.to_string());
                    }
                    Err(e) => {
                        warn!(filename, error = %e, "image download failed");
                        failed_image_files.push(filename.to_string());
                    }
                }
            } else {
                // Fallback for unhandled file types (video, PDF, Office docs,
                // archives, generic binary): download to disk so the agent
                // gets a local path it can hand to ffmpeg / pdftotext / etc.
                // If the disk write fails, inject a failure-notice block so
                // the agent still knows the file existed (fail loudly, don't
                // silently drop).
                match media::download_to_disk(
                    url,
                    filename,
                    mimetype,
                    size,
                    Some(bot_token),
                    &ts,
                )
                .await
                {
                    Some(block) => {
                        debug!(filename, "adding file attachment via disk path");
                        extra_blocks.push(block);
                    }
                    None => {
                        let human_size = format_size_human(size);
                        let notice = format!(
                            "[Slack file attachment — download failed]\n\
                             - filename: {filename}\n\
                             - mimetype: {mimetype}\n\
                             - size: {human_size}\n\
                             \n\
                             OpenAB tried to save this file locally but the download or write \
                             failed (see logs). The user shared the file; acknowledge it but \
                             note you couldn't access the contents."
                        );
                        warn!(filename, mimetype, size, "download_to_disk failed, emitting failure notice");
                        extra_blocks.push(ContentBlock::Text { text: notice });
                    }
                }
            }
        }
    }

    // Notify user if any images couldn't be processed.
    if !failed_image_files.is_empty() {
        let warn_channel = ChannelRef {
            platform: "slack".into(),
            channel_id: channel_id.clone(),
            thread_id: thread_ts.clone().or_else(|| Some(ts.clone())),
            parent_id: None,
            origin_event_id: None,
        };
        let file_list = failed_image_files
            .iter()
            .map(|n| sanitize_slack_filename(n))
            .collect::<Vec<_>>()
            .join("`, `");
        let msg = format!(
            ":warning: I couldn't process the file(s) you shared (`{file_list}`). \
             This can happen when the bot lacks the `files:read` OAuth scope, \
             the file format isn't supported (PNG/JPEG/GIF/WebP only), \
             or the file is too large."
        );
        if let Err(e) = adapter.send_message(&warn_channel, &msg).await {
            warn!(error = %e, "failed to send image validation warning to user");
        }
    }

    // Resolve Slack display name (best-effort, fallback to user_id)
    let display_name = adapter
        .resolve_user_name(&user_id)
        .await
        .unwrap_or_else(|| user_id.clone());

    let sender = SenderContext {
        schema: "openab.sender.v1".into(),
        sender_id: user_id.clone(),
        sender_name: display_name.clone(),
        display_name,
        channel: "slack".into(),
        channel_id: channel_id.clone(),
        thread_id: thread_ts.clone(),
        is_bot: is_bot_msg,
        timestamp: Some(crate::timestamp::slack_ts_to_iso8601(&ts)),
        message_id: Some(ts.clone()),
        receiver_id: bot_id.map(|id| id.to_string()),
    };

    let trigger_msg = MessageRef {
        channel: ChannelRef {
            platform: "slack".into(),
            channel_id: channel_id.clone(),
            thread_id: thread_ts.clone(),
            parent_id: None,
            origin_event_id: None,
        },
        message_id: ts.clone(),
    };

    // Determine thread: if already in a thread, continue it; otherwise start a new thread
    let thread_channel = ChannelRef {
        platform: "slack".into(),
        channel_id: channel_id.clone(),
        thread_id: Some(thread_ts.unwrap_or(ts)),
        parent_id: None,
        origin_event_id: None,
    };

    // Serialize sender context with Slack-native key names so agents calling
    // the Slack API directly see "thread_ts" rather than the generic "thread_id".
    let sender_json = {
        let mut v = serde_json::to_value(&sender).unwrap();
        if let Some(obj) = v.as_object_mut() {
            if let Some(tid) = obj.remove("thread_id") {
                obj.insert("thread_ts".to_string(), tid);
            }
        }
        v.to_string()
    };

    let adapter_dyn: Arc<dyn ChatAdapter> = adapter.clone();
    let other_bot_present = {
        let cache = adapter.multibot_threads.lock().await;
        thread_channel.thread_id.as_deref().is_some_and(|ts| {
            cache
                .get(ts)
                .is_some_and(|inst| inst.elapsed() < adapter.session_ttl)
        })
    };

    // Best-effort echo before the agent reply so the user can verify STT.
    crate::stt::post_echo(
        &adapter_dyn,
        &thread_channel,
        &trigger_msg,
        &echo_entries,
        stt_config,
    )
    .await;

    let thread_id = thread_channel
        .thread_id
        .as_deref()
        .unwrap_or(&thread_channel.channel_id);
    let thread_key = dispatcher.key("slack", thread_id, &sender.sender_id);
    let estimated_tokens = crate::dispatch::estimate_tokens(&prompt, &extra_blocks);
    let buf_msg = crate::dispatch::BufferedMessage {
        sender_json,
        sender_name: sender.sender_name.clone(),
        prompt,
        extra_blocks,
        trigger_msg,
        arrived_at: std::time::Instant::now(),
        estimated_tokens,
        other_bot_present,
        recipient: stream_recipient,
    };
    if let Err(e) = dispatcher
        .submit(thread_key, thread_channel, adapter_dyn, buf_msg)
        .await
    {
        error!("Slack dispatcher submit error: {e}");
    }
}

/// Strip all occurrences of the bot's own `<@BOT_UID>` or `<@BOT_UID|handle>` mention.
/// Other users' mentions stay intact so the LLM can @-mention them back.
/// If the bot UID isn't known, fall back to returning the text trimmed —
/// safer than stripping all mentions and losing user addressability.
fn resolve_slack_mentions(text: &str, bot_id: Option<&str>) -> String {
    let Some(id) = bot_id else {
        return text.trim().to_string();
    };
    let prefix = format!("<@{id}");
    let mut out = String::with_capacity(text.len());
    let mut s = text;
    while let Some(pos) = s.find(&prefix) {
        let after = &s[pos + prefix.len()..];
        match after.as_bytes().first() {
            Some(b'>') => {
                out.push_str(&s[..pos]);
                s = &after[1..];
            }
            Some(b'|') => {
                if let Some(close) = after.find('>') {
                    out.push_str(&s[..pos]);
                    s = &after[close + 1..];
                } else {
                    out.push_str(&s[..pos + prefix.len()]);
                    s = after;
                }
            }
            _ => {
                out.push_str(&s[..pos + prefix.len()]);
                s = after;
            }
        }
    }
    out.push_str(s);
    out.trim().to_string()
}

/// Pick the best download URL for a Slack file object. `url_private_download`
/// streams the raw bytes; `url_private` is the fallback for older file shapes.
/// Returns `""` when neither is present (caller should skip the file).
fn slack_file_download_url(file: &serde_json::Value) -> &str {
    file["url_private_download"]
        .as_str()
        .or_else(|| file["url_private"].as_str())
        .unwrap_or("")
}

/// Format a byte count in a human-readable way (e.g. "2.2 MB", "456 KB").
/// Used in the unhandled-file metadata block so the agent sees a friendly size.
fn format_size_human(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else if bytes > 0 {
        format!("{bytes} bytes")
    } else {
        "unknown size".to_string()
    }
}

/// Strip MIME parameters so type-detection helpers see the bare media type.
/// Delegates to media::strip_mime_params (single source of truth).
/// Needed because Slack occasionally sends `text/plain; charset=utf-8` and
/// `media::is_text_file` expects the bare form.
fn strip_mime_params(mimetype: &str) -> &str {
    media::strip_mime_params(mimetype)
}

/// Sanitize a filename for safe embedding in a Slack mrkdwn message.
///
/// Ampersands (`&`), backticks (`` ` ``), and angle brackets (`<`, `>`) are escaped.
/// `&` is encoded as `&amp;` first because Slack decodes HTML entities before parsing
/// mrkdwn — a filename like `&lt;@here&gt;` would otherwise round-trip back to
/// `<@here>` and trigger a mention ping. Backticks and angle brackets are Slack
/// mrkdwn delimiters; without escaping, `<!here>` or `` `<@U123>` `` would render
/// as mentions or @-here pings.
pub(crate) fn sanitize_slack_filename(s: &str) -> String {
    s.replace('&', "&amp;").replace('`', "'").replace('<', "(").replace('>', ")")
}

/// Returns `true` if `text` contains a Slack user mention for `uid`.
///
/// Accepts both `<@U...>` (bare) and `<@U...|handle>` (labelled) wire forms.
/// Slack (and bots addressing peers) can emit the labelled form; `<@UID>` is
/// not a substring of `<@UID|handle>`, so a bare `contains("<@UID>")` silently
/// misses it.
fn text_mentions_uid(text: &str, uid: &str) -> bool {
    let prefix = format!("<@{uid}");
    text.match_indices(&prefix)
        .any(|(i, _)| matches!(text.as_bytes().get(i + prefix.len()), Some(b'>') | Some(b'|')))
}

fn bot_id_matches_trusted(
    trusted_bot_ids: &HashSet<String>,
    event_bot_id: &str,
    resolved_user_id: Option<&str>,
) -> bool {
    if event_bot_id.is_empty() {
        return false;
    }

    trusted_bot_ids.contains(event_bot_id)
        || resolved_user_id.is_some_and(|uid| trusted_bot_ids.contains(uid))
}

/// True only when a Slack non-bot event represents a real user message
/// that should reset the bot-turn counter.
///
/// Many Slack subtypes (pinned_item, channel_name, channel_archive,
/// group_join / group_leave / group_topic / group_purpose, reminder_add,
/// tombstone, …) carry a `user` field so the event loop sees
/// `is_bot == false`, but they represent administrative/system actions,
/// not conversation. Resetting the counter on them would let runaway
/// bot-to-bot loops re-arm whenever any pin / rename / archive happens.
///
/// Mirrors Discord's `MessageType::Regular | InlineReply` + non-empty
/// content gate in `src/discord.rs`. Regression parity for
/// openabdev/openab#497.
fn is_plain_user_message(subtype: &str, text: &str) -> bool {
    if text.is_empty() {
        return false;
    }
    matches!(
        subtype,
        "" | "me_message" | "thread_broadcast" | "file_share",
    )
}

/// Slack caps a single Block Kit `markdown` block at 12,000 characters; we use
/// 11,900 to keep ~100 chars of headroom. Doubles as the Slack `message_limit`
/// so the router splits long replies into separate messages at the same bound
/// (one markdown block per message stays under the API cap).
const MARKDOWN_BLOCK_LIMIT: usize = 11_900;

/// True if a Slack API error indicates the `blocks` payload was rejected, so the
/// caller should retry text-only:
/// - `invalid_blocks` — workspace can't render the Block Kit `markdown` block
///   (malformed/unsupported payload).
/// - `msg_blocks_too_long` — content exceeds Slack's cumulative ~12k cap across
///   all `markdown` blocks in one message. Reachable by direct `send_message`
///   callers that bypass the router's `message_limit` pre-split (e.g. STT echo).
///
/// `invalid_arguments` is deliberately excluded — it's a Slack catch-all (bad
/// channel, missing/invalid `ts`, malformed `thread_ts`, …) and would trigger a
/// pointless text-only retry that fails identically.
///
/// Matches the Slack error *code* exactly (the trailing token of `api_post`'s
/// `"Slack API <method>: <code>"` message), not a substring of the message —
/// so a future code like `invalid_blocks_field` does not falsely match.
fn is_block_payload_rejected(e: &anyhow::Error) -> bool {
    let s = e.to_string();
    let code = s.rsplit(": ").next().unwrap_or(s.as_str()).trim();
    code == "invalid_blocks" || code == "msg_blocks_too_long"
}

/// Build Block Kit `markdown` blocks from raw Markdown. Slack renders these
/// natively — real headings, lists, tables, blockquotes, and language-tagged
/// code fences — unlike the legacy `text` mrkdwn field, which flattens headings
/// to bold and cannot render tables. Long content is split at the block limit,
/// reusing `format::split_message` so code-fence balance is preserved.
///
/// Follow-up (non-blocking): `split_message` is not table-aware — a single
/// Markdown table exceeding `MARKDOWN_BLOCK_LIMIT` (11,900 chars) splits at line
/// boundaries, so continuation blocks lack the header/separator rows and render
/// as raw pipes. The 4000→11,900 bump makes this rare; a future improvement is
/// to re-emit the table header at the top of each continuation chunk.
fn build_markdown_blocks(content: &str) -> Vec<serde_json::Value> {
    let chunks = if content.len() <= MARKDOWN_BLOCK_LIMIT {
        vec![content.to_string()]
    } else {
        crate::format::split_message(content, MARKDOWN_BLOCK_LIMIT)
    };
    chunks
        .into_iter()
        .map(|chunk| serde_json::json!({ "type": "markdown", "text": chunk }))
        .collect()
}

/// Body for `chat.postMessage`: Block Kit `markdown` blocks (rich rendering)
/// plus a `text` fallback used for notifications and accessibility.
fn build_post_message_body(
    channel_id: &str,
    thread_ts: Option<&str>,
    content: &str,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "channel": channel_id,
        "blocks": build_markdown_blocks(content),
        "text": markdown_to_mrkdwn(content),
    });
    if let Some(ts) = thread_ts {
        body["thread_ts"] = serde_json::Value::String(ts.to_string());
    }
    body
}

/// Body for `chat.update`: same Block Kit `markdown` blocks + `text` fallback.
fn build_update_body(channel_id: &str, ts: &str, content: &str) -> serde_json::Value {
    serde_json::json!({
        "channel": channel_id,
        "ts": ts,
        "blocks": build_markdown_blocks(content),
        "text": markdown_to_mrkdwn(content),
    })
}

/// Text-only `chat.postMessage` body (no `blocks`) — degradation path when a
/// workspace rejects the Block Kit `markdown` block.
fn build_post_message_text_only(
    channel_id: &str,
    thread_ts: Option<&str>,
    content: &str,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "channel": channel_id,
        "text": markdown_to_mrkdwn(content),
    });
    if let Some(ts) = thread_ts {
        body["thread_ts"] = serde_json::Value::String(ts.to_string());
    }
    body
}

/// Text-only `chat.update` body (no `blocks`) — see `build_post_message_text_only`.
fn build_update_text_only(channel_id: &str, ts: &str, content: &str) -> serde_json::Value {
    serde_json::json!({
        "channel": channel_id,
        "ts": ts,
        "text": markdown_to_mrkdwn(content),
    })
}

/// Convert Markdown (as output by Claude Code) to Slack mrkdwn format.
/// Used for the `text` fallback field that accompanies Block Kit blocks
/// (shown in notification previews and to assistive tech).
fn markdown_to_mrkdwn(text: &str) -> String {
    static BOLD_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\*\*(.+?)\*\*").unwrap());
    static ITALIC_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\*([^*]+?)\*").unwrap());
    static LINK_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap());
    static HEADING_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?m)^#{1,6}\s+(.+)$").unwrap());
    static CODE_BLOCK_LANG_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"```\w+\n").unwrap());

    // Order: bold first (** → placeholder), then italic (* → _), then restore bold
    let text = BOLD_RE.replace_all(text, "\x01$1\x02"); // **bold** → \x01bold\x02
    let text = ITALIC_RE.replace_all(&text, "_${1}_"); // *italic* → _italic_
                                                       // Restore bold: \x01bold\x02 → *bold*
    let text = text.replace(['\x01', '\x02'], "*");
    let text = LINK_RE.replace_all(&text, "<$2|$1>"); // [text](url) → <url|text>
    let text = HEADING_RE.replace_all(&text, "*$1*"); // # heading → *heading*
    let text = CODE_BLOCK_LANG_RE.replace_all(&text, "```\n"); // ```rust → ```
    text.into_owned()
}

fn build_start_stream_body(channel: &str, thread_ts: &str, user_id: &str, team_id: &str) -> serde_json::Value {
    serde_json::json!({
        "channel": channel,
        "thread_ts": thread_ts,
        "recipient_user_id": user_id,
        "recipient_team_id": team_id,
    })
}

fn build_append_stream_body(channel: &str, ts: &str, delta: &str) -> serde_json::Value {
    serde_json::json!({
        "channel": channel,
        "ts": ts,
        "markdown_text": delta,
    })
}

fn build_set_status_body(channel_id: &str, thread_ts: &str, status: &str) -> serde_json::Value {
    serde_json::json!({
        "channel_id": channel_id,
        "thread_ts": thread_ts,
        "status": status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- builder tests ---

    #[test]
    fn build_start_stream_body_has_recipient() {
        let b = build_start_stream_body("C1", "1700.1", "U2", "T3");
        assert_eq!(b["channel"], "C1");
        assert_eq!(b["thread_ts"], "1700.1");
        assert_eq!(b["recipient_user_id"], "U2");
        assert_eq!(b["recipient_team_id"], "T3");
    }

    #[test]
    fn build_append_stream_body_is_markdown_text_chunk() {
        let b = build_append_stream_body("C1", "1700.9", "hello");
        assert_eq!(b["channel"], "C1");
        assert_eq!(b["ts"], "1700.9");
        assert_eq!(b["markdown_text"], "hello");
    }

    #[test]
    fn build_set_status_body_shape() {
        let b = build_set_status_body("C1", "1700.1", "Thinking\u{2026}");
        assert_eq!(b["channel_id"], "C1");
        assert_eq!(b["thread_ts"], "1700.1");
        assert_eq!(b["status"], "Thinking\u{2026}");
    }

    #[tokio::test]
    async fn degraded_stream_append_accumulates() {
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            std::time::Duration::from_secs(60),
            AllowBots::Off,
            true,
            HashSet::new(),
            HashSet::new(),
            None,
            true,
        );
        adapter.streams.lock().await.insert(
            "TS".into(),
            StreamEntry { active: false, degraded_buf: String::new() },
        );
        assert_eq!(adapter.accumulate_degraded("TS", "a").await.as_deref(), Some("a"));
        assert_eq!(adapter.accumulate_degraded("TS", "b").await.as_deref(), Some("ab"));
        // missing stream is not resurrected:
        assert_eq!(adapter.accumulate_degraded("MISSING", "x").await, None);
    }
    use crate::adapter::ChatAdapter;

    /// Bot's own `<@UID>` trigger mention is stripped.
    #[test]
    fn resolve_mentions_strips_bot_mention() {
        let out = resolve_slack_mentions("<@U1BOT> hello", Some("U1BOT"));
        assert_eq!(out, "hello");
    }

    /// Other users' mentions are preserved so the LLM can address them back —
    /// this is the core fix: the old `strip_slack_mention` wiped all `<@...>`.
    #[test]
    fn resolve_mentions_preserves_other_user_mentions() {
        let out = resolve_slack_mentions("<@U1BOT> say hi to <@U2ALICE>", Some("U1BOT"));
        assert_eq!(out, "say hi to <@U2ALICE>");
    }

    /// Multiple occurrences of the bot mention all get stripped.
    #[test]
    fn resolve_mentions_strips_repeated_bot_mentions() {
        let out = resolve_slack_mentions("<@U1BOT> ping <@U1BOT>", Some("U1BOT"));
        assert_eq!(out, "ping");
    }

    /// When the bot UID is unknown, fall back to preserving the text
    /// (safer than stripping all user mentions).
    #[test]
    fn resolve_mentions_unknown_bot_preserves_all() {
        let out = resolve_slack_mentions("<@U1BOT> hi <@U2ALICE>", None);
        assert_eq!(out, "<@U1BOT> hi <@U2ALICE>");
    }

    /// Labelled form of another user's mention (`<@UID|handle>`) is preserved.
    #[test]
    fn resolve_mentions_preserves_labelled_other_user_mention() {
        let out = resolve_slack_mentions("<@U1BOT> say hi to <@U2ALICE|alice>", Some("U1BOT"));
        assert_eq!(out, "say hi to <@U2ALICE|alice>");
    }

    /// Labelled form `<@UID|handle>` is stripped the same as bare form.
    #[test]
    fn resolve_mentions_strips_labelled_bot_mention() {
        let out = resolve_slack_mentions("<@U1BOT|my-bot> hello", Some("U1BOT"));
        assert_eq!(out, "hello");
    }

    /// Labelled form mid-sentence is stripped and surrounding text preserved.
    #[test]
    fn resolve_mentions_strips_labelled_mid_sentence() {
        let out = resolve_slack_mentions("please ask <@U1BOT|handle> to run", Some("U1BOT"));
        assert_eq!(out, "please ask  to run");
    }

    /// Mixed bare and labelled forms of the same UID in one string are both stripped.
    #[test]
    fn resolve_mentions_strips_mixed_bare_and_labelled() {
        let out = resolve_slack_mentions("<@U1BOT> and <@U1BOT|handle> run", Some("U1BOT"));
        assert_eq!(out, "and  run");
    }

    /// Malformed unclosed `<@UID|label` (no closing `>`) is preserved verbatim.
    #[test]
    fn resolve_mentions_malformed_unclosed_label_preserved() {
        let out = resolve_slack_mentions("ask <@U1BOT|nolabel to run", Some("U1BOT"));
        assert!(out.contains("<@U1BOT"));
    }

    #[test]
    fn resolve_mentions_preserves_longer_uid_prefix() {
        let out = resolve_slack_mentions("<@U1BOTX> hello", Some("U1BOT"));
        assert_eq!(out, "<@U1BOTX> hello");
    }

    // --- text_mentions_uid tests ---

    #[test]
    fn mentions_uid_bare_form() {
        assert!(text_mentions_uid("<@U123BOT> hello", "U123BOT"));
    }

    #[test]
    fn mentions_uid_labelled_form() {
        assert!(text_mentions_uid("<@U123BOT|my-bot> hello", "U123BOT"));
    }

    #[test]
    fn mentions_uid_labelled_form_mid_sentence() {
        assert!(text_mentions_uid("please ask <@U123BOT|handle> to run", "U123BOT"));
    }

    #[test]
    fn mentions_uid_no_match() {
        assert!(!text_mentions_uid("hello world", "U123BOT"));
    }

    #[test]
    fn mentions_uid_no_false_positive_on_uid_prefix() {
        assert!(!text_mentions_uid("<@U123BOT> hello", "U123"));
    }

    #[test]
    fn mentions_uid_second_mention_matches() {
        assert!(text_mentions_uid("<@U999OTHER> and <@U123BOT>", "U123BOT"));
    }

    #[test]
    fn mentions_uid_empty_label_form() {
        assert!(text_mentions_uid("<@U123BOT|> hello", "U123BOT"));
    }

    #[test]
    fn mentions_uid_truncated_no_closing_delimiter() {
        assert!(!text_mentions_uid("<@U123BOT", "U123BOT"));
    }

    // --- is_plain_user_message tests (regression for openabdev/openab#497 parity) ---

    /// Empty message text never counts as a user message (regardless of subtype).
    #[test]
    fn empty_text_is_not_plain_user_message() {
        assert!(!is_plain_user_message("", ""));
        assert!(!is_plain_user_message("me_message", ""));
    }

    /// No subtype + non-empty text = plain user message (the common case).
    #[test]
    fn no_subtype_nonempty_text_is_plain_user_message() {
        assert!(is_plain_user_message("", "hello"));
    }

    /// Whitelisted subtypes with non-empty text are user messages.
    #[test]
    fn whitelisted_subtypes_are_plain_user_messages() {
        assert!(is_plain_user_message("me_message", "waves"));
        assert!(is_plain_user_message("thread_broadcast", "see channel"));
        assert!(is_plain_user_message("file_share", "caption"));
    }

    /// System-ish subtypes (even from real users) are NOT user messages —
    /// resetting the counter on them would let bot-to-bot loops re-arm.
    #[test]
    fn system_subtypes_are_not_plain_user_messages() {
        for subtype in [
            "pinned_item",
            "unpinned_item",
            "channel_name",
            "channel_archive",
            "channel_unarchive",
            "group_join",
            "group_leave",
            "group_topic",
            "group_purpose",
            "reminder_add",
            "tombstone",
        ] {
            assert!(
                !is_plain_user_message(subtype, "some text"),
                "subtype {subtype} must not count as a user message",
            );
        }
    }

    // --- slack_file_download_url tests ---

    /// Prefers url_private_download when both fields are present —
    /// that endpoint always streams raw bytes even for browser-previewed types.
    #[test]
    fn slack_file_url_prefers_download_variant() {
        let file = serde_json::json!({
            "url_private_download": "https://files.slack.com/.../download/log.txt",
            "url_private":          "https://files.slack.com/.../preview/log.txt",
        });
        assert_eq!(
            slack_file_download_url(&file),
            "https://files.slack.com/.../download/log.txt",
        );
    }

    /// Falls back to url_private when url_private_download is absent.
    #[test]
    fn slack_file_url_falls_back_to_private() {
        let file = serde_json::json!({
            "url_private": "https://files.slack.com/.../log.txt",
        });
        assert_eq!(
            slack_file_download_url(&file),
            "https://files.slack.com/.../log.txt",
        );
    }

    /// Externally-backed files with no private URL return empty — caller skips.
    #[test]
    fn slack_file_url_empty_for_external_only() {
        let file = serde_json::json!({
            "external_type": "gdrive",
            "permalink": "https://docs.google.com/...",
        });
        assert_eq!(slack_file_download_url(&file), "");
    }

    // --- sanitize_slack_filename tests ---

    #[test]
    fn sanitize_leaves_normal_filename_unchanged() {
        assert_eq!(sanitize_slack_filename("photo.png"), "photo.png");
        assert_eq!(sanitize_slack_filename("my file (1).jpg"), "my file (1).jpg");
    }

    #[test]
    fn sanitize_replaces_backtick() {
        assert_eq!(sanitize_slack_filename("file`name.png"), "file'name.png");
    }

    #[test]
    fn sanitize_replaces_angle_brackets() {
        // Angle brackets are Slack mrkdwn delimiters; they must not pass through.
        assert_eq!(sanitize_slack_filename("<@U123>"), "(@U123)");
        assert_eq!(sanitize_slack_filename("<!here>"), "(!here)");
    }

    #[test]
    fn sanitize_combined_injection_attempt() {
        // A filename constructed to inject a Slack @here ping.
        assert_eq!(
            sanitize_slack_filename("`<!here>`"),
            "'(!here)'"
        );
    }

    #[test]
    fn sanitize_escapes_ampersand_before_angle_brackets() {
        // Slack mrkdwn decodes HTML entities before markup parsing.
        // "&lt;@here&gt;" would round-trip back to "<@here>" and trigger a mention
        // ping if & is not escaped. The & must be escaped first so downstream
        // Slack entity decoding cannot reconstruct a mrkdwn delimiter.
        assert_eq!(sanitize_slack_filename("&lt;@here&gt;"), "&amp;lt;@here&amp;gt;");
        assert_eq!(sanitize_slack_filename("file&name.png"), "file&amp;name.png");
    }

    // --- strip_mime_params tests ---

    /// MIME with charset parameter strips to bare media type.
    #[test]
    fn strip_mime_params_removes_charset() {
        assert_eq!(strip_mime_params("text/plain; charset=utf-8"), "text/plain");
    }

    /// Bare MIME is unchanged.
    #[test]
    fn strip_mime_params_bare_unchanged() {
        assert_eq!(strip_mime_params("image/png"), "image/png");
    }

    /// Empty input is unchanged.
    #[test]
    fn strip_mime_params_empty() {
        assert_eq!(strip_mime_params(""), "");
    }

    /// Surrounding whitespace is trimmed.
    #[test]
    fn strip_mime_params_trims_whitespace() {
        assert_eq!(strip_mime_params("  text/plain  "), "text/plain");
    }

    // --- bot_id_matches_trusted tests ---

    #[test]
    fn trusted_bot_ids_accepts_raw_slack_bot_id() {
        let trusted = HashSet::from(["B123BOT".to_string()]);
        assert!(bot_id_matches_trusted(&trusted, "B123BOT", None));
    }

    #[test]
    fn trusted_bot_ids_accepts_resolved_bot_user_id() {
        let trusted = HashSet::from(["U123BOT".to_string()]);
        assert!(bot_id_matches_trusted(
            &trusted,
            "B123BOT",
            Some("U123BOT")
        ));
    }

    #[test]
    fn trusted_bot_ids_rejects_unknown_bot_when_resolution_fails() {
        let trusted = HashSet::from(["U123BOT".to_string()]);
        assert!(!bot_id_matches_trusted(&trusted, "B999BOT", None));
    }

    #[test]
    fn trusted_bot_ids_rejects_empty_event_bot_id() {
        let trusted = HashSet::from(["".to_string()]);
        assert!(!bot_id_matches_trusted(&trusted, "", None));
    }

    /// Per-thread streaming: ON by default, OFF when another bot is present (#534).
    /// Single-bot deployment: streaming enabled in config, no trusted peers.
    #[test]
    fn streaming_per_thread() {
        let ttl = std::time::Duration::from_secs(300);
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Mentions,
            true,
            HashSet::new(),
            HashSet::new(),
            None,
            true,
        );

        assert!(
            adapter.use_streaming(false),
            "should stream when no other bot"
        );
        assert!(
            !adapter.use_streaming(true),
            "should NOT stream when other bot present"
        );
    }

    /// (B) `[slack].streaming = false` disables streaming outright, regardless
    /// of thread state.
    #[test]
    fn streaming_config_master_switch_off() {
        let ttl = std::time::Duration::from_secs(300);
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Mentions,
            false,
            HashSet::new(),
            HashSet::new(),
            None,
            true,
        );

        assert!(!adapter.use_streaming(false), "streaming=false must win even with no other bot");
        assert!(!adapter.use_streaming(true));
    }

    /// (A) A deployment with trusted peer bots configured never streams — even
    /// before any peer bot has posted in the thread (the race the trait doc
    /// admits). This is what stops a streamed "@peer" mention from being eaten
    /// by the message_changed skip.
    #[test]
    fn streaming_off_when_trusted_bots_configured() {
        let ttl = std::time::Duration::from_secs(300);
        let trusted = HashSet::from(["U0B6FQF0GTD".to_string()]);
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Mentions,
            true,
            trusted,
            HashSet::new(),
            None,
            true,
        );

        assert!(
            !adapter.use_streaming(false),
            "multi-bot deployment must not stream even when no peer has posted yet"
        );
        assert!(!adapter.use_streaming(true));
    }

    // --- extract_file_send_markers tests (added 2026-05-26 with line-anchoring fix) ---

    #[test]
    fn marker_extracts_anchored_single_line() {
        let input = "Here is the file:\n<<openab-send-file /tmp/report.pdf>>\nHope it helps.";
        let (stripped, paths) = extract_file_send_markers(input).expect("marker should fire");
        assert_eq!(paths, vec!["/tmp/report.pdf".to_string()]);
        // Marker line is removed entirely; surrounding text joins directly.
        assert_eq!(stripped, "Here is the file:\nHope it helps.");
    }

    #[test]
    fn marker_extracts_multiple_anchored_lines() {
        let input = "Files:\n<<openab-send-file /a.txt>>\n<<openab-send-file /b.txt>>\nDone.";
        let (stripped, paths) = extract_file_send_markers(input).expect("markers should fire");
        assert_eq!(paths, vec!["/a.txt".to_string(), "/b.txt".to_string()]);
        assert_eq!(stripped, "Files:\nDone.");
    }

    #[test]
    fn marker_inline_is_not_extracted() {
        // Self-trigger regression: agent quotes the marker mid-sentence.
        // Should be preserved verbatim; no upload attempted.
        let input = "The marker syntax is `<<openab-send-file /abs/path>>` you write in chat.";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn marker_indented_still_anchored() {
        // Whitespace-only padding on the marker line should still anchor.
        let input = "Result:\n    <<openab-send-file /tmp/x.png>>   \nDone.";
        let (stripped, paths) = extract_file_send_markers(input).expect("marker should fire");
        assert_eq!(paths, vec!["/tmp/x.png".to_string()]);
        assert_eq!(stripped, "Result:\nDone.");
    }

    #[test]
    fn marker_at_bof_and_eof() {
        // Marker as very first / last line should still fire.
        let bof = "<<openab-send-file /a>>\ntrailing";
        let (s1, p1) = extract_file_send_markers(bof).expect("BOF marker should fire");
        assert_eq!(p1, vec!["/a".to_string()]);
        assert_eq!(s1, "trailing");

        let eof = "leading\n<<openab-send-file /b>>";
        let (s2, p2) = extract_file_send_markers(eof).expect("EOF marker should fire");
        assert_eq!(p2, vec!["/b".to_string()]);
        assert_eq!(s2, "leading");
    }

    #[test]
    fn marker_with_trailing_text_on_same_line_is_ignored() {
        // Marker followed by non-whitespace on the same line → not anchored,
        // preserved as literal.
        let input = "<<openab-send-file /a.txt>> caption text";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn marker_self_trigger_regression_quoted_source_code() {
        // Regression for 2026-05-26 incident: Tifa read slack.rs and quoted
        // the marker prefix verbatim in a Slack reply. Old impl tried to
        // upload garbage as a file. New impl preserves it as literal.
        let input = "OpenAB marker syntax `<<openab-send-file ` and suffix `>>` plus other words like `<<openab-relay-to discord:swat-team>>` describe the design.";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn marker_empty_path_drops_line_without_upload() {
        // Defensive: anchored marker with no path between prefix/suffix
        // shouldn't produce a phantom upload entry.
        let input = "Before\n<<openab-send-file >>\nAfter";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[tokio::test]
    async fn assistant_mode_gates_status_and_native_streaming() {
        let ttl = std::time::Duration::from_secs(60);
        // assistant_mode=true → status API on; native streaming on (no other bot),
        // off when another bot is present; post+edit streaming on regardless.
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Off,
            true,
            HashSet::new(),
            HashSet::new(),
            None,
            true,
        );
        assert!(adapter.uses_assistant_status(), "assistant_mode enables status API");
        assert!(adapter.use_streaming(false), "post+edit streaming on when no other bot");
        assert!(adapter.uses_native_streaming(false), "native streaming on when no other bot");
        assert!(!adapter.uses_native_streaming(true), "other bot present disables native");
        // assistant_mode=false → no status API, no native streaming; post+edit still streams.
        let adapter2 = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Off,
            true,
            HashSet::new(),
            HashSet::new(),
            None,
            false,
        );
        assert!(!adapter2.uses_assistant_status());
        assert!(adapter2.use_streaming(false), "post+edit streaming independent of assistant_mode");
        assert!(!adapter2.uses_native_streaming(false), "native streaming requires assistant_mode");
    }

    /// chat.postMessage body carries Block Kit `markdown` blocks with the raw
    /// Markdown preserved (NOT downgraded), plus a `text` fallback and thread_ts.
    #[test]
    fn post_message_body_uses_raw_markdown_blocks() {
        let b = build_post_message_body("C1", Some("1700.1"), "## Heading\n- item");
        assert_eq!(b["channel"], "C1");
        assert_eq!(b["thread_ts"], "1700.1");
        assert_eq!(b["blocks"][0]["type"], "markdown");
        // Raw markdown preserved — heading is NOT flattened to `*Heading*`.
        assert_eq!(b["blocks"][0]["text"], "## Heading\n- item");
        assert!(b["text"].is_string(), "text fallback present for a11y/notifs");
    }

    /// thread_ts is omitted (top-level post) when the channel has no thread.
    #[test]
    fn post_message_body_omits_thread_ts_when_none() {
        let b = build_post_message_body("C1", None, "hi");
        assert!(b.get("thread_ts").is_none());
    }

    /// chat.update body also uses Block Kit `markdown` blocks with raw markdown.
    #[test]
    fn update_body_uses_raw_markdown_blocks() {
        let b = build_update_body("C1", "1700.9", "**bold**");
        assert_eq!(b["channel"], "C1");
        assert_eq!(b["ts"], "1700.9");
        assert_eq!(b["blocks"][0]["type"], "markdown");
        assert_eq!(b["blocks"][0]["text"], "**bold**");
    }

    /// Content over the per-block cap (11,900) splits into multiple markdown
    /// blocks, each within the limit. Assert on char count — `split_message`
    /// enforces `chars().count() <= limit`, not byte length.
    #[test]
    fn long_content_splits_into_multiple_markdown_blocks() {
        let big = "lorem ipsum dolor\n".repeat(1000); // > MARKDOWN_BLOCK_LIMIT
        assert!(big.chars().count() > MARKDOWN_BLOCK_LIMIT);
        let blocks = build_markdown_blocks(&big);
        assert!(blocks.len() >= 2, "should split into multiple blocks");
        for blk in &blocks {
            assert_eq!(blk["type"], "markdown");
            assert!(blk["text"].as_str().unwrap().chars().count() <= MARKDOWN_BLOCK_LIMIT);
        }
    }

    /// Regression for the long-table split: a Markdown table that overflows the
    /// old 4000 limit but fits the new 11,900 message_limit must stay in a single
    /// chunk, so it isn't split mid-table into raw pipe text.
    #[test]
    fn typical_long_table_stays_in_one_chunk() {
        let ttl = std::time::Duration::from_secs(300);
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Mentions,
            true,
            HashSet::new(),
            HashSet::new(),
            None,
            true,
        );
        let limit = adapter.message_limit();
        assert_eq!(limit, MARKDOWN_BLOCK_LIMIT);
        let mut table = String::from("| col a | col b | col c |\n|---|---|---|\n");
        for i in 0..150 {
            table.push_str(&format!("| row {i} aaaa | bbbb {i} | cccc {i} |\n"));
        }
        assert!(table.chars().count() > 4000, "table must exceed old limit");
        assert!(table.chars().count() < limit, "but fit the new one");
        assert_eq!(
            crate::format::split_message(&table, limit).len(),
            1,
            "table within message_limit must not be split mid-table"
        );
    }

    /// Text-only fallback bodies carry `text` and no `blocks` — used when a
    /// workspace rejects the Block Kit markdown block.
    #[test]
    fn text_only_fallback_bodies_have_no_blocks() {
        let post = build_post_message_text_only("C1", Some("1700.1"), "## H\n- x");
        assert!(post.get("blocks").is_none());
        assert!(post["text"].is_string());
        assert_eq!(post["thread_ts"], "1700.1");
        let upd = build_update_text_only("C1", "1700.9", "**b**");
        assert!(upd.get("blocks").is_none());
        assert!(upd["text"].is_string());
    }

    /// Error classifier matches `invalid_blocks` (malformed/unsupported blocks)
    /// and `msg_blocks_too_long` (over the cumulative block cap) → degrade to
    /// text. `invalid_arguments` is a Slack catch-all and must NOT trigger a
    /// pointless text-only retry; unrelated errors are ignored too.
    #[test]
    fn detects_block_payload_rejected_errors() {
        assert!(is_block_payload_rejected(&anyhow!(
            "Slack API chat.postMessage: invalid_blocks"
        )));
        assert!(
            is_block_payload_rejected(&anyhow!("Slack API chat.postMessage: msg_blocks_too_long")),
            "oversize block payload should degrade to text-only"
        );
        assert!(
            !is_block_payload_rejected(&anyhow!("Slack API chat.update: invalid_arguments")),
            "invalid_arguments is a catch-all, not a block-rejection signal"
        );
        assert!(!is_block_payload_rejected(&anyhow!(
            "Slack API chat.postMessage: channel_not_found"
        )));
        // Exact error-code match, not substring: a future code that merely
        // contains `invalid_blocks` must NOT trigger a text-only retry.
        assert!(
            !is_block_payload_rejected(&anyhow!("Slack API chat.postMessage: invalid_blocks_field")),
            "must match the error code exactly, not as a substring"
        );
    }

    /// Slack opts into native table rendering (Block Kit markdown / markdown_text
    /// stream chunks), so the router skips the table→code-block conversion.
    #[test]
    fn slack_renders_native_tables() {
        let ttl = std::time::Duration::from_secs(300);
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Mentions,
            true,
            HashSet::new(),
            HashSet::new(),
            None,
            true,
        );
        assert!(adapter.renders_native_tables());
    }
}
