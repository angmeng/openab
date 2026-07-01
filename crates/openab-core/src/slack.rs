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

const SLACK_API: &str = "https://slack.com/api";

/// Marker syntax for outbound file attachments in agent text output.
/// The agent writes `<<openab-send-file /abs/path/to/file>>` in its response,
/// OpenAB intercepts before posting and uploads the file via Slack's files API.
///
/// IMPORTANT: do NOT use colons in this marker (`:something:` would collide
/// with Slack's emoji shortcode parser and get rendered as a gray-box
/// placeholder even before our interceptor runs). The prefix/suffix and the
/// line-anchoring rules MUST stay in sync with `relay::strip_file_send_markers`,
/// which neutralizes the same marker on relay (peer-dispatch) bodies.
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

/// Marker syntax for setting a channel's description (Slack calls it the channel
/// "purpose") — e.g. the S6 `Branches:` block that a PM bot mirrors into the ticket
/// channel. Line-anchored like the others. `channel=` is optional (defaults to the
/// channel the reply is posted in); `text="…"` is the purpose, with literal `\n`
/// decoded to real newlines for multi-line blocks. Needs `channels:manage` /
/// `groups:write` (same scope the create path uses).
/// `<<openab-set-purpose [channel=C123] text="Branches:\n  be: …\n  fe: …">>`
const SET_PURPOSE_MARKER_PREFIX: &str = "<<openab-set-purpose ";
const SET_PURPOSE_MARKER_SUFFIX: &str = ">>";

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

/// Max inbound message IDs retained for dispatch dedup (see `event_dedup`).
const EVENT_DEDUP_MAX: usize = 2048;

/// Bounded FIFO set: O(1) contains/insert, evicts the oldest key past `cap`.
struct FifoSet {
    cap: usize,
    set: HashSet<String>,
    order: std::collections::VecDeque<String>,
}
impl FifoSet {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            set: HashSet::with_capacity(cap),
            order: std::collections::VecDeque::with_capacity(cap),
        }
    }
    /// True if `key` was already present (a duplicate). Inserts when new,
    /// evicting the oldest entry once past `cap`.
    fn check_and_insert(&mut self, key: String) -> bool {
        if self.set.contains(&key) {
            return true;
        }
        if self.order.len() >= self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        self.set.insert(key.clone());
        self.order.push_back(key);
        false
    }
}

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
    /// Persistent disk cache for multibot thread detection (survives restarts).
    multibot_cache: crate::multibot_cache::MultibotCache,
    /// TTL for participation cache entries (matches session_ttl_hours from config).
    session_ttl: std::time::Duration,
    /// Assistant mode: stream via chat.startStream + assistant.threads.setStatus.
    assistant_mode: bool,
    /// Master streaming switch. When false, the adapter always posts a single
    /// final message (send-once): no native streaming, no post+edit placeholder.
    /// Default true. Set false to avoid streamed-message edit states (e.g. a
    /// reply that @-mentions another bot re-firing app_mention in multi-agent
    /// threads). Mirrors `[gateway] streaming`.
    streaming: bool,
    /// Trusted peer-bot IDs (from `[slack].trusted_bot_ids`). Used by
    /// `use_streaming` / `uses_native_streaming` to force send-once whenever ANY
    /// trusted bot is configured: a streamed reply reaches peer bots only as
    /// `message_changed` events (which every bot's handler skips), so a streamed
    /// @mention of a peer bot never triggers it. This is race-free, unlike the
    /// runtime `other_bot_present` cache, which is false when a peer bot is
    /// addressed before it has posted in the thread.
    trusted_bot_ids: HashSet<String>,
    /// streaming message ts → state. active=false = degraded (post+edit fallback).
    /// Lifecycle: stream_begin inserts, stream_finish removes; insert_stream
    /// bounds the map (STREAM_CACHE_MAX) as a safety net against aborted turns.
    streams: tokio::sync::Mutex<HashMap<String, StreamEntry>>,
    /// Inbound-event dedup, keyed on `{channel}|{ts}`. Slack delivers BOTH an
    /// `app_mention` AND a `message` event for the same message when a (trusted)
    /// peer bot — or a human — @mentions us; both reach the unified dispatch
    /// body, so without this the message is dispatched twice → a duplicate,
    /// separately-generated reply (observed 2026-06-22: thread …424059, two
    /// `session/prompt`s, same message_id, 27s apart). Makes dispatch idempotent
    /// per message; also absorbs Socket-Mode envelope redelivery. FIFO-bounded;
    /// entries only need to outlive the sub-second gap between the paired events.
    event_dedup: tokio::sync::Mutex<FifoSet>,
    /// Dedup set for file uploads — keyed on `{channel}|{thread_ts}|{path}`.
    /// The streaming path calls `edit_message` repeatedly with the final reply,
    /// each potentially containing the file-send marker. Without dedup we'd
    /// re-upload the same file dozens of times. Cleared once per session restart.
    file_upload_cache: tokio::sync::Mutex<HashSet<String>>,
    /// Channel allowlist (from `[slack].allowed_channels`), shared mutable so a
    /// channel the bot CREATES (via the `<<openab-create-channel>>` marker) or is
    /// INVITED into can be added at runtime — otherwise the bot is deaf in its
    /// own ticket channels until the next restart (observed 2026-06-04: @mention
    /// in a freshly-created `at-2043-…` channel was dropped at the gate). The gate
    /// reads a snapshot per message; `allow_channel_now` inserts on create/invite.
    /// `allow_all_channels` (separate flag) still bypasses this entirely.
    allowed_channels: Arc<tokio::sync::RwLock<HashSet<String>>>,
    /// Path to the on-disk config file, so runtime allowlist additions (a channel
    /// the bot is invited into, or creates) can be PERSISTED — otherwise they're
    /// lost on restart. None when the config came from a URL (can't write back).
    config_path: Option<std::path::PathBuf>,
}

impl SlackAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bot_token: String,
        session_ttl: std::time::Duration,
        _allow_bot_messages: AllowBots,
        assistant_mode: bool,
        multibot_cache: crate::multibot_cache::MultibotCache,
        streaming: bool,
        trusted_bot_ids: HashSet<String>,
        allowed_channels: HashSet<String>,
        config_path: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            // Bound every Slack Web API call; an unbounded inline gating call in the
            // read loop could otherwise stall the Socket Mode idle-timeout watchdog.
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            bot_token,
            bot_user_id: tokio::sync::OnceCell::new(),
            user_cache: tokio::sync::Mutex::new(HashMap::new()),
            bot_id_cache: tokio::sync::Mutex::new(HashMap::new()),
            participated_threads: tokio::sync::Mutex::new(HashMap::new()),
            multibot_threads: tokio::sync::Mutex::new(HashMap::new()),
            multibot_cache,
            session_ttl,
            assistant_mode,
            streaming,
            trusted_bot_ids,
            streams: tokio::sync::Mutex::new(HashMap::new()),
            event_dedup: tokio::sync::Mutex::new(FifoSet::new(EVENT_DEDUP_MAX)),
            file_upload_cache: tokio::sync::Mutex::new(HashSet::new()),
            allowed_channels: Arc::new(tokio::sync::RwLock::new(allowed_channels)),
            config_path,
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
        {
            let mut cache = self.multibot_threads.lock().await;
            cache
                .entry(thread_ts.to_string())
                .or_insert_with(tokio::time::Instant::now);
            enforce_cache_bounds(&mut cache, self.session_ttl);
        }
        // Persist to disk — multibot is irreversible
        self.multibot_cache.mark_multibot(thread_ts).await;
    }

    /// True if this `{channel}|{ts}` was already dispatched (duplicate event).
    /// Check-and-insert is atomic under the mutex; the Socket-Mode read loop
    /// processes envelopes sequentially, so the app_mention/message pair can't
    /// both pass. Empty `ts` (no id) is never suppressed.
    async fn already_dispatched(&self, channel_id: &str, ts: &str) -> bool {
        if ts.is_empty() {
            return false;
        }
        self.event_dedup
            .lock()
            .await
            .check_and_insert(format!("{channel_id}|{ts}"))
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

    /// Post a plain text message — the original `send_message` body, extracted
    /// so the file-send marker-aware paths can reuse it for captions and error
    /// notices. Uses mrkdwn (no Block Kit `markdown` block) so it can't trip the
    /// block-payload-rejected fallback dance — these are short, plain strings.
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
    ///   2. POST raw bytes to that upload_url
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

        // --- Step 2: POST raw bytes to the signed URL ---
        let mut file_bytes = Vec::with_capacity(size as usize);
        tokio::fs::File::open(&path_buf)
            .await?
            .read_to_end(&mut file_bytes)
            .await?;

        let step2_resp = self.client.post(&upload_url).body(file_bytes).send().await?;

        if !step2_resp.status().is_success() {
            let status = step2_resp.status();
            let body = step2_resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "upload POST failed: HTTP {status} — body: {}",
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

        let step3_resp = self
            .api_post("files.completeUploadExternal", complete_body)
            .await?;

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

    /// Set a channel's description (Slack calls this the channel "purpose") —
    /// e.g. the S6 `Branches:` block. Required bot scope: `channels:manage`
    /// (public) / `groups:write` (private) — the same scope the create path uses.
    /// `api_post` turns a non-`ok` Slack response into an `Err` carrying the code
    /// (e.g. `missing_scope`, `not_in_channel`), so the caller surfaces it.
    async fn set_channel_purpose_in_slack(&self, channel_id: &str, purpose: &str) -> Result<()> {
        self.api_post(
            "conversations.setPurpose",
            serde_json::json!({ "channel": channel_id, "purpose": purpose }),
        )
        .await?;
        info!(channel_id = %channel_id, "slack: channel purpose (description) set");
        Ok(())
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
        } || self.multibot_cache.is_multibot(thread_ts);

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
        // other_bot_present relies solely on early detection + disk cache;
        // no longer scanned from fetched messages (200-msg window was unreliable).
        let other_bot_present = cached_multibot;

        if involved {
            self.cache_participation(thread_ts).await;
        }

        (involved, other_bot_present)
    }

    /// Fetch a thread's messages and render them as a plain-text transcript for
    /// agent context. Reuses the same `conversations.replies` read path as
    /// `bot_participated_in_thread` — the token already has the scope. Returns
    /// `None` on API error or empty thread (fail-soft: missing context degrades
    /// the reply, it shouldn't drop the turn). The trigger message itself is
    /// excluded — it arrives via the normal prompt, so including it here would
    /// duplicate it. 2026-06-03: added for 2a (thread summarise), since OpenAB
    /// exposes no agent-callable read tool.
    async fn fetch_thread_context(
        &self,
        channel: &str,
        thread_ts: &str,
        trigger_ts: &str,
    ) -> Option<String> {
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

        // Set a channel's description (Slack "purpose") — e.g. the S6 Branches:
        // block a PM bot mirrors into the ticket channel. Stripped before posting
        // regardless of outcome; failure is surfaced (never silent).
        if let Some((residual, maybe_spec)) = extract_set_purpose_marker(content) {
            let residual = residual.trim();
            let Some(spec) = maybe_spec else {
                // Marker present but malformed — already stripped; post the residual
                // (or a note) so the raw marker never leaks to the channel.
                let body = if residual.is_empty() {
                    "⚠️ (ignored a malformed set-purpose marker)".to_string()
                } else {
                    residual.to_string()
                };
                return self.send_plain_text(channel, &body).await;
            };
            let current = channel.channel_id.as_str();
            let target = spec.channel.as_deref().unwrap_or(current);
            // Authorization: a marker may only set the CURRENT channel's description
            // or one already in the runtime allowlist (channels the bot operates in).
            // Without this, a prompt-injected marker could rewrite ANY channel the
            // bot token can manage (create-channel is stricter still — DM-only).
            let authorized =
                target == current || self.allowed_channels.read().await.contains(target);
            let body = if !authorized {
                let refuse =
                    format!("⚠️ set-purpose refused: `{target}` is not this channel or in the allowlist");
                if residual.is_empty() {
                    refuse
                } else {
                    format!("{residual}\n\n{refuse}")
                }
            } else {
                match self.set_channel_purpose_in_slack(target, &spec.purpose).await {
                    Ok(()) if residual.is_empty() => "📌 Channel description updated.".to_string(),
                    Ok(()) => residual.to_string(),
                    Err(e) => {
                        let err = format!("⚠️ Failed to set channel description: {e}");
                        if residual.is_empty() {
                            err
                        } else {
                            format!("{residual}\n\n{err}")
                        }
                    }
                }
            };
            return self.send_plain_text(channel, &body).await;
        }

        // Scan for file-send markers `<<openab-send-file PATH>>`. If found,
        // intercept: post the residual text (caption) first, then upload each
        // file via Slack's files API. Returns the MessageRef of the last action.
        if let Some((stripped_text, file_paths)) = extract_file_send_markers(content) {
            let mut last_msg: Option<MessageRef> = None;

            // Post the residual text first (if non-empty), so the file appears
            // AFTER any caption — matches the reading order users expect.
            let trimmed = stripped_text.trim();
            if !trimmed.is_empty() {
                last_msg = Some(self.send_plain_text(channel, trimmed).await?);
            }

            // Upload each file. Failure on one shouldn't block the rest — log
            // and surface the failure to the user, then continue.
            for path in &file_paths {
                match self.send_file_to_slack(channel, path).await {
                    Ok(msg_ref) => {
                        info!(path = %path, "slack: file uploaded");
                        last_msg = Some(msg_ref);
                    }
                    Err(e) => {
                        error!(path = %path, error = %e, "slack: file upload failed");
                        let err_text = format!(
                            "⚠️ Failed to send file `{}`: {}\n(See OpenAB logs for details.)",
                            path, e
                        );
                        last_msg = Some(self.send_plain_text(channel, &err_text).await?);
                    }
                }
            }

            // If we somehow had only markers and no surviving text, return a stub.
            return last_msg
                .ok_or_else(|| anyhow!("no message sent (empty after marker strip)"));
        }

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
        // Set-purpose marker on the STREAMING path. Slack defaults to
        // `streaming=true`, where the final reply is finalized via edit_message
        // (stream_finish → edit_message), NOT send_message — so the send_message
        // interception is bypassed there. Apply the side-effect and strip the
        // marker so it never displays, then fall through to the normal render with
        // the cleaned text. conversations.setPurpose is idempotent, so the repeated
        // edits of the degraded post+edit path are harmless; failure is logged here
        // (the send-once path in send_message is the one that surfaces it to the user).
        let cleaned: String;
        let content: &str =
            if let Some((residual, maybe_spec)) = extract_set_purpose_marker(content) {
                if let Some(spec) = maybe_spec {
                    let current = msg.channel.channel_id.as_str();
                    let target = spec.channel.as_deref().unwrap_or(current);
                    let authorized =
                        target == current || self.allowed_channels.read().await.contains(target);
                    if authorized {
                        if let Err(e) =
                            self.set_channel_purpose_in_slack(target, &spec.purpose).await
                        {
                            warn!(channel_id = %target, error = %e, "slack: set-purpose (streaming) failed");
                        }
                    } else {
                        warn!(channel_id = %target, "slack: set-purpose (streaming) refused — not current channel or allowlisted");
                    }
                }
                cleaned = if residual.trim().is_empty() {
                    "📌 Channel description updated.".to_string()
                } else {
                    residual
                };
                &cleaned
            } else {
                content
            };

        // Marker handling for the streaming path: OpenAB streams the final agent
        // response via repeated edit_message calls against a placeholder. If the
        // text contains file-send markers, strip them from the edit (so the
        // placeholder shows clean text) and trigger uploads as separate
        // follow-up messages in the same channel/thread.
        //
        // Idempotency: each file must upload ONCE per session, not on every
        // streaming edit. Tracked via `file_upload_cache` keyed on
        // (channel_id, thread_ts, path).
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

            // Upload each file as a follow-up, deduped via the cache.
            //
            // Race-free check-and-claim: insert into the cache FIRST under the
            // lock, then upload. If insertion returns false (key already
            // present), another edit_message call already claimed the slot —
            // skip. A check-then-act with the lock released between check and
            // insert would let two streaming edits within the upload-latency
            // window both pass the check and double-upload.
            for path in &file_paths {
                let cache_key = format!(
                    "{}|{}|{}",
                    msg.channel.channel_id,
                    msg.channel.thread_id.as_deref().unwrap_or(""),
                    path
                );
                let claimed = {
                    let mut cache = self.file_upload_cache.lock().await;
                    cache.insert(cache_key.clone()) // true if newly inserted
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
        // If any trusted peer bots are configured this is a multi-bot
        // deployment: force send-once regardless of thread state. A streamed
        // reply reaches peer bots only as `message_changed` events (which every
        // bot's handler skips), so a streamed @mention of a peer bot never
        // triggers it. Race-free, unlike `other_bot_present`, which is false
        // when a peer bot is addressed before it has posted in the thread.
        self.streaming && self.trusted_bot_ids.is_empty() && !other_bot_present
    }

    fn renders_native_tables(&self) -> bool {
        true
    }

    fn uses_assistant_status(&self) -> bool {
        self.assistant_mode
    }

    fn uses_native_streaming(&self, other_bot_present: bool) -> bool {
        // Same multi-bot guard as `use_streaming`: never stream (native either)
        // when trusted peer bots are configured — a streamed @mention reaches
        // them only as `message_changed` events, which their handlers skip.
        let native = self.streaming
            && self.assistant_mode
            && self.trusted_bot_ids.is_empty()
            && !other_bot_present;
        debug!(
            streaming = self.streaming,
            assistant_mode = self.assistant_mode,
            trusted_bots = !self.trusted_bot_ids.is_empty(),
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

/// Socket Mode keepalive. Slack's inbound WebSocket can go half-open (e.g. a NAT
/// idle-timeout silently drops inbound frames with no Close/FIN), which leaves
/// `read.next()` blocked forever, so the reconnect loop never fires and the bot
/// goes deaf while still showing as connected. We proactively ping and force a
/// reconnect when no inbound frame (including Slack's own pings) has arrived
/// within the idle window. Reconnect backoff mirrors the gateway adapter.
const PING_INTERVAL_SECS: u64 = 30;
const IDLE_TIMEOUT_SECS: u64 = 75;
const MAX_BACKOFF_SECS: u64 = 30;

/// Next reconnect delay: double, capped. Reset to 1 on a successful connect.
fn next_backoff(cur: u64) -> u64 {
    (cur * 2).min(MAX_BACKOFF_SECS)
}

/// The socket is considered dead (half-open) when no inbound frame has arrived
/// within `timeout`; Slack sends periodic pings, so silence past the window
/// means the inbound path is gone.
fn socket_idle(since_last_inbound: std::time::Duration, timeout: std::time::Duration) -> bool {
    since_last_inbound >= timeout
}

/// Run the Slack adapter using Socket Mode (persistent WebSocket, no public URL needed).
/// Reconnects automatically on disconnect.
#[allow(clippy::too_many_arguments)]
pub async fn run_slack_adapter(
    adapter: Arc<SlackAdapter>,
    app_token: String,
    allow_all_channels: bool,
    allow_all_users: bool,
    allowed_users: HashSet<String>,
    dm_allowed_users: HashSet<String>,
    invite_allowed_users: HashSet<String>,
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
    // Warm the bot-user-id cache once so the per-message path never does the
    // cold-cache `auth.test` inline in the read loop.
    let _ = adapter.get_bot_user_id().await;
    let mut backoff_secs = 1u64;

    loop {
        // Check for shutdown before (re)connecting
        if *shutdown_rx.borrow() {
            info!("Slack adapter shutting down");
            return Ok(());
        }

        let ws_url = match get_socket_mode_url(&app_token).await {
            Ok(url) => url,
            Err(e) => {
                error!(err = %e, backoff = backoff_secs, "failed to get Socket Mode URL, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                    _ = shutdown_rx.changed() => { return Ok(()); }
                }
                backoff_secs = next_backoff(backoff_secs);
                continue;
            }
        };
        info!(url = %ws_url, "connecting to Slack Socket Mode");

        // Bound the WebSocket handshake for the same reason as the HTTP call
        // above — `connect_async` can otherwise hang indefinitely on a half-up
        // network. On timeout, warn and fall through to the backoff retry below
        // so the bridge keeps trying until the network returns (ac1fa7b).
        match tokio::time::timeout(
            std::time::Duration::from_secs(20),
            tokio_tungstenite::connect_async(&ws_url),
        )
        .await
        {
            Ok(Ok((ws_stream, _))) => {
                info!("Slack Socket Mode connected");
                backoff_secs = 1; // reset on success
                let (mut write, mut read) = ws_stream.split();
                let mut ping_interval =
                    tokio::time::interval(std::time::Duration::from_secs(PING_INTERVAL_SECS));
                ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut last_inbound = std::time::Instant::now();

                loop {
                    tokio::select! {
                        msg_result = read.next() => {
                            last_inbound = std::time::Instant::now();
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
                                            // Unified path: `app_mention` and `message` are two deliveries
                                            // of the same logical inbound message. Slack sends BOTH for a
                                            // (trusted) peer-bot / human @mention. Routing them through one
                                            // gating + dispatch body (with `event_dedup` collapsing the pair
                                            // to a single dispatch) makes loop-protection authoritative and
                                            // preserves the coverage app_mention alone provides (invite-by-
                                            // mention, app_mention-only subscriptions, DMs).
                                            "app_mention" | "message" => {
                                                let is_app_mention = event_type == "app_mention";
                                                let channel_id = event["channel"].as_str().unwrap_or("");
                                                let has_thread = event["thread_ts"].is_string();
                                                let is_bot = event["bot_id"].is_string()
                                                    || event["subtype"].as_str() == Some("bot_message");
                                                let subtype = event["subtype"].as_str().unwrap_or("");
                                                let msg_text = event["text"].as_str().unwrap_or("");
                                                let bot_uid_opt = adapter.get_bot_user_id().await.map(|s| s.to_string());
                                                // app_mention proves the mention regardless of bot-id
                                                // resolution; a text-parsed mention covers peer-bot mentions
                                                // Slack delivers only as `message`. Either delivery honours an
                                                // explicit address.
                                                let mentions_bot = is_app_mention
                                                    || bot_uid_opt
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
                                                    is_app_mention,
                                                    text = msg_text,
                                                    "inbound event received"
                                                );

                                                // Bot invited into an existing channel: Slack emits a
                                                // `channel_join` message whose `user` is the joiner. When that's
                                                // the bot itself, self-heal the allowlist (runtime + persist) so
                                                // the bot can hear that channel without a manual config edit +
                                                // restart (2026-06-04 — symmetric with the create-channel path).
                                                // Still falls through to skip_subtype below (no agent dispatch for
                                                // a join notice).
                                                //
                                                // invite_allowed_users gate (2026-07-01): if set, only self-heal
                                                // when the INVITER is authorised (owner-only invites). Otherwise the
                                                // bot was dragged in by someone it doesn't take orders from — stay a
                                                // silent, non-listening member. Empty list = accept any invite.
                                                if subtype == "channel_join"
                                                    && bot_uid_opt.as_deref().is_some()
                                                    && event_user_id == bot_uid_opt.as_deref()
                                                {
                                                    let inviter = event["inviter"].as_str();
                                                    if invite_accepted(&invite_allowed_users, inviter) {
                                                        adapter.allow_channel_now(channel_id, "invited").await;
                                                    } else {
                                                        info!(
                                                            channel_id,
                                                            inviter = inviter.unwrap_or("<none>"),
                                                            "slack: ignoring channel invite from unauthorised inviter \
                                                             (invite_allowed_users set); bot stays silent here"
                                                        );
                                                    }
                                                }

                                                // Skip non-message subtypes
                                                let skip_subtype = matches!(subtype,
                                                    "message_changed" | "message_deleted" |
                                                    "channel_join" | "channel_leave" |
                                                    "channel_topic" | "channel_purpose"
                                                );
                                                if skip_subtype { continue; }

                                                // Idempotency — claim (channel, ts) HERE, before any
                                                // side-effecting step (multibot note, bot-turn count) or
                                                // gating decision. The Socket-Mode loop is sequential, so the
                                                // first of the app_mention/message pair to arrive wins and runs
                                                // the pipeline exactly once; the partner (and Slack redeliveries)
                                                // bail here. Claiming early is what makes the bot-turn STOP
                                                // authoritative for the whole pair (a turn-limit `continue` no
                                                // longer leaves the partner free to dispatch). See event_dedup.
                                                if adapter
                                                    .already_dispatched(
                                                        channel_id,
                                                        event["ts"].as_str().unwrap_or(""),
                                                    )
                                                    .await
                                                {
                                                    debug!(
                                                        ts = event["ts"].as_str().unwrap_or(""),
                                                        "duplicate inbound event (app_mention+message pair or redelivery) — skipping"
                                                    );
                                                    continue;
                                                }

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
                                                // Classify under the lock (order-sensitive, kept in the read
                                                // loop), but run any warning send AFTER releasing it; holding
                                                // the tracker mutex across `chat.postMessage` would stall turn
                                                // tracking for every thread, not just this one.
                                                let turn_action = {
                                                    let mut tracker = bot_turns.lock().await;
                                                    if is_bot {
                                                        tracker.classify_bot_message(&turn_key)
                                                    } else {
                                                        if is_plain_user_message(subtype, msg_text) {
                                                            tracker.on_human_message(&turn_key);
                                                        }
                                                        TurnAction::Continue
                                                    }
                                                };
                                                if is_bot {
                                                    // Diagnostic for the "peer-bot @mention never triggers me" class
                                                    // of report. NOTE: this is the LIMITER verdict only — it fires
                                                    // BEFORE the trusted_bot_ids / allow_bot_messages gauntlet below,
                                                    // so `turn_action=Continue` here does NOT mean accepted. The
                                                    // gauntlet logs its own "DROP: ..." line, and a fully-accepted
                                                    // message logs "slack bot mention ACCEPTED -> dispatching".
                                                    info!(
                                                        channel_id,
                                                        turn_key = %turn_key,
                                                        is_app_mention,
                                                        mentions_bot,
                                                        bot_uid_ok = bot_uid_opt.is_some(),
                                                        event_bot_id = event["bot_id"].as_str().unwrap_or(""),
                                                        ?turn_action,
                                                        ?allow_bot_messages,
                                                        "slack bot inbound: limiter verdict (pre-gating, NOT yet accepted)"
                                                    );
                                                }
                                                match turn_action {
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
                                                            let adapter = adapter.clone();
                                                            tokio::spawn(async move {
                                                                if let Err(e) = adapter.send_message(&warn_channel, &user_message).await {
                                                                    warn!(error = %e, "failed to send bot turn limit warning");
                                                                }
                                                            });
                                                        }
                                                        continue;
                                                    }
                                                }

                                                // Ignore own bot messages (after counting toward turns)
                                                if is_own_bot_msg { continue; }

                                                // (No mention-defer here anymore. Previously the message arm
                                                // `continue`d on a human @mention to let the separate app_mention
                                                // arm handle it; now both deliveries share THIS body and the
                                                // early event_dedup claim collapses the pair to one dispatch, so
                                                // an @mention must fall through to gating like any other message.)

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
                                                            info!(
                                                                event_bot_id,
                                                                trusted_bot_ids = ?trusted_bot_ids,
                                                                "DROP: bot not in trusted_bot_ids (peer-bot mention rejected HERE). \
                                                                 Add this event_bot_id (B…) or the bot's user-id (U…) to \
                                                                 trusted_bot_ids, or set trusted_bot_ids=[] to trust all bots."
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                    // Bot messages must be in a thread (top-level bot posts are
                                                    // loop chatter, not addressed to us) — UNLESS they explicitly
                                                    // mention us. A top-level @mention is a deliberate address,
                                                    // delivered as app_mention or (for peer bots) as a text
                                                    // mention on the message event; both set mentions_bot.
                                                    if !has_thread && !mentions_bot { continue; }
                                                }

                                                // --- User message gating ---
                                                if !is_bot {
                                                    if is_dm {
                                                        // DM: implicit mention — always process
                                                    } else if mentions_bot {
                                                        // Explicit @mention — a deliberate address; always
                                                        // answer regardless of allow_user_messages mode.
                                                        // Preserves the pre-unification behaviour where the
                                                        // app_mention arm dispatched mentions unconditionally
                                                        // (involvement/multibot gates apply only to NON-mention
                                                        // messages, handled by the match below).
                                                    } else {
                                                        match allow_user_messages {
                                                            AllowUsers::Mentions => {
                                                                // Reached only when !mentions_bot (the mentions
                                                                // case is handled by the branch above), so a
                                                                // non-mention message is dropped.
                                                                continue;
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
                                                                // (mentions_bot is handled by the branch above, so
                                                                // here a non-owner has not mentioned the bot.)
                                                                let is_owner = event_user_id
                                                                    .is_some_and(|u| allowed_users.contains(u));
                                                                if !is_owner {
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
                                                let allowed_users = allowed_users.clone();
                                                let dm_allowed_users = dm_allowed_users.clone();
                                                let stt_config = stt_config.clone();
                                                let dispatcher = dispatcher.clone();
                                                if is_bot {
                                                    info!(
                                                        channel_id,
                                                        event_bot_id = event["bot_id"].as_str().unwrap_or(""),
                                                        "slack bot mention ACCEPTED -> dispatching"
                                                    );
                                                }
                                                tokio::spawn(async move {
                                                    handle_message(
                                                        &event,
                                                        &team_id,
                                                        &adapter,
                                                        &bot_token,
                                                        allow_all_channels,
                                                        allow_all_users,
                                                        &allowed_users,
                                                        &dm_allowed_users,
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
                        _ = ping_interval.tick() => {
                            if socket_idle(
                                last_inbound.elapsed(),
                                std::time::Duration::from_secs(IDLE_TIMEOUT_SECS),
                            ) {
                                warn!(
                                    idle_secs = last_inbound.elapsed().as_secs(),
                                    "Slack Socket Mode idle past timeout (likely half-open), forcing reconnect"
                                );
                                break;
                            }
                            if let Err(e) = write.send(tungstenite::Message::Ping(Vec::new())).await {
                                warn!(error = %e, "Slack Socket Mode ping failed, reconnecting");
                                break;
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
                error!(err = %e, backoff = backoff_secs, "failed to connect to Slack Socket Mode, retrying");
            }
            Err(_) => {
                warn!(backoff = backoff_secs, "connect_async timed out after 20s, retrying");
            }
        }

        warn!(backoff = backoff_secs, "reconnecting to Slack Socket Mode");
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
            _ = shutdown_rx.changed() => { return Ok(()); }
        }
        backoff_secs = next_backoff(backoff_secs);
    }
}

/// Call apps.connections.open to get a WebSocket URL for Socket Mode.
async fn get_socket_mode_url(app_token: &str) -> Result<String> {
    // Bound the HTTP call. A bare `reqwest::Client::new()` has no default
    // timeout, so a hung TCP connect (e.g. waking from laptop sleep onto a
    // not-yet-ready network) would block the reconnect loop forever — process
    // alive, last log line "connecting…", no "connected", silent (ac1fa7b).
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(20))
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
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

/// Decide whether a human sender is denied by the Slack user allow-lists,
/// applying the per-surface DM override. Pure so the security gate is unit-tested
/// (see tests below) and can't silently regress.
///
/// - In a DM with a non-empty `dm_allowed_users`: gate strictly against that list
///   (owner-only DMs), ignoring `allow_all_users` and `allowed_users`.
/// - Otherwise (channel, or DM with no override): the historical rule —
///   allowed unless `allow_all_users` is false AND the sender isn't in `allowed_users`.
fn slack_user_denied(
    is_dm: bool,
    allow_all_users: bool,
    allowed_users: &HashSet<String>,
    dm_allowed_users: &HashSet<String>,
    user_id: &str,
) -> bool {
    if is_dm && !dm_allowed_users.is_empty() {
        !dm_allowed_users.contains(user_id)
    } else {
        !allow_all_users && !allowed_users.contains(user_id)
    }
}

/// Decide whether a channel invite should be accepted (bot starts listening).
/// Pure + unit-tested. Empty `invite_allowed_users` = accept any invite (prior
/// behaviour). Non-empty = accept only if the inviter is known AND in the list.
/// An absent inviter (Slack omitted the field) with a non-empty list is REJECTED
/// — fail closed, since we can't verify the invite came from an authorised user.
fn invite_accepted(invite_allowed_users: &HashSet<String>, inviter: Option<&str>) -> bool {
    invite_allowed_users.is_empty()
        || inviter.is_some_and(|inv| invite_allowed_users.contains(inv))
}

#[allow(clippy::too_many_arguments)]
async fn handle_message(
    event: &serde_json::Value,
    team_id: &str,
    adapter: &Arc<SlackAdapter>,
    bot_token: &str,
    allow_all_channels: bool,
    allow_all_users: bool,
    allowed_users: &HashSet<String>,
    dm_allowed_users: &HashSet<String>,
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
    // Read the (runtime-mutable) allowlist snapshot here — picks up channels the
    // bot just created or was invited into (see `allow_channel_now`).
    if !is_dm
        && !allow_all_channels
        && !adapter.allowed_channels.read().await.contains(&channel_id)
    {
        return;
    }

    // Check allowed users — skip for bot messages (they go through trusted_bot_ids instead).
    // Per-surface gate: in DMs, a non-empty `dm_allowed_users` REPLACES `allowed_users`
    // (and ignores allow_all_users) so the owner can lock DMs to themselves while the
    // full team keeps channel @mention access. Empty dm list → DMs fall back to the
    // channel list, preserving prior behaviour.
    if !is_bot_msg {
        let denied = slack_user_denied(
            is_dm,
            allow_all_users,
            allowed_users,
            dm_allowed_users,
            &user_id,
        );
        if denied {
            // 2026-06-03 (洺哥): silently ignore denied users — no 🚫 reaction. The
            // reaction marked non-allowlisted senders in shared channels, but it
            // surfaces the bot's presence to people it won't talk to, which reads as
            // rude. Log-only denial is quieter; the user simply gets no response.
            tracing::info!(user_id, is_dm, "denied Slack user, ignoring (no reaction)");
            return;
        }
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
        if let Some(ctx) = adapter
            .fetch_thread_context(&channel_id, thread_ts, &ts)
            .await
        {
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
            } else {
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
                    Err(media::MediaFetchError::NotAnImage) => {
                        // Non-image binary (video, PDF, Office docs, archives,
                        // generic binary): download to disk so the agent gets a
                        // local path it can hand to ffmpeg / pdftotext / etc.
                        // If the disk write fails, inject a failure-notice block
                        // so the agent still knows the file existed (fail loudly,
                        // don't silently drop).
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
                                let notice = format!(
                                    "[Slack file attachment — download failed]\n\
                                     - filename: {filename}\n\
                                     - mimetype: {mimetype}\n\
                                     - size: {size} bytes\n\
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
    } || thread_channel
        .thread_id
        .as_deref()
        .is_some_and(|ts| adapter.multibot_cache.is_multibot(ts));

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

struct SetPurposeSpec {
    /// Target channel id. `None` = the channel the reply is posted in.
    channel: Option<String>,
    /// Purpose text, with `\n`/`\t` already decoded to real whitespace.
    purpose: String,
}

/// Parse outbound text for a single set-purpose marker
/// `<<openab-set-purpose [channel=C123] text="…">>`. Line-anchored like the
/// create-channel marker (must occupy its own trimmed line). Returns the residual
/// text (marker line stripped) plus the spec.
fn extract_set_purpose_marker(content: &str) -> Option<(String, Option<SetPurposeSpec>)> {
    if !content.contains(SET_PURPOSE_MARKER_PREFIX) {
        return None;
    }

    let mut saw_marker = false;
    let mut spec: Option<SetPurposeSpec> = None;
    let mut kept_lines: Vec<&str> = Vec::new();

    for line in content.split('\n') {
        let trimmed = line.trim();
        if !saw_marker
            && trimmed.starts_with(SET_PURPOSE_MARKER_PREFIX)
            && trimmed.ends_with(SET_PURPOSE_MARKER_SUFFIX)
        {
            saw_marker = true;
            let inner = trimmed[SET_PURPOSE_MARKER_PREFIX.len()
                ..trimmed.len() - SET_PURPOSE_MARKER_SUFFIX.len()]
                .trim();
            // parse may fail (empty/invalid text) → spec stays None, but the line is
            // stripped either way so the raw marker never leaks to the channel.
            spec = parse_set_purpose_args(inner);
            continue;
        }
        kept_lines.push(line);
    }

    // Outer Some = "a marker line was seen and stripped"; inner Option = the parsed
    // spec (None if malformed → caller just posts the stripped residual).
    saw_marker.then(|| (kept_lines.join("\n"), spec))
}

/// Parse the inner args of a set-purpose marker. Grammar:
///   text="<free text, \n = newline>"   (required)
///   channel=<C-id>                      (optional; default = current channel)
fn parse_set_purpose_args(inner: &str) -> Option<SetPurposeSpec> {
    // Pull out text="..." first (it may contain spaces/decoded newlines), then
    // parse the rest as whitespace-separated tokens.
    let mut rest = inner.to_string();
    let mut purpose: Option<String> = None;
    if let Some(start) = rest.find("text=\"") {
        let after = start + "text=\"".len();
        if let Some(end_rel) = rest[after..].find('"') {
            let t = &rest[after..after + end_rel];
            if !t.trim().is_empty() {
                // decode literal \n / \t so multi-line blocks (Branches:) render
                purpose = Some(t.replace("\\n", "\n").replace("\\t", "\t"));
            }
            let end = after + end_rel + 1;
            rest.replace_range(start..end, " ");
        }
    }

    let mut channel: Option<String> = None;
    for tok in rest.split_whitespace() {
        if let Some(v) = tok.strip_prefix("channel=") {
            let v = v.trim().trim_start_matches('#');
            if !v.is_empty() {
                channel = Some(v.to_string());
            }
        }
    }

    purpose.map(|purpose| SetPurposeSpec { channel, purpose })
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

/// Parse outbound text for file-send markers `<<openab-send-file PATH>>`.
/// Returns `Some((text_without_markers, paths))` if at least one marker line is
/// found, `None` otherwise (fast-path).
///
/// **Line-anchored**: the marker must occupy a line on its own (after trimming
/// whitespace). Inline occurrences inside running text are preserved as literal
/// — this prevents the agent from self-triggering when quoting its own source
/// code or documentation that mentions the marker (2026-05-26 regression).
///
/// This is the slack-send counterpart to `relay::strip_file_send_markers`: the
/// relay helper only strips markers from peer-dispatch bodies (it returns no
/// paths, because relay targets never upload), whereas this also extracts the
/// paths to upload. Both use identical prefix/suffix and line-anchoring rules.
fn extract_file_send_markers(content: &str) -> Option<(String, Vec<String>)> {
    if !content.contains(FILE_SEND_MARKER_PREFIX) {
        return None;
    }

    let mut paths: Vec<String> = Vec::new();
    let mut kept_lines: Vec<&str> = Vec::new();

    for line in content.split('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with(FILE_SEND_MARKER_PREFIX)
            && trimmed.ends_with(FILE_SEND_MARKER_SUFFIX)
        {
            // Verified marker line: extract path between prefix and suffix.
            let inner = &trimmed
                [FILE_SEND_MARKER_PREFIX.len()..trimmed.len() - FILE_SEND_MARKER_SUFFIX.len()];
            let path = inner.trim();
            if !path.is_empty() {
                paths.push(path.to_string());
            }
            // Either way, drop the marker line from output (don't leak an empty
            // `<<openab-send-file >>` to the user).
            continue;
        }
        // Not a marker line — preserve verbatim. Inline marker text is kept literal.
        kept_lines.push(line);
    }

    if paths.is_empty() {
        return None;
    }
    Some((kept_lines.join("\n"), paths))
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

    // --- user gate (per-surface DM override) ---

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn dm_override_locks_dm_to_owner_but_not_channel() {
        let team = set(&["OWNER", "MEMBER"]);
        let dm = set(&["OWNER"]);
        // DM: only the owner passes; a team member is denied.
        assert!(!slack_user_denied(true, false, &team, &dm, "OWNER"));
        assert!(slack_user_denied(true, false, &team, &dm, "MEMBER"));
        // Channel: the DM override does not apply — the full team list gates.
        assert!(!slack_user_denied(false, false, &team, &dm, "MEMBER"));
        assert!(slack_user_denied(false, false, &team, &dm, "STRANGER"));
    }

    #[test]
    fn empty_dm_override_falls_back_to_allowed_users() {
        let team = set(&["OWNER", "MEMBER"]);
        let dm = HashSet::new();
        // Both surfaces use allowed_users (prior behaviour) when dm list is empty.
        assert!(!slack_user_denied(true, false, &team, &dm, "MEMBER"));
        assert!(slack_user_denied(true, false, &team, &dm, "STRANGER"));
        assert!(!slack_user_denied(false, false, &team, &dm, "MEMBER"));
    }

    #[test]
    fn allow_all_users_bypasses_channel_but_dm_override_still_wins() {
        let empty = HashSet::new();
        let dm = set(&["OWNER"]);
        // allow_all_users opens the channel to anyone...
        assert!(!slack_user_denied(false, true, &empty, &empty, "ANYONE"));
        // ...but a DM override, being non-empty, still restricts DMs to the owner.
        assert!(slack_user_denied(true, true, &empty, &dm, "ANYONE"));
        assert!(!slack_user_denied(true, true, &empty, &dm, "OWNER"));
    }

    // --- channel invite gate ---

    #[test]
    fn empty_invite_list_accepts_any_invite() {
        let empty = HashSet::new();
        assert!(invite_accepted(&empty, Some("ANYONE")));
        assert!(invite_accepted(&empty, None)); // prior behaviour: no gate
    }

    #[test]
    fn non_empty_invite_list_is_owner_only() {
        let owner = set(&["OWNER"]);
        assert!(invite_accepted(&owner, Some("OWNER")));
        assert!(!invite_accepted(&owner, Some("TEAMMATE")));
        // fail closed: unknown/absent inviter is rejected when the gate is on
        assert!(!invite_accepted(&owner, None));
    }

    // --- set-purpose marker ---

    #[test]
    fn set_purpose_marker_parses_channel_and_decodes_newlines() {
        let content =
            "done\n<<openab-set-purpose channel=C123 text=\"Branches:\\n  be: AT-1-x-be\">>\nok";
        let (residual, spec) = extract_set_purpose_marker(content).expect("marker");
        let spec = spec.expect("valid spec");
        assert_eq!(residual, "done\nok");
        assert_eq!(spec.channel.as_deref(), Some("C123"));
        assert_eq!(spec.purpose, "Branches:\n  be: AT-1-x-be");
    }

    #[test]
    fn set_purpose_marker_channel_defaults_to_none() {
        let content = "<<openab-set-purpose text=\"hello\">>";
        let (residual, spec) = extract_set_purpose_marker(content).expect("marker");
        let spec = spec.expect("valid spec");
        assert_eq!(residual, "");
        assert!(spec.channel.is_none());
        assert_eq!(spec.purpose, "hello");
    }

    #[test]
    fn set_purpose_marker_absent_is_none_malformed_is_stripped_not_leaked() {
        // no marker at all → None
        assert!(extract_set_purpose_marker("no marker here").is_none());
        // marker present but empty text → seen+stripped (Some residual), spec None,
        // so the caller posts the residual and the raw marker never leaks (P3 fix).
        let (residual, spec) =
            extract_set_purpose_marker("before\n<<openab-set-purpose text=\"\">>\nafter")
                .expect("marker seen");
        assert_eq!(residual, "before\nafter");
        assert!(spec.is_none());
    }

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
        let adapter = SlackAdapter::new("xoxb-test".into(), std::time::Duration::from_secs(60), AllowBots::Off, true, crate::multibot_cache::MultibotCache::load("/dev/null".into()), true, HashSet::new(), HashSet::new(), None);
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

    // --- extract_file_send_markers tests (line-anchoring, 2026-05-26) ---

    #[test]
    fn marker_extracts_anchored_single_line() {
        let input = "Here is the file:\n<<openab-send-file /tmp/report.pdf>>\nHope it helps.";
        let (stripped, paths) = extract_file_send_markers(input).expect("marker should fire");
        assert_eq!(paths, vec!["/tmp/report.pdf".to_string()]);
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
        let input = "The marker syntax is `<<openab-send-file /abs/path>>` you write in chat.";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn marker_indented_still_anchored() {
        let input = "Result:\n    <<openab-send-file /tmp/x.png>>   \nDone.";
        let (stripped, paths) = extract_file_send_markers(input).expect("marker should fire");
        assert_eq!(paths, vec!["/tmp/x.png".to_string()]);
        assert_eq!(stripped, "Result:\nDone.");
    }

    #[test]
    fn marker_at_bof_and_eof() {
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
        let input = "<<openab-send-file /a.txt>> caption text";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn marker_self_trigger_regression_quoted_source_code() {
        // 2026-05-26 incident: an agent read slack.rs and quoted the marker
        // prefix verbatim in a reply. The line-anchor keeps it literal.
        let input = "OpenAB marker syntax `<<openab-send-file ` and suffix `>>` plus other words like `<<openab-relay-to discord:swat-team>>` describe the design.";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn marker_empty_path_drops_line_without_upload() {
        let input = "Before\n<<openab-send-file >>\nAfter";
        assert!(extract_file_send_markers(input).is_none());
    }

    #[test]
    fn share_message_ts_probes_public_and_private() {
        let public = serde_json::json!({
            "files": [{ "shares": { "public": { "C1": [{ "ts": "1700.1" }] } } }]
        });
        assert_eq!(
            extract_share_message_ts(&public, "C1"),
            Some("1700.1".to_string())
        );
        let private = serde_json::json!({
            "files": [{ "shares": { "private": { "C2": [{ "ts": "1700.2" }] } } }]
        });
        assert_eq!(
            extract_share_message_ts(&private, "C2"),
            Some("1700.2".to_string())
        );
        // Shape mismatch → None.
        let bad = serde_json::json!({ "files": [{ "shares": {} }] });
        assert_eq!(extract_share_message_ts(&bad, "C1"), None);
    }

    /// Per-thread streaming: ON by default, OFF when another bot is present (#534).
    #[test]
    fn streaming_per_thread() {
        let ttl = std::time::Duration::from_secs(300);
        let adapter = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Mentions, false, crate::multibot_cache::MultibotCache::load("/dev/null".into()), true, HashSet::new(), HashSet::new(), None);

        assert!(
            adapter.use_streaming(false),
            "should stream when no other bot"
        );
        assert!(
            !adapter.use_streaming(true),
            "should NOT stream when other bot present"
        );
    }

    /// Multi-bot guard: any configured trusted_bot_ids forces send-once
    /// regardless of thread state — a streamed reply reaches peer bots only as
    /// `message_changed` events (which their handlers skip), so a streamed
    /// @mention of a peer bot never triggers it. Race-free vs `other_bot_present`.
    #[test]
    fn streaming_disabled_when_trusted_bots_configured() {
        let ttl = std::time::Duration::from_secs(300);
        let trusted = HashSet::from(["U_PEER".to_string()]);
        // assistant_mode=true so we also exercise the native-streaming path.
        let adapter = SlackAdapter::new(
            "xoxb-test".into(),
            ttl,
            AllowBots::Mentions,
            true,
            crate::multibot_cache::MultibotCache::load("/dev/null".into()),
            true,
            trusted,
            HashSet::new(),
            None,
        );
        assert!(
            !adapter.use_streaming(false),
            "trusted_bot_ids set => send-once even with no other bot in-thread (race-free)"
        );
        assert!(
            !adapter.use_streaming(true),
            "trusted_bot_ids set => send-once when other bot present too"
        );
        assert!(
            !adapter.uses_native_streaming(false),
            "trusted_bot_ids set => native streaming off even alone"
        );
    }

    #[tokio::test]
    async fn assistant_mode_gates_status_and_native_streaming() {
        let ttl = std::time::Duration::from_secs(60);
        // assistant_mode=true → status API on; native streaming on (no other bot),
        // off when another bot is present; post+edit streaming on regardless.
        let adapter = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Off, true, crate::multibot_cache::MultibotCache::load("/dev/null".into()), true, HashSet::new(), HashSet::new(), None);
        assert!(adapter.uses_assistant_status(), "assistant_mode enables status API");
        assert!(adapter.use_streaming(false), "post+edit streaming on when no other bot");
        assert!(adapter.uses_native_streaming(false), "native streaming on when no other bot");
        assert!(!adapter.uses_native_streaming(true), "other bot present disables native");
        // assistant_mode=false → no status API, no native streaming; post+edit still streams.
        let adapter2 = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Off, false, crate::multibot_cache::MultibotCache::load("/dev/null".into()), true, HashSet::new(), HashSet::new(), None);
        assert!(!adapter2.uses_assistant_status());
        assert!(adapter2.use_streaming(false), "post+edit streaming independent of assistant_mode");
        assert!(!adapter2.uses_native_streaming(false), "native streaming requires assistant_mode");

        // streaming=false → send-once: neither post+edit nor native, even alone.
        let adapter3 = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Off, true, crate::multibot_cache::MultibotCache::load("/dev/null".into()), false, HashSet::new(), HashSet::new(), None);
        assert!(!adapter3.use_streaming(false), "streaming=false forces send-once (no post+edit)");
        assert!(!adapter3.uses_native_streaming(false), "streaming=false disables native even with assistant_mode");
        assert!(adapter3.uses_assistant_status(), "streaming switch does not affect assistant status API");
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
        let adapter = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Mentions, true, crate::multibot_cache::MultibotCache::load("/dev/null".into()), true, HashSet::new(), HashSet::new(), None);
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
        let adapter = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Mentions, true, crate::multibot_cache::MultibotCache::load("/dev/null".into()), true, HashSet::new(), HashSet::new(), None);
        assert!(adapter.renders_native_tables());
    }
}

#[cfg(test)]
mod socket_keepalive_tests {
    use super::{next_backoff, socket_idle, IDLE_TIMEOUT_SECS, MAX_BACKOFF_SECS};
    use std::time::Duration;

    /// Backoff doubles and caps, matching the gateway adapter (1,2,4,8,16,30,30…).
    #[test]
    fn backoff_doubles_then_caps() {
        let mut b = 1u64;
        let seq: Vec<u64> = (0..8)
            .map(|_| {
                let cur = b;
                b = next_backoff(b);
                cur
            })
            .collect();
        assert_eq!(seq, vec![1, 2, 4, 8, 16, MAX_BACKOFF_SECS, MAX_BACKOFF_SECS, MAX_BACKOFF_SECS]);
        assert_eq!(next_backoff(MAX_BACKOFF_SECS), MAX_BACKOFF_SECS);
    }

    /// A half-open socket (no inbound past the window) is detected; an active one
    /// (recent inbound, e.g. a Slack ping) is not. This is the deaf-socket guard.
    #[test]
    fn idle_detects_half_open_at_boundary() {
        let timeout = Duration::from_secs(IDLE_TIMEOUT_SECS);
        assert!(!socket_idle(Duration::from_secs(0), timeout));
        assert!(!socket_idle(Duration::from_secs(IDLE_TIMEOUT_SECS - 1), timeout));
        assert!(socket_idle(Duration::from_secs(IDLE_TIMEOUT_SECS), timeout));
        assert!(socket_idle(Duration::from_secs(IDLE_TIMEOUT_SECS + 10), timeout));
    }
}
