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
    /// Dedup set for file uploads — keyed on `{channel}|{thread_ts}|{path}`.
    /// Streaming edit_message can fire repeatedly during a single agent turn,
    /// each potentially containing the file-send marker. Without dedup we'd
    /// re-upload the same file dozens of times. Cleared once per session restart.
    file_upload_cache: tokio::sync::Mutex<HashSet<String>>,
}

impl SlackAdapter {
    pub fn new(
        bot_token: String,
        session_ttl: std::time::Duration,
        _allow_bot_messages: AllowBots,
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
            file_upload_cache: tokio::sync::Mutex::new(HashSet::new()),
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
        4000
    }

    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
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

        // Standard path — no markers, just text.
        self.send_plain_text(channel, content).await
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

        let mrkdwn = markdown_to_mrkdwn(content);
        self.api_post(
            "chat.update",
            serde_json::json!({
                "channel": msg.channel.channel_id,
                "ts": msg.message_id,
                "text": mrkdwn,
            }),
        )
        .await?;
        Ok(())
    }

    fn use_streaming(&self, other_bot_present: bool) -> bool {
        !other_bot_present
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
    allowed_channels: HashSet<String>,
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

        let ws_url = match get_socket_mode_url(&app_token).await {
            Ok(url) => url,
            Err(e) => {
                error!("failed to get Socket Mode URL: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        info!(url = %ws_url, "connecting to Slack Socket Mode");

        match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                info!("Slack Socket Mode connected");
                let (mut write, mut read) = ws_stream.split();

                loop {
                    tokio::select! {
                        msg_result = read.next() => {
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
                                                let allowed_channels = allowed_channels.clone();
                                                let allowed_users = allowed_users.clone();
                                                let stt_config = stt_config.clone();
                                                let dispatcher = dispatcher.clone();
                                                tokio::spawn(async move {
                                                    handle_message(
                                                        &event,
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
                                                                    || allowed_channels.contains(channel_id);
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

                                                // Skip messages that @mention the bot — app_mention handles those
                                                // (except in DMs where app_mention doesn't fire)
                                                if mentions_bot && !is_dm { continue; }

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
                                                        }
                                                    }
                                                }

                                                // Dispatch to handle_message (per-thread serialization comes
                                                // from Dispatcher consumer task in batched mode and from
                                                // pool.with_connection in per-message mode).
                                                let event = event.clone();
                                                let adapter = adapter.clone();
                                                let bot_token = bot_token.clone();
                                                let allowed_channels = allowed_channels.clone();
                                                let allowed_users = allowed_users.clone();
                                                let stt_config = stt_config.clone();
                                                let dispatcher = dispatcher.clone();
                                                tokio::spawn(async move {
                                                    handle_message(
                                                        &event,
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
            Err(e) => {
                error!("failed to connect to Slack Socket Mode: {e}");
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

    // Check allowed channels
    if !allow_all_channels && !allowed_channels.contains(&channel_id) {
        return;
    }

    // Check allowed users — skip for bot messages (they go through trusted_bot_ids instead)
    if !is_bot_msg && !allow_all_users && !allowed_users.contains(&user_id) {
        tracing::info!(user_id, "denied Slack user, ignoring");
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
        let _ = adapter.add_reaction(&msg_ref, "🚫").await;
        return;
    }

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
    let mut echo_entries: Vec<crate::stt::EchoEntry> = Vec::new();
    let mut text_file_bytes: u64 = 0;
    let mut text_file_count: u32 = 0;

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
                if let Some(block) = media::download_and_encode_image(
                    url,
                    Some(mimetype),
                    filename,
                    size,
                    Some(bot_token),
                ).await {
                    debug!(filename, "adding image attachment");
                    extra_blocks.push(block);
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

/// Strip MIME parameters like `; charset=utf-8` so type-detection helpers see
/// the bare media type. Slack occasionally sends mimetypes like
/// `text/plain; charset=utf-8`; `media::is_text_file` expects the bare form.
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

fn strip_mime_params(mimetype: &str) -> &str {
    mimetype.split(';').next().unwrap_or(mimetype).trim()
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

/// Convert Markdown (as output by Claude Code) to Slack mrkdwn format.
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

#[cfg(test)]
mod tests {
    use super::*;
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
    #[test]
    fn streaming_per_thread() {
        let ttl = std::time::Duration::from_secs(300);
        let adapter = SlackAdapter::new("xoxb-test".into(), ttl, AllowBots::Mentions);

        assert!(
            adapter.use_streaming(false),
            "should stream when no other bot"
        );
        assert!(
            !adapter.use_streaming(true),
            "should NOT stream when other bot present"
        );
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
}
