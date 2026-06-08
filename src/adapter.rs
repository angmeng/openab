use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::borrow::Cow;
use std::sync::Arc;
use tracing::{error, warn};

use crate::acp::{classify_notification, AcpEvent, ContentBlock, SessionPool};
use crate::config::{ReactionsConfig, ToolDisplay};
use crate::error_display::{format_coded_error, format_user_error};
use crate::format;
use crate::markdown::{self, TableMode};
use crate::reactions::StatusReactionController;
use crate::relay::{self, RelayContext, RelayDirective};

// --- Output directive parsing ---

/// Parsed directives from agent output header block.
/// Consecutive `[[key:value]]` lines at the start of output are directives.
#[derive(Default, Debug)]
pub struct OutputDirectives {
    /// Message ID to reply to (Discord: message_reference)
    pub reply_to: Option<String>,
}

/// Parse `[[key:value]]` directives from the beginning of agent output.
/// Returns parsed directives and the remaining content (directives stripped).
pub fn parse_output_directives(content: &str) -> (OutputDirectives, String) {
    let mut directives = OutputDirectives::default();
    let mut content_start = 0;
    let mut trailing_content: Option<&str> = None;

    for line in content.lines() {
        let trimmed = line.trim();
        // Try to match [[key:value]] at the start of the line (lenient: allows trailing content)
        if let Some(after_open) = trimmed.strip_prefix("[[") {
            if let Some(close_pos) = after_open.find("]]") {
                let inner = &after_open[..close_pos];
                if let Some((key, value)) = inner.split_once(':') {
                    match key.trim() {
                        "reply_to" => {
                            let v = value.trim();
                            // Validate: non-empty, reasonable length, no whitespace/control chars
                            if !v.is_empty() && v.len() <= 64 && v.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_') {
                                directives.reply_to = Some(v.to_string());
                            }
                        }
                        _ => {
                            tracing::debug!(key = key.trim(), "unknown output directive ignored");
                        }
                    }
                    // Check for trailing content after ]]
                    let remainder = after_open[close_pos + 2..].trim();
                    if !remainder.is_empty() {
                        trailing_content = Some(remainder);
                        // Advance past this line
                        content_start += line.len();
                        if content.as_bytes().get(content_start) == Some(&b'\r') {
                            content_start += 1;
                        }
                        if content.as_bytes().get(content_start) == Some(&b'\n') {
                            content_start += 1;
                        }
                        break; // Trailing content ends directive header
                    }
                    // Advance past this line + its line ending (handles both \n and \r\n)
                    content_start += line.len();
                    if content.as_bytes().get(content_start) == Some(&b'\r') {
                        content_start += 1;
                    }
                    if content.as_bytes().get(content_start) == Some(&b'\n') {
                        content_start += 1;
                    }
                } else {
                    // [[X]] without colon — not a directive, stop parsing
                    break;
                }
            } else {
                // No closing ]] found — not a directive, stop parsing
                break;
            }
        } else {
            break;
        }
    }

    let remaining = if let Some(trailing) = trailing_content {
        if content_start < content.len() {
            format!("{}\n{}", trailing, &content[content_start..])
        } else {
            trailing.to_string()
        }
    } else if content_start < content.len() {
        content[content_start..].to_string()
    } else {
        String::new()
    };
    (directives, remaining)
}

// --- Platform-agnostic types ---

/// Identifies a channel or thread across platforms.
///
/// Used for **routing**: `channel_id` is the ID the adapter sends messages to.
/// For Discord threads, this is the thread's own channel ID (Discord API
/// requires it for `say`/`edit`). Use `parent_id` to find the parent channel.
///
/// Compare with `SenderContext`, which is **metadata for the agent**: there
/// `channel_id` is the parent channel and `thread_id` is the thread,
/// matching Slack's model for cross-platform consistency.
#[derive(Clone, Debug)]
pub struct ChannelRef {
    pub platform: String,
    pub channel_id: String,
    /// Thread within a channel (e.g. Slack thread_ts, Telegram topic_id).
    /// For Discord, threads are separate channels so this is None.
    pub thread_id: Option<String>,
    /// Parent channel if this is a thread-as-channel (Discord).
    pub parent_id: Option<String>,
    /// Originating gateway event ID, propagated back in `GatewayReply.reply_to`
    /// so the gateway can correlate replies with inbound events (e.g. LINE reply tokens).
    /// Excluded from Hash/Eq — two ChannelRefs pointing to the same channel are
    /// equal regardless of which event they originated from.
    pub origin_event_id: Option<String>,
}

impl PartialEq for ChannelRef {
    fn eq(&self, other: &Self) -> bool {
        self.platform == other.platform
            && self.channel_id == other.channel_id
            && self.thread_id == other.thread_id
            && self.parent_id == other.parent_id
    }
}

impl Eq for ChannelRef {}

impl std::hash::Hash for ChannelRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.platform.hash(state);
        self.channel_id.hash(state);
        self.thread_id.hash(state);
        self.parent_id.hash(state);
    }
}

/// Identifies a message across platforms.
#[derive(Clone, Debug)]
pub struct MessageRef {
    pub channel: ChannelRef,
    pub message_id: String,
}

/// Bundles per-message parameters for `AdapterRouter::handle_message`.
///
/// Introduced to reduce parameter count and make the signature extensible
/// (e.g. streaming policy, rate limit hints) without breaking call sites.
pub struct MessageContext {
    pub thread_channel: ChannelRef,
    pub sender_json: String,
    pub prompt: String,
    pub extra_blocks: Vec<ContentBlock>,
    pub trigger_msg: MessageRef,
    pub other_bot_present: bool,
}

/// Sender identity injected into prompts for downstream agent context.
///
/// This is **metadata for the agent** — `channel_id` always refers to the
/// logical parent channel, and `thread_id` identifies the thread (if any).
/// This convention is consistent across platforms (Slack, Discord, Telegram).
///
/// Compare with `ChannelRef`, which is used for **routing**: there
/// `channel_id` is the ID the adapter sends messages to (for Discord
/// threads, that's the thread's own channel ID, not the parent).
#[derive(Clone, Debug, Serialize)]
pub struct SenderContext {
    pub schema: String,
    pub sender_id: String,
    pub sender_name: String,
    pub display_name: String,
    pub channel: String,
    pub channel_id: String,
    /// Thread identifier, if the message is inside a thread.
    /// Slack: thread_ts. Discord: thread channel ID (channel_id holds the parent).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub is_bot: bool,
    /// Platform message creation time (ISO 8601 UTC), if available.
    /// Discord/Slack: platform timestamp. Gateway: broker receive time (best-effort).
    /// Additive optional field — schema version stays openab.sender.v1 (no consumer
    /// breakage). If future additions require breaking changes, bump to v1.1+.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    /// Platform message ID. Agents can use this to reply to a specific message
    /// via the `[[reply_to:<message_id>]]` output directive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// The platform user ID of the receiving bot/agent.
    /// Enables agents to identify themselves when multiple agents share the same backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receiver_id: Option<String>,
}

// --- ChatAdapter trait ---

#[async_trait]
pub trait ChatAdapter: Send + Sync + 'static {
    /// Platform name for logging and session key namespacing.
    fn platform(&self) -> &'static str;

    /// Maximum message length for this platform (e.g. 2000 for Discord, 4000 for Slack).
    fn message_limit(&self) -> usize;

    /// Send a new message, returns a reference to the sent message.
    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef>;

    /// Create a thread from a trigger message, returns the thread channel ref.
    async fn create_thread(
        &self,
        channel: &ChannelRef,
        trigger_msg: &MessageRef,
        title: &str,
    ) -> Result<ChannelRef>;

    /// Add a reaction/emoji to a message.
    async fn add_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()>;

    /// Remove a reaction/emoji from a message.
    async fn remove_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()>;

    /// Edit an existing message in-place (for streaming updates).
    /// Default: unsupported (send-once only).
    async fn edit_message(&self, _msg: &MessageRef, _content: &str) -> Result<()> {
        Err(anyhow::anyhow!("edit_message not supported"))
    }

    /// Send a message as a reply to a specific message (Discord: message_reference).
    /// Default: falls back to plain send_message (ignores reply_to).
    async fn send_message_with_reply(
        &self,
        channel: &ChannelRef,
        content: &str,
        reply_to_message_id: &str,
    ) -> Result<MessageRef> {
        let _ = reply_to_message_id; // unused in default impl
        self.send_message(channel, content).await
    }

    /// Delete a message. Used to remove streaming placeholders when reply_to is set.
    /// Default: edits to zero-width space (fallback for platforms without delete support).
    async fn delete_message(&self, msg: &MessageRef) -> Result<()> {
        self.edit_message(msg, "\u{200b}").await
    }

    /// Whether this adapter should use streaming edit (true) or send-once (false).
    /// `other_bot_present` indicates if another bot has posted in the current thread.
    /// Streaming should be disabled in multi-bot threads to avoid edit interference.
    /// NOTE: Slight race window exists — the multibot cache is checked before
    /// handle_message, so a bot arriving between the check and the response will
    /// not be detected until the next message. This is acceptable: the first
    /// response may stream, but subsequent ones will correctly use send-once.
    fn use_streaming(&self, other_bot_present: bool) -> bool;
}

// --- AdapterRouter ---

/// Shared logic for routing messages to ACP agents, managing sessions,
/// streaming edits, and controlling reactions. Platform-independent.
pub struct AdapterRouter {
    pool: Arc<SessionPool>,
    reactions_config: ReactionsConfig,
    table_mode: TableMode,
    prompt_hard_timeout: std::time::Duration,
    /// Polling cadence for the recv-loop liveness check (#732).
    liveness_check_interval: std::time::Duration,
    /// Cross-channel relay registry. `None` = relay disabled or not configured.
    relay_ctx: Option<Arc<RelayContext>>,
}

/// Silence threshold for the dropped-turn watchdog. If the agent emits content
/// and then goes quiet for this long — with no tool call running — and never
/// sends a stopReason, the recv loop treats the turn as finished and flushes
/// the buffered reply, instead of waiting out `prompt_hard_timeout` (30m).
///
/// Why this exists: `alive()` only detects a *closed* stdout (process exit). A
/// hung-but-alive agent (stdout open, no further output, no stopReason) is
/// invisible to it, so the loop would otherwise block for the full 30m hard
/// ceiling — long enough for a follow-up message to evict this connection and
/// silently drop the fully-composed reply sitting in `text_buf`. That is the
/// 2026-06-05 dropped-turn incident (silent message loss, zero error logged).
///
/// Sized well above any legitimate no-tool gap (model picking its next action
/// takes seconds) but far below the hard ceiling. Gated on `!has_running_tool`
/// so a legitimately slow tool (e.g. a multi-minute bash) is never cut short.
const IDLE_FLUSH_AFTER: std::time::Duration = std::time::Duration::from_secs(120);

impl AdapterRouter {
    pub fn new(
        pool: Arc<SessionPool>,
        reactions_config: ReactionsConfig,
        table_mode: TableMode,
        prompt_hard_timeout_secs: u64,
        liveness_check_secs: u64,
        relay_ctx: Option<Arc<RelayContext>>,
    ) -> Self {
        if liveness_check_secs >= prompt_hard_timeout_secs {
            warn!(
                liveness_check_secs,
                prompt_hard_timeout_secs,
                "pool.liveness_check_secs >= pool.prompt_hard_timeout_secs; \
                 the hard ceiling will only fire after the next liveness tick \
                 and may be effectively bypassed. Lower liveness_check_secs."
            );
        }
        Self {
            pool,
            reactions_config,
            table_mode,
            prompt_hard_timeout: std::time::Duration::from_secs(prompt_hard_timeout_secs),
            liveness_check_interval: std::time::Duration::from_secs(liveness_check_secs),
            relay_ctx,
        }
    }

    /// Cheap relay-enabled check. Called once before the receive loop and
    /// hoisted into a local bool so the hot streaming path doesn't re-read
    /// the Arc on every text chunk.
    fn relay_enabled(&self) -> bool {
        self.relay_ctx
            .as_ref()
            .is_some_and(|c| c.config.enabled)
    }

    /// The recv-loop hard ceiling (pool.prompt_hard_timeout_secs). Exposed so
    /// the dispatcher's per-lane watchdog (Fix 3) can derive a wall-clock batch
    /// bound that sits just above it.
    pub fn prompt_hard_timeout(&self) -> std::time::Duration {
        self.prompt_hard_timeout
    }

    /// Access the underlying session pool (e.g. for config option queries).
    pub fn pool(&self) -> &Arc<SessionPool> {
        &self.pool
    }

    /// Access the reactions config (used by dispatch.rs).
    pub fn reactions_config(&self) -> &ReactionsConfig {
        &self.reactions_config
    }

    /// Pack one arrival event into ContentBlocks. Per-arrival layout:
    ///   Text { "<sender_context>\n{json}\n</sender_context>" }   <- delimiter
    ///   [Text blocks from extra_blocks (e.g. STT transcripts)]
    ///   Text { "{prompt}" }                                       <- omitted if empty
    ///   [non-Text blocks from extra_blocks (e.g. Image)]
    ///
    /// The sender_context block stands alone so it can serve as a structural
    /// delimiter between arrivals in batched dispatch — agents can scan for
    /// `<sender_context>` openers to find arrival boundaries. Within an arrival,
    /// transcript text precedes the typed prompt to match pre-batching adapter
    /// behavior (voice content first), and images trail the prompt as before.
    /// This is the single packing code path for both per-message and batched
    /// dispatch (ADR §3.5). For a batch of N messages, call this N times and
    /// concatenate.
    pub fn pack_arrival_event(
        sender_json: &str,
        prompt: &str,
        extra_blocks: Vec<ContentBlock>,
    ) -> Vec<ContentBlock> {
        let header = format!("<sender_context>\n{}\n</sender_context>", sender_json);
        let (texts, others): (Vec<_>, Vec<_>) = extra_blocks
            .into_iter()
            .partition(|b| matches!(b, ContentBlock::Text { .. }));
        let mut blocks = Vec::with_capacity(2 + texts.len() + others.len());
        blocks.push(ContentBlock::Text { text: header });
        blocks.extend(texts);
        if !prompt.is_empty() {
            blocks.push(ContentBlock::Text {
                text: prompt.to_string(),
            });
        }
        blocks.extend(others);
        blocks
    }

    /// Handle an incoming user message. The adapter is responsible for
    /// filtering, resolving the thread, and building the SenderContext.
    /// This method handles sender context injection, session management, and streaming.
    pub async fn handle_message(
        &self,
        adapter: &Arc<dyn ChatAdapter>,
        ctx: MessageContext,
    ) -> Result<()> {
        tracing::debug!(platform = adapter.platform(), "processing message");

        let content_blocks =
            Self::pack_arrival_event(&ctx.sender_json, &ctx.prompt, ctx.extra_blocks);

        let thread_key = format!(
            "{}:{}",
            adapter.platform(),
            ctx.thread_channel
                .thread_id
                .as_deref()
                .unwrap_or(&ctx.thread_channel.channel_id)
        );

        if let Err(e) = self.pool.get_or_create(&thread_key).await {
            let msg = format_user_error(&e.to_string());
            let _ = adapter
                .send_message(&ctx.thread_channel, &format!("⚠️ {msg}"))
                .await;
            error!("pool error: {e}");
            return Err(e);
        }

        let reactions = Arc::new(StatusReactionController::new(
            self.reactions_config.enabled,
            adapter.clone(),
            ctx.trigger_msg.clone(),
            self.reactions_config.emojis.clone(),
            self.reactions_config.timing.clone(),
        ));
        reactions.set_queued().await;

        let result = self
            .stream_prompt(
                adapter,
                &thread_key,
                content_blocks,
                &ctx.thread_channel,
                reactions.clone(),
                ctx.other_bot_present,
            )
            .await;

        match &result {
            Ok(()) => reactions.set_done().await,
            Err(_) => reactions.set_error().await,
        }

        let hold_ms = if result.is_ok() {
            self.reactions_config.timing.done_hold_ms
        } else {
            self.reactions_config.timing.error_hold_ms
        };
        if self.reactions_config.remove_after_reply {
            let reactions = reactions;
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(hold_ms)).await;
                reactions.clear().await;
            });
        }

        if let Err(ref e) = result {
            let _ = adapter
                .send_message(&ctx.thread_channel, &format!("⚠️ {e}"))
                .await;
        }

        result
    }

    async fn stream_prompt(
        &self,
        adapter: &Arc<dyn ChatAdapter>,
        thread_key: &str,
        content_blocks: Vec<ContentBlock>,
        thread_channel: &ChannelRef,
        reactions: Arc<StatusReactionController>,
        other_bot_present: bool,
    ) -> Result<()> {
        self.stream_prompt_blocks(
            adapter,
            thread_key,
            content_blocks,
            thread_channel,
            reactions,
            other_bot_present,
        )
        .await
    }

    /// Drive one ACP turn with the given pre-packed ContentBlocks.
    /// Called by both `handle_message` (per-message mode) and `dispatch::dispatch_batch`
    /// (batched mode).
    pub async fn stream_prompt_blocks(
        &self,
        adapter: &Arc<dyn ChatAdapter>,
        thread_key: &str,
        content_blocks: Vec<ContentBlock>,
        thread_channel: &ChannelRef,
        reactions: Arc<StatusReactionController>,
        other_bot_present: bool,
    ) -> Result<()> {
        let adapter = adapter.clone();
        let thread_channel = thread_channel.clone();
        let message_limit = adapter.message_limit();
        let streaming = adapter.use_streaming(other_bot_present);
        let table_mode = self.table_mode;
        let tool_display = self.reactions_config.tool_display;
        let reply_mode = self.reactions_config.reply_mode;
        let prompt_hard_timeout = self.prompt_hard_timeout;
        let liveness_check_interval = self.liveness_check_interval;
        // Hoist relay state into the closure: bool for the hot streaming path,
        // Arc<RelayContext> for the post-stream dispatch path.
        let relay_enabled = self.relay_enabled();
        let relay_ctx = self.relay_ctx.clone();

        self.pool
            .with_connection(thread_key, |conn| {
                let content_blocks = content_blocks.clone();
                Box::pin(async move {
                    let reset = conn.session_reset;
                    conn.session_reset = false;

                    let (mut rx, request_id) = conn.session_prompt(content_blocks).await?;
                    reactions.set_thinking().await;

                    let mut text_buf = String::new();
                    // reply_mode = post_tool_only: `post_tool_text` accumulates the text
                    // block emitted after the most recent tool event; `last_nonempty_block`
                    // is the fallback if that final block is empty. Only consulted when
                    // reply_mode == PostToolOnly (see finalization below).
                    let mut post_tool_text = String::new();
                    let mut last_nonempty_block = String::new();
                    let mut any_tool_ran = false;
                    let mut tool_lines: Vec<ToolEntry> = Vec::new();
                    // Track the latest usage_update for this prompt; used to
                    // append a context-usage footer to the final reply (Slack only).
                    let mut latest_usage: Option<(u64, u64)> = None;

                    if reset {
                        text_buf.push_str("⚠️ _Session expired, starting fresh..._\n\n");
                    }

                    // Streaming edit: send placeholder, spawn edit loop
                    let (buf_tx, placeholder_msg) = if streaming {
                        let initial = if reset {
                            "⚠️ _Session expired, starting fresh..._\n\n…".to_string()
                        } else {
                            "…".to_string()
                        };
                        let msg = adapter.send_message(&thread_channel, &initial).await?;
                        let (tx, rx) = tokio::sync::watch::channel(initial);
                        let edit_adapter = adapter.clone();
                        let edit_msg = msg.clone();
                        let limit = message_limit;
                        let mut buf_rx = rx;
                        tokio::spawn(async move {
                            let mut last = String::new();
                            loop {
                                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                                if buf_rx.has_changed().unwrap_or(false) {
                                    let content = buf_rx.borrow_and_update().clone();
                                    if content != last {
                                        let display = if content.chars().count() > limit - 100 {
                                            format!(
                                                "…{}",
                                                format::truncate_chars_tail(&content, limit - 100)
                                            )
                                        } else {
                                            content.clone()
                                        };
                                        let _ =
                                            edit_adapter.edit_message(&edit_msg, &display).await;
                                        last = content;
                                    }
                                }
                                if buf_rx.has_changed().is_err() {
                                    break;
                                }
                            }
                        });
                        (Some(tx), Some(msg))
                    } else {
                        (None, None)
                    };

                    // (#732) Liveness-aware recv loop. Filters stale id-bearing
                    // messages and abandons cleanly on dead agent / hard ceiling
                    // so late responses cannot leak into the next prompt.
                    let mut response_error: Option<String> = None;
                    let prompt_start = tokio::time::Instant::now();
                    // Last sign of life from the agent; drives the dropped-turn
                    // idle watchdog below. Reset on every notification received.
                    let mut last_activity = prompt_start;
                    // Whether the agent has produced actual reply text. Distinct
                    // from `!text_buf.is_empty()`, which is already true when a
                    // reset banner was pre-seeded above — the idle watchdog must
                    // not fire on the banner alone before any agent output.
                    let mut agent_emitted_text = false;
                    loop {
                        let notification = tokio::select! {
                            // Prefer draining a ready notification over a timeout
                            // tick: if both are ready, consuming the notification
                            // first avoids an idle-flush racing a just-resumed turn.
                            biased;
                            msg = rx.recv() => match msg {
                                Some(n) => n,
                                // Reader saw EOF and already drained pending; nothing to abandon.
                                None => break,
                            },
                            _ = tokio::time::sleep(liveness_check_interval) => {
                                if !conn.alive() {
                                    response_error = Some("Agent process died".into());
                                    conn.abandon_request(request_id).await;
                                    break;
                                }
                                if prompt_start.elapsed() > prompt_hard_timeout {
                                    response_error = Some(format!(
                                        "Agent exceeded hard timeout ({}s)",
                                        prompt_hard_timeout.as_secs(),
                                    ));
                                    conn.abandon_request(request_id).await;
                                    break;
                                }
                                // Dropped-turn watchdog: agent emitted content but
                                // has gone quiet without a stopReason. If no tool is
                                // running, treat it as turn-end and flush text_buf
                                // rather than risk a silent drop on eviction. No
                                // response_error is set, so the buffered reply is
                                // delivered clean. See IDLE_FLUSH_AFTER.
                                let has_running_tool = tool_lines
                                    .iter()
                                    .any(|e| matches!(e.state, ToolState::Running));
                                if agent_emitted_text
                                    && !has_running_tool
                                    && last_activity.elapsed() > IDLE_FLUSH_AFTER
                                {
                                    warn!(
                                        idle_secs = last_activity.elapsed().as_secs(),
                                        buffered_chars = text_buf.len(),
                                        "agent idle after emitting content without \
                                         stopReason; flushing buffered reply to avoid \
                                         silent drop (dropped-turn watchdog)"
                                    );
                                    conn.abandon_request(request_id).await;
                                    break;
                                }
                                continue;
                            }
                        };
                        if let Some(notification_id) = notification.id {
                            if notification_id != request_id {
                                // Stale response from a previously-abandoned prompt.
                                // No automated test seam: this path only triggers when a
                                // real subprocess emits a late response after the broker
                                // already called abandon_request — covered by manual
                                // repro against a live agent (see #732 PR description).
                                continue;
                            }
                            if let Some(ref err) = notification.error {
                                response_error = Some(format_coded_error(err.code, &err.message, err.data_message()));
                            }
                            break;
                        }

                        if let Some(event) = classify_notification(&notification) {
                            // Reset the dropped-turn idle watchdog only on REAL
                            // forward progress — reply text or tool activity — NOT on
                            // empty keepalive thought-chunks. Those otherwise pin the
                            // watchdog open and let a finished reply sit buffered until
                            // the next inbound prompt flushes it. (2026-06-08 stuck-reply
                            // bug — see AI-Memory/shared/2026-06-08-openab-buffered-reply-stuck-bug.md)
                            if matches!(
                                event,
                                AcpEvent::Text(_)
                                    | AcpEvent::ToolStart { .. }
                                    | AcpEvent::ToolDone { .. }
                            ) {
                                last_activity = tokio::time::Instant::now();
                            }
                            // post_tool_only block tracking: a tool event closes the
                            // current text block (stashing it as last-non-empty) so
                            // `post_tool_text` holds only the text emitted after the most
                            // recent tool. Cuts on event boundaries, never mid-string, so
                            // relay markers / output directives stay intact.
                            match &event {
                                AcpEvent::Text(t) => post_tool_text.push_str(t),
                                AcpEvent::ToolStart { .. } | AcpEvent::ToolDone { .. } => {
                                    any_tool_ran = true;
                                    if !post_tool_text.trim().is_empty() {
                                        last_nonempty_block = std::mem::take(&mut post_tool_text);
                                    } else {
                                        post_tool_text.clear();
                                    }
                                }
                                _ => {}
                            }
                            match event {
                                AcpEvent::Text(t) => {
                                    text_buf.push_str(&t);
                                    agent_emitted_text = true;
                                    if let Some(tx) = &buf_tx {
                                        let _ = tx.send(compose_streaming_display(
                                            &tool_lines,
                                            &text_buf,
                                            relay_enabled,
                                            tool_display,
                                        ));
                                    }
                                }
                                AcpEvent::Thinking => {
                                    reactions.set_thinking().await;
                                }
                                AcpEvent::ToolStart { id, title } if !title.is_empty() => {
                                    reactions.set_tool(&title).await;
                                    let title = sanitize_title(&title);
                                    if let Some(slot) = tool_lines.iter_mut().find(|e| e.id == id) {
                                        slot.title = title;
                                        slot.state = ToolState::Running;
                                    } else {
                                        tool_lines.push(ToolEntry {
                                            id,
                                            title,
                                            state: ToolState::Running,
                                        });
                                    }
                                    if let Some(tx) = &buf_tx {
                                        let _ = tx.send(compose_streaming_display(
                                            &tool_lines,
                                            &text_buf,
                                            relay_enabled,
                                            tool_display,
                                        ));
                                    }
                                }
                                AcpEvent::ToolDone { id, title, status } => {
                                    reactions.set_thinking().await;
                                    let new_state = if status == "completed" {
                                        ToolState::Completed
                                    } else {
                                        ToolState::Failed
                                    };
                                    if let Some(slot) = tool_lines.iter_mut().find(|e| e.id == id) {
                                        if !title.is_empty() {
                                            slot.title = sanitize_title(&title);
                                        }
                                        slot.state = new_state;
                                    } else if !title.is_empty() {
                                        tool_lines.push(ToolEntry {
                                            id,
                                            title: sanitize_title(&title),
                                            state: new_state,
                                        });
                                    }
                                    if let Some(tx) = &buf_tx {
                                        let _ = tx.send(compose_streaming_display(
                                            &tool_lines,
                                            &text_buf,
                                            relay_enabled,
                                            tool_display,
                                        ));
                                    }
                                }
                                AcpEvent::ConfigUpdate { options } => {
                                    conn.config_options = options;
                                }
                                AcpEvent::UsageUpdate { used, size, .. } => {
                                    latest_usage = Some((used, size));
                                }
                                _ => {}
                            }
                        }
                    }

                    conn.prompt_done().await;
                    // Stop the edit loop
                    drop(buf_tx);

                    // reply_mode = post_tool_only: when a tool ran, keep only the final
                    // text block (the answer emitted after the last tool call) and drop
                    // inter-step narration. Single selection point, so the normal-end,
                    // dropped-turn watchdog, and cancelled paths all share it. Fallback
                    // chain: final block → last non-empty block → full buffer (never
                    // silently empty). Lossy by design — see the 2026-06-09 note.
                    let text_buf = select_reply_text(
                        reply_mode,
                        any_tool_ran,
                        post_tool_text,
                        last_nonempty_block,
                        text_buf,
                    );

                    // Parse output directives from raw text_buf BEFORE compose_display.
                    // Directives are agent meta-layer, not content — must be stripped
                    // before tool lines are composed into the display output.
                    let (directives, stripped_text) = parse_output_directives(&text_buf);
                    let text_buf = stripped_text;

                    // Cross-channel relay extraction. Runs on raw agent text BEFORE
                    // compose_display + split_message. Tool-display lines never enter
                    // relay parsing. See AI-Memory/shared/proposals/2026-05-26-openab-
                    // cross-channel-relay.md for the full spec.
                    let text_buf = if let Some(ctx) = relay_ctx
                        .as_ref()
                        .filter(|c| c.config.enabled)
                    {
                        let result = relay::extract_relay_blocks(&text_buf);

                        if !result.errors.is_empty() {
                            reactions.set_error().await;
                            let detail = result
                                .errors
                                .iter()
                                .map(|e| format!("- {e:?}"))
                                .collect::<Vec<_>>()
                                .join("\n");
                            let _ = adapter
                                .send_message(
                                    &thread_channel,
                                    &format!("⚠️ relay parse error(s):\n{detail}"),
                                )
                                .await;
                        }

                        for block in &result.directives {
                            if let Err(e) = dispatch_relay_block(
                                ctx,
                                &adapter,
                                &thread_channel,
                                block,
                            )
                            .await
                            {
                                warn!(
                                    error = ?e,
                                    target = %format!("{}:{}", block.target_platform, block.target_alias),
                                    "relay dispatch failed"
                                );
                                let _ = adapter
                                    .send_message(
                                        &thread_channel,
                                        &format!(
                                            "⚠️ relay to `{}:{}` failed: {}",
                                            block.target_platform, block.target_alias, e
                                        ),
                                    )
                                    .await;
                            }
                        }

                        result.residual
                    } else {
                        text_buf
                    };

                    // Build final content
                    let final_content =
                        compose_display(&tool_lines, &text_buf, false, tool_display);
                    let final_content = if final_content.is_empty() {
                        if let Some(err) = response_error {
                            format!("⚠️ {err}")
                        } else {
                            "_(no response)_".to_string()
                        }
                    } else if let Some(err) = response_error {
                        format!("⚠️ {err}\n\n{final_content}")
                    } else {
                        final_content
                    };

                    let final_content = markdown::convert_tables(&final_content, table_mode);

                    // Append context-usage footer (Slack only, threshold-gated).
                    // Per 2026-05-25-openab-context-usage-footer.md proposal.
                    let final_content = if let Some((used, size)) = latest_usage {
                        let threshold = context_footer_threshold();
                        match format_context_footer(adapter.platform(), used, size, threshold) {
                            Some(footer) => format!("{final_content}\n\n{footer}"),
                            None => final_content,
                        }
                    } else {
                        final_content
                    };

                    let chunks = format::split_message(&final_content, message_limit);
                    if let Some(msg) = placeholder_msg {
                        if let Some(ref reply_id) = directives.reply_to {
                            // reply_to directive: send reply first, then delete placeholder.
                            // Only delete if send succeeds — preserves placeholder on failure.
                            let mut send_ok = false;
                            let mut first = true;
                            for chunk in &chunks {
                                if first {
                                    match adapter.send_message_with_reply(
                                        &thread_channel,
                                        chunk,
                                        reply_id,
                                    ).await {
                                        Ok(_) => { send_ok = true; }
                                        Err(e) => {
                                            tracing::warn!(error = ?e, "reply_to send failed; preserving placeholder");
                                        }
                                    }
                                } else {
                                    let _ = adapter.send_message(&thread_channel, chunk).await;
                                }
                                first = false;
                            }
                            if send_ok {
                                if let Err(e) = adapter.delete_message(&msg).await {
                                    tracing::warn!(error = ?e, "delete placeholder failed; placeholder will remain visible");
                                }
                            }
                        } else {
                            // Normal streaming: edit first chunk into placeholder, send rest
                            if let Some(first) = chunks.first() {
                                let _ = adapter.edit_message(&msg, first).await;
                            }
                            for chunk in chunks.iter().skip(1) {
                                let _ = adapter.send_message(&thread_channel, chunk).await;
                            }
                        }
                    } else {
                        // Send-once: all chunks as new messages
                        // First chunk uses reply_to directive if present
                        let mut first = true;
                        for chunk in &chunks {
                            if first {
                                if let Some(ref reply_id) = directives.reply_to {
                                    let _ = adapter.send_message_with_reply(
                                        &thread_channel,
                                        chunk,
                                        reply_id,
                                    ).await;
                                } else {
                                    let _ = adapter.send_message(&thread_channel, chunk).await;
                                }
                            } else {
                                let _ = adapter.send_message(&thread_channel, chunk).await;
                            }
                            first = false;
                        }
                    }

                    Ok(())
                })
            })
            .await
    }
}

/// Select which accumulated turn text to send, given the reply mode.
///
/// `post_tool_only` keeps only the final text block (text emitted after the last
/// tool call) when a tool ran, dropping inter-step narration. Fallback chain so the
/// reply is never silently emptied: final block → last non-empty block → full buffer.
/// Lossy by design — a presentation heuristic, not a semantic "final answer" detector.
fn select_reply_text(
    mode: crate::config::ReplyMode,
    any_tool_ran: bool,
    post_tool: String,
    last_nonempty: String,
    full: String,
) -> String {
    if mode == crate::config::ReplyMode::PostToolOnly && any_tool_ran {
        if !post_tool.trim().is_empty() {
            post_tool
        } else if !last_nonempty.trim().is_empty() {
            last_nonempty
        } else {
            full
        }
    } else {
        full
    }
}

/// Flatten a tool-call title into a single line safe for inline-code spans.
fn sanitize_title(title: &str) -> String {
    title
        .replace('\r', "")
        .replace('\n', " ; ")
        .replace('`', "'")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolState {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
struct ToolEntry {
    id: String,
    title: String,
    state: ToolState,
}

impl ToolEntry {
    fn render(&self) -> String {
        let icon = match self.state {
            ToolState::Running => "🔧",
            ToolState::Completed => "✅",
            ToolState::Failed => "❌",
        };
        let suffix = if self.state == ToolState::Running {
            "..."
        } else {
            ""
        };
        format!("{icon} `{}`{}", self.title, suffix)
    }
}

/// Maximum number of finished tool entries to show individually
/// during streaming before collapsing into a summary line.
const TOOL_COLLAPSE_THRESHOLD: usize = 3;

/// Streaming-time wrapper around `compose_display`. Masks in-progress relay
/// blocks from the placeholder edit loop when relay is enabled. Pure
/// fast-path (`Cow::Borrowed`) when disabled or text has no opener marker.
///
/// Centralized so all 3 streaming call sites (`Text`, `ToolStart`, `ToolDone`
/// arms) route through it — otherwise a relay block can leak via tool events.
fn compose_streaming_display(
    tool_lines: &[ToolEntry],
    text_buf: &str,
    relay_enabled: bool,
    tool_display: ToolDisplay,
) -> String {
    let masked: Cow<'_, str> = if relay_enabled {
        relay::mask_streaming_relay_blocks(text_buf)
    } else {
        Cow::Borrowed(text_buf)
    };
    compose_display(tool_lines, &masked, true, tool_display)
}

fn compose_display(
    tool_lines: &[ToolEntry],
    text: &str,
    streaming: bool,
    tool_display: ToolDisplay,
) -> String {
    let mut out = String::new();
    if !tool_lines.is_empty() && tool_display != ToolDisplay::None {
        let done = tool_lines
            .iter()
            .filter(|e| e.state == ToolState::Completed)
            .count();
        let failed = tool_lines
            .iter()
            .filter(|e| e.state == ToolState::Failed)
            .count();
        let running = tool_lines
            .iter()
            .filter(|e| e.state == ToolState::Running)
            .count();
        let finished = done + failed;

        match tool_display {
            ToolDisplay::Compact => {
                // Always show count summary, never per-tool details
                let mut parts = Vec::new();
                if done > 0 {
                    parts.push(format!("✅ {done}"));
                }
                if failed > 0 {
                    parts.push(format!("❌ {failed}"));
                }
                if running > 0 {
                    parts.push(format!("🔧 {running}"));
                }
                if !parts.is_empty() {
                    out.push_str(&format!("{} tool(s)\n", parts.join(" · ")));
                }
            }
            ToolDisplay::Full => {
                if streaming {
                    let running_entries: Vec<_> = tool_lines
                        .iter()
                        .filter(|e| e.state == ToolState::Running)
                        .collect();

                    if finished <= TOOL_COLLAPSE_THRESHOLD {
                        for entry in tool_lines.iter().filter(|e| e.state != ToolState::Running) {
                            out.push_str(&entry.render());
                            out.push('\n');
                        }
                    } else {
                        let mut parts = Vec::new();
                        if done > 0 {
                            parts.push(format!("✅ {done}"));
                        }
                        if failed > 0 {
                            parts.push(format!("❌ {failed}"));
                        }
                        out.push_str(&format!("{} tool(s) completed\n", parts.join(" · ")));
                    }

                    if running_entries.len() <= TOOL_COLLAPSE_THRESHOLD {
                        for entry in &running_entries {
                            out.push_str(&entry.render());
                            out.push('\n');
                        }
                    } else {
                        let hidden = running_entries.len() - TOOL_COLLAPSE_THRESHOLD;
                        out.push_str(&format!("🔧 {hidden} more running\n"));
                        for entry in running_entries.iter().skip(hidden) {
                            out.push_str(&entry.render());
                            out.push('\n');
                        }
                    }
                } else {
                    for entry in tool_lines {
                        out.push_str(&entry.render());
                        out.push('\n');
                    }
                }
            }
            ToolDisplay::None => {} // guarded above, but safe no-op
        }
        if !out.is_empty() {
            out.push('\n');
        }
    }
    out.push_str(text.trim_end());
    out
}

/// Format the context-usage footer per the 2026-05-25 OpenAB proposal.
///
/// Returns `None` when usage is below `threshold_pct`, or when called with
/// non-Slack platform (Discord etc. have their own proposal track).
///
/// Returned string is a single italics line (Slack mrkdwn `_..._`) intended
/// to be appended to the final message body with `\n\n` separator.
pub(crate) fn format_context_footer(
    platform: &str,
    used: u64,
    size: u64,
    threshold_pct: u8,
) -> Option<String> {
    if platform != "slack" {
        return None;
    }
    if size == 0 {
        return None;
    }
    let pct = (used as f64 / size as f64 * 100.0).round() as u64;
    if pct < threshold_pct as u64 {
        return None;
    }
    Some(format!(
        "_⚠ ctx: {pct}% ({used}/{size})_",
        used = format_token_count(used),
        size = format_token_count(size),
    ))
}

/// Format a token count compactly: 84231 → "84k", 1_000_000 → "1M".
fn format_token_count(n: u64) -> String {
    if n >= 1_000_000 {
        let m = n as f64 / 1_000_000.0;
        if (m - m.round()).abs() < 0.05 {
            format!("{}M", m.round() as u64)
        } else {
            format!("{m:.1}M")
        }
    } else if n >= 1_000 {
        format!("{}k", (n as f64 / 1_000.0).round() as u64)
    } else {
        format!("{n}")
    }
}

/// Read `OPENAB_CTX_FOOTER_MIN_PCT` env var, default 70.
/// Setting `100` disables the footer entirely. Setting `0` enables always-show.
pub(crate) fn context_footer_threshold() -> u8 {
    std::env::var("OPENAB_CTX_FOOTER_MIN_PCT")
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .filter(|&v| v <= 100)
        .unwrap_or(70)
}

/// Upper bound on a single relay send_message call. A relay send runs inline on
/// the per-lane consumer loop, so an unbounded hang wedges the whole lane (see
/// Fix 1 note in dispatch_relay_block). 15s comfortably covers a normal Slack/
/// Discord post + retry while still failing loudly if the call never returns.
const RELAY_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Forward a single relay block to its peer adapter. Caller (the post-stream
/// hook in `stream_prompt_blocks`) handles error reporting back to the origin
/// channel.
async fn dispatch_relay_block(
    ctx: &RelayContext,
    origin_adapter: &Arc<dyn ChatAdapter>,
    _origin_channel: &ChannelRef,
    block: &RelayDirective,
) -> Result<()> {
    let alias_key = format!("{}:{}", block.target_platform, block.target_alias);
    let alias = ctx
        .config
        .aliases
        .get(&alias_key)
        .ok_or_else(|| anyhow!("unknown relay alias: {}", alias_key))?;

    // Fix 2: same-platform relay (e.g. slack→slack: a Slack thread relaying into
    // another Slack channel like #ai-workflow) uses the ORIGIN adapter directly.
    // Resolving ctx.peer("slack") returns the same shared adapter we're already
    // running on (self-peer) — routing the send back through a Weak::upgrade of
    // ourselves is needless indirection. Only reach for the peer registry when
    // the target is a genuinely different platform.
    let sender: Arc<dyn ChatAdapter> = if block.target_platform == origin_adapter.platform() {
        origin_adapter.clone()
    } else {
        ctx.peer(&block.target_platform)
            .ok_or_else(|| anyhow!("no peer adapter for platform: {}", block.target_platform))?
    };

    if relay::contains_relay_marker(&block.body) {
        return Err(anyhow!(
            "loop-guard: relayed body still contains relay marker"
        ));
    }

    let persona = std::env::var("OPENAB_PERSONA_ID").unwrap_or_else(|_| "unknown".to_string());
    let formatted = relay::format_relay_body(
        &ctx.config.prefix_template,
        &persona,
        origin_adapter.platform(),
        &block.body,
    );

    // Security: strip file-send markers from relay bodies. Slack's send_message
    // extracts and uploads file-send markers (slack.rs:620); a relayed body
    // containing `<<openab-send-file /path>>` would silently upload when
    // targeting Slack. Stripped uniformly for audit consistency.
    let formatted = relay::strip_file_send_markers(&formatted);

    // When relaying INTO Discord, rewrite plain-text @persona mentions (the
    // origin platform flattens cross-platform mentions to text) into real
    // <@id> mentions, so discord.rs's inbound mention gate actually fires for
    // the target bot. No-op for other target platforms / empty map.
    let formatted = if block.target_platform == "discord" {
        relay::resolve_discord_mentions(&formatted, &ctx.config.discord_mentions)
    } else {
        formatted
    };

    let target = ChannelRef {
        platform: block.target_platform.clone(),
        channel_id: alias.channel_id.clone(),
        thread_id: None,
        parent_id: None,
        origin_event_id: None,
    };

    // Peer-side splitting. Adapters' send_message does NOT split internally
    // (split is normally done by AdapterRouter before adapter.send_message).
    // Since relay bypasses that path, we split here using the sender's own
    // message_limit. Loop send_message over chunks.
    //
    // Fix 1: each send is bounded by RELAY_SEND_TIMEOUT. dispatch_relay_block is
    // awaited inline on the per-lane consumer_loop (dispatch.rs) — a send that
    // never returns would wedge that lane forever, silently queuing every
    // subsequent message on the same (thread, sender) key (observed 2026-05-29:
    // a slack→slack relay turn never returned, so the originating DM thread
    // stopped responding until a fresh non-thread DM opened a new lane). The
    // timeout converts a hang into an Err the caller already handles
    // (warn + user-facing ⚠️), guaranteeing the consumer loop drains.
    let chunks = format::split_message(&formatted, sender.message_limit());
    for chunk in &chunks {
        tokio::time::timeout(RELAY_SEND_TIMEOUT, sender.send_message(&target, chunk))
            .await
            .map_err(|_| {
                anyhow!(
                    "relay send timed out after {}s (target {}:{})",
                    RELAY_SEND_TIMEOUT.as_secs(),
                    block.target_platform,
                    block.target_alias
                )
            })??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReplyMode;

    #[test]
    fn reply_mode_full_always_sends_full_text() {
        let got = select_reply_text(
            ReplyMode::Full,
            true,
            "post".into(),
            "block".into(),
            "FULL".into(),
        );
        assert_eq!(got, "FULL");
    }

    #[test]
    fn post_tool_only_without_a_tool_sends_full_text() {
        // No tool ran → nothing to trim, keep everything (e.g. a plain Q&A turn).
        let got = select_reply_text(
            ReplyMode::PostToolOnly,
            false,
            String::new(),
            String::new(),
            "FULL ANSWER".into(),
        );
        assert_eq!(got, "FULL ANSWER");
    }

    #[test]
    fn post_tool_only_keeps_final_block_after_last_tool() {
        let got = select_reply_text(
            ReplyMode::PostToolOnly,
            true,
            "the final answer".into(),
            "earlier narration".into(),
            "narration + the final answer".into(),
        );
        assert_eq!(got, "the final answer");
    }

    #[test]
    fn post_tool_only_falls_back_to_last_nonempty_block_when_final_empty() {
        // e.g. ...text... [tool] (no text after) → final block empty.
        let got = select_reply_text(
            ReplyMode::PostToolOnly,
            true,
            "   ".into(),
            "the answer before the trailing tool".into(),
            "full buffer".into(),
        );
        assert_eq!(got, "the answer before the trailing tool");
    }

    #[test]
    fn post_tool_only_falls_back_to_full_when_no_blocks() {
        // Degenerate: tool ran but both block buffers empty — never silently empty.
        let got = select_reply_text(
            ReplyMode::PostToolOnly,
            true,
            String::new(),
            "  ".into(),
            "full buffer".into(),
        );
        assert_eq!(got, "full buffer");
    }

    /// Compile-time regression guard: use_streaming() is a required trait method
    /// (no default). Any adapter that forgets to implement it will fail to compile.
    /// This test documents the contract — see PR #503 / issue #502 for context.
    #[test]
    fn use_streaming_is_required_method() {
        // If use_streaming() had a default impl, this test module would still
        // compile even if an adapter forgot to override it. The real guard is
        // the trait definition itself — this test exists as documentation and
        // to catch if someone re-adds a default.
        struct TestAdapter;

        #[async_trait]
        impl ChatAdapter for TestAdapter {
            fn platform(&self) -> &'static str {
                "test"
            }
            fn message_limit(&self) -> usize {
                2000
            }
            async fn send_message(&self, _: &ChannelRef, _: &str) -> Result<MessageRef> {
                unimplemented!()
            }
            async fn create_thread(
                &self,
                _: &ChannelRef,
                _: &MessageRef,
                _: &str,
            ) -> Result<ChannelRef> {
                unimplemented!()
            }
            async fn add_reaction(&self, _: &MessageRef, _: &str) -> Result<()> {
                Ok(())
            }
            async fn remove_reaction(&self, _: &MessageRef, _: &str) -> Result<()> {
                Ok(())
            }
            // use_streaming() MUST be declared — removing this line should fail compilation
            fn use_streaming(&self, _other_bot_present: bool) -> bool {
                false
            }
        }

        let adapter = TestAdapter;
        // Verify the method is callable and returns the declared value
        assert!(!adapter.use_streaming(false));
    }

    #[test]
    fn origin_event_id_excluded_from_eq() {
        let a = ChannelRef {
            platform: "line".into(),
            channel_id: "U123".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("evt_aaa".into()),
        };
        let b = ChannelRef {
            platform: "line".into(),
            channel_id: "U123".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("evt_bbb".into()),
        };
        assert_eq!(a, b, "same channel with different event IDs must be equal");
    }

    #[test]
    fn origin_event_id_excluded_from_hash() {
        use std::collections::HashMap;
        let a = ChannelRef {
            platform: "line".into(),
            channel_id: "U123".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("evt_aaa".into()),
        };
        let b = ChannelRef {
            platform: "line".into(),
            channel_id: "U123".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("evt_bbb".into()),
        };
        let mut map = HashMap::new();
        map.insert(a, "first");
        // b should hit the same bucket and overwrite
        map.insert(b, "second");
        assert_eq!(map.len(), 1);
        assert_eq!(map.values().next(), Some(&"second"));
    }

    #[test]
    fn origin_event_id_survives_clone() {
        let ch = ChannelRef {
            platform: "line".into(),
            channel_id: "U123".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("evt_abc".into()),
        };
        // Simulates create_thread propagation: clone preserves origin_event_id
        let thread_ch = ChannelRef {
            thread_id: Some("topic_1".into()),
            origin_event_id: ch.origin_event_id.clone(),
            ..ch.clone()
        };
        assert_eq!(thread_ch.origin_event_id.as_deref(), Some("evt_abc"));
    }

    fn tool(id: &str, title: &str, state: ToolState) -> ToolEntry {
        ToolEntry {
            id: id.into(),
            title: title.into(),
            state,
        }
    }

    #[test]
    fn compose_display_full_shows_complete_title() {
        let tools = vec![tool(
            "1",
            "curl -s https://example.com",
            ToolState::Completed,
        )];
        let out = compose_display(&tools, "done", false, ToolDisplay::Full);
        assert!(out.contains("`curl -s https://example.com`"));
    }

    #[test]
    fn compose_display_compact_shows_count_summary() {
        let tools = vec![
            tool("1", "curl -s https://example.com", ToolState::Completed),
            tool("2", "grep -r pattern src/", ToolState::Completed),
            tool("3", "cat /etc/hosts", ToolState::Failed),
        ];
        let out = compose_display(&tools, "done", false, ToolDisplay::Compact);
        assert!(out.contains("✅ 2"), "expected completed count: {out}");
        assert!(out.contains("❌ 1"), "expected failed count: {out}");
        assert!(out.contains("tool(s)"), "expected tool(s) label: {out}");
        // Must NOT contain individual tool names
        assert!(!out.contains("curl"), "should not show tool names: {out}");
        assert!(!out.contains("grep"), "should not show tool names: {out}");
    }

    #[test]
    fn compose_display_compact_shows_running_count() {
        let tools = vec![
            tool("1", "curl", ToolState::Completed),
            tool("2", "npm install", ToolState::Running),
        ];
        let out = compose_display(&tools, "", true, ToolDisplay::Compact);
        assert!(out.contains("✅ 1"), "expected completed count: {out}");
        assert!(out.contains("🔧 1"), "expected running count: {out}");
    }

    #[test]
    fn compose_display_none_hides_tools() {
        let tools = vec![tool(
            "1",
            "curl -s https://example.com",
            ToolState::Completed,
        )];
        let out = compose_display(&tools, "response text", false, ToolDisplay::None);
        assert_eq!(out, "response text");
    }

    #[test]
    fn context_footer_skips_below_threshold() {
        // 69% on a 1M window with default 70 threshold → no footer.
        assert!(format_context_footer("slack", 690_000, 1_000_000, 70).is_none());
    }

    #[test]
    fn context_footer_appears_at_or_above_threshold() {
        let out = format_context_footer("slack", 780_000, 1_000_000, 70).unwrap();
        assert!(out.starts_with("_⚠ ctx: 78%"));
        assert!(out.contains("(780k/1M)"));
        assert!(out.ends_with("_"));
    }

    #[test]
    fn context_footer_skipped_for_non_slack() {
        // Proposal scope is Slack only; Discord gets its own track later.
        assert!(format_context_footer("discord", 900_000, 1_000_000, 70).is_none());
    }

    #[test]
    fn context_footer_handles_zero_size_gracefully() {
        // Defensive: don't divide by zero if ACP ever sends size=0.
        assert!(format_context_footer("slack", 1000, 0, 70).is_none());
    }

    #[test]
    fn context_footer_threshold_zero_always_shows() {
        // Edge case: threshold=0 means show even at low usage.
        assert!(format_context_footer("slack", 1000, 1_000_000, 0).is_some());
    }

    #[test]
    fn format_token_count_compact() {
        assert_eq!(format_token_count(999), "999");
        assert_eq!(format_token_count(1_000), "1k");
        assert_eq!(format_token_count(84_231), "84k");
        assert_eq!(format_token_count(999_999), "1000k");
        assert_eq!(format_token_count(1_000_000), "1M");
        assert_eq!(format_token_count(1_500_000), "1.5M");
    }
}

#[cfg(test)]
mod directive_tests {
    use super::parse_output_directives;

    #[test]
    fn parse_reply_to_directive() {
        let input = "[[reply_to:1502606076451885136]]\nHello world";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("1502606076451885136".to_string()));
        assert_eq!(content, "Hello world");
    }

    #[test]
    fn parse_no_directives() {
        let input = "Just plain content\nwith multiple lines";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, None);
        assert_eq!(content, input);
    }

    #[test]
    fn parse_multiple_directives() {
        let input = "[[reply_to:123456]]\n[[unknown_key:value]]\nContent here";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("123456".to_string()));
        assert_eq!(content, "Content here");
    }

    #[test]
    fn parse_invalid_reply_to_rejects_whitespace() {
        let input = "[[reply_to:has spaces]]\nContent";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, None);
        assert_eq!(content, "Content");
    }

    #[test]
    fn parse_slack_ts_format_accepted() {
        let input = "[[reply_to:1234567890.123456]]\nContent";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("1234567890.123456".to_string()));
        assert_eq!(content, "Content");
    }

    #[test]
    fn parse_empty_reply_to() {
        let input = "[[reply_to:]]\nContent";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, None);
        assert_eq!(content, "Content");
    }

    #[test]
    fn parse_crlf_line_endings() {
        let input = "[[reply_to:999]]\r\nContent with CRLF";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("999".to_string()));
        assert_eq!(content, "Content with CRLF");
    }

    #[test]
    fn parse_directive_only_no_content() {
        let input = "[[reply_to:123]]";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("123".to_string()));
        assert_eq!(content, "");
    }

    #[test]
    fn parse_non_directive_line_stops_parsing() {
        let input = "Normal first line\n[[reply_to:123]]\nMore content";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, None);
        assert_eq!(content, input);
    }

    #[test]
    fn parse_duplicate_reply_to_last_wins() {
        let input = "[[reply_to:111]]\n[[reply_to:222]]\nContent";
        let (directives, content) = parse_output_directives(input);
        // Last value wins
        assert_eq!(directives.reply_to, Some("222".to_string()));
        assert_eq!(content, "Content");
    }

    #[test]
    fn parse_crlf_multiple_directives() {
        let input = "[[reply_to:456]]\r\n[[unknown:x]]\r\nContent after CRLF";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("456".to_string()));
        assert_eq!(content, "Content after CRLF");
    }

    #[test]
    fn parse_bracket_without_colon_preserved() {
        // [[Note]] has no colon — not a directive, preserved as content
        let input = "[[Summary]]\nThis is body text";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, None);
        assert_eq!(content, input);
    }

    #[test]
    fn parse_reply_to_with_inline_content() {
        // Agent puts content on same line as directive — should still parse
        let input = "[[reply_to:1502724086474870926]]  @BOT I'm on standby";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("1502724086474870926".to_string()));
        assert_eq!(content, "@BOT I'm on standby");
    }

    #[test]
    fn parse_reply_to_inline_with_more_lines() {
        let input = "[[reply_to:123]]  First line\nSecond line\nThird line";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("123".to_string()));
        assert_eq!(content, "First line\nSecond line\nThird line");
    }

    #[test]
    fn parse_reply_to_no_space_before_content() {
        // No space between ]] and content
        let input = "[[reply_to:1502724086474870926]]收到";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("1502724086474870926".to_string()));
        assert_eq!(content, "收到");
    }

    #[test]
    fn parse_reply_to_inline_with_mention() {
        // Real-world case: directive followed by Discord mention
        let input = "[[reply_to:1502724086474870926]]  <@1490365068863606784> 我 standby";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("1502724086474870926".to_string()));
        assert_eq!(content, "<@1490365068863606784> 我 standby");
    }

    #[test]
    fn parse_reply_to_inline_only_spaces() {
        // Trailing spaces only — no real content, should be empty
        let input = "[[reply_to:123]]   ";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("123".to_string()));
        assert_eq!(content, "");
    }

    #[test]
    fn parse_reply_to_with_brackets_in_content() {
        // Content after ]] contains brackets — should not confuse parser
        let input = "[[reply_to:456]]  看看 [[這個]] 怎麼樣";
        let (directives, content) = parse_output_directives(input);
        assert_eq!(directives.reply_to, Some("456".to_string()));
        assert_eq!(content, "看看 [[這個]] 怎麼樣");
    }
}
