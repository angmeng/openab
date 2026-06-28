# ADR: Ambient Mode

- **Status:** Proposed
- **Date:** 2026-06-26
- **Author:** chaodu-agent

## Context

Today, OpenAB only dispatches messages to an agent when the agent is explicitly
mentioned (or the message is in a thread the agent participates in). This means
agents are deaf to surrounding conversation unless invoked — they cannot
proactively contribute context, answer questions addressed to no one in
particular, or notice when a discussion touches their area of expertise.

We want agents to behave more like attentive team members who listen to the room
and speak up when they have something valuable to add, without requiring an
explicit `@mention` every time.

## Prior Art

### OpenClaw — `/activation always`

OpenClaw supports an `always` activation mode as a cross-platform group chat
feature (WhatsApp, Telegram, Discord, Slack, iMessage — configured via
`agents.list[].groupChat.mentionPatterns` and `channels.*.groups`):
- Every message is dispatched to the agent (no mention required).
- Agent returns the sentinel token `NO_REPLY` when it has nothing to add;
  the gateway discards silently.
- Pending messages (up to 50) are accumulated as context and injected as
  `[Chat messages since your last reply - for context]`.
- Per-group toggle via `/activation always` or `/activation mention`.
- **Limitation:** messages are dispatched one-by-one — each message triggers a
  separate LLM invocation, even if it results in `NO_REPLY`. No batching.

### Hermes Agent — `free_response_channels`

Hermes provides `DISCORD_FREE_RESPONSE_CHANNELS` and
`DISCORD_REQUIRE_MENTION=false`:
- The bot responds to **every** message in designated channels without mention.
- Free-response channels skip auto-threading (replies inline) and isolate
  sessions per user (`group_sessions_per_user: true` by default).
- History backfill (`DISCORD_HISTORY_BACKFILL`) recovers missed channel context
  on `@mention` — only triggered when `require_mention: true` and skipped in
  free-response channels and DMs where the transcript is already complete.
  Scans up to 50 messages backwards, stopping at the bot's own last message.
- **Limitation:** no autonomous decision-making — the bot always replies. There
  is no `NO_REPLY` equivalent; it's either "respond to everything" or
  "respond only on mention."

### Research — "Controlling AI Agent Participation in Group Conversations"

(arXiv 2501.17258, Jan 2025) — studies user preferences for AI agent behavior
in group settings. Key finding: users benefited from having the AI in the group,
but disliked when the agent dominated the conversation and desired controls
over its interactive behaviors.

### Gap Our Design Fills

Neither OpenClaw nor Hermes implements **batch flush** — they dispatch per
message. Our design accumulates messages and flushes them as a batch, which:
1. Reduces LLM invocations (one call per batch instead of N).
2. Gives the agent fuller conversational context for better judgment.
3. Provides natural rate-limiting without additional cooldown mechanisms.

## Decision

Introduce an **Ambient Mode** using a **batch flush** strategy.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Discord Gateway (events)                            │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ MESSAGE_CREATE
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Handler::message()                                                         │
│                                                                             │
│  ┌─────────────┐    YES    ┌──────────────────────────────────────────┐     │
│  │ Own bot msg?├──────────►│ DROP (echo prevention)                   │     │
│  └──────┬──────┘           └──────────────────────────────────────────┘     │
│         │ NO                                                                │
│         ▼                                                                   │
│  ┌─────────────┐    YES    ┌──────────────────────────────────────────┐     │
│  │ @mention?   ├──────────►│ Normal Mention Dispatch Path             │     │
│  │             │           │ • Discard ambient buffer for this channel│     │
│  └──────┬──────┘           │ • Cancel in-flight ambient flush         │     │
│         │ NO               │ • Thread + reactions + full session       │     │
│         ▼                  └──────────────────────────────────────────┘     │
│  ┌──────────────────┐ NO   ┌──────────────────────────────────────────┐    │
│  │ Ambient channel?  ├────►│ IGNORE (not ambient-enabled)             │    │
│  └──────┬────────────┘     └──────────────────────────────────────────┘    │
│         │ YES                                                               │
│         ▼                                                                   │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │              Ambient Dispatcher::submit(msg)                      │      │
│  └──────────────────────────────┬───────────────────────────────────┘      │
└─────────────────────────────────┼───────────────────────────────────────────┘
                                  │ mpsc::channel (bounded)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Ambient Consumer Loop (per-channel)                                        │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Buffer Phase                                                          │  │
│  │                                                                       │  │
│  │   msg ──► [  buffer  ] ◄── accumulate until trigger                   │  │
│  │                                                                       │  │
│  │   Flush triggers (whichever first):                                   │  │
│  │     • Timer expired (flush_interval ± 20% jitter)                     │  │
│  │     • Count reached (flush_max_messages = 10)                         │  │
│  │     • Hard cap hit  (flush_hard_cap = 50)                             │  │
│  └───────────────────────────────┬───────────────────────────────────────┘  │
│                                  │ swap-and-drain                            │
│                                  ▼                                           │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Flush Phase                                                           │  │
│  │                                                                       │  │
│  │   1. Acquire global semaphore (max_concurrent_flushes = 3)            │  │
│  │   2. Set channel_flushing = true                                      │  │
│  │   3. Fetch context_window (20 msgs) from Discord API                  │  │
│  │   4. Build payload:                                                   │  │
│  │      ┌──────────────────────────────────────────────────────┐         │  │
│  │      │ System: "You are in ambient mode..."                 │         │  │
│  │      │ [Ambient context — channel history]                  │         │  │
│  │      │ [Ambient batch — N new messages]                     │         │  │
│  │      │ [End of batch — reply [NO_REPLY] if nothing to add]  │         │  │
│  │      └──────────────────────────────────────────────────────┘         │  │
│  │   5. Send to LLM (ambient session pool)                               │  │
│  │   6. Set channel_flushing = false                                     │  │
│  │   7. Release semaphore                                                │  │
│  └───────────────────────────────┬───────────────────────────────────────┘  │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Response Router                                                            │
│                                                                             │
│   LLM Response ──► trim + lowercase                                         │
│                        │                                                    │
│              ┌─────────┴─────────┐                                          │
│              ▼                   ▼                                           │
│   "[no_reply]"              Actual content                                  │
│        │                         │                                          │
│        ▼                         ▼                                          │
│   🗑️ Discard silently      📤 Post to channel                              │
│   (no reactions,           (no 👀🤔🔥 reactions,                            │
│    no threading)            direct message post)                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dual-Path Concurrency (Mention vs Ambient)

```
Channel #general (ambient-enabled)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Timeline:
  t=0s   UserA: "how do I deploy?"        ──► ambient buffer
  t=3s   UserB: "check the wiki"          ──► ambient buffer
  t=8s   UserA: "wiki is outdated"        ──► ambient buffer
  t=15s  UserC: "@bot help me deploy"     ──► MENTION PATH
                                               │
         ┌─────────────────────────────────────┘
         ▼
  ┌────────────────────────────────┐    ┌────────────────────────────┐
  │ Mention Dispatch               │    │ Ambient Buffer             │
  │                                │    │                            │
  │ Session: discord:<thread_id>   │    │ [msg1, msg2, msg3]         │
  │ Pool: Main                     │    │         │                  │
  │ Reactions: 👀🤔🔥🆗            │    │         ▼                  │
  │ Response: in new thread        │    │   DISCARDED (not flushed)  │
  └────────────────────────────────┘    └────────────────────────────┘

  t=16s  (new buffer cycle starts fresh)
  t=20s  UserD: "anyone tried helm 3.15?"  ──► new ambient buffer
  ...
  t=76s  flush_interval elapsed            ──► flush new buffer
```

### Mechanism

```
Discord Channel
────────────────────────────────────────────────────────────────────
  msg1 (t=0s)  │
  msg2 (t=3s)  │  accumulate in buffer
  msg3 (t=8s)  │
  msg4 (t=12s) │
               ▼
         ┌─────────────────────────────┐
         │ Flush trigger fired         │
         │ (60s elapsed OR 10 msgs)    │
         └─────────────┬───────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  OpenAB Gateway                                                  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Ambient Dispatch (batch)                                   │  │
│  │                                                            │  │
│  │ • Drain messages from mpsc channel into batch              │  │
│  │   (new messages queue for next flush cycle)                │  │
│  │ • Prepend: channel history (context_window via API)        │  │
│  │ • Prepend system instruction:                              │  │
│  │   "You are in ambient mode. Below is a batch of recent     │  │
│  │    messages. If you have nothing valuable to add, reply    │  │
│  │    exactly: [NO_REPLY]"                                    │  │
│  │ • Send batch to agent                                      │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                  │
│                               ▼                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Response Router                                            │  │
│  │                                                            │  │
│  │ • Agent replies "[NO_REPLY]" → discard silently            │  │
│  │ • Agent replies with content  → post to Discord            │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Buffer Lifecycle

Conceptually, the ambient buffer operates as a **swap-and-drain** model —
ingestion is decoupled from flush processing so neither blocks the other.

In practice, this is implemented via the existing `Dispatcher` infrastructure
using a bounded `mpsc::channel` per ambient channel (see Implementation Notes).
The mpsc channel provides the same decoupling guarantees: `submit()` pushes
messages into the channel (non-blocking for senders), and the ambient consumer
loop drains messages via `rx.recv()` with a deadline. The "swap" happens
implicitly — once the consumer drains and moves into flush processing, new
messages queue in the channel buffer for the next cycle.

The key invariant: flush processing never blocks message ingestion.

### Flush Triggers

Messages are accumulated in a per-channel buffer and flushed when **any**
condition is met (whichever comes first):

| Trigger | Default | Description |
|---------|---------|-------------|
| Time | `flush_interval_seconds = 60` | Seconds since first buffered message |
| Count | `flush_max_messages = 10` | Max messages to accumulate before flush |
| Hard cap | `flush_hard_cap = 50` | Safety cap — force flush if `flush_max_messages` is set high or disabled |

**Relationship between `flush_max_messages` and `flush_hard_cap`:**
`flush_max_messages` is the operational trigger (default 10). `flush_hard_cap`
is a safety net for deployments where operators set `flush_max_messages` to a
very large value (or 0 to disable count-based flushing) to rely solely on
timer-based flushes. In that scenario, `flush_hard_cap` prevents unbounded
buffer growth during message spikes. With default values (10 and 50), the hard
cap is effectively dormant — this is intentional.
| Mention | immediate | @mention discards buffer, triggers normal mention dispatch (not a flush) |

**Flush interval jitter:** to prevent thundering herd when many channels flush
simultaneously, the actual interval is `flush_interval_seconds ± 20%` (random
per-channel, recomputed each cycle).

**@mention handling:** when a `@mention` is detected at the Handler level
(before the buffer), the system:
1. Discards the ambient buffer for that channel (messages are lost — not flushed).
2. If an ambient flush is already in-flight, sets a `cancelled` flag to suppress
   the ambient response before posting (see Concurrent reply prevention below).
3. Dispatches the @mention via the normal mention-triggered path (with full
   reactions, threading, etc.).

Rationale: the bot is about to reply directly via mention — flushing the ambient
buffer simultaneously would produce a redundant double-reply. The discarded
messages provided conversational context that the mention dispatch can retrieve
via `context_window` (Discord API history fetch) if needed.

**Concurrent reply prevention:** to prevent double-replying to the same channel:
- The ambient consumer holds a per-channel `AtomicBool` flag (`flushing`) with
  a **safety timeout** (`flush_timeout_seconds = 120`). If the flag remains set
  beyond the timeout (e.g., consumer panicked/OOM), it is automatically reset
  and the condition is logged as an error.
- When a @mention arrives on an ambient channel (detected at Handler level,
  before the buffer), the system:
  1. If ambient consumer is idle (not flushing): discard buffer, dispatch
     mention normally.
  2. If ambient consumer is mid-flush: set a `cancelled` flag **and** suppress
     the ambient response atomically using a `post_guard` — the guard is
     acquired before the `[NO_REPLY]` check and held through `post_response()`.
     The mention dispatch path invalidates the guard, ensuring no ambient
     message is posted after cancellation.
- Conversely, if a mention dispatch is already in-flight, the ambient consumer
  skips posting its response — the user already got a direct reply.
- This ensures at most one bot response is posted to a channel at any given
  moment from the ambient + mention paths combined.

### Message Filtering for Buffer

Not all messages enter the ambient buffer:

- ✅ **User messages** in ambient-enabled channels (without @mention) → buffer
- ✅ **Bot messages from other bots** (if `allow_bot_messages` permits) → buffer
- ❌ **Own bot messages** → never buffered (prevents echo loops)
- ❌ **Messages that @mention the bot** → bypass buffer, discard existing
  buffer contents + normal mention dispatch
- ❌ **Messages in threads created by the bot** → handled by existing
  thread-based session logic, not ambient

### Batch Payload

The flushed batch is formatted as a conversation block:

```
[Ambient context — recent channel history]
[12:00:01] UserC: I pushed the helm fix yesterday
[12:00:02] UserB: cool

[Ambient batch — 4 new messages since last flush]
[12:03:01] UserA: Anyone know how to fix the helm release?
[12:03:04] UserB: Which chart version?
[12:03:11] UserA: 0.8.5
[12:03:15] UserC: Try rolling back first

[End of batch — reply only if you can add meaningful value.
 Otherwise reply exactly: [NO_REPLY]]
```

### Session Strategy

Ambient dispatches use a **dedicated session pool**, separate from the main
mention-triggered pool:

| Aspect | Mention dispatch | Ambient dispatch |
|--------|-----------------|-----------------|
| Session key | `discord:<thread_id>` | `ambient:discord:<channel_id>` |
| Pool | Main pool (`[pool]`) | Ambient pool (`[pool.ambient]`) |
| Lifetime | Long-lived (session_ttl_hours) | Short-lived (ambient_session_ttl_minutes) |
| Cross-flush memory | Full transcript | Rolling window (last N flushes) |
| Reactions | ✅ Full (👀🤔🔥🆗) | ❌ Suppressed |

**Why separate pools:** prevents ambient traffic from exhausting the main pool
and blocking normal @mention responses. The ambient pool has its own
`max_sessions` cap.

**Cross-flush context:** the ambient session retains a rolling window of the
last `ambient_context_flushes` (default: 3) flush interactions, so the agent
has memory of what it said/declined recently. Sessions expire after
`ambient_session_ttl_minutes` (default: 60) of inactivity.

### Configuration

All ambient settings are grouped under a top-level `[ambient]` section:

```toml
[ambient]
enabled = false                       # master switch
flush_interval_seconds = 60           # time-based flush trigger (±20% jitter applied)
flush_max_messages = 10               # count-based flush trigger
flush_hard_cap = 50                   # safety cap — force flush at this count
context_window = 20                   # historical messages fetched via Discord API before batch
max_concurrent_flushes = 3            # max simultaneous LLM calls across all ambient channels
flush_timeout_seconds = 120           # safety timeout — auto-reset flushing flag if exceeded

[ambient.pool]
max_sessions = 5                      # separate pool for ambient dispatches
session_ttl_minutes = 60              # ambient session inactivity timeout
context_flushes = 3                   # rolling window of retained flush history

[ambient.discord]
channels = ["1490282656913559673"]    # required — explicit allowlist, empty = disabled
allow_bot_messages = false            # whether other bots' messages enter ambient buffer
```

**Design rationale:** flush parameters (`flush_interval_seconds`, `flush_max_messages`,
etc.) are platform-agnostic and live at the `[ambient]` level. Platform-specific
settings (channel allowlists) live under `[ambient.<platform>]`. This allows
future multi-platform support (`[ambient.slack]`, `[ambient.telegram]`) without
restructuring.

**`channels` semantics:** an explicit allowlist is **required**. If `channels`
is empty or omitted while `enabled = true`, ambient mode is **not activated**
for any channel (fail-safe). This prevents accidental global ambient activation.

**`context_window`:** fetches the N most recent messages from the Discord
channel history API (before the batch window) to provide additional context.
This is a Discord API call with standard rate limiting. If fewer than N messages
exist, all available messages are included. These messages are **not** counted
toward `flush_max_messages`.

### Error Handling

| Scenario | Behavior |
|----------|----------|
| LLM timeout / network error | Batch is **discarded** (not retried). Next flush cycle starts fresh. Logged as warning. |
| Agent returns tool calls | Treated as normal response — if final output is not `[NO_REPLY]`, post it. Tool calls execute normally within the ambient session. |
| Agent returns empty response | Treated as `[NO_REPLY]` (discard silently). |
| Buffer grows beyond `flush_hard_cap` | Force flush immediately, regardless of timer state. |
| Discord API rate limit on `context_window` fetch | Skip context window, flush batch without historical context. Log warning. |

### Sentinel Value

The sentinel is `[NO_REPLY]` (case-insensitive, trimmed). Chosen because:
- Unlikely to appear in natural agent output.
- Simple to detect with a string match (no regex needed).
- Easy for any LLM to produce reliably.
- Consistent with OpenClaw's established `NO_REPLY` convention.

## Consequences

### Benefits

- **Token efficient** — one LLM call per batch instead of per message. A
  channel with 10 messages in 60 seconds costs 1 invocation, not 10.
- **Better judgment** — agent sees a complete conversational thread, making
  it far more likely to know when a question was already answered (→ NO_REPLY)
  vs. when it should contribute.
- **Natural rate limiting** — the flush interval + jitter acts as inherent
  rate-limiting. Combined with `max_concurrent_flushes`, prevents cost spikes.
- **Agents behave like real team members** — aware of context, able to
  contribute organically.
- **User-configurable** — operators decide the cost/intelligence trade-off.
- **Fail-safe defaults** — disabled by default, requires explicit channel list,
  separate session pool prevents impact on normal operations.

### Trade-offs

- **Latency** — ambient replies are delayed by up to `flush_interval_seconds`.
  Acceptable because ambient replies are unsolicited; explicitly mentioned
  messages bypass the buffer entirely via the normal dispatch path.
- **Token cost still increases** — even with batching, each flush is an LLM
  call. Mitigations: per-channel opt-in, tunable flush interval/count,
  `max_concurrent_flushes` cap.
- **Potential for noise** — a poorly-tuned prompt or model may reply too
  eagerly. The batch format and explicit instructions mitigate this.
- **No retry on failure** — ambient batches are fire-and-forget. If a flush
  fails, those messages are lost context. Acceptable because ambient is
  best-effort by nature.

## Alternatives Considered

1. **Per-message dispatch (OpenClaw-style)** — dispatch every message
   individually. Rejected because it burns N invocations for N messages,
   most of which return NO_REPLY. Batch flush achieves the same goal with
   ~1/N the cost.
2. **Keyword pre-filter** — only dispatch if the message matches certain
   keywords. Rejected as primary mechanism because it defeats the purpose of
   intelligent, context-aware participation. May be added later as a cost
   optimization layer.
3. **Separate lightweight classifier** — use a small/cheap model to decide
   whether to invoke the main agent. Viable as a future enhancement but adds
   complexity for v1.
4. **Periodic summary mode** — batch N messages and summarize them before
   sending to agent. Rejected because the agent should see raw messages for
   full context; summarization loses nuance.

## Implementation Notes

### Reuse of Existing `Dispatcher` Infrastructure

OpenAB already has a **turn-boundary batching** system (PR #686,
`message_processing_mode` config) with `Dispatcher`, per-thread `mpsc::channel`,
and `consumer_loop`. Ambient Mode should extend this infrastructure rather than
building a parallel buffer system.

**What we reuse:**
- `Dispatcher::submit()` — message ingestion into bounded mpsc channel
- `BufferedMessage` struct — carries prompt, sender_context, attachments
- `consumer_loop` — long-lived task that drains and dispatches
- `dispatch_batch` → `pack_arrival_event` — packing N messages into
  `Vec<ContentBlock>` with repeated `<sender_context>` delimiters
- `ThreadHandle` lifecycle — idle eviction, SendError retry

**What differs for ambient mode:**

| Aspect | Turn-boundary (existing) | Ambient consumer |
|--------|-------------------------|-----------------|
| Drain trigger | Turn completion (greedy drain when agent finishes) | Timer (`flush_interval ± jitter`) OR count (`flush_max_messages`) |
| Key | `(platform, thread_id)` | `ambient:(platform, channel_id)` |
| Prerequisite | Message already passed mention/involved gate | Message has NO mention (new gate path) |
| Response handling | Normal post | `[NO_REPLY]` check before posting |
| Reactions | Full (👀🤔🔥🆗) | Suppressed |
| Session pool | Main pool | Ambient pool (separate `max_sessions`) |

**Not a new `message_processing_mode`:** ambient mode is a **parallel dispatch
path**, not a replacement for the primary processing mode. The existing
`message_processing_mode` enum (`per-message`, `per-thread`, `per-lane`) is
unchanged. Ambient mode is configured separately via `[ambient]` and runs as
a separate Dispatcher instance alongside the primary one.

```toml
# Existing modes (unchanged) — applies to mention-triggered messages:
message_processing_mode = "per-message"   # 1 msg → 1 turn
message_processing_mode = "per-thread"    # batch at turn boundary
message_processing_mode = "per-lane"      # batch at turn boundary, per-sender

# Ambient mode is independent — configured in [ambient], not here.
```

Ambient mode runs as a **separate Dispatcher instance** alongside the primary
one. The primary Dispatcher handles mention-triggered messages (using whatever
`message_processing_mode` is configured). The ambient Dispatcher handles
non-mentioned messages in ambient-enabled channels with a timer-based consumer.

### Ambient Consumer Loop

```rust
// Pseudocode — ambient consumer differs from turn-boundary consumer:
async fn ambient_consumer_loop(rx, config, flush_semaphore, channel_flushing) {
    loop {
        let first = match rx.recv().await {
            Some(msg) => msg,
            None => return,                    // channel closed, exit consumer
        };
        let deadline = Instant::now() + config.flush_interval_jittered();
        let mut batch = vec![first];

        loop {
            let remaining = deadline - Instant::now();
            match timeout(remaining, rx.recv()).await {
                Ok(Some(msg)) => {
                    batch.push(msg);
                    if batch.len() >= config.flush_max_messages { break; }
                    if batch.len() >= config.flush_hard_cap { break; }
                }
                Ok(None) => break,             // channel closed
                Err(_) => break,               // timer expired
            }
        }

        // Acquire global concurrency permit (blocks if max_concurrent_flushes reached)
        let _permit = flush_semaphore.acquire().await;

        // Mark channel as flushing (with safety timeout for auto-reset)
        let _flushing_guard = FlushingGuard::new(
            channel_flushing,
            config.flush_timeout_seconds,
        ); // auto-resets on drop OR timeout

        // Flush: dispatch batch with [NO_REPLY] system prompt
        match dispatch_ambient_batch(batch).await {
            Ok(response) => {
                if !response.trim().eq_ignore_ascii_case("[NO_REPLY]") {
                    // Atomic check-and-post: acquire post_guard to prevent
                    // race with mention cancellation
                    if let Some(_guard) = post_guard.try_acquire() {
                        post_response(response).await;
                    }
                    // else: mention arrived, cancelled — skip posting
                }
            }
            Err(e) => {
                warn!("ambient flush failed, discarding batch: {e}");
            }
        }

        // _flushing_guard dropped here — resets channel_flushing
        // _permit dropped here — releases semaphore slot
    }
}
```

### Other Implementation Details

- **No thinking message:** ambient dispatches do NOT send a "..." placeholder
  message. Unlike normal mention dispatch, ambient responses are posted directly
  as a single message (or discarded). This eliminates visual flickering in
  the channel.
- **`[NO_REPLY]` check:** applied after `stream_prompt` completes. If the
  trimmed final content equals `[NO_REPLY]` (case-insensitive), no message is
  posted.
- **Bot-to-bot loop prevention:** the bot's own messages never enter the buffer
  (existing `bot_id` check). **For ambient channels, `allow_bot_messages`
  defaults to `"off"`** regardless of the global setting — other bots' messages
  are excluded from the ambient buffer unless the operator explicitly opts in.
  Even if opted in, the existing `max_bot_turns` config (default 100, hard
  cap 1000 — applied at ingest, before `submit`) prevents infinite loops. The ambient system prompt
  also explicitly instructs: "Do not reply to other bot messages unless directly
  relevant to a human's question."
- **Mention detection reuse:** the existing `is_mentioned` logic in
  `Handler::message()` (src/discord.rs) fires **before** the buffer push.
  If mentioned, the message takes the normal dispatch path; the ambient buffer
  for that channel is discarded (not flushed).
- **Bot echo prevention:** `msg.author.id == bot_id` check (already exists in
  Handler::message) ensures bot's own messages never enter the buffer.
- **Reactions suppressed:** ambient dispatches skip `StatusReactionController`
  entirely — no 👀🤔🔥 on every channel message.
- **Serialization with normal dispatch:** the ambient session key
  (`ambient:discord:<channel_id>`) is different from mention session keys
  (`discord:<thread_id>`), so they never contend on the same session lock.
  However, concurrent reply prevention (see above) ensures at most one response
  is posted per channel — if a @mention arrives mid-flush, the ambient response
  is cancelled via the `post_guard` mechanism.
