# Ambient Mode

Ambient mode allows your bot to passively listen to all messages in configured channels and autonomously decide whether to respond. Unlike the default @mention mode, the bot observes the full conversation flow and only speaks up when it has something valuable to add.

## How It Works

1. Messages in configured channels (that are **not** @mentions) are buffered per-channel.
2. When a **time trigger** (`flush_interval_seconds`) or **count trigger** (`flush_max_messages`) fires, the batch is sent to the LLM.
3. The LLM evaluates the conversation and either:
   - **Replies** with a helpful response → posted to the channel.
   - **Returns `[NO_REPLY]`** → silently suppressed, nothing is posted.
4. If someone **@mentions** the bot in an ambient channel, the buffer is discarded and the mention is handled normally (immediate response).

## Configuration

```toml
[ambient]
enabled = true
flush_interval_seconds = 60      # Time trigger (±20% jitter applied)
flush_max_messages = 10           # Count trigger
flush_hard_cap = 50               # Safety cap on buffer size
max_concurrent_flushes = 3        # Global LLM concurrency limit
flush_timeout_seconds = 120       # Safety timeout per flush
instructions_file = "~/.openab/config/ambient.md"  # Custom system prompt (optional)
debug = false                     # ⚠️ Only enable in test channels (exposes prompt + messages)

[ambient.discord]
channels = ["1234567890"]         # Channel IDs to monitor (and their threads)
allow_bot_messages = true         # Include other bots' messages in buffer (default)
```

### Custom Instructions

The `instructions_file` field points to a Markdown file whose content is used as the ambient system prompt. This works like a user-defined `.cursorrules` or `CLAUDE.md` — you control how the bot behaves in ambient mode by editing one file.

- **Default path:** `~/.openab/config/ambient.md`
- **Max length:** First 2000 characters are used; content beyond that is truncated.
- **Fallback:** If the file does not exist, the built-in default instructions are used.
- **Restart required:** The file is read once at startup. To apply changes, restart the bot.

Example `~/.openab/config/ambient.md`:

```markdown
You are passively observing a Discord channel.

- Reply EXACTLY `[NO_REPLY]` if you have nothing to add
- Only speak up for technical corrections or when directly asked
- Keep replies concise
```

### Configuration fields

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Master switch. Must be explicitly enabled. |
| `debug` | `false` | When true, posts system prompt + transcript to channel on each flush and lets `[NO_REPLY]` responses through (not suppressed). ⚠️ **Only use in private/test channels** — exposes system prompt and buffered messages. |
| `flush_interval_seconds` | `60` | Seconds between time-based flushes. ±20% jitter prevents thundering herd. Min: 1. |
| `flush_max_messages` | `10` | Flush when this many messages accumulate. Min: 1. |
| `flush_hard_cap` | `50` | Maximum buffer size. Messages beyond this are dropped. Min: 1. |
| `max_concurrent_flushes` | `3` | Max simultaneous LLM calls across all ambient channels. Min: 1. |
| `flush_timeout_seconds` | `120` | Safety timeout — resets flushing state if exceeded. Clamped to [5, 600]. |
| `instructions_file` | `~/.openab/config/ambient.md` | Path to custom instructions file. First 2000 chars used as system prompt. Falls back to built-in default if missing. |
| `channels` | `[]` | Explicit channel allowlist (required). Empty = ambient disabled. |
| `allow_bot_messages` | `true` | Whether other bots' messages enter the ambient buffer. |

> **Threads are observed by default.** Messages in **threads** whose parent is a configured channel are buffered too (most OpenAB conversation happens in auto-created threads, not the parent channel). **Both** bot-owned and non-owned threads are observed — the bot passively follows all thread conversation under an ambient channel. An @mention in any thread discards its buffer and triggers immediate dispatch, so there is no double-reply. Each thread batches independently (keyed by the thread ID).

> **`[ambient.discord].channels` vs `[discord].allowed_channels`** — these are independent allowlists with an OR relationship:
>
> | Config | Purpose | Effect |
> |--------|---------|--------|
> | `[discord].allowed_channels` | Normal dispatch | Bot responds to @mentions and direct messages in these channels/threads |
> | `[ambient.discord].channels` | Passive observation | Bot silently buffers messages (no @mention required) and decides whether to reply |
>
> A channel can appear in one or both. Ambient observation does **not** require the channel to also be in `allowed_channels`.
>
> **@mention always works in ambient channels.** Even if a channel is only in `[ambient.discord].channels` (not in `allowed_channels`), an @mention still triggers immediate normal dispatch — the ambient buffer is discarded and the bot responds directly. The only difference between the two configs is behavior **without** a mention: `allowed_channels` ignores unmentioned messages entirely, while `ambient.discord.channels` passively observes them.

### Reserved fields (v2, not yet enforced)

| Field | Default | Description |
|-------|---------|-------------|
| `context_window` | `20` | Historical messages to fetch before each batch (not yet implemented). |
| `pool.max_sessions` | `5` | Max concurrent ambient sessions (not yet enforced). |
| `pool.session_ttl_minutes` | `60` | Session inactivity timeout (not yet enforced). |
| `pool.context_flushes` | `3` | Rolling flush history window (not yet enforced). |

## Behavior

### @mention priority

When someone @mentions the bot in an ambient channel:
1. The ambient buffer is immediately invalidated (current batch discarded).
2. The mention is handled via normal dispatch (immediate response).
3. After the mention is handled, ambient buffering resumes for new messages.

Buffered messages that arrived before the mention are **not lost** — they carry into the next ambient cycle.

### [NO_REPLY] filtering

The bot uses a system prompt that instructs it to respond with `[NO_REPLY]` when it has nothing to add. This sentinel is intercepted **before delivery** — it never appears in the channel. The filtering uses a capture adapter that forces non-streaming mode to ensure the full response is evaluated before any message is sent.

### Session isolation

Ambient sessions use the namespace `ambient:discord:<channel_id>`, separate from normal dispatch sessions. There is no collision with @mention sessions.

### Cost control

- **Jittered intervals** prevent all channels from flushing simultaneously.
- **Global semaphore** caps concurrent LLM calls (default: 3).
- **`[NO_REPLY]`** means most flushes produce no visible output (only one LLM call, no channel message).
- **`enabled = false`** default means zero cost until explicitly opted in.

## Limitations (v1)

| Limitation | Description | Planned fix |
|-----------|-------------|-------------|
| Tool access | Ambient flushes have full tool access (same as @mention). | v2: restricted dispatch target |
| In-flight cancel | A @mention during LLM generation cannot stop the ambient response mid-stream. | v2: `tokio::select!` preemption |
| Consumer supervision | If a consumer task panics, that channel's ambient is permanently disabled until restart. | v2: health check + respawn |
| No history fetch | `context_window` (Discord API history before batch) is not yet implemented. | v2 |
| No cooldown | No minimum interval between consecutive flushes for a single channel. | v2 |

## Example

Minimal config to enable ambient mode on one channel:

```toml
[ambient]
enabled = true

[ambient.discord]
channels = ["1490282656913559673"]
```

This uses all defaults: 60s flush interval, max 10 messages per batch, 3 concurrent flushes.
