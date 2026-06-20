# Emoji Reaction Mapping

Map emoji reactions to text commands. When a user reacts with a configured emoji on a message, OAB treats it as if the user sent the corresponding text message through the normal dispatch pipeline.

## Quick Start

Add to your `config.toml`:

```toml
[reactions.mapping]
":thumbsup:" = "OK"
":thumbsdown:" = "不行"
":arrows_counterclockwise:" = "重新 review"
":white_check_mark:" = "approve"
```

That's it. Reacting with 👍 on any message in a monitored thread now behaves as if you typed "OK".

## Config Keys

Keys can be:
- **Unicode emoji**: `"👍"`, `"✅"`, `"🔄"`
- **Discord/GitHub shortcodes**: `":thumbsup:"`, `":white_check_mark:"`

Shortcodes are resolved to unicode at config load time using the [gemoji](https://github.com/github/gemoji) database (same as GitHub and Discord). Unrecognized shortcodes are kept as-is and will never match a reaction.

## Requirements

1. **`GUILD_MESSAGE_REACTIONS` intent** — must be enabled in the Discord Developer Portal. This is a standard (non-privileged) intent; no approval required.

2. **Bot involvement** — reactions are only processed in threads where the bot has already participated (posted at least one message). This follows the same `allow_user_messages` gating as text messages.

## Gating & Access Control

Reaction mapping respects all existing access control policies:

| Policy | Behavior |
|--------|----------|
| `allowed_users` / `allow_all_users` | Denied users cannot trigger via reactions |
| `allow_bot_messages` | `off`/`mentions` → bot reactions ignored; `all` → allowed (respects `trusted_bot_ids`) |
| `allow_user_messages` | `mentions` → reactions disabled entirely; `involved`/`multibot-mentions` → only in threads where bot has participated |
| `multibot-mentions` + multibot thread | Only responds if the reaction is on **this bot's** message (implicit targeting) |
| Channel/thread allowlist | Same ACL as text messages |

## Limitations

- **Bot's own reactions are always ignored** — the bot's status emoji (👀, 🤔, 🆗, etc.) will never trigger mapping, even if they match a configured key. This prevents feedback loops.
- **Unicode emoji only** — custom server emoji (`:custom_name:`) are silently ignored.
- **Thread-only** (in `involved`/`multibot-mentions` mode) — reactions in non-thread channels are ignored unless `allow_user_messages` is set to a mode that allows them.
- **No reaction removal handling** — only `reaction_add` events trigger mapping; removing a reaction has no effect.
- **One emoji = one dispatch** — each mapped reaction dispatches independently through the pipeline.

## Examples

### PR Review Workflow

```toml
[reactions.mapping]
":white_check_mark:" = "approve"
":x:" = "reject"
":arrows_counterclockwise:" = "重新 review"
```

### Task Acknowledgment

```toml
[reactions.mapping]
":thumbsup:" = "OK"
":eyes:" = "looking into it"
":rocket:" = "deploying now"
```
