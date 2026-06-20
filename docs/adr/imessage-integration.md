# ADR: iMessage Integration via macOS Gateway

- **Status:** Proposed
- **Date:** 2026-06-18
- **Author:** @chaodu-agent
- **Reviewers:** @pahud
- **Tracking issues:** (none yet)

---

## 1. Context & Decision

Enable OAB agents to receive and respond to iMessage conversations, allowing users to interact with their agent team through Apple's native messaging platform. This extends OAB's multi-platform adapter architecture (see [ADR: Multi-Platform Adapters](./multi-platform-adapters.md)) to a platform that lacks an official API.

**Decision:** Implement iMessage as a built-in adapter within the Custom Gateway, running natively on macOS. The gateway polls `chat.db` for inbound messages and sends outbound via AppleScript, connecting to OAB core (running in OrbStack/containers) over WebSocket. This is an iMessage-only deployment — no separate bridge binary needed.

---

## 2. Motivation

- Users want to interact with OAB agents from iMessage — the default messaging app on iOS/macOS with 1B+ active users
- iMessage offers a more personal, low-friction interaction surface compared to Discord/Slack
- Apple provides **no official iMessage API** — all third-party integrations rely on macOS Messages.app as a bridge
- The existing `agy-acp` component already proves the "poll SQLite DB" pattern works reliably in OAB
- iMessage bridging requires macOS-native access (`chat.db` + AppleScript) — it **cannot** run inside a container

### Why iMessage Over Existing Channels

iMessage fills a gap that LINE, Telegram, and Slack cannot:

| Advantage | Detail |
|-----------|--------|
| **North America default** | iPhone-to-iPhone messaging uses iMessage automatically — no app install required. Dominant in US/Canada market. |
| **Zero-friction reach** | Only requires a phone number. No "add friend" / "find bot username" step. Ideal for cold outreach: customer support, appointment reminders, order notifications. |
| **High trust signal** | Conversations appear alongside friends/family in the native Messages.app. Users perceive it as personal communication, not "yet another bot." |
| **Apple ecosystem integration** | Siri dictation, Apple Watch, CarPlay, Focus Mode — notifications are not filtered as "app push." |
| **No new app required** | Enterprise scenario: clients/employees already have iPhones; no need to mandate LINE/Telegram/Slack installation. |

**When NOT to use iMessage:**

| Scenario | Better choice |
|----------|--------------|
| Asia market (Taiwan/Japan/Thailand) | LINE |
| Developer/tech communities | Discord / Telegram |
| Cross-platform users (Android + iPhone) | Telegram / WhatsApp |
| Rich UI (buttons, carousels) | LINE / Slack |
| Group bot interactions | Discord / Slack |
| Message editing / threading | Discord / Slack |

**Summary:** iMessage's core value is **North American market + zero-install barrier + high trust perception**. For Asian markets or technical communities, LINE/Telegram remain more practical.

---

## 3. Architecture

### 3.1 Primary: Custom Gateway on macOS Host + OAB in OrbStack

```
┌────────────────┐
│  iPhone User   │
│  (iMessage)    │
└───────┬────────┘
        │
        ▼
═══════════════════
║ Apple iMessage  ║
║    Network      ║
═══════════════════
        │
        ▼
┌──────────────────────────────────────────┐
│  macOS host (Mac mini / MacBook)         │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │  Messages.app                      │  │
│  │  ~/Library/Messages/chat.db        │  │
│  └──────────────┬─────────────────────┘  │
│                 │ poll every 100-500ms    │
│  ┌──────────────▼─────────────────────┐  │
│  │  Custom Gateway (native binary)    │  │
│  │                                    │  │
│  │  ├── iMessage adapter (built-in)   │  │
│  │  │   ├── poll chat.db (SQLite)     │  │
│  │  │   └── send via osascript        │  │
│  │  └── WebSocket client → OAB        │  │
│  └──────────────┬─────────────────────┘  │
│                 │                         │
└─────────────────┼─────────────────────────┘
                  │ WebSocket (outbound connection)
                  ▼
┌──────────────────────────────────────────┐
│  OrbStack (Linux containers)             │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │  OAB Core                          │  │
│  │  ├── AdapterRouter                 │  │
│  │  ├── SessionPool                   │  │
│  │  └── ACP agents                    │  │
│  └────────────────────────────────────┘  │
│                                          │
└──────────────────────────────────────────┘
```

**Why the gateway must run on macOS host (not in a container):**

1. **`chat.db` access** — macOS TCC (Transparency, Consent, Control) grants Full Disk Access per-app. A containerized process cannot inherit FDA permissions.
2. **SQLite WAL locking** — `chat.db` is held by Messages.app with WAL lock. A cross-VM mount risks lock contention.
3. **AppleScript IPC** — `osascript` communicates with Messages.app via macOS IPC (Apple Events). This only works from the same macOS session.

### 3.2 Alternative: Photon Spectrum (Cloud, No Mac Required)

```
┌────────────────┐        ═══════════════        ┌──────────────────┐
│  iPhone User   │───────║ Apple iMessage ║──────│  Photon Cloud     │
│  (iMessage)    │        ═══════════════        │  (managed Mac群)  │
└────────────────┘                               └────────┬─────────┘
                                                          │ gRPC stream
                                                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  OrbStack (Linux containers)                                        │
│                                                                     │
│  ┌──────────────────┐     ┌───────────────────────────────────────┐ │
│  │ Spectrum Sidecar  │────►│ OAB Core                             │ │
│  │ (Node.js/Bun)    │     │                                       │ │
│  └──────────────────┘     └───────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

Photon Spectrum manages the Mac infrastructure — no local Mac needed. Useful as fallback if Apple restricts local `chat.db` access.

---

## 4. How iMessage Bridging Works

Apple does not provide an iMessage API. The gateway's iMessage adapter relies on two macOS-native mechanisms:

### 4.1 Receiving Messages (Inbound)

Messages.app writes all received messages to a local SQLite database at `~/Library/Messages/chat.db`. The adapter polls this DB for new rows:

```sql
SELECT rowid, text, handle_id, date, is_from_me, cache_roomnames
FROM message
WHERE rowid > ?last_seen_rowid
ORDER BY rowid ASC
```

This is the **same pattern as `agy-acp`**, which polls `conversations/*.db` for new `step_payload` rows. Both use:
- SQLite read-only connection with WAL mode
- Polling interval (100-500ms)
- Monotonically increasing row ID as cursor

### 4.2 Sending Messages (Outbound)

Messages are sent by invoking AppleScript via `osascript`:

```applescript
tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to participant "+1234567890" of targetService
    send "Hello from OAB" to targetBuddy
end tell
```

For group chats, send to a specific chat ID:

```applescript
tell application "Messages"
    set targetChat to chat id "iMessage;+;chat123456"
    send "Hello from OAB" to targetChat
end tell
```

### 4.3 Comparison with agy-acp

| Aspect | agy-acp | Custom Gateway (iMessage adapter) |
|--------|---------|-----------------------------------|
| Monitored program | `agy` (Gemini CLI) | Messages.app |
| Data source | `conversations/*.db` | `~/Library/Messages/chat.db` |
| Poll mechanism | `WHERE idx > last` every 100ms | `WHERE rowid > last` every 100-500ms |
| Data format | protobuf `step_payload` field 20.1 | `attributedBody` blob / plain `text` column |
| Send mechanism | spawn `agy -p "prompt"` | spawn `osascript` (AppleScript) |
| Output | JSON-RPC streaming notifications | WebSocket to OAB core (direct) |

---

## 5. Message Flow

### Inbound (User → Agent)

```
1. User sends iMessage from iPhone
2. Apple iMessage network delivers to Mac's Messages.app
3. Messages.app writes row to chat.db
4. Gateway's iMessage adapter detects new row (poll)
5. Adapter formats as OpenAB inbound event:
   { "platform": "imessage", "sender": "+1234567890",
     "text": "...", "channel_id": "iMessage;-;+1234567890" }
6. Gateway sends event to OAB core via WebSocket
7. AdapterRouter dispatches to SessionPool → agent
```

### Outbound (Agent → User)

```
1. Agent produces response via ACP session
2. OAB core sends outbound event via WebSocket to gateway
3. Gateway's iMessage adapter invokes AppleScript
4. Messages.app → Apple network → User's iPhone
```

---

## 6. Platform Limitations

- No message editing (iMessage supports "edit" natively on iOS 16+ but AppleScript cannot trigger it)
- No threading (conversations are flat)
- Reactions map to tapbacks (❤️, 👍, 👎, 😂, ‼️, ❓) — only 6 options
- No typing indicators via AppleScript (Spectrum Cloud supports this)
- No structured @mention field (see §6.1 below)

### 6.1 @Mention Detection (Group Chat)

iMessage supports @mentions (iOS 14+, displayed as bold blue text), but `chat.db` does **not** expose them as a structured column. The mention data is embedded inside the `attributedBody` blob — a serialized `NSAttributedString` (NSKeyedArchiver / typedstream format).

**To extract mentions, the adapter must:**

1. Read `message.attributedBody` (binary blob)
2. Decode NSKeyedArchiver binary plist
3. Locate ranges where `__kIMMessagePartAttributeName` = 1 (indicates a mention)
4. Extract the mentioned handle ID from `__kIMMentionConfirmedMention`

**Comparison with other platforms:**

| Platform | Mention detection | Complexity |
|----------|-------------------|-----------|
| Discord | `message.mentions` array | Trivial — structured field |
| LINE | `mentionees` in webhook payload | Trivial — structured field |
| Slack | `<@BOT_ID>` in text + `app_mention` event type | Easy — text pattern |
| iMessage | Parse binary `attributedBody` blob | Hard — undocumented binary format |

**Implications for group chat:**
- **1:1 conversations (Phase 1):** No mention detection needed — all messages are directed at the bot
- **Group chat (Phase 3):** Adapter must parse `attributedBody` to know when the bot is mentioned, or fall back to keyword-prefix trigger (e.g. `/ask ...`)
- The `attributedBody` format is undocumented and may change across macOS versions — Rust `plist` crate can decode the binary plist, but the internal schema requires reverse-engineering
- **Outbound:** AppleScript `send` does not support sending @mentions — bot replies are plain text only

---

## 7. Config Design

```toml
[gateway.imessage]
enabled = true
poll_interval_ms = 200
chat_db_path = "~/Library/Messages/chat.db"  # default; override for testing

# OAB core WebSocket endpoint (OrbStack container)
[gateway]
oab_ws_url = "ws://localhost:8080/ws"  # OrbStack port-forwards to OAB

# Spectrum mode (alternative — no local Mac needed)
[gateway.imessage.spectrum]
enabled = false
project_id = "${PHOTON_PROJECT_ID}"
project_secret = "${PHOTON_PROJECT_SECRET}"
```

---

## 8. Security Considerations

### 8.1 macOS Host Risks

| Risk | Mitigation |
|------|-----------|
| No container isolation | Dedicated macOS user account with minimal privileges |
| Mac compromise → iMessage access | Gateway is a thin adapter; all agent logic stays in OrbStack |
| Apple account credential exposure | Use a dedicated Apple ID, not personal |
| AppleScript injection | Sanitize all outbound text; no user input in script template |
| Full Disk Access requirement | Grant FDA only to the gateway binary, not the user shell |

### 8.2 Photon Spectrum Risks

| Risk | Mitigation |
|------|-----------|
| Third-party dependency | Photon manages Apple infra; you trust their SLA |
| Shared phone numbers (free tier) | Upgrade to dedicated line ($250/mo) for consistent identity |
| Apple ToS enforcement | Photon assumes this risk; self-hosted is fallback |
| gRPC stream reliability | SDK handles auto-reconnect; SMS/RCS fallback |

### 8.3 General

- Messages contain PII — gateway and OAB must encrypt in transit (WebSocket over TLS)
- Rate limiting on outbound to avoid Apple throttling/blocking
- Gateway binary should be code-signed to satisfy macOS Gatekeeper

---

## 9. Deployment

### Primary: macOS + OrbStack

```
macOS host:
  - Custom Gateway binary (Rust, native arm64/x86_64)
  - Runs as launchd service (auto-restart, auto-start on boot)
  - Requires: Full Disk Access for chat.db, Accessibility for AppleScript

OrbStack (on same Mac):
  - OAB Core container (Linux)
  - Exposes WebSocket endpoint on localhost
```

**Why not containerize the gateway:**
- `chat.db` cannot be volume-mounted into Linux VMs (macOS TCC blocks cross-process FDA)
- AppleScript IPC (`osascript`) requires native macOS session — unavailable in containers
- SQLite WAL mode may conflict with cross-VM file locking

| Deployment | Where | Cost | Complexity |
|-----------|-------|------|------------|
| Primary (this ADR) | Mac (gateway) + OrbStack (OAB) | ~$600 Mac mini | Low |
| Spectrum alternative | OrbStack only (no Mac needed) | $0-250/mo | Low |

---

## 10. Implementation Phases

| Phase | Scope | Dependencies |
|-------|-------|-------------|
| **Phase 1** | iMessage adapter in Custom Gateway: poll chat.db, send via AppleScript, WebSocket to OAB | Custom Gateway ([ADR](./custom-gateway.md)) |
| **Phase 2** | Spectrum sidecar adapter as alternative (no Mac required) | Photon account |
| **Phase 3** | Rich features: tapback reactions, group chat support, @mention parsing, attachment handling | Phase 1 |

---

## 11. Apple Compliance & Risks

Apple does not provide an official iMessage API. All known approaches rely on:

1. **macOS Messages.app + SQLite** — reading `chat.db` (requires Full Disk Access)
2. **AppleScript automation** — sending via `osascript` (uses public macOS APIs)
3. **No protocol reverse-engineering** — no private framework usage

**Current landscape (as of 2026-06):**
- BlueBubbles, AirMessage have operated for years without Apple enforcement
- Photon Spectrum launched April 2026, commercially offering managed iMessage lines
- Apple has not issued cease-and-desist to any known project
- Risk: Apple could restrict `chat.db` access or AppleScript Messages automation in a future macOS update

**Mitigation:** The adapter architecture is modular — if Apple blocks the self-hosted path, Spectrum Cloud remains as fallback (they absorb the compliance risk). If both paths are blocked, the adapter can be disabled without affecting other OAB platforms.

---

## 12. Open Questions

| # | Question | Options | Notes |
|---|----------|---------|-------|
| 1 | Poll interval default | 100ms vs 200ms vs 500ms | Tradeoff: latency vs CPU. agy-acp uses 100ms |
| 2 | Group chat support in Phase 1? | Yes / defer to Phase 3 | Recommend defer — no structured @mention field means bot can't reliably detect when addressed. 1:1 is the sweet spot. |
| 3 | Should gateway run as launchd service? | Yes (auto-restart) / manual | launchd is macOS best practice for daemons |
| 4 | Photon free tier shared numbers acceptable? | Yes for POC / require dedicated | Shared numbers may confuse recipients |

---

_This ADR was drafted based on research into Photon Spectrum (photon-hq/spectrum-ts), imessage-kit, and the existing agy-acp polling pattern in OAB._
