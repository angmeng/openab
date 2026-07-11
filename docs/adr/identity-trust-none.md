# ADR: Identity Trust-None Default & Trust Pyramid

- **Status:** Proposed
- **Date:** 2026-06-30
- **Author:** @chaodu-agent
- **Reviewers:** @pahud
- **Tracking issues:** #1262
- **Depends on:** [First-Class Per-Platform Configuration](first-class-platform-config.md) — per-platform `allowed_users` live in the first-class `[platform]` sections defined there.

---

## 1. Context & Decision

Flip the default trust model from **allow-all** to **identity trust-none**: when a
platform's `allowed_users` is empty and `allow_all_users` is not explicitly set to
`true`, deny all incoming messages and echo the sender their own ID so they can
request access.

Trust is enforced at a **single router-level gate** (`AdapterRouter::handle_message()`),
not scattered across adapters.

## 2. Motivation: trust-all default is insecure

All adapters currently auto-detect: empty `allowed_users` → `allow_all_users = true`.
A fresh deployment trusts **everyone** by default. For publicly discoverable bots
(e.g. anyone can DM a Telegram bot), this means any stranger can drive the agent.

## 3. Trust Pyramid (Defense in Depth)

Three layers with **clearly separated responsibilities** — only L1 and L3 are
security boundaries. L2 is operator scoping, not authorization.

```
                          ▲
                         ╱ ╲
                        ╱   ╲
                       ╱ L3  ╲         🔒 Layer 3: Identity Trust Control  (SECURITY)
                      ╱       ╲        allowed_users per platform — default DENY-ALL
                     ╱ sender  ╲       "Is THIS IDENTITY allowed?"  covers every path incl. DMs
                    ╱  allowed? ╲
                   ╱─────────────╲
                  ╱               ╲
                 ╱      L2         ╲    🔓 Layer 2: Channel/Group Scope Control  (NOT security)
                ╱                   ╲   allowed_channels, allowed_groups, allow_dm — default OPEN
               ╱  surface open?      ╲  "Which CONVERSATION SURFACES does the bot engage in?"
              ╱  (channel/group/DM)   ╲  optional operator scoping (noise/cost), not authorization
             ╱─────────────────────────╲
            ╱                           ╲
           ╱           L1                ╲   🔒 Layer 1: Platform Authentication  (SECURITY)
          ╱                               ╲  "Is this request REALLY from the platform?"
         ╱   webhook signature / JWT /     ╲
        ╱    secret token / IP range        ╲
       ╱─────────────────────────────────────╲
```

**Default posture:** L1 always on (edge) · **L2 open** unless explicitly disabled · **L3 deny-all** unless explicitly allowed.

### Layer 1: Platform Authentication (gateway layer — transport)

Verifies the request is genuinely from the platform, not spoofed. The **only**
security check at the gateway level.

| Platform | Auth Mechanism | How it works |
|----------|---------------|--------------|
| **Telegram** | Secret Token + IP Range | `X-Telegram-Bot-Api-Secret-Token` header; source IP in Telegram subnet (149.154.160.0/20, 91.108.4.0/22) |
| **LINE** | HMAC-SHA256 Signature | `X-Line-Signature` = HMAC(channel_secret, request_body) |
| **Feishu** | SHA256 Signature + Encrypt Key | SHA256(timestamp + nonce + encrypt_key + body) |
| **WeCom** | Token Signature + AES Decrypt | SHA1(sort(token, timestamp, nonce, encrypt)); AES-256-CBC body decryption |
| **Google Chat** | JWT (RS256) | Bearer token verified via Google JWKS; email claim = `chat@system.gserviceaccount.com` |
| **MS Teams** | JWT (OpenID Connect) | RS256 JWT verified via Bot Framework OpenID metadata + JWKS |
| **Slack** | Socket Mode WebSocket | App-Level Token (xapp-...) authenticates WS connection |
| **Discord** | Gateway WebSocket | Bot Token authenticates WS connection |

### Layer 2: Channel/Group Scope Control (core layer) — NOT a security boundary

Controls **which conversation surfaces** the bot engages in — channels, groups,
and DMs (`allow_dm`). Already implemented.

This is **operator scoping, not authorization**. The platform itself already
guarantees the bot only receives events from channels/groups it is a member of
with read permission — you cannot receive a message from a channel you were never
added to. So `allowed_channels` does not defend against "unauthorized channels"
(L1/the platform already does); it only narrows an over-permissioned bot to the
surfaces an operator wants it active in. Its value is noise/cost control.

**Default: OPEN** (`allow_all_channels = true`, `allow_dm = true`). Operators
*disable* surfaces only for hard scoping (e.g. a group-only bot sets
`allow_dm = false`).

**DMs are an L2 surface with a critical asymmetry:** unlike groups, a DM has **no
platform membership gate** — anyone can open a DM with a public bot. So when
`allow_dm = true`, the **only** protection on that path is L3. Enabling the DM
surface is an L2 decision; guarding who may use it is L3.

### Layer 3: Identity Trust Control (core layer) ← This ADR — the SECURITY gate

Controls which individual senders can trigger agent actions. Currently defaults
to allow-all; this ADR flips it to **deny-all**. This is the one authorization
boundary at the policy layer, and it covers **every** ingress path — including
DMs, where it is the sole protection.

**Why L2 must stay open for the deny UX to work:** the "echo your UID so you can
request access" reply only fires if an untrusted sender's message actually
*reaches* L3. If L2 defaulted closed (e.g. `allow_dm = false`), a new user would
be silently dropped at the scope layer with no path to onboard. L2-open + L3-deny
gives the intended self-service flow:

```
stranger messages the bot
  → L1 ✅ authentic platform request
  → L2 ✅ surface open by default (channel / DM)
  → L3 ❌ identity not in allowed_users
  → echo "⚠️ You're not trusted. Your ID: 123456789. Ask the admin to add you."
  → drop — no agent action
```

This flips **only L3** from today's allow-all to deny-all; L2 stays open. Minimal
breaking surface, maximal safety: nothing acts for an untrusted identity, yet
strangers still get a way to request access.

## 4. Decision

### 4.1 Trust-none default (identity layer)

```
Current:  empty allowed_users → allow_all_users = true  (TRUST ALL)
Proposed: empty allowed_users → allow_all_users = false (TRUST NONE)
```

When a message arrives from an untrusted sender:
1. Log the event (sender ID, platform, timestamp)
2. Reply with an echo message showing the sender their own ID
3. Do NOT dispatch to any agent

### 4.2 Trust check at router level (single gate)

Trust enforcement happens in **one place only**: `AdapterRouter::handle_message()`.
The gateway remains a pure transport layer (L1 only).

## 5. Architecture

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Telegram   │  │     LINE     │  │    Feishu    │  │  WeCom / GC  │
│   Webhook    │  │   Webhook    │  │  WebSocket   │  │   Webhook    │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │                 │
       ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│           openab-gateway — L1: Platform Authentication               │
│                                                                     │
│  ✅ Verify webhook signature / JWT / secret token / IP              │
│  ✅ Normalize → GatewayEvent                                        │
│  ✅ Forward ALL authenticated events                                 │
│  ❌ No user filtering (L3 is in core)                               │
└────────────────────────────┬────────────────────────────────────────┘
                             │ WebSocket
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    openab-core — L2 + L3                              │
│                                                                     │
│  ┌───────────┐ ┌───────────┐ ┌─────────────────────────────┐       │
│  │  Discord  │ │   Slack   │ │  GatewayAdapter             │       │
│  │  Handler  │ │  Handler  │ │  (TG/LINE/Feishu/WeCom/GC)  │       │
│  └─────┬─────┘ └─────┬─────┘ └──────────────┬──────────────┘       │
│        └──────────────┼──────────────────────┘                      │
│                       ▼                                             │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ 🔒 AdapterRouter::handle_message()                            │  │
│  │                                                               │  │
│  │   L2: scope check (optional, default-open; channel/group/DM)  │  │
│  │   L3: TrustConfig::is_allowed(platform, sender_id) — DENY dflt │  │
│  │                                                               │  │
│  │   if denied → log + echo sender ID → RETURN                   │  │
│  │   if allowed → dispatch to ACP ✅                              │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                       │                                             │
│                       ▼                                             │
│              ┌─────────────────┐                                    │
│              │  ACP Session    │                                    │
│              └─────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Per-platform TrustConfig

```rust
pub struct TrustConfig {
    // L2 — scope control (NOT security). Defaults OPEN.
    pub allow_all_channels: bool,           // default true
    pub allowed_channels: HashSet<String>,
    pub allow_dm: bool,                      // default true (DM surface open)

    // L3 — identity trust (THE security gate). Defaults DENY-ALL.
    pub allow_all_users: bool,               // explicit opt-in, default false
    pub allowed_users: HashSet<String>,
}

impl TrustConfig {
    /// L2: is this conversation surface in scope? (default-open)
    pub fn surface_allowed(&self, channel_id: &str, is_dm: bool) -> bool {
        if is_dm {
            return self.allow_dm;
        }
        self.allow_all_channels || self.allowed_channels.contains(channel_id)
    }

    /// L3: is this identity trusted? (default-deny)
    pub fn is_allowed(&self, sender_id: &str) -> bool {
        self.allow_all_users || self.allowed_users.contains(sender_id)
    }
}

/// Router holds one TrustConfig per platform
pub struct PlatformTrustConfigs {
    configs: HashMap<String, TrustConfig>,  // keyed by platform name
}

impl PlatformTrustConfigs {
    pub fn get(&self, platform: &str) -> &TrustConfig {
        self.configs.get(platform).unwrap_or(&DEFAULT)
    }
}

/// Default: L2 open (act anywhere the platform allows), L3 deny-all.
static DEFAULT: TrustConfig = TrustConfig {
    allow_all_channels: true,
    allowed_channels: HashSet::new(),
    allow_dm: true,
    allow_all_users: false,                  // trust-none on identity
    allowed_users: HashSet::new(),
};
```

### Trait & Type Changes (no new trait)

The trust gate is **uniform logic**, not per-platform behavior, so it is a plain
`TrustConfig` + a router method — **not** a `ChatAdapter` method and **not** a new
trait (see Rejected Alternatives). The `ChatAdapter` trait is unchanged:
`platform()` already keys the `TrustConfig` and `send_message()` already performs
the echo. What changes are the **shared data carriers** that feed the router:

**1. `MessageContext` — carry structured sender identity (not opaque JSON).**
Today the router only receives `sender_json` (a serialized blob); it would have to
parse JSON to read `sender_id`. Pass the `SenderContext` struct so L3 can read
`sender_id` / `is_bot` directly (the router can still serialize it for the agent):

```rust
pub struct MessageContext {
    pub thread_channel: ChannelRef,
    pub sender: SenderContext,        // ← was: sender_json: String
    pub prompt: String,
    pub extra_blocks: Vec<ContentBlock>,
    pub trigger_msg: MessageRef,
    pub other_bot_present: bool,
}
```

**2. `ChannelRef` — add an `is_dm` flag.**
DM detection is platform-specific *structural* knowledge the adapter already has at
construction time (Discord DM channel vs Telegram private chat vs Slack IM), so it
is a **field the adapter populates**, not a trait method. This lets the router
evaluate `allow_dm` (L2) uniformly:

```rust
pub struct ChannelRef {
    pub platform: String,
    pub channel_id: String,
    pub is_dm: bool,                  // ← new; excluded from Hash/Eq like origin_event_id
    pub thread_id: Option<String>,
    pub parent_id: Option<String>,
    pub origin_event_id: Option<String>,
}
```

**3. Remove scattered trust checks from adapters.**
The real refactor is deleting the `allowed_channels` / `allowed_users` checks
currently in `discord.rs`, `slack.rs`, and `gateway.rs`, and letting the data flow
into `MessageContext` / `ChannelRef` so the single router gate is the only place
trust is enforced — this is what makes L3 un-bypassable. Structural concerns
(thread detection, @mention gating, multibot detection, bot-ownership) **stay in
the adapters** — they are not trust.

### Echo reply on deny

```rust
// In AdapterRouter::handle_message()
let echo = format!(
    "⚠️ You are not in the trusted list.\nYour ID: {}\nPlease ask the admin to add you to [{}].allowed_users.",
    msg.sender_id,
    adapter.platform()
);
let _ = adapter.send_message(&msg.channel, &echo).await;
```

## 6. Migration

### Breaking change

Existing deployments with no `allowed_users` configured will stop accepting messages.

### Migration path

```toml
# Before (implicit trust-all):
[discord]
bot_token = "..."

# After (explicit trust-all to keep old behavior):
[discord]
bot_token = "..."
allow_all_users = true
```

## 7. Implementation Plan

1. **Define `TrustConfig` + `PlatformTrustConfigs`** in `openab-core`
2. **Extend carriers** — add `is_dm` to `ChannelRef`; pass `SenderContext` in `MessageContext`
3. **Wire trust gate into `AdapterRouter::handle_message()`** — L2 (scope) then L3 (identity), single check point
4. **Remove scattered trust checks** from:
   - `is_denied_user()` in Discord EventHandler
   - `should_skip_event()` user/channel filter in `gateway.rs`
   - `allowed_users` checks in Slack / Feishu adapters
5. **Add echo reply** on deny using `ChatAdapter::send_message()`
6. **Update `config.toml.example`** and docs; migration guide in release notes

## 8. Rejected Alternatives

### Per-adapter `InboundGate` trait

Each adapter implements `is_trusted_sender()`. Rejected because:
- Trust logic is identical across all platforms (`allowed_users.contains(id)`)
- Forces N identical implementations with no polymorphic benefit
- New adapter forgetting to implement = security hole
- Router-level gate is impossible to bypass by construction

### Trust check at gateway layer

Gateway adapters filter untrusted senders before forwarding. Rejected because:
- Gateway is transport (L1) — mixing L3 policy violates layer separation
- Trust config lives in core's `config.toml`, not gateway env vars
- Reply capability already wired in core via `ChatAdapter::send_message()`

### Treating L2 (channel) as a security layer

Rejected: the platform already enforces channel/group membership, so L2 is
operator scoping, not authorization. Modeling it as security would wrongly imply
DMs are protected by channel rules — they are not (a DM has no membership gate;
only L3 protects it).

### L2 default-closed

Rejected: closing surfaces by default breaks the echo/request-access onboarding
flow (an untrusted sender would be dropped before reaching L3 and never learn how
to request access).
