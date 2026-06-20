# ADR: Separate Binaries with Opt-In Unified Build

- **Status:** Accepted
- **Date:** 2026-06-15
- **Author:** @pahud
- **Amends:** Deployment model from [ADR: Custom Gateway](./custom-gateway.md) — outbound-only + gateway sidecar remains the default; unified mode is an opt-in alternative path.
- **Implementation:** [PR #1146](https://github.com/openabdev/openab/pull/1146)

---

## 1. Context & Problem

Today, supporting webhook-based platforms (Telegram, LINE, Feishu, Google Chat, WeCom, Teams) requires running **two processes** — `openab` core and `openab-gateway` — wired together via WebSocket, often in the same pod with a shared volume for colocate-mode media passing.

This creates operational friction:

- **Two containers** in a single pod (or two separate services)
- **Shared volume** required for media colocate mode
- **WebSocket wiring** between core and gateway (auth token, reconnect logic)
- **Version matrix** — gateway releases independently, version mismatches cause subtle bugs
- **Double serialization** — every message is serialized to JSON, sent over WS, then deserialized

For most users who just want "Discord + Telegram in one bot", the two-process model is unnecessary complexity.

---

## 2. Decision

Restructure the project as a **Cargo workspace** that keeps the two-binary model by default (core + standalone gateway), while allowing users to compile everything into a **single unified binary** via a feature flag — requiring zero code changes, only `--features unified` or a Cargo config toggle.

### Workspace Layout

```
openab/
├── Cargo.toml              (workspace root + binary crate)
├── src/                    (core modules — Discord, Slack, ACP, Dispatcher,
│                            SessionPool, ChatAdapter trait)
├── crates/
│   └── openab-gateway/     (webhook adapters: Telegram, LINE, Feishu,
│                             Google Chat, WeCom, Teams — impl ChatAdapter)
└── gateway/                (standalone gateway binary — kept for backward compat)
```

> **Note:** Extracting `openab-core` as a separate library crate was considered but deferred
> to Phase 2 — it would require changing visibility on 30+ internal modules. The current
> approach keeps core in the root crate and only extracts gateway adapters.

### Feature Flags (on the root binary crate)

```toml
[features]
# Default: core adapters (Discord + Slack) + infrastructure.
default = ["discord", "slack", "secrets-aws", "agentcore"]

# Opt-in: compile all gateway adapters into a single unified binary
unified = ["telegram", "line", "feishu", "googlechat", "wecom", "teams"]

# Core adapters (directly in root crate)
discord    = ["dep:serenity"]
slack      = []

# Infrastructure
secrets-aws = ["dep:aws-sdk-secretsmanager", "dep:aws-config"]
agentcore   = ["dep:aws-config", "dep:aws-sigv4", ...]

# Gateway adapters (each pulls in the gateway crate as optional dep)
telegram   = ["dep:openab-gateway", "openab-gateway/telegram"]
line       = ["dep:openab-gateway", "openab-gateway/line"]
feishu     = ["dep:openab-gateway", "openab-gateway/feishu"]
googlechat = ["dep:openab-gateway", "openab-gateway/googlechat"]
wecom      = ["dep:openab-gateway", "openab-gateway/wecom"]
teams      = ["dep:openab-gateway", "openab-gateway/teams"]
```

The `dep:openab-gateway` syntax requires the gateway crate as an optional dependency:

```toml
[dependencies]
openab-gateway = { path = "crates/openab-gateway", default-features = false, optional = true }
```

Users who want the unified single-binary experience:
```bash
cargo build --features unified   # all adapters in one binary
```

Or pick specific adapters:
```bash
cargo build --features telegram,line   # just these two added to core
```

### Runtime Activation

Adapters start **only if their config section is present and has required fields** (e.g., `bot_token`). Compiled-in but unconfigured adapters have zero runtime overhead.

```toml
# Only Discord and Telegram start — others dormant
[discord]
bot_token = "${DISCORD_BOT_TOKEN}"
allowed_channels = ["123456789"]

[telegram]
bot_token = "${TELEGRAM_BOT_TOKEN}"
```

---

## 3. Architecture — Before & After

### Before (two-process model)

```
┌─────────────────────────────┐     ┌───────────────────────────────────┐
│  openab core                │     │  openab-gateway (sidecar)         │
│                             │     │                                   │
│  Discord ──┐                │     │  Telegram ──┐                     │
│  Slack ────┤► Dispatcher    │◄─WS─┤  LINE ──────┤► axum → GatewayEvent│
│            │                │     │  Feishu ────┘                     │
│  GatewayAdapter (WS client) │     │                                   │
└─────────────────────────────┘     └───────────────────────────────────┘
        shared volume for media colocate
```

### After — Default (two binaries, same as today but workspace-structured)

```
┌─────────────────────────────┐     ┌───────────────────────────────────┐
│  openab core                │     │  openab-gateway                   │
│                             │     │                                   │
│  Discord ──┐                │     │  Telegram ──┐                     │
│  Slack ────┤► Dispatcher    │◄─WS─┤  LINE ──────┤► axum → GatewayEvent│
│            │                │     │  Feishu ────┘                     │
│  GatewayAdapter (WS client) │     │                                   │
└─────────────────────────────┘     └───────────────────────────────────┘
```

### After — Opt-in Unified (`--features unified`)

```
┌────────────────────────────────────────────────────────────────┐
│  openab (single binary)                                        │
│                                                                │
│  Discord ────┐                                                 │
│  Slack ──────┤                                                 │
│  Telegram ───┤► Dispatcher → SessionPool → ACP (child process) │
│  LINE ───────┤                                                 │
│  Feishu ─────┘                                                 │
│                                                                │
│  axum HTTP (:9090) — only starts if webhook adapters active    │
└────────────────────────────────────────────────────────────────┘
```

---

## 4. Message Flow Change

```
BEFORE:
  Platform → HTTP → gateway/telegram.rs → serialize GatewayEvent
    → WebSocket → core/gateway.rs → deserialize → Dispatcher.submit()

AFTER:
  Platform → HTTP → src/telegram.rs → Dispatcher.submit() (direct call)
```

Reply path is similarly direct — the adapter calls the platform API in its `ChatAdapter` impl without WS round-trip.

---

## 5. Published Artifacts

| Image | Contents | Use case |
|-------|----------|----------|
| `openab:latest` | Discord + Slack only (core) | Default — lightweight, same as today |
| `openab-gateway:latest` | Standalone gateway (all webhook adapters) | Default companion for webhook platforms |
| `openab:unified` | All adapters in single binary | Simplified deployment for users who want one container |

Custom builds via feature flags:
```bash
cargo build --features unified             # all-in-one binary
cargo build --features telegram,line       # core + specific adapters only
```

### Dockerfile Build Arg

The root Dockerfile supports both modes via `BUILD_MODE` and `FEATURES` args:

```dockerfile
ARG BUILD_MODE=default
ARG FEATURES=""

FROM rust:1-bookworm AS builder
ARG BUILD_MODE
ARG FEATURES

WORKDIR /build
COPY . .

RUN if [ "$BUILD_MODE" = "unified" ]; then \
      cargo build --release --features unified; \
    elif [ -n "$FEATURES" ]; then \
      cargo build --release --no-default-features --features "$FEATURES"; \
    else \
      cargo build --release; \
    fi
```

**Build semantics differ between root and agent Dockerfiles:**

| Dockerfile | `FEATURES` behavior | Rationale |
|------------|--------------------|-----------| 
| Root (`Dockerfile`) | `--no-default-features --features "$FEATURES"` | Explicit control — user specifies exactly which adapters |
| Agent (`Dockerfile.<agent>`) | `--features "$FEATURES"` (additive) | Adds adapters on top of defaults (Discord + Slack) |

```bash
# Root Dockerfile examples:
docker build -t openab:latest .                                    # default
docker build --build-arg BUILD_MODE=unified -t openab:unified .    # all adapters
docker build --build-arg FEATURES=telegram,line -t openab:custom . # ONLY these (no Discord/Slack)

# Agent Dockerfile examples (additive — Discord + Slack always included):
docker build -f Dockerfile.claude --build-arg FEATURES=telegram -t openab-claude:tg .
```

For image tagging conventions (`stable/beta/latest/semver/pr<N>`), see [docs/image-tags.md](../image-tags.md).

---

## 6. Migration Path

| Phase | Description |
|-------|-------------|
| **Phase 1** | Restructure into workspace. Keep two-binary default. Add `unified` feature flag. Ship `openab:unified` image for early adopters. |
| **Phase 2** | Gather feedback from unified adopters. Improve single-binary DX (combined health endpoint, unified log format). |
| **Phase 3** | If community consensus shifts toward unified-by-default, flip the default in a future major release. |

### Backward Compatibility

- Default behavior is **unchanged** — existing two-binary deployments continue to work with no migration
- The `unified` feature is purely additive — opting in requires only a build flag or image swap
- No breaking change to config schema — `[gateway]` section continues to work for users who keep the two-binary model
- Platform-specific config sections (`[telegram]`, `[line]`, etc.) are **unified-mode additions** — they are only read when the corresponding adapter is compiled in via feature flags

---

## 7. Trade-offs

### Advantages (of this approach)

- **Zero disruption** — default behavior unchanged; existing deployments need no migration
- **Opt-in simplicity** — users who want a single binary get it with one flag (`--features unified`)
- **Smaller default binary** — `openab:latest` stays ~12MB without webhook adapter deps
- **Independent release cadence** — gateway can still release independently by default
- **Progressive adoption** — community can move to unified at their own pace

### Advantages (of unified mode, when opted in)

- **One container, one config, one release** — dramatically simpler deployment
- **Lower latency** — no WS serialization hop
- **One log stream** — easier debugging
- **No shared volume** — media passed in-process
- **Smaller attack surface** — no exposed WS port between containers

### Disadvantages

- **Two images to maintain** — CI must build both default and unified variants
- **Unified binary is larger** — ~25MB vs ~12MB. Acceptable as opt-in.
- **Feature flag complexity** — conditional compilation adds `#[cfg]` gates. Mitigated by clean workspace boundary (all gateway code lives in `openab-gateway` crate).

---

## 8. Core Changes Required

| Area | Change | Scope |
|------|--------|-------|
| `main.rs` | Start axum server + register adapter routes | ~50 lines |
| `config.rs` | Add `TelegramConfig`, `LineConfig`, etc. | ~100 lines (additive) |
| `Cargo.toml` | Workspace restructure + feature flags | Medium |
| Adapter code | Move from `gateway/src/adapters/` → `crates/openab-gateway/src/` | Mechanical move |
| Per-adapter glue | Replace WS broadcast with `Dispatcher.submit()` | ~10 lines each |
| Existing modules | **Zero changes** — ACP, pool, dispatcher, discord, slack untouched | None |

---

## 9. Rejected Alternatives

### A. Unified binary as default

Ship all adapters compiled in by default; users opt out for slim builds. Rejected — forces a larger binary and more deps on users who don't need webhook adapters, and deprecates the standalone gateway prematurely.

### B. Compile-time only (no pre-built unified image)

Users must `docker build` themselves with desired features. Poor UX — rejected. We publish `openab:unified` as a pre-built image.

### C. Merge all code into one crate

Couples platform-specific complexity (Feishu AES-CBC, WeCom XML) with clean core abstractions. Rejected in favor of workspace separation.

### D. Keep current architecture, no workspace restructure

The workspace restructure is needed regardless — it enables feature flags, cleaner builds, and the opt-in unified path. Rejected.
