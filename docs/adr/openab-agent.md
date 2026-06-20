# ADR: openab-agent — Native Rust Coding Agent with Built-in ACP

- **Status:** Proposed
- **Date:** 2026-05-26
- **Author:** @chaodu-agent

---

## 1. Context & Motivation

Today, every coding agent in OpenAB follows the same pattern:

```
openab (Rust) ──stdio JSON-RPC──► adapter (Node/Rust) ──spawns──► CLI agent ──HTTP──► LLM API
```

This introduces 3–4 layers of indirection, each with its own runtime, dependencies, and failure modes. Every agent requires:

- A separate Dockerfile (300–800MB images)
- A Node.js or Python runtime
- An ACP adapter wrapper (pi-acp, codex-acp, agy-acp, etc.)
- npm/pip supply-chain management

Meanwhile, the actual work an agent does is simple:

1. Receive a prompt
2. Call an LLM API (HTTP POST + SSE)
3. Execute tool calls (read/write/edit/bash)
4. Return the result

**Proposal:** Build `openab-agent` — a single Rust binary that is both the ACP server and the coding agent, with no external runtime, no wrapper, and no adapter layer.

---

## 2. Design

### Architecture

```
┌─ openab-agent (single Rust binary) ──────────────┐
│                                                   │
│  ┌─────────────────────────────────────────────┐  │
│  │  ACP Layer (stdin/stdout JSON-RPC)          │  │
│  │  - session/new, session/prompt, cancel      │  │
│  └──────────────────┬──────────────────────────┘  │
│                     │                             │
│  ┌──────────────────▼──────────────────────────┐  │
│  │  Agent Core                                 │  │
│  │  - Prompt assembly (system + user + tools)  │  │
│  │  - Tool dispatch loop                       │  │
│  │  - Session tree (branching history)         │  │
│  └──────────────────┬──────────────────────────┘  │
│                     │                             │
│  ┌──────────────────▼──────────────────────────┐  │
│  │  LLM Client (reqwest + SSE)                 │  │
│  │  - OpenAI-compatible (GPT, Codex, DeepSeek) │  │
│  │  - Anthropic (Claude)                       │  │
│  │  - Google (Gemini)                          │  │
│  └─────────────────────────────────────────────┘  │
│                                                   │
│  ┌─────────────────────────────────────────────┐  │
│  │  Tools (4 only)                             │  │
│  │  - read: file/directory reading             │  │
│  │  - write: file creation                     │  │
│  │  - edit: string replacement in files        │  │
│  │  - bash: command execution (sandboxed)      │  │
│  └─────────────────────────────────────────────┘  │
│                                                   │
└───────────────────────────────────────────────────┘
```

### Comparison with Existing Agents

| Aspect | Existing (e.g. Pi, Codex) | openab-agent |
|--------|---------------------------|--------------|
| Layers | openab → adapter → CLI → LLM | openab → **agent** → LLM |
| Runtime | Node.js / Python | None (static binary) |
| Image size | 300–800MB | ~20MB (distroless) |
| Cold start | 1–3s | <50ms |
| ACP support | Requires wrapper | Native, zero overhead |
| Dependencies | npm ecosystem | Minimal crates |
| Supply-chain risk | High (node_modules) | Low (cargo audit) |

### Required Crates

Only four crates are needed beyond what openab core already uses:

- `reqwest` — HTTP client (LLM API calls)
- `serde` / `serde_json` — JSON serialization
- `tokio` — async runtime + process management (`tokio::process`) (already used in openab)
- `futures` — `Stream` trait and `BoxStream` for async streaming

> **Note:** `tokio-process` was merged into `tokio::process` in tokio 0.2 and the standalone crate is deprecated. All process spawning uses `tokio::process::Command` directly.

Nothing else. Can share code with openab core (ACP types, session pool logic).

### Key Advantage: Deep Integration

Because we own the agent and it shares the same language as openab core, deep integration is possible — a future library mode can bypass stdio entirely, using in-process function calls to eliminate all IPC overhead.

### Design Principles (inspired by Pi)

1. **Minimal tool surface** — 4 tools only (read, write, edit, bash). Maximizes context window for actual code.
2. **Tiny system prompt** — Agent instructions fit in ~500 tokens. No bloated tool descriptions.
3. **Multi-model** — Provider-agnostic. Switch models via config or mid-session command.
4. **Session trees** — Branching conversation history. Explore multiple approaches without losing context.
5. **No SDK dependency** — LLM APIs are just HTTP. A thin `reqwest` client (~300 lines) covers all providers.

---

## 3. LLM Client Design

The LLM client is intentionally thin — no SDK, just HTTP:

```rust
use futures::stream::BoxStream;

// Unified trait for all providers
trait LlmProvider: Send + Sync {
    fn chat<'a>(
        &'a self,
        messages: &'a [Message],
        tools: &'a [Tool],
    ) -> Pin<Box<dyn Future<Output = Result<BoxStream<'a, Event>>> + Send + 'a>>;
}

// Implementations are ~100 lines each
struct OpenAiProvider { base_url: String, api_key: String, model: String }
struct AnthropicProvider { api_key: String, model: String }
struct GoogleProvider { api_key: String, model: String }
```

> **Note:** `Stream` is a trait (from `futures`), not a concrete type. Returning it directly from a trait method would not compile. We use `BoxStream<'a, Event>` (i.e., `Pin<Box<dyn Stream<Item = Event> + Send + 'a>>`) to provide a type-erased, object-safe return type. The `async fn` in traits is similarly desugared to a boxed future for object safety.

All three major APIs follow the same pattern:
- POST JSON body with messages + tool definitions
- Stream SSE events back
- Parse tool_use / function_call blocks
- Loop until stop

Provider differences are minor (header format, JSON schema for tools, SSE event names) and well-contained in ~300 lines per provider.

### API Change Tracking & Version Pinning

Without an official SDK, API changes must be tracked deliberately. Strategy:

- **Pin API versions in headers**: `anthropic-version: 2023-06-01`, `x-api-version` for OpenAI (when available)
- **Model version pinning**: use dated model snapshots (e.g., `claude-sonnet-4-20250514`) not aliases (`claude-sonnet-4`) in default config
- **CI canary job**: weekly integration test against each provider's API with a minimal prompt. Failures trigger alerts, not breakage.
- **Provider feature flags**: new API features (extended thinking, computer use, etc.) are gated behind feature flags, not auto-enabled
- **Changelog tracking**: maintain `PROVIDERS.md` documenting supported API versions, known breaking changes, and migration notes
- **OpenAI-compatible fallback**: providers implementing the OpenAI chat completions spec (DeepSeek, Groq, Together, Ollama, etc.) require zero additional code — only `base_url` changes

---

## 4. Tool Implementation

| Tool | Input | Behavior |
|------|-------|----------|
| `read` | path, optional line range | Read file contents or list directory |
| `write` | path, content | Create or overwrite file |
| `edit` | path, old_str, new_str | Replace exact string in file |
| `bash` | command, optional working_dir | Execute shell command, return stdout/stderr |

### Path Security (read/write/edit tools)

All file tools enforce **path confinement** to prevent path traversal attacks:

- All paths are canonicalized (`std::fs::canonicalize`) before access
- Resolved path must be within `working_dir` or explicitly allowed directories
- Symlinks are resolved and checked against the boundary
- Attempts to escape (e.g., `../../etc/passwd`) return an error, not file contents

```rust
fn validate_path(path: &Path, working_dir: &Path) -> Result<PathBuf> {
    let canonical = path.canonicalize()?;
    if !canonical.starts_with(working_dir) {
        return Err(Error::PathTraversal(path.display().to_string()));
    }
    Ok(canonical)
}
```

### Sandboxing (bash tool)

The `bash` tool runs commands via `tokio::process::Command` with:

- Configurable timeout (default: 120s)
- **Process group kill on timeout** — uses `setsid` + `kill(-pgid)` to ensure all child/grandchild processes are terminated, preventing orphan process leaks
- Optional `bubblewrap` (bwrap) sandboxing on Linux; falls back to basic process isolation on macOS (see Cross-Platform Sandboxing below)
- Working directory scoped to agent's `working_dir`
- No network access restriction by default (agent needs to call APIs, git, etc.)

### Environment Variable Filtering

The `bash` tool does **NOT** inherit the agent's full environment. Instead:

- **Deny-list by default**: sensitive variables are stripped before spawning child processes:
  - `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`, `OPENAB_AGENT_*` (all provider keys)
  - Any variable matching `*_SECRET`, `*_TOKEN`, `*_KEY` patterns (configurable)
- **Allow-list passthrough**: only explicitly declared safe variables are inherited:
  - `PATH`, `HOME`, `USER`, `LANG`, `TERM`, `SHELL`
  - Variables listed in `OPENAB_AGENT_BASH_ENV_ALLOW` (comma-separated)
- This prevents prompt injection attacks from exfiltrating API keys via `curl`/`wget`

```rust
fn build_env(config: &AgentConfig) -> HashMap<String, String> {
    let mut env = HashMap::new();
    // Only pass safe defaults
    for key in &["PATH", "HOME", "USER", "LANG", "TERM", "SHELL"] {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }
    // Add user-configured allow-list
    for key in &config.bash_env_allow {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }
    env
}
```

### Cross-Platform Sandboxing

| Platform | Sandboxing | Notes |
|----------|-----------|-------|
| Linux | `bubblewrap` (bwrap) | Full filesystem/network namespace isolation |
| macOS | Process group isolation + env filtering | Primary mechanism for local dev. `sandbox-exec` is deprecated by Apple and may be removed in future macOS versions — not relied upon. |
| Fallback | Process group isolation + env filtering | Minimum viable security without platform-specific tools |

For production (Linux containers), `bubblewrap` is the primary mechanism. For local macOS development, the env filtering + path confinement provides baseline security without requiring bwrap.

---

## 5. Session Tree

Sessions are stored as a tree structure, not a flat list:

```
root
├── turn-1 (user: "explain this code")
│   └── turn-2 (assistant: "This code does...")
│       ├── turn-3a (user: "refactor it") ← branch A
│       │   └── turn-4a (assistant: "Here's the refactored...")
│       └── turn-3b (user: "add tests instead") ← branch B
│           └── turn-4b (assistant: "Here are the tests...")
```

Benefits:
- Explore multiple approaches from a decision point
- Rollback without losing the exploration
- Persisted to disk as JSON for session resume

### Garbage Collection / Pruning

To prevent unbounded growth in long-running deployments:

- **Max tree depth**: configurable (default: 200 turns per branch). Oldest turns are summarized when exceeded.
- **Max branches**: configurable (default: 20 per session). Least-recently-used branches are pruned on overflow.
- **Inactive branch TTL**: branches not accessed for N hours (default: 24h) are eligible for pruning.
- **Disk persistence cap**: per-session JSON file capped at 10MB. Exceeding triggers forced summarization of oldest branches.
- **GC trigger**: runs on every Nth turn (default: 10) or when memory pressure is detected.

---

## 5a. Context Window Management

The agent must operate within LLM context limits (typically 128K–200K tokens). Strategy:

### Token Counting

- Use `tiktoken-rs` for OpenAI models, character-based estimation (×0.3) for others
- Track cumulative token usage per session branch

### Window Overflow Strategy (ordered by priority)

1. **Tool output truncation** — large `bash` stdout or `read` results are truncated to configurable max (default: 30K tokens) with a "truncated, showing first/last N lines" indicator
2. **Oldest turn summarization** — when context exceeds 80% of model limit, oldest turns (excluding system prompt and last 4 turns) are replaced with a one-paragraph summary generated by the same LLM
3. **Branch instead of truncate** — if the user explicitly branches, the new branch starts with a compact summary of the parent path, preserving full context in the original branch
4. **Hard cap rejection** — if a single tool output exceeds 50% of context window, reject and ask the user to narrow the request

### Configuration

```toml
[agent.context]
max_context_percent = 80          # trigger summarization at 80% of model limit
max_tool_output_tokens = 30000    # truncate individual tool outputs
summarize_after_turns = 20        # summarize turns older than the last 20
```

---

## 6. Configuration

```toml
[agent]
command = "openab-agent"
working_dir = "/home/agent"

[agent.env]
# Provider selection (one of):
OPENAB_AGENT_PROVIDER = "anthropic"          # or "openai", "google", "openai-compatible"
OPENAB_AGENT_MODEL = "claude-sonnet-4-20250514"

# Auth (provider-specific):
ANTHROPIC_API_KEY = "${ANTHROPIC_API_KEY}"
# or: OPENAI_API_KEY, GOOGLE_API_KEY, etc.

# Optional:
OPENAB_AGENT_MAX_TOKENS = "8192"
OPENAB_AGENT_TIMEOUT_SECS = "120"
OPENAB_AGENT_MAX_TOOL_LOOPS = "50"       # max tool-call iterations per prompt (default 50)
```

### Steering Files

openab-agent reads steering files in the same pattern as other agents:

- `AGENTS.md` in working directory (hot memory, always loaded)
- `.openab-agent/system.md` — custom system prompt override
- `.openab-agent/append.md` — append to default system prompt

---

## 7. Future: Library Mode (Deferred — Not in v1 Scope)

> **Note:** This section documents a potential future optimization. It is explicitly **out of scope** for v0.1–v0.3 and will require its own ADR if pursued.

Because openab-agent is Rust (same as openab core), a future optimization is **in-process mode** — no stdio, no JSON-RPC serialization:

```rust
// Current: IPC over stdio (v0.1–v0.3)
openab::spawn_process("openab-agent", &["--acp"]) // stdin/stdout JSON-RPC

// Future: direct function call (requires separate ADR)
let agent = openab_agent::Agent::new(config);
let response = agent.prompt(session_id, messages).await; // zero-copy
```

### Known Risks (to be addressed in future ADR)

- **Panic propagation**: an agent panic (e.g., malformed SSE parse) would crash the entire openab process. Mitigation: `catch_unwind` boundaries or `tokio::task::spawn` with panic hooks.
- **Resource isolation**: in-process mode shares memory/threads with openab core. A runaway agent could starve the session pool.
- **Blast radius**: process isolation (current design) provides natural fault containment. Library mode trades this for performance.

**Decision**: v1 ships as a standalone binary with stdio ACP. Library mode is a v2+ exploration only if IPC overhead proves to be a measurable bottleneck in production.

---

## 8. Rollout Plan

| Phase | Scope | Deliverable |
|-------|-------|-------------|
| **v0.1** | Scaffold + ACP layer + single provider (Anthropic) | Working agent, 4 tools, flat session |
| **v0.2** | Multi-provider + session tree + steering files | Feature parity with Pi's core |
| **v0.3** | Dockerfile + Helm chart + CI | Production-ready deployment |
| **v0.4** | Library mode exploration | In-process integration with openab core |

---

## 9. Testing Strategy

### Unit Test Boundaries

Following the project's unit test ADR, operations involving network, filesystem, or subprocess are **integration tests only**. Unit tests cover pure logic:

| Layer | Unit-Testable | How |
|-------|--------------|-----|
| Prompt assembly | ✅ | Hand-written mock `LlmProvider` returning canned `BoxStream` |
| Tool dispatch routing | ✅ | Mock tool implementations (no real FS/process) |
| Session tree operations | ✅ | Pure data structure manipulation |
| Token counting / context management | ✅ | Pure computation |
| SSE event parsing | ✅ | Feed raw bytes, assert parsed `Event` structs |
| LLM HTTP calls | ❌ (integration) | Real HTTP against provider or local mock server |
| File tools (read/write/edit) | ❌ (integration) | Real filesystem in temp dirs |
| Bash tool | ❌ (integration) | Real subprocess execution |

### Hand-Written Mocks (no `mockall`)

Per team convention, all mocks are hand-written:

```rust
struct MockLlmProvider {
    responses: Vec<Vec<Event>>,
    call_count: AtomicUsize,
}

impl LlmProvider for MockLlmProvider {
    fn chat<'a>(
        &'a self,
        _messages: &'a [Message],
        _tools: &'a [Tool],
    ) -> Pin<Box<dyn Future<Output = Result<BoxStream<'a, Event>>> + Send + 'a>> {
        let idx = self.call_count.fetch_add(1, Ordering::SeqCst);
        let events = self.responses[idx].clone();
        Box::pin(async move {
            Ok(Box::pin(futures::stream::iter(events.into_iter())) as BoxStream<'a, Event>)
        })
    }
}
```

### Integration Tests

- Tagged with `#[cfg(test)]` + `#[ignore]` for CI gating
- LLM integration tests require `OPENAB_TEST_PROVIDER` env var
- File/bash tool tests use `tempdir` for isolation
- CI runs integration tests in a separate job with real credentials (not on every PR)

### CI Pipeline

```
PR push → cargo fmt --check → cargo clippy → cargo test (unit only)
                                                    ↓
merge to main → cargo test (unit + integration) → canary deploy
```

---

## 10. Open Questions

| Question | Options | Notes |
|----------|---------|-------|
| **Crate name** | `openab-agent` as a workspace member vs separate repo | Workspace member keeps it close to openab core |
| **Subscription auth** | Support OAuth flows (Claude Pro, ChatGPT Plus) or API-key only for v1? | API-key only for v1; subscription auth adds complexity |
| **Permission model** | Auto-approve all tool calls vs interactive approval? | Auto-approve for v1 (matches OpenAB's `--trust-all-tools` pattern) |
| **Context window management** | Truncate old turns vs summarize vs session tree branching? | Session tree branching for v1; summarization for v2 |

---

## Consequences

### Positive

- **Zero external runtime** — no Node.js, Python, or npm. Single static binary.
- **Minimal attack surface** — no node_modules supply chain, no adapter layer vulnerabilities.
- **Fastest cold start** — <50ms vs 1–3s for Node-based agents.
- **Smallest image** — ~20MB distroless vs 300–800MB for existing agents.
- **Native ACP** — no wrapper overhead, no adapter bugs, no version mismatches.
- **Same language as openab** — shared types, potential library mode, unified toolchain.
- **Full control** — no upstream CLI breaking changes; we own the entire stack.

### Negative

- **LLM API maintenance** — must track API changes manually without official SDKs.
- **No subscription auth (v1)** — API key only initially; users with Claude Pro/ChatGPT Plus subscriptions still need Pi or Codex.
- **Feature gap** — v1 will lack features mature agents have (image support, MCP, web search tools).
- **Development effort** — building from scratch vs leveraging existing open-source agents.

### Risks

- **API instability** — if providers make breaking changes frequently, maintenance burden grows. Mitigated by pinning API versions and weekly CI canary.
- **Scope creep** — temptation to add more tools/features. Mitigated by the "4 tools only" design principle as a hard constraint for v1.

---

## References

- [Pi coding agent](https://github.com/earendil-works/pi) — design inspiration (minimal tools, session trees, multi-model)
- [Agent Client Protocol](https://github.com/anthropics/agent-protocol) — ACP spec
- [OpenAB](https://github.com/openabdev/openab) — host runtime
