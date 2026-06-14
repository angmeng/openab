# ADR: AgentCore Runtime Backend

- **Status:** Proposed
- **Date:** 2026-06-10
- **Author:** @chaodu-agent

---

## 1. Context & Motivation

Today, OpenAB dispatches messages exclusively via ACP (Agent Client Protocol) — JSON-RPC over stdio to a co-located subprocess:

```
Discord/Slack msg ──► OpenAB ──stdio──► coding CLI (kiro, claude, codex…)
```

This means:
- **One agent per container.** The coding CLI binary must be bundled inside the same pod as OpenAB.
- **No parallelism across agents.** Running Claude Code *and* Kiro simultaneously requires deploying two full OpenAB stacks.
- **Pod-bound lifecycle.** If the pod restarts, the agent process (and any in-flight work) dies with it.
- **Resource coupling.** The agent shares CPU/memory/disk with OpenAB — a 90-minute refactor starves the broker.

AWS recently launched **Amazon Bedrock AgentCore Runtime**, which hosts coding agents (Kiro, Claude Code, Codex, etc.) in isolated Firecracker microVMs with persistent filesystems, session management, and streaming invoke APIs. This creates an opportunity: OpenAB can route messages to remote AgentCore sessions, decoupling the agent lifecycle from the broker.

### What this unlocks

1. **Dynamic multi-agent routing** — one OpenAB instance routes to N different AgentCore runtimes based on @mention or config.
2. **True isolation** — each agent runs in its own microVM; no shared localhost, no credential leakage.
3. **Background execution** — agents survive pod restarts, laptop lid closures, and network drops.
4. **Cost efficiency** — microVMs spin down when idle (pay per use), no always-on pod per agent.

---

## 2. Integration Approaches

Two viable approaches exist. We recommend **Option B**.

### Option A: OAB native SDK backend

Add a new `backend = "agentcore"` inside OAB that calls `InvokeAgentRuntime` directly via the AWS SDK.

```
Discord → OAB ──AWS SDK──► AgentCore Runtime (microVM)
```

### Option B: `agentcore-acp` adapter (recommended)

Write a standalone ACP-compatible adapter binary that bridges ACP stdio to AgentCore SDK calls. OAB treats it like any other coding CLI — zero OAB changes.

```
Discord → OAB ──ACP stdio──► agentcore-acp ──AWS SDK──► AgentCore Runtime (microVM)
```

### Why Option B wins

| Dimension | A. OAB native SDK | B. agentcore-acp adapter |
|-----------|-------------------|--------------------------|
| OAB code changes | Large — new trait, new backend, AWS SDK dep | **Zero** |
| Thin bridge philosophy | Violated — OAB learns AWS specifics | **Preserved** — OAB only speaks ACP |
| Onboarding pattern | New pattern for operators | **Same as kiro/claude/codex** |
| Independent dev/test | Coupled to OAB release cycle | **Standalone binary**, own repo/release |
| Language flexibility | Must be Rust (inside OAB) | **Any language** — Python PoC in hours |
| Multi-runtime routing | Requires OAB routing logic | Multiple OAB instances or adapter-level routing |
| Deployment | OAB pod needs IRSA for AgentCore | Adapter subprocess needs IRSA (same pod, same SA) |
| Streaming fidelity | Direct event-stream consumption | Adapter translates to ACP notifications (tiny overhead) |

Option A remains viable for future consideration if we find the ACP translation layer adds unacceptable latency or loses information. But given that every other agent integration (kiro-cli, claude-agent-acp, codex --acp, gemini --acp, opencode acp) follows the adapter pattern, Option B is the natural extension.

---

## 3. Design: `agentcore-acp`

### Architecture

```
┌─ agentcore-acp (subprocess, started by OAB) ─────────────────────┐
│                                                                   │
│  stdin ◄── ACP JSON-RPC from OAB                                  │
│  stdout ──► ACP JSON-RPC notifications to OAB                     │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  ACP Server Layer                                           │  │
│  │  - session/new → create/resume AgentCore session            │  │
│  │  - session/prompt → InvokeAgentRuntime (streaming)          │  │
│  │  - cancel → StopRuntimeSession (best-effort)               │  │
│  └──────────────────────┬──────────────────────────────────────┘  │
│                         │                                         │
│  ┌──────────────────────▼──────────────────────────────────────┐  │
│  │  AgentCore Client                                           │  │
│  │  - boto3 / aws-sdk-rust / JS SDK                            │  │
│  │  - invoke_agent_runtime(runtimeArn, sessionId, payload)     │  │
│  │  - Stream text/event-stream → ACP content notifications     │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │  AgentCore Runtime (AWS)  │
                │  ┌────────────────────┐  │
                │  │ microVM            │  │
                │  │ Kiro / Claude /    │  │
                │  │ Codex / etc.       │  │
                │  │ /mnt/workspace     │  │
                │  └────────────────────┘  │
                └──────────────────────────┘
```

### ACP Protocol Mapping

| ACP Method (from OAB) | agentcore-acp Action |
|------------------------|---------------------|
| `session/new` | Generate `runtimeSessionId` from thread key, return session_id |
| `session/prompt` | `invoke_agent_runtime(payload={"prompt": text})` → stream response → emit ACP `notifications/content` blocks on stdout |
| `session/load` | Resume with same `runtimeSessionId`. If session was idle-terminated (404 `ResourceNotFoundException`), transparently invoke as new — AgentCore auto-provisions a new microVM and mounts the persisted filesystem. Return success either way. |
| `cancel` | **Known limitation:** see §Known Limitations. Default: call `StopRuntimeSession`. Configurable via `--cancel-strategy=noop|stop`. |

### Concurrency Control

AgentCore's behavior with concurrent `InvokeAgentRuntime` calls to the same session is undefined. The adapter **must** serialize invocations per session:

```
Thread A prompt arrives → acquire per-session mutex → invoke → release
Thread A prompt 2 arrives (while invoke in-flight) → wait for mutex → invoke
```

This matches OAB's existing invariant (I2: at most one in-flight ACP turn per thread), so in practice the mutex is a safety net, not a bottleneck.

### Error Code Mapping

| AgentCore Error | ACP Behavior |
|-----------------|-------------|
| `ThrottlingException` (429) | ACP error response `{"code": -32000, "message": "rate limited, retry later"}` |
| `ResourceNotFoundException` (404) | ACP error `{"code": -32001, "message": "session not found"}` → OAB shows error to user |
| `ValidationException` (400) | ACP error `{"code": -32602, "message": "invalid params: ..."}` |
| `ServiceQuotaExceededException` (402) | ACP error `{"code": -32000, "message": "quota exceeded"}` |
| `RuntimeClientError` (424) | ACP error `{"code": -32603, "message": "agent runtime error: ..."}` |
| Network timeout / connection drop | Adapter emits ACP error, does NOT crash (OAB sees error, not a dead process) |

### Streaming Translation

AgentCore returns `text/event-stream` (SSE). The adapter **must** use a proper SSE parser (e.g., `httpx-sse`, `aiohttp` SSE support) — not naive `iter_lines()` — because:
- SSE chunks may split across TCP packets (partial `data:` lines)
- Multiple events may arrive in a single chunk
- Keep-alive comments (`:`) must be filtered

Parsed SSE events are translated to ACP JSON-RPC notifications:
```
SSE:  data: I'll analyze the code...
ACP:  {"jsonrpc":"2.0","method":"notifications/content","params":{"type":"text","text":"I'll analyze the code..."}}

SSE:  data: The issue is in line 42...
ACP:  {"jsonrpc":"2.0","method":"notifications/content","params":{"type":"text","text":"The issue is in line 42..."}}
```

This is the same format OAB already consumes from kiro-cli, claude-agent-acp, etc. — zero changes needed in OAB's streaming/edit logic.

### Session ID Mapping

AgentCore requires `runtimeSessionId` ≥ 33 characters. The adapter builds this deterministically from the **sender context embedded in each prompt**.

**How the adapter gets the thread ID (no OAB changes needed):**

OAB wraps every prompt with a `<sender_context>` block containing the full routing metadata:
```json
<sender_context>
{"schema":"openab.sender.v1","channel_id":"1490282656913559673","thread_id":"1514294613853208667",...}
</sender_context>
```

The adapter parses this on the first `session/prompt` to extract `thread_id` (or `channel_id` if no thread). This is the same sender context that all ACP agents already receive — no protocol changes needed.

**Mapping examples (guaranteed ≥33 chars):**

```
Discord thread 1514294613853208667
  → runtimeSessionId = "oab-discord-thread-1514294613853208667"  (38 chars ✓)

Slack thread C0123456789.1234567890.123456
  → runtimeSessionId = "oab-slack-thread-C0123456789-1234567890-123456"  (47 chars ✓)

Short/missing thread ID (edge case): channel_id only "1490282656913559673"
  → runtimeSessionId = "oab-discord-channel-1490282656913559673"  (39 chars ✓)
```

**Length guarantee:** If the constructed ID is < 33 chars, the adapter pads with zero suffix. Alternatively, use a UUID v5 namespace hash of the thread key (always 36 chars).

**Fallback:** If `<sender_context>` is absent (e.g., non-OAB usage), the adapter falls back to a per-process monotonic session ID. This loses cross-restart resume but still functions.

Deterministic mapping means:
- No persistent state file needed in the adapter
- Resume works automatically after adapter restart
- Multiple adapter instances can share the same AgentCore sessions

---

## 4. Configuration

From the OAB operator's perspective, `agentcore-acp` is just another agent command:

```toml
[agent]
command = "agentcore-acp"
args = ["--runtime-arn", "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/kiro-agent", "--region", "us-west-2"]
working_dir = "/home/agent"
# IAM credentials come from pod's service account (IRSA) — no env vars needed
```

For multi-agent setups, deploy multiple OAB instances each pointing to a different runtime:

```toml
# Instance 1: Kiro
[agent]
command = "agentcore-acp"
args = ["--runtime-arn", "arn:aws:...:runtime/kiro-agent"]

# Instance 2: Claude Code
[agent]
command = "agentcore-acp"
args = ["--runtime-arn", "arn:aws:...:runtime/claude-agent"]
```

Or, if the adapter supports it, a single adapter could route based on hints in the prompt/sender context (future enhancement).

---

## 5. AgentCore Runtime Characteristics

Key properties that the adapter must handle:

| Property | Value | Implication |
|----------|-------|-------------|
| Session idle timeout | 15 min (configurable, up to 8hr) | Each invoke resets the timer; long gaps = cold start |
| Max session lifetime | 8 hours | Long-running work needs session rotation |
| Cold start | ~5-15s (microVM boot) | First invoke per session has visible latency |
| Filesystem persistence | 14 days after last use | Agent state survives across session restarts |
| Streaming response | `text/event-stream` over HTTP/2 | Real-time token delivery, translatable to ACP |
| Parallelism | Independent microVM per session | No resource contention between sessions |
| Cost model | CPU-seconds + peak memory | Idle sessions cost nothing after termination |

### Comparison with local ACP

| Concern | ACP (local subprocess) | agentcore-acp (remote) |
|---------|----------------------|------------------------|
| Agent location | Same container | Remote microVM |
| Startup | Already running | Cold start on first invoke (~5-15s) |
| Session state | In-memory (process) | Persistent filesystem (/mnt/workspace) |
| Credential isolation | Shared pod env | Fully isolated (IAM + AgentCore Gateway) |
| Tool permission prompt | Supported (mid-turn) | Not supported — agents run autonomously |
| Max session duration | Unlimited (until pod dies) | 8 hours (configurable) |
| Resume after restart | Lost (unless session/save) | Automatic (filesystem persists) |
| Parallelism | Shared CPU per pod | One microVM per session |

---

## 6. Implementation Plan

### Phase 1: Python PoC

Minimal `agentcore-acp` in Python (fastest path to validation):

1. ACP stdio server (read JSON-RPC from stdin, write to stdout)
2. `session/new` → generate runtimeSessionId
3. `session/prompt` → `boto3.client('bedrock-agentcore').invoke_agent_runtime()` with streaming
4. Parse `response["response"].iter_lines()` → emit ACP content notifications
5. Package as a single script or small pip package

**Deliverable:** Working end-to-end demo: Discord message → OAB → agentcore-acp → AgentCore → streaming reply in Discord.

### Phase 2: Production hardening

1. Proper error handling (throttling, session terminated, cold start detection)
2. Cold start UX: emit a "⏳ Starting agent environment..." notification before streaming begins
3. Session resume logic (detect if session was idle-terminated, re-invoke transparently)
4. Config file support (runtime ARN, region, payload template, timeout)
5. Logging and observability (structured logs, latency metrics)

### Phase 3: Advanced features

1. Multi-runtime routing within a single adapter instance
2. `InvokeAgentRuntimeCommand` support for deterministic operations (exposed as an ACP tool?)
3. Rust rewrite for performance/single-binary distribution (if Python overhead is measurable)
4. Integration with AgentCore Gateway MCP for tool access

---

## 7. Known Limitations

1. **Cancel is safe for filesystem, destructive for in-memory state.** AgentCore has no mid-invoke cancel API. When OAB sends `cancel`, the adapter can call `StopRuntimeSession` which terminates the microVM. **Filesystem (`/mnt/workspace`) persists for 14 days** — files, git history, installed packages are all safe. What is lost: in-memory agent state (conversation context, running processes, partial computations). The next invoke on the same session ID mounts the same filesystem on a fresh microVM. This makes cancel = stop session a reasonable default for most coding workflows.

2. **Cold start latency (~5-15s).** First invoke per session requires microVM boot. The adapter handles this proactively: if no SSE event arrives within 3 seconds of invoking, the adapter emits an early ACP notification:
   ```json
   {"jsonrpc":"2.0","method":"notifications/content","params":{"type":"text","text":"⏳ Starting agent environment..."}}
   ```
   OAB displays this immediately, so the user knows the system is working. Once streaming begins, normal content flow takes over. This is defined behavior, not dependent on OAB's stall detection.

3. **8-hour max lifetime.** Sessions cannot exceed `maxLifetime` (default 8hr). Long-running work needs the adapter to transparently rotate to a new session and re-mount the same filesystem.

---

## 8. Open Questions

1. **Payload format** — Different AgentCore runtimes may expect different payload schemas (`{"prompt": "..."}` vs raw text vs MCP). Do we need a `--payload-template` flag?

2. **Session context passthrough** — Should the adapter forward OAB's sender context (user name, channel, etc.) in the payload so the remote agent knows who's asking?

3. **Human-in-the-loop** — ACP supports mid-turn tool permission prompts. AgentCore agents run autonomously. Is this acceptable, or do we need a callback mechanism via the adapter?

4. **Multi-agent routing** — Single adapter routing to multiple runtimes, or multiple adapter instances? Former is more convenient, latter is simpler.

5. **Language choice for production** — Python (fast to write, boto3 native), Rust (single binary, matches OAB ecosystem), or Node.js (middle ground)?

---

## 9. Alternatives Considered

### A. OAB native SDK backend (not recommended for now)

Add `backend = "agentcore"` directly inside OAB with AWS SDK calls. This works but:
- Violates the "thin bridge" philosophy — OAB shouldn't understand AWS specifics
- Adds `aws-sdk-bedrockagentcore` as a compile-time dependency to OAB
- Different release cycle (AgentCore API changes shouldn't require OAB rebuild)
- Breaks the consistent "all agents are ACP subprocesses" mental model

May revisit if the ACP translation layer proves to be a bottleneck.

### B. Deploy OAB itself on AgentCore

Run the entire OpenAB + agent container on AgentCore Runtime. This works but:
- Still couples agent to container
- Doesn't leverage AgentCore's multi-session isolation
- Doesn't enable dynamic routing from one OAB instance
- Loses the thin bridge role

### C. WebSocket relay to AgentCore

Persistent WebSocket between OAB and a custom proxy. Rejected:
- Adds another service to deploy
- `InvokeAgentRuntime` already streams; no intermediary needed
- More moving parts, same result

### D. MCP-based integration via AgentCore Gateway

Use Gateway's MCP endpoint as a tool layer for local agents. Complementary (could add for tools) but doesn't solve the agent lifecycle coupling problem.

---

## 10. References

- [AWS Blog: Hosting Coding Agents on AgentCore](https://aws.amazon.com/blogs/machine-learning/its-safe-to-close-your-laptop-now-hosting-coding-agents-on-amazon-bedrock-agentcore/)
- [InvokeAgentRuntime API Reference](https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntime.html)
- [InvokeAgentRuntimeCommand API Reference](https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntimeCommand.html)
- [AgentCore Runtime Lifecycle Settings](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-lifecycle-settings.html)
- [AgentCore Session Storage (Preview)](https://aws.amazon.com/about-aws/whats-new/2026/03/bedrock-agentcore-runtime-session-storage/)
- [Handle Long-Running Agents](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-long-run.html)
- [OpenAB DESIGN.md](../../DESIGN.md) — "Thin Bridge" philosophy
- [ADR: openab-agent](./openab-agent.md) — Native agent pattern (similar standalone approach)
