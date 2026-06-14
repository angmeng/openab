# AgentCore Runtime Backend

Run your coding agent (Kiro, Claude Code, Codex, etc.) remotely on [Amazon Bedrock AgentCore Runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime.html) instead of bundling it inside the OAB container.

## Why

- **No coding CLI in your OAB image** — smaller, faster pulls, simpler upgrades
- **True isolation** — each agent session runs in its own Firecracker microVM
- **Persistent workspace** — `/mnt/workspace` survives across turns (14-day retention)
- **Background execution** — agents survive pod restarts
- **Multi-agent routing** — one OAB routes to N runtimes by config

## Quick Start

```toml
# config.toml
[discord]
bot_token = "${DISCORD_BOT_TOKEN}"

[agentcore]
runtime_arn = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my-kiro-agent"
```

That's it. OAB auto-spawns the native AgentCore bridge.

## Prerequisites

1. **An AgentCore Runtime** with your coding agent deployed (see [Deploying a Kiro Runtime](#deploying-a-kiro-runtime) below)
2. **AWS credentials** on the OAB pod with `bedrock-agentcore:InvokeAgentRuntimeCommandShell` permission
3. **Runtime deployed after June 5, 2026** (interactive shells support required)

## Architecture

```
┌─ ECS / Kubernetes Pod ────────────────────────────────────────┐
│  openab (PID 1)                                               │
│    └─ openab agentcore-bridge (child process)                 │
│         ├─ stdin  ◄── JSON-RPC from OAB                       │
│         ├─ stdout ──► JSON-RPC to OAB                         │
│         └─ WebSocket ──► AgentCore (SigV4 signed)             │
└───────────────────────────────────────────────────────────────┘
          │ InvokeAgentRuntimeCommandShell (wss://)
          ▼
┌─ AgentCore MicroVM ───────────────────────────────────────────┐
│  PTY shell (persistent per shellId)                           │
│    └─ kiro-cli acp --trust-all-tools (long-lived)             │
│         ├─ stdin  ◄── JSON-RPC (initialize, session/prompt)   │
│         └─ stdout ──► JSON-RPC (responses, notifications)     │
│                                                               │
│  /mnt/agent (14-day persistent storage)                       │
│    └─ .local/share/kiro-cli/data.sqlite3 (OAuth)              │
│  /tmp/kiro-cli/data.sqlite3 (local copy — SQLite locks work)  │
└───────────────────────────────────────────────────────────────┘
```

## Config Reference

```toml
[agentcore]
runtime_arn = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my-agent"  # required
shell_command = "kiro-cli acp --trust-all-tools"  # default; any ACP agent
cancel_strategy = "stop"       # "stop" (default) or "noop"
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `runtime_arn` | yes | — | AgentCore Runtime ARN (region is extracted from it) |
| `shell_command` | no | `kiro-cli acp --trust-all-tools` | ACP agent command to run in the PTY |
| `cancel_strategy` | no | `stop` | What to do on cancel: `stop` terminates the session, `noop` ignores |

If you need full control, use `[agent]` directly:

```toml
[agent]
command = "openab"
args = ["agentcore-bridge", "--runtime-arn", "arn:aws:...", "--region", "us-east-1"]
```

### Priority rules

1. Explicit `[agent]` with `command = "..."` always wins — `[agentcore]` is ignored
2. `OPENAB_AGENT_COMMAND` env var alone does NOT count as explicit — `[agentcore]` overrides it
3. If neither `[agent].command` nor `[agentcore]` is set, falls back to `OPENAB_AGENT_COMMAND` or `openab-agent`

## Docker Image

Use `ghcr.io/openabdev/openab-agentcore` — a minimal image (~20MB) with only the OAB binary. No Python, no coding CLI bundled.

```bash
docker pull ghcr.io/openabdev/openab-agentcore:latest
```

## Deploying a Kiro Runtime

> **Note:** AWS does not currently offer a pre-built managed Kiro runtime. You build and deploy the container yourself. This applies to all coding agents (Claude Code, Codex, Cursor, etc.) — AgentCore hosts your container, it doesn't provide one. This may change as AgentCore evolves.

### 1. Build the container (arm64 required)

A ready-to-build Kiro runtime is at `agentcore/runtimes/kiro/`. Or build your own:

```dockerfile
FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN dnf install -y git curl python3 python3-pip unzip && dnf clean all
RUN useradd -m -d /home/agent -u 1000 agent

# Install kiro-cli
USER agent
RUN curl -fsSL https://cli.kiro.dev/install | bash
USER root

RUN pip3 install boto3
COPY healthcheck.py /app/healthcheck.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

ENV PATH="/home/agent/.local/bin:${PATH}"
WORKDIR /app
EXPOSE 8080
USER agent
CMD ["python3", "/app/healthcheck.py"]
```

### 2. Push to ECR and create the runtime

```bash
# Push image
aws ecr create-repository --repository-name agentcore-kiro --region us-east-1
docker buildx build --platform linux/arm64 -t <ACCOUNT>.dkr.ecr.us-east-1.amazonaws.com/agentcore-kiro:latest . --push

# Create runtime
aws bedrock-agentcore-control create-agent-runtime \
  --agent-runtime-name kiro_agent \
  --agent-runtime-artifact '{"containerConfiguration":{"containerUri":"<ACCOUNT>.dkr.ecr.us-east-1.amazonaws.com/agentcore-kiro:latest"}}' \
  --role-arn "arn:aws:iam::<ACCOUNT>:role/agentcore-execution-role" \
  --network-configuration '{"networkMode":"PUBLIC"}' \
  --protocol-configuration '{"serverProtocol":"HTTP"}' \
  --region us-east-1
```

### 3. Store API key in Token Vault

```bash
aws bedrock-agentcore-control create-workload-identity --name kiro-coding-agent --region us-east-1
aws bedrock-agentcore-control create-api-key-credential-provider \
  --name kiro-api-key --api-key "$KIRO_API_KEY" --region us-east-1
```

The runtime fetches the key at boot — no plaintext secrets in env vars or config.

## How It Works

```
┌─────────┐       ┌─────────┐  ACP   ┌───────────────────┐  WebSocket  ┌─────────────────────┐
│ Discord │──────▶│   OAB   │───────▶│ agentcore-bridge  │────────────▶│  AgentCore Runtime  │
│  Slack  │       │         │ stdio  │  (Rust, in-tree)  │  (PTY/WS)  │  (Firecracker μVM)  │
└─────────┘       └─────────┘        └───────────────────┘            │  ┌───────────────┐  │
                                                                      │  │ kiro-cli acp  │  │
                                                                      │  │ (long-lived)  │  │
                                                                      │  └───────────────┘  │
                                                                      └─────────────────────┘
```

1. OAB spawns `openab agentcore-bridge` as a subprocess (ACP stdio protocol)
2. Bridge opens a SigV4-signed WebSocket to AgentCore (`InvokeAgentRuntimeCommandShell`)
3. Inside the persistent PTY shell, `kiro-cli acp --trust-all-tools` runs as a long-lived process
4. JSON-RPC messages flow bidirectionally: OAB ↔ bridge ↔ WebSocket ↔ kiro-cli
5. Same `shell_id` per thread ensures session continuity across messages

## Session Memory

Each Discord/Slack thread maps to a deterministic `runtimeSessionId`. AgentCore keeps the same microVM alive for 15 minutes (configurable up to 8 hours). The persistent filesystem means:

- Kiro's conversation history survives across turns (via `--resume`)
- Git repos, node_modules, build caches all persist
- No re-clone on every message

## IAM Policy

Minimum permissions for the OAB pod:

```json
{
  "Effect": "Allow",
  "Action": ["bedrock-agentcore:InvokeAgentRuntimeCommandShell"],
  "Resource": ["arn:aws:bedrock-agentcore:us-east-1:<ACCOUNT>:runtime/*"]
}
```

## Comparison

| | Local ACP (default) | AgentCore |
|---|---|---|
| Agent location | Same container | Remote microVM |
| Image size | ~500MB+ | ~50MB (agentcore variant) |
| Session state | In-memory (lost on restart) | Persistent filesystem (14 days) |
| Parallelism | Shared CPU | Independent microVM per session |
| Cold start | None | ~5-15s first invoke |
| Cost | Always-on pod | Pay per CPU-second |
