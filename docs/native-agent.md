# Native Agent (openab-agent)

A lightweight, native Rust coding agent with built-in ACP support and ChatGPT subscription authentication. No Node.js, no Python, no adapter layer.

## Quick Start

```bash
# Build
cd openab-agent && cargo build --release

# Authenticate (browser flow — recommended)
openab-agent auth codex-oauth

# Headless server (paste callback URL)
openab-agent auth codex-oauth --no-browser

# Run as ACP server (used by openab core)
openab-agent
```

## Configuration

```toml
[agent]
command = "openab-agent"
working_dir = "/home/agent"
env = { OPENAB_AGENT_OPENAI_MODEL = "gpt-5.4-mini" }
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAB_AGENT_OPENAI_MODEL` | `gpt-4.1-nano` | Model to use |
| `OPENAB_AGENT_OPENAI_BASE_URL` | `https://chatgpt.com/backend-api` | API base URL |
| `OPENAB_AGENT_PROVIDER` | auto-detect | Force provider (`anthropic`, `openai`, `codex`) |
| `OPENAB_AGENT_MAX_TOKENS` | `8192` | Max output tokens |
| `OPENAB_AGENT_OAUTH_CLIENT_ID` | Pi's client | Custom OAuth client ID |
| `ANTHROPIC_API_KEY` | — | Anthropic API key (alternative to OAuth) |

## Authentication

### Browser PKCE Flow (recommended)

```bash
openab-agent auth codex-oauth
```

Opens browser to authenticate with your ChatGPT Plus/Pro subscription.

### Headless Server (paste flow)

```bash
openab-agent auth codex-oauth --no-browser
```

1. Prints an authorization URL
2. Open it in any browser and approve
3. Browser redirects to `localhost:1455` (fails on remote server)
4. Copy the full URL from the browser address bar
5. Paste it back into the terminal

### Device Code Flow

```bash
openab-agent auth codex-device
```

Note: Device flow currently has limited scopes and may not work with all models.

### API Key (Anthropic)

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

No login needed — set the env var and the agent auto-detects it.

## Custom System Prompt

Place an `AGENTS.md` file in the working directory (`cwd`). It will be prepended to the default system prompt at session creation.

```
/home/agent/
├── AGENTS.md        ← read at session start
├── .openab/
│   └── agent/
│       └── auth.json
└── (your project files)
```

> **Note:** Skills and MCP servers are NOT supported yet. Only `AGENTS.md` at `cwd` is read. Skills and MCP support are planned for v0.2.

## Docker

```bash
docker build -f Dockerfile.native -t openab-native:latest .
```

Image is ~20MB (debian-slim + static Rust binaries). No runtime dependencies.

## Memory Usage

~7MB per session — 28x lighter than Pi, 55x lighter than Kiro CLI.

## Supported Models (ChatGPT Subscription)

- `gpt-5.2`
- `gpt-5.3-codex`
- `gpt-5.3-codex-spark`
- `gpt-5.4`
- `gpt-5.4-mini`
- `gpt-5.5`

## Tools

4 built-in tools:
- `read` — file contents or directory listing
- `write` — create/overwrite file
- `edit` — string replacement
- `bash` — shell execution with process group isolation
