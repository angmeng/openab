# openab-auth-proxy

Generic OAuth proxy sidecar for LLM APIs. Authenticates via OIDC (PKCE or device-code flow) and injects Bearer tokens into proxied requests.

Ships with a built-in **xAI/SuperGrok** preset. Configure any OAuth-protected API via a TOML config file.

## Quick start (xAI default)

```bash
# Login (device-code for headless, or browser PKCE)
openab-auth-proxy login-device
openab-auth-proxy login

# Start proxy
openab-auth-proxy serve --port 9090

# Use with any OpenAI-compatible client
export OPENAI_BASE_URL=http://127.0.0.1:9090/v1
export OPENAI_API_KEY=dummy
opencode
```

## Custom provider

Create `auth-proxy.toml`:

```toml
[provider]
name = "my-provider"
discovery_url = "https://auth.example.com/.well-known/openid-configuration"
client_id = "my-client-id"
scopes = "openid offline_access api:access"
upstream_base_url = "https://api.example.com"
redirect_port = 8080
```

```bash
openab-auth-proxy -c auth-proxy.toml login-device
openab-auth-proxy -c auth-proxy.toml serve
```

## Architecture

```
┌─ Pod / Host ──────────────────────────────────────────────────┐
│                                                               │
│  agent (any OpenAI-compatible client)                         │
│               │  POST /v1/chat/completions                    │
│               ▼                                               │
│         openab-auth-proxy :9090                               │
│           • Reads OAuth token from disk                       │
│           • Injects Authorization: Bearer header              │
│           • Auto-refreshes 120s before expiry                 │
│               │                                               │
│  Token: ~/.openab-auth-proxy/<provider>/tokens.json           │
└───────────────┼───────────────────────────────────────────────┘
                ▼
        upstream API (configured via TOML)
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_PROXY_TOKEN_PATH` | `~/.openab-auth-proxy/<provider>/tokens.json` | Custom token file path |
| `XAI_PROXY_TOKEN_PATH` | (legacy) | Backward-compatible alias |
| `RUST_LOG` | `openab_auth_proxy=info` | Log level |

## Docker

```bash
docker build -t openab-auth-proxy .
docker run --rm -v ~/.openab-auth-proxy:/root/.openab-auth-proxy openab-auth-proxy serve --bind 0.0.0.0
```

## Presets

| Provider | Config needed? | Notes |
|----------|---------------|-------|
| xAI SuperGrok | No (built-in default) | Uses Grok CLI public OAuth client |
| Custom | Yes (`auth-proxy.toml`) | Any OIDC provider with device-code or PKCE |
