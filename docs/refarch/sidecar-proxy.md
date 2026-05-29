# Reference Architecture: OAuth Sidecar Proxy

> **Note:** For xAI/Grok models, OpenCode ≥1.15.0 supports native xAI OAuth.
> The sidecar proxy is no longer required for OpenCode deployments.
> See [docs/xai-proxy.md](../xai-proxy.md) for the recommended approach.

This document describes the **sidecar proxy pattern** implemented by
`openab-auth-proxy` — a generic OAuth proxy that injects Bearer tokens into
upstream API requests.

## When to use this pattern

- Agents **without** built-in OAuth (Hermes, custom agents)
- Centralizing token management across multiple containers in a pod
- Proxying to any OAuth-protected API (not just xAI)

## Architecture

```
┌─ Kubernetes Pod ──────────────────────────────────────────────┐
│                                                               │
│  agent container (any OpenAI-compatible client)               │
│               │  POST /v1/chat/completions                    │
│               │  (no auth header needed)                      │
│               ▼                                               │
│         openab-auth-proxy :9090                               │
│           • Reads OAuth token from disk                       │
│           • Injects Authorization: Bearer header              │
│           • Auto-refreshes 120s before expiry                 │
│               │                                               │
│  Token: ~/.openab-auth-proxy/<provider>/tokens.json           │
└───────────────┼───────────────────────────────────────────────┘
                ▼
        upstream API (configured via TOML or xAI default)
```

## Configuration

Without a config file, `openab-auth-proxy` defaults to xAI/SuperGrok.

For other providers, create `auth-proxy.toml`:

```toml
[provider]
name = "my-provider"
discovery_url = "https://auth.example.com/.well-known/openid-configuration"
client_id = "my-client-id"
scopes = "openid offline_access api:access"
upstream_base_url = "https://api.example.com"
redirect_port = 8080
```

## Helm deployment (xAI example)

```bash
# 1. Login locally
openab-auth-proxy login-device

# 2. Create K8s secret
kubectl create secret generic auth-proxy-tokens \
  --from-file=tokens.json=$HOME/.openab-auth-proxy/xai/tokens.json

# 3. Deploy with sidecar
helm install openab openab/openab \
  --set agents.mybot.command=opencode \
  --set-json 'agents.mybot.args=["acp"]' \
  --set agents.mybot.image=ghcr.io/openabdev/openab-opencode \
  --set-json 'agents.mybot.extraContainers=[{"name":"auth-proxy","image":"ghcr.io/openabdev/openab-auth-proxy:latest","args":["serve","--bind","0.0.0.0"],"ports":[{"containerPort":9090}],"volumeMounts":[{"name":"data","mountPath":"/home/agent"}]}]' \
  --set-json 'agents.mybot.extraInitContainers=[{"name":"copy-tokens","image":"busybox","command":["sh","-c","mkdir -p /dest/.openab-auth-proxy/xai && cp /src/tokens.json /dest/.openab-auth-proxy/xai/tokens.json"],"volumeMounts":[{"name":"tokens-src","mountPath":"/src","readOnly":true},{"name":"data","mountPath":"/dest"}]}]' \
  --set-json 'agents.mybot.extraVolumes=[{"name":"tokens-src","secret":{"secretName":"auth-proxy-tokens"}}]'
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_PROXY_TOKEN_PATH` | `~/.openab-auth-proxy/<provider>/tokens.json` | Token file location |
| `XAI_PROXY_TOKEN_PATH` | (legacy alias) | Backward-compatible |
| `RUST_LOG` | `openab_auth_proxy=info` | Log verbosity |

## See also

- [openab-auth-proxy source](../../openab-auth-proxy/) — Rust implementation
- [docs/xai-proxy.md](../xai-proxy.md) — xAI-specific quick-start
