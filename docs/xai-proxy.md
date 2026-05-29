# xAI / SuperGrok Integration

## Recommended: Native xAI OAuth (OpenCode ≥1.15.0)

OpenCode now has **built-in xAI OAuth support** — no sidecar proxy needed.

1. Run `/connect` inside OpenCode → select **xAI Grok OAuth (Headless / Remote / VPS)**
2. Approve the device-code on any browser
3. Select your model with `/models` (e.g. `grok-4.3`)

OpenCode handles token storage and auto-refresh internally.

---

## Alternative: openab-auth-proxy sidecar

For agents **without** native xAI OAuth (Hermes, custom agents), use
`openab-auth-proxy` — a generic OAuth sidecar that defaults to xAI.

```bash
# Login (one-time)
openab-auth-proxy login-device

# Start proxy
openab-auth-proxy serve --port 9090

# Point any OpenAI-compatible client at the proxy
export OPENAI_BASE_URL=http://127.0.0.1:9090/v1
export OPENAI_API_KEY=dummy
```

See [docs/refarch/sidecar-proxy.md](refarch/sidecar-proxy.md) for the full
architecture, Helm deployment, and custom provider configuration.

## Comparison

| | Native OAuth | openab-auth-proxy sidecar |
|---|---|---|
| **Requires** | OpenCode ≥1.15.0 | Any OpenAI-compatible agent |
| **Extra container** | No | Yes |
| **Token management** | Built into OpenCode | Proxy handles refresh |
| **Multi-agent sharing** | Each agent needs own auth | Single proxy serves all |
| **Custom providers** | xAI only | Any OIDC provider via TOML config |
