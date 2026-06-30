# openab-telegram

OpenAB + Telegram in a single pod with Cloudflare Tunnel.

> **Unified Mode (v0.9.0+):** This chart now uses the unified OAB binary with the embedded Telegram adapter. Only 2 containers are needed (OAB + cloudflared) — no separate gateway sidecar. Set `TELEGRAM_BOT_TOKEN` as an env var and the embedded webhook server activates on `:8080`.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Pod: openab-telegram                                        │
│                                                             │
│  ┌───────────┐                              ┌───────────┐  │
│  │  openab   │                              │cloudflared│  │
│  │  (unified)│◄─────── localhost ──────────►│ (sidecar) │  │
│  │  :8080    │                              └─────┬─────┘  │
│  │  /webhook/│                                    │        │
│  │  telegram │                              Cloudflare     │
│  └─────┬─────┘                              Tunnel         │
│        │                                                    │
│        │ /etc/openab/config.toml                            │
│        │ /home/agent (PVC)                                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

Run these on your **local machine** (or CI) — one-time setup, no browser required.

### 1. Create a Telegram bot

```bash
# Use the Telegram Bot API directly (no app needed):
curl "https://api.telegram.org/bot<YOUR_MAIN_BOT_TOKEN>/sendMessage" \
  -d "chat_id=@BotFather" -d "text=/newbot"

# Or message @BotFather in Telegram and save the token it returns.
# The token looks like: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz
```

### 2. Create a Cloudflare Tunnel (fully headless)

```bash
# Install cloudflared
# macOS: brew install cloudflared
# Linux: curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared

# Authenticate with API token (no browser — create token at https://dash.cloudflare.com/profile/api-tokens or via Terraform)
# Required permissions: Account:Cloudflare Tunnel:Edit, Zone:DNS:Edit
export CLOUDFLARE_API_TOKEN="your-api-token"

# Or use service token auth:
cloudflared tunnel login  # only option if no API token; opens browser once

# Create the tunnel
cloudflared tunnel create my-telegram-bot

# Route DNS (creates CNAME: bot.example.com → <tunnel-id>.cfargotunnel.com)
cloudflared tunnel route dns my-telegram-bot bot.example.com

# Configure ingress (what the tunnel serves)
mkdir -p ~/.cloudflared
cat > ~/.cloudflared/config.yml <<EOF
tunnel: $(cloudflared tunnel info my-telegram-bot -o json | jq -r '.id')
ingress:
  - hostname: bot.example.com
    service: http://localhost:8080
  - service: http_status:404
EOF

# Get the tunnel token for helm (encapsulates credentials for remote mode)
cloudflared tunnel token my-telegram-bot
# → eyJ...  (pass this as cloudflareTunnelToken)
```

### 3. Set the Telegram webhook

```bash
export BOT_TOKEN="123456789:ABCdef..."
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
  -d "url=https://bot.example.com/webhook/telegram"
```

## Quick Start

```bash
# Find your Telegram user ID by messaging @userinfobot on Telegram.
helm install my-bot oci://ghcr.io/openabdev/charts/openab-telegram \
  --set telegramBotToken="<token-from-botfather>" \
  --set cloudflareTunnelToken="$(cloudflared tunnel token my-telegram-bot)" \
  --set webhookDomain=bot.example.com \
  --set platform.allowedUsers="{<your-telegram-user-id>}" \
  --namespace openab --create-namespace
```

## Credential Management

Three options, from simplest to most secure:

### Option 1: `--set` (simple, least secure)

```bash
helm install my-bot oci://ghcr.io/openabdev/charts/openab-telegram \
  --set telegramBotToken="123:ABC" \
  --set cloudflareTunnelToken="eyJ..." \
  --namespace openab --create-namespace
```

⚠️ Credentials are stored in Helm release metadata (a K8s Secret) and visible via `helm get values`. Suitable for dev/testing.

### Option 2: `--from-literal` (better)

Create the K8s Secret yourself, then reference it:

```bash
kubectl create secret generic my-bot-creds -n openab \
  --from-literal=telegram-bot-token="123:ABC" \
  --from-literal=cloudflare-tunnel-token="eyJ..."

helm install my-bot oci://ghcr.io/openabdev/charts/openab-telegram \
  --set existingSecret=my-bot-creds \
  --namespace openab
```

Credentials don't appear in Helm values, but they briefly exist in shell history/process memory.

### Option 3: `--from-env-file` with process substitution (most secure)

Pull directly from an external secret manager (e.g., AWS Secrets Manager) without touching local disk:

```bash
kubectl create secret generic my-bot-creds -n openab \
  --from-env-file=<(aws secretsmanager get-secret-value \
    --secret-id oab --query SecretString --output text | \
    jq -r '{"telegram-bot-token": .telegramBotToken, "cloudflare-tunnel-token": .cloudflareTunnelToken} | to_entries[] | "\(.key)=\(.value)"')

helm install my-bot oci://ghcr.io/openabdev/charts/openab-telegram \
  --set existingSecret=my-bot-creds \
  --namespace openab
```

Credentials flow from AWS → K8s Secret without touching local disk or shell variables. The process substitution (`<(...)`) is ephemeral.

> **Expected Secret keys:** `telegram-bot-token`, `cloudflare-tunnel-token`

## Post-Install

### Configure tunnel ingress (required for remote mode)

The chart runs cloudflared in **remote mode** (token-based). Ingress rules must be configured via the Cloudflare API or dashboard — local config files are ignored.

**Option A — API (recommended for AI-assisted installs):**

Add `cloudflare-api-token` to your K8s Secret, then the helm NOTES provide a ready-to-run command. The AI can extract all credentials from the secret and configure ingress automatically.

```bash
# Add API token to secret (required permissions: Account:Cloudflare Tunnel:Edit)
kubectl create secret generic my-bot-creds -n openab \
  --from-literal=telegram-bot-token="123:ABC" \
  --from-literal=cloudflare-tunnel-token="eyJ..." \
  --from-literal=cloudflare-api-token="cfut_..."

# Extract IDs and configure
ACCOUNT_ID=$(kubectl get secret my-bot-creds -n openab -o jsonpath='{.data.cloudflare-tunnel-token}' | base64 -d | base64 -d | jq -r .a)
TUNNEL_ID=$(kubectl get secret my-bot-creds -n openab -o jsonpath='{.data.cloudflare-tunnel-token}' | base64 -d | base64 -d | jq -r .t)
CF_API_TOKEN=$(kubectl get secret my-bot-creds -n openab -o jsonpath='{.data.cloudflare-api-token}' | base64 -d)

curl -X PUT "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/cfd_tunnel/${TUNNEL_ID}/configurations" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"config":{"ingress":[{"hostname":"bot.example.com","service":"http://localhost:8080"},{"service":"http_status:404"}]}}'

# Restart to pick up ingress
kubectl rollout restart deployment/my-bot -n openab
```

**Option B — Dashboard:**

Go to https://one.dash.cloudflare.com/ → Networks → Tunnels → your tunnel → Public Hostname → Add:
- Hostname: `bot.example.com`
- Type: HTTP
- URL: `localhost:8080`

### Authenticate the agent

Kiro CLI requires a one-time OAuth login. The PVC persists tokens across restarts.

```bash
kubectl exec -it deployment/my-bot -n openab -c openab -- kiro-cli login --use-device-flow
kubectl rollout restart deployment/my-bot -n openab
```

## AI-Assisted Install

To have an AI agent handle the full install, prompt it with:

> Follow the openab-telegram chart README at https://github.com/openabdev/openab/blob/main/charts/openab-telegram/README.md to deploy a Telegram bot on my Kubernetes cluster.
>
> I already have:
> - A Telegram bot token: `<token>`
> - A Cloudflare account with `cloudflared` authenticated
> - A domain: `bot.example.com`
> - kubectl access to my cluster
>
> Create the tunnel, install the chart, and complete all post-install steps from the helm NOTES output (including configuring tunnel ingress via the API and setting the webhook). Store the cloudflare-api-token in the K8s secret so ingress can be configured programmatically.

## Values

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `telegramBotToken` | Yes* | `""` | Telegram bot token |
| `cloudflareTunnelToken` | Yes* | `""` | Cloudflare Tunnel token |
| `existingSecret` | No | `""` | Pre-existing Secret name (skips token fields) |
| `webhookDomain` | No | `""` | Shown in post-install notes |
| `image.repository` | No | `ghcr.io/openabdev/openab` | Agent image |
| `image.tag` | No | `appVersion` | Agent image tag |
| `gateway.tag` | No | `v0.5.0` | Gateway image tag |
| `agent.command` | No | `kiro-cli` | Agent command |
| `platform.allowAllUsers` | No | `false` | Allow any Telegram user (opt-in) |
| `platform.allowedUsers` | No | `[]` | Allowed Telegram user IDs (get yours from [@userinfobot](https://t.me/userinfobot)) |
| `persistence.enabled` | No | `true` | Enable PVC for agent state |
| `persistence.size` | No | `1Gi` | PVC size |

*Required unless `existingSecret` is set.
