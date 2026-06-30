# Reference Architecture: Telegram via Cloudflare Tunnel

Deploy OpenAB on K3s with Telegram webhooks through a Cloudflare Tunnel — no public IP, no ingress controller, no TLS certificates required.

## Deployment Modes

| Mode | Containers | When to use |
|------|------------|-------------|
| **Unified** (recommended) | 2 (OAB + cloudflared) | New deployments, simpler setup |
| **Standalone Gateway** (legacy) | 3 (OAB + gateway + cloudflared) | Custom routing, multi-gateway fan-out |

---

## Unified Mode (Recommended)

The OAB binary embeds the Telegram adapter directly — no separate gateway container needed.

### Architecture

```
Telegram Cloud
    │ HTTPS POST
    ▼
Cloudflare Edge (bot.example.com)
    │ Tunnel (QUIC)
    ▼
┌─────────────────────────────────────────────────────┐
│ Single Pod                                          │
│                                                     │
│ ┌─────────────┐     ┌──────────────────────────┐   │
│ │ cloudflared │────▶│  OAB :8080               │   │
│ │ (sidecar)   │     │  /webhook/telegram       │   │
│ └─────────────┘     │  (embedded adapter)      │   │
│    localhost         └──────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

Two containers in the **same pod**:
- **cloudflared** — tunnel client, forwards Cloudflare traffic to `localhost:8080`
- **OAB** — embeds the webhook server + adapter + agent in one binary

### Prerequisites

| Requirement | Notes |
|-------------|-------|
| K3s cluster | Any single-node or multi-node K3s setup |
| Helm 3 | Installed on the node or a workstation with kubeconfig access |
| Cloudflare account | Free plan is sufficient |
| Telegram Bot Token | Create via [@BotFather](https://t.me/BotFather) |
| Domain on Cloudflare | DNS managed by Cloudflare |

### Step 1: Create a Cloudflare Tunnel

1. Go to **Zero Trust → Networks → Tunnels → Create a tunnel**
2. Name it (e.g. `openab-telegram`)
3. Copy the **tunnel token**
4. Add a **public hostname**:
   - Subdomain: your choice (e.g. `bot`)
   - Domain: your Cloudflare-managed domain
   - Service: `http://localhost:8080`

### Step 2: Deploy with Helm

```bash
helm install my-bot oci://ghcr.io/openabdev/charts/openab-telegram \
  --set telegramBotToken="<token-from-botfather>" \
  --set cloudflareTunnelToken="$(cloudflared tunnel token my-telegram-bot)" \
  --set webhookDomain=bot.example.com \
  --set platform.allowedUsers="{<your-telegram-user-id>}" \
  --namespace openab --create-namespace
```

The unified image (`ghcr.io/openabdev/openab-unified`) embeds all platform adapters. No `[gateway]` config section needed — set `TELEGRAM_BOT_TOKEN` as env var and the embedded webhook server activates automatically.

### Step 3: Authenticate the Agent

```bash
kubectl exec -it deployment/my-bot -n openab -c openab -- kiro-cli login --use-device-flow
kubectl rollout restart deployment/my-bot -n openab
```

### Step 4: Set the Telegram Webhook

```bash
curl "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook" \
  -d "url=https://bot.example.com/webhook/telegram"
```

### Resulting Resources

```
$ kubectl get pods -n openab
NAME                                    READY   STATUS    AGE
my-bot-xxxxx-yyyyy                      2/2     Running   ...
```

Two containers: `openab` (unified binary with embedded adapter) and `cloudflared`.

### Configuration

Minimal `config.toml` (no `[gateway]` needed):

```toml
[agent]
env = { KIRO_API_KEY = "${KIRO_API_KEY}" }

[pool]
max_sessions = 3
session_ttl_hours = 1

[reactions]
tool_display = "compact"

[markdown]
tables = "off"
```

---

## Standalone Gateway Mode (Legacy)

For deployments that need a separate gateway process (custom webhook routing, multi-gateway fan-out).

### Architecture

```
Telegram Cloud
    │ HTTPS POST
    ▼
Cloudflare Edge (your_custom.domain.com)
    │ Tunnel (QUIC)
    ▼
┌─────────────────────────────────────────────────────┐
│ Single Pod                                          │
│                                                     │
│ ┌─────────────┐ ┌──────────────┐ ┌──────────────┐  │
│ │ cloudflared │─▶│gateway :8080 │◀─│     OAB     │  │
│ │ (sidecar)   │ │  (sidecar)   │ws│   (main)    │  │
│ └─────────────┘ └──────────────┘ └──────────────┘  │
│    localhost          localhost                      │
└─────────────────────────────────────────────────────┘
```

All three components run as containers in the **same pod**:

- **cloudflared** — tunnel client, forwards Cloudflare traffic to `localhost:8080`
- **gateway** — receives Telegram webhooks, normalizes events, serves WebSocket on `:8080`
- **OAB** — connects to `ws://localhost:8080/ws`, runs the agent

This keeps all communication on `localhost` — no K8s Services or cross-pod networking required.

### Prerequisites

| Requirement | Notes |
|-------------|-------|
| K3s cluster | Any single-node or multi-node K3s setup |
| Helm 3 | Installed on the node or a workstation with kubeconfig access |
| Cloudflare account | Free plan is sufficient |
| Telegram Bot Token | Create via [@BotFather](https://t.me/BotFather) |
| Domain on Cloudflare | DNS managed by Cloudflare |

### Step 1: Create a Cloudflare Tunnel

1. Go to **Zero Trust → Networks → Tunnels → Create a tunnel**
2. Name it (e.g. `openab-telegram`)
3. Copy the **tunnel token**
4. Add a **public hostname**:
   - Subdomain: your choice (e.g. `bot`)
   - Domain: your Cloudflare-managed domain
   - Service: `http://localhost:8080`

### Step 2: Deploy with Helm

```bash
cd openab

RELEASE_NAME="my-openab"

helm upgrade --install "$RELEASE_NAME" ./charts/openab \
  --set agents.kiro.discord.enabled=false \
  --set agents.kiro.gateway.enabled=true \
  --set agents.kiro.gateway.deploy=false \
  --set agents.kiro.gateway.url="ws://localhost:8080/ws" \
  --set agents.kiro.gateway.platform=telegram \
  --set agents.kiro.extraContainers[0].name=gateway \
  --set agents.kiro.extraContainers[0].image="ghcr.io/openabdev/openab-gateway:0.4.0" \
  --set agents.kiro.extraContainers[0].env[0].name=TELEGRAM_BOT_TOKEN \
  --set-literal agents.kiro.extraContainers[0].env[0].value="<TELEGRAM_BOT_TOKEN>" \
  --set agents.kiro.extraContainers[1].name=cloudflared \
  --set agents.kiro.extraContainers[1].image="cloudflare/cloudflared:latest" \
  --set agents.kiro.extraContainers[1].args[0]="tunnel" \
  --set agents.kiro.extraContainers[1].args[1]="--no-autoupdate" \
  --set agents.kiro.extraContainers[1].args[2]="run" \
  --set agents.kiro.extraContainers[1].args[3]="--token" \
  --set-literal agents.kiro.extraContainers[1].args[4]="<CLOUDFLARE_TUNNEL_TOKEN>" \
  --namespace openab --create-namespace
```

> **Key difference:** `gateway.deploy=false` skips the separate gateway Deployment/Service. Instead, gateway and cloudflared run as `extraContainers` sidecars in the OAB pod, communicating over `localhost`.

### Step 3: Authenticate the Agent

```bash
kubectl exec -it deployment/${RELEASE_NAME}-kiro -n openab -- kiro-cli login --use-device-flow
```

After login, restart the pod to pick up credentials:

```bash
kubectl rollout restart deployment/${RELEASE_NAME}-kiro -n openab
```

### Step 4: Set the Telegram Webhook

```bash
curl "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook" \
  -d "url=https://your_custom.domain.com/webhook/telegram"
```

Verify:

```bash
curl "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getWebhookInfo"
```

### Resulting Resources

```
$ kubectl get pods -n openab
NAME                                    READY   STATUS    AGE
my-openab-kiro-xxxxx-yyyyy              3/3     Running   ...
```

The single pod runs 3 containers: `kiro` (OAB agent), `gateway`, and `cloudflared`.

### Configuration

The rendered `config.toml` for the OAB agent:

```toml
[agent]
command = "kiro-cli"
args = ["acp", "--trust-all-tools"]
working_dir = "/home/agent"

[pool]
max_sessions = 10
session_ttl_hours = 24

[reactions]
enabled = true
remove_after_reply = false

[gateway]
url = "ws://localhost:8080/ws"
platform = "telegram"
url = "ws://localhost:8080/ws"
platform = "telegram"
allow_all_channels = true
allowed_channels = []
# ⚠️ Recommended: restrict to specific Telegram user IDs
allow_all_users = false
allowed_users = ["<YOUR_TELEGRAM_USER_ID>"]
```

### Restricting Access

To limit which Telegram users can interact with the bot:

```bash
helm upgrade $RELEASE_NAME ./charts/openab \
  ... \
  --set agents.kiro.gateway.allowAllUsers=false \
  --set-string agents.kiro.gateway.allowedUsers[0]="<TELEGRAM_USER_ID>"
```

### Why Cloudflare Tunnel?

- **No public IP required** — the K3s node can be behind NAT or a firewall.
- **No TLS management** — Cloudflare terminates TLS at the edge.
- **No ingress controller config** — bypasses Traefik/nginx entirely.
- **Single-pod simplicity** — all components share `localhost`, no cross-pod networking needed.
