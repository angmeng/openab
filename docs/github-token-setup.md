# GitHub Token Setup for Agents

Step-by-step guide to give your agent secure access to GitHub via `gh` CLI.

## Overview

Agents sometimes need to interact with GitHub — push branches, open PRs, comment on issues. The recommended approach is to store a GitHub fine-grained personal access token centrally (AWS Secrets Manager for ECS, or a Kubernetes secret for k8s) and inject it into the agent's environment.

### OAuth device flow vs. PAT stored in Secrets Manager

**OAuth device flow** is ad-hoc authentication/authorization — you run `gh auth login` (or `codex login --device-auth`, etc.) interactively, complete a browser flow, and the resulting token is generated and persisted under `$HOME/` inside the container.

**PAT in Secrets Manager** is the opposite: the token is pre-generated once from the GitHub console (Step 1 below), then stored and protected in AWS Secrets Manager with IAM access control — no interactive step, no per-container state.

| | OAuth device flow | PAT in Secrets Manager (`aws-sm://`) |
|---|---|---|
| **Setup** | Manual — exec into each container, run `gh auth login`, complete browser flow | One-time secret creation; every new/restarted task picks it up automatically |
| **Survives restart** | No — lost unless persisted to a volume (EFS/PVC); container restart on ephemeral storage requires re-auth | Yes — resolved fresh from Secrets Manager on every boot, no persistent volume needed |
| **Centralized rotation** | No — each container's token is independent; rotating means re-authenticating every instance | Yes — rotate once in Secrets Manager, all consumers get the new value on next restart |
| **Access control** | Tied to the OAuth app's scopes granted at login time; revocation happens on GitHub's side per-token | IAM-based — grant/revoke `secretsmanager:GetSecretValue` on the task role; no GitHub-side action needed |
| **Multi-bot sharing** | Each bot needs its own interactive login | One secret + one IAM grant can be shared read-only across many bots (e.g. `OAB_BX_GITHUB_PAT_RO`) |
| **Audit trail** | GitHub's OAuth app authorization log only | CloudTrail logs every `GetSecretValue` call — who/what/when |
| **Best for** | The single "lead" bot that needs write access under its own GitHub identity | Fleets of read-only bots, or any ECS/Fargate deployment without persistent storage |

In practice: use OAuth device flow for the one bot that needs to push/comment as itself (see [gh-auth-device-flow.md](gh-auth-device-flow.md)), and a shared read-only PAT via Secrets Manager for the rest of the fleet.

## 1. Create a Fine-Grained Personal Access Token

1. Go to [GitHub Settings → Developer settings → Personal access tokens → Fine-grained tokens](https://github.com/settings/tokens?type=beta)
2. Click **Generate new token**
3. Configure:
   - **Token name**: e.g. `openab-masami`
   - **Expiration**: set a reasonable expiry (e.g. 90 days)
   - **Repository access**: select only the repos the agent needs
   - **Permissions**:
     - Contents: Read and write (push branches)
     - Pull requests: Read and write (create/comment on PRs)
     - Issues: Read and write (comment on issues)
     - Workflows: Read and write (if the agent needs to modify workflows)
4. Click **Generate token** and copy it immediately

## 2. Store the Token

Choose the approach that matches your deployment target.

### Option A: AWS Secrets Manager (`aws-sm://`) — recommended for ECS

If you're running on ECS (or any AWS deployment), this is the recommended pattern. It has three parts:

1. **Centralized PAT storage** — the GitHub PAT lives in AWS Secrets Manager, not scattered across task definitions, Helm values, or shell history. One secret can be shared across multiple agents/bots that need read-only GitHub access, with rotation handled in one place.
2. **IAM role for access control** — only task roles explicitly granted `secretsmanager:GetSecretValue` on that secret ARN can read it. This is your access boundary: revoke the IAM permission and every agent using that role loses access immediately, no need to rotate the token itself.
3. **Secret ref → agent env inheritance** — `config.toml`'s `[secrets.refs]` resolves the secret in-memory at boot and injects it into `[agent].env` as `GITHUB_TOKEN`. Because the agent process inherits its parent's environment, `gh` (and any other tool that reads `GITHUB_TOKEN`) picks it up automatically when the agent runs `gh` commands — no separate auth step.

```
┌───────────────────────────┐
│  AWS Secrets Manager      │
│  openab/github            │
│  { "github_pat": "..." }  │
└─────────────┬─────────────┘
              │ secretsmanager:GetSecretValue
              │ (only allowed for roles granted below)
              ▼
┌───────────────────────────┐
│  ECS Task Role            │  ← IAM access control boundary
│  openab-ecs-task-role     │    (grant/revoke here, not the token)
└─────────────┬─────────────┘
              │ resolved in-memory at boot
              ▼
┌───────────────────────────┐
│  config.toml               │
│  [secrets.refs]             │
│  github_token = "aws-sm://openab/github#github_pat"
└─────────────┬─────────────┘
              │ ${secrets.github_token}
              ▼
┌───────────────────────────┐
│  [agent].env                │
│  GITHUB_TOKEN = "${secrets.github_token}"
└─────────────┬─────────────┘
              │ inherited by child process
              ▼
┌───────────────────────────┐
│  Agent process (kiro-cli,  │
│  codex, etc.)               │
│    └─ gh pr create          │  ← gh CLI reads GITHUB_TOKEN
│         (auto-authenticated,│    from its own env, no
│          no gh auth login)  │    separate login step
└───────────────────────────┘
```

**Step 1 — store the token** (as a JSON key in a secret; can share a secret with other keys):

```bash
aws secretsmanager create-secret \
  --name openab/github \
  --secret-string '{"github_pat":"<YOUR_GITHUB_TOKEN>"}'
```

**Step 2 — grant IAM access.** Add `secretsmanager:GetSecretValue` on that secret's ARN to the ECS task role (this is the access control boundary — only roles with this permission can resolve the secret):

```json
{
  "Effect": "Allow",
  "Action": ["secretsmanager:GetSecretValue"],
  "Resource": "arn:aws:secretsmanager:<region>:<account-id>:secret:openab/github-*"
}
```

**Step 3 — reference it in `config.toml`** via `[secrets.refs]`, then wire it into `[agent].env` as `GITHUB_TOKEN` so the agent process (and any `gh` invocation inside it) inherits it:

```toml
[secrets.refs]
github_token = "aws-sm://openab/github#github_pat"

[agent]
env = { GITHUB_TOKEN = "${secrets.github_token}" }
```

See [secrets-management.md](secrets-management.md) for the full `aws-sm://` reference format, including how to point at a shared secret with multiple keys (e.g. a fleet-wide read-only PAT reused across several bots):

```toml
[secrets.refs]
github_token = "aws-sm://oab#OAB_BX_GITHUB_PAT_RO"

[agent]
env = { GITHUB_TOKEN = "${secrets.github_token}" }
```

This keeps the token out of the task definition, container environment dumps, and shell history — it's resolved in-memory at boot from Secrets Manager and only reachable by roles you've explicitly granted access to.

### Option B: Kubernetes Secret — for k8s deployments

Create a dedicated secret for the GitHub token:

```bash
kubectl create secret generic gh-token-secret \
  --from-literal=gh-token="<YOUR_GITHUB_TOKEN>"
```

## 3. Inject via Helm Chart (k8s only)

Use `envFrom` in your Helm values to inject the token as `GH_TOKEN`:

```yaml
# values.yaml
envFrom:
  - secretRef:
      name: gh-token-secret
```

> **Recommended**: Use `envFrom` with a separate secret so the token doesn't appear in shell history or Helm release metadata.

As a fallback, you can pass it directly during install — but note this exposes the token in shell history:

```bash
helm install openab openab/openab \
  --set env.GH_TOKEN="<YOUR_GITHUB_TOKEN>"
```

The `gh` CLI automatically picks up `GH_TOKEN` (or `GITHUB_TOKEN`, used by the AWS Secrets Manager path in Option A above) — no additional auth setup needed.

## 4. Install `gh` CLI in the Agent Container

Ensure `gh` is available in your Dockerfile. Note: `gh` is not in the default Debian repos — you need to add the GitHub CLI apt repository first:

```dockerfile
RUN apt-get update && apt-get install -y curl gpg && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      | gpg --dearmor -o /usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      | tee /etc/apt/sources.list.d/github-cli.list > /dev/null && \
    apt-get update && apt-get install -y gh && \
    rm -rf /var/lib/apt/lists/*
```

See the [official install docs](https://github.com/cli/cli/blob/trunk/docs/install_linux.md) for other methods.

## 5. Verify

Once the agent pod is running:

```bash
# Check auth status
gh auth status

# Should show:
# ✓ Logged in to github.com as your-agent-user (GH_TOKEN)
```

The agent can now run `gh` commands: `gh pr create`, `gh issue comment`, `gh repo fork`, etc.

## Security Best Practices

- **Fine-grained tokens only** — avoid classic tokens; fine-grained tokens limit access to specific repos and permissions
- **Least privilege** — only grant the permissions the agent actually needs
- **Set expiration** — rotate tokens regularly; don't use non-expiring tokens
- **One token per agent** — if you run multiple agents, give each its own token with its own GitHub account
- **Never log tokens** — ensure your agent doesn't echo `$GH_TOKEN` in responses or logs
- **Dedicated GitHub account** — create a bot account (e.g. `masami-agent`) rather than using a personal account

## Troubleshooting

- **`gh auth status` fails** — check that `GH_TOKEN` or `GITHUB_TOKEN` is set: `echo ${GH_TOKEN:+exists}${GITHUB_TOKEN:+exists}`
- **Permission denied on push** — the token's repo access doesn't include the target repo, or write permission is missing
- **403 on PR create** — the token needs Pull requests: Read and write permission
- **Token expired** — generate a new one and update the k8s secret
