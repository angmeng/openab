# ADR: Lifecycle Hooks

- **Status:** Accepted
- **Date:** 2026-05-27
- **Author:** @chaodu-agent

---

## 1. Problem Statement

When an OpenAB pod starts, the agent CLI needs more than just the binary — it needs `AGENTS.md`, `docs/*`, `bin/*`, skills, and potentially OAuth tokens to be fully operational on first message. Today, operators manually `kubectl exec` to set up these files or bake custom images, which is error-prone and not reproducible.

**Goal:** By the time the agent CLI receives its first JSON-RPC request, all required files and credentials are in place — zero manual intervention after deploy.

---

## 2. Approaches Considered

### A. Custom Dockerfile (ENTRYPOINT bake)

Operators build their own image layering content on top of the base openab image:

```dockerfile
FROM ghcr.io/openabdev/openab:latest
COPY AGENTS.md /home/node/AGENTS.md
COPY bin/ /home/node/bin/
COPY .kiro/ /home/node/.kiro/
```

**Pros:**
- Immutable, reproducible
- No startup latency

**Cons:**
- Forces every operator to maintain a Dockerfile
- OAuth tokens cannot be baked in (expire, shouldn't be in images)
- Every content change requires image rebuild + push + rollout

### B. initContainer + PVC

A Kubernetes init container populates the PVC before the main container starts:

```yaml
initContainers:
  - name: bootstrap
    image: alpine/git
    command: ["sh", "-c", "git clone --depth 1 https://github.com/org/agent-config /data/config && cp -r /data/config/* /data/home/"]
    volumeMounts:
      - name: data
        mountPath: /data/home
```

**Pros:**
- Separates content provisioning from runtime image
- PVC persists tokens across restarts (existing pattern)
- Helm chart can define init containers natively

**Cons:**
- Startup latency (git clone, downloads)
- Kubernetes-only — doesn't help Docker Compose or bare-metal users
- Debugging init container failures is less visible

### C. openab `[hooks.pre_boot]` in config.toml

openab executes a user-defined script before spawning the agent CLI:

```toml
[hooks.pre_boot]
script = "/data/pre-boot.sh"
timeout_seconds = 60
```

**Pros:**
- Works everywhere (Docker, k8s, bare metal, ECS)
- Configured in the same file operators already manage
- openab can log success/failure and abort cleanly
- Can be conditional (skip if files already exist)

**Cons:**
- Adds shell execution responsibility to openab
- Security surface if config is user-supplied (mitigated: config is operator-controlled)
- Logs mixed with openab startup output

---

## 3. Decision

**Use a layered approach: initContainer for heavy provisioning + openab `[hooks.pre_boot]` for last-mile setup.**

| Content Type | Mechanism |
|---|---|
| Static assets (AGENTS.md, docs, skills, bins) | initContainer, ConfigMap, or custom image |
| OAuth tokens | PVC (existing pattern, persists across restarts) |
| Dynamic/conditional setup | openab `[hooks.pre_boot]` |

The `[hooks.pre_boot]` hook is the primary new feature openab needs to implement. The initContainer pattern is documented as a recommended practice but requires no openab code changes.

---

## 4. `[hooks]` Specification

### Config Structure

Hooks are organized as 2-level TOML tables under `[hooks]`. Each hook is its own sub-table with independent configuration:

```toml
[hooks.pre_boot]
timeout_seconds = 60                  # max wall-clock time; 0 = no timeout
on_failure = "abort"                  # "abort" (default) | "warn"

# Script source — exactly ONE of the following three:
script = "/etc/openab/pre-boot.sh"    # Option A: path to executable file
# inline = '''...'''                  # Option B: script content in config
# url = "https://..."                 # Option C: remote script URL
# sha256 = "abc123..."               #           (required with url)
```

### Three Script Source Options

**Option A: `script` — file path** (k8s ConfigMap, EFS, baked in image)

```toml
[hooks.pre_boot]
script = "/etc/openab/pre-boot.sh"
```

**Option B: `inline` — embedded in config.toml** (ECS, Docker Compose, bare metal)

```toml
[hooks.pre_boot]
inline = '''
#!/bin/sh
set -e
aws s3 sync "$BOOTSTRAP_URI" "$HOME/"
'''
```

openab writes the content to a temp file, `chmod +x`, executes it, then deletes it.

**Option C: `url` + `sha256` — remote script** (editable without redeploy, any platform)

```toml
[hooks.pre_boot]
url = "https://raw.githubusercontent.com/acme/openab-config/main/pre-boot.sh"
sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
```

openab fetches the URL, verifies the SHA-256 checksum, then executes. Fails hard on mismatch or network error.

| Option | Best for | Requires redeploy? | Network at boot? |
|--------|----------|--------------------|--------------------|
| `script` | k8s (ConfigMap mount), EFS, image bake | Only if image-baked | No |
| `inline` | ECS, Docker Compose, bare metal | Config change only | No |
| `url` + `sha256` | Central script repo, multi-cluster | No (update sha256 to roll) | Yes |

Exactly one of `script`, `inline`, or `url` must be set. Setting multiple is a config error.

### Common Fields

- `timeout_seconds` — max wall-clock time before kill. Default: 60.
- `on_failure` — `"abort"` (default): openab exits non-zero (pod restarts). `"warn"`: log error, continue.

### Hook Lifecycle

| Hook | Timing | Runs | Phase 1 |
|------|--------|------|---------|
| `pre_boot` | Before agent CLI spawns | Once per process start | ✅ |
| `pre_shutdown` | On SIGTERM, before exit | Once per process stop | ✅ |
| `post_boot` | After first successful agent spawn | Once per process start | ❌ |
| `pre_session` | Before each ACP session creation | Per session | ❌ |
| `post_session` | After ACP session teardown | Per session | ❌ |

### Execution Semantics (`pre_boot`)

1. openab starts, parses config.
2. Resolves script source:
   - `script` → use file directly
   - `inline` → write to temp file, chmod +x
   - `url` → fetch, verify sha256, write to temp file, chmod +x
3. Spawns the resolved script as a child process.
4. Waits for exit (respecting timeout).
5. On exit 0 → proceed to spawn agent sessions normally.
6. On non-zero exit, timeout, or sha256 mismatch → apply `on_failure` policy.
7. Cleans up temp files (if `inline` or `url`).
8. The script runs **once per openab process start**, not per session.

### Security

- `script` path must be absolute (no relative path traversal).
- `url` requires `sha256` — openab refuses to execute unverified remote code.
- The script inherits the same sanitized environment as agent subprocesses (`env_clear()` + whitelist).
- `DISCORD_BOT_TOKEN` and other openab secrets are NOT exposed to the bootstrap script.
- The script runs as the container's UID (not root, unless the container runs as root).

---

## 5. Helm Chart Integration

The Helm chart adds optional support for:

1. **Bootstrap script via ConfigMap:**

```yaml
# values.yaml
agents:
  kiro:
    hooks:
      preBoot:
        enabled: true
        script: |
          #!/bin/sh
          set -e
          # Clone agent config if not present
          if [ ! -f "$HOME/AGENTS.md" ]; then
            git clone --depth 1 https://github.com/org/config /tmp/cfg
            cp /tmp/cfg/AGENTS.md "$HOME/"
            cp -r /tmp/cfg/bin/ "$HOME/bin/"
          fi
```

2. **initContainer (opt-in):**

```yaml
agents:
  kiro:
    initContainers:
      - name: fetch-config
        image: alpine/git
        command: ["git", "clone", "--depth", "1", "https://github.com/org/config", "/data/home/config"]
```

---

## 6. Examples

### Minimal: just AGENTS.md from a ConfigMap

```toml
# No [hooks] needed — mount AGENTS.md via ConfigMap volume
```

### Medium: clone a config repo on startup

```bash
#!/bin/sh
# /etc/openab/pre-boot.sh
set -e
REPO="https://github.com/myorg/agent-config"
DEST="$HOME/.agent-config"

if [ ! -d "$DEST/.git" ]; then
  git clone --depth 1 "$REPO" "$DEST"
else
  git -C "$DEST" pull --ff-only 2>/dev/null || true
fi

ln -sf "$DEST/AGENTS.md" "$HOME/AGENTS.md"
ln -sf "$DEST/bin" "$HOME/bin"
```

### Advanced: token refresh + conditional setup

```bash
#!/bin/sh
# /etc/openab/pre-boot.sh
set -e

# Refresh OAuth if expired (token file on PVC)
if [ -f "$HOME/.kiro/auth.json" ]; then
  EXPIRES=$(jq -r '.expires' "$HOME/.kiro/auth.json")
  NOW=$(date +%s)
  if [ "$NOW" -gt "$EXPIRES" ]; then
    echo "[pre_boot] OAuth token expired, attempting refresh..."
    kiro-cli auth refresh || echo "[pre_boot] refresh failed, will use device flow on first request"
  fi
fi

# Ensure skills are present
cp -rn /etc/openab/skills/ "$HOME/.kiro/skills/" 2>/dev/null || true
```

---

## 7. Enterprise Cloud Patterns

Enterprise deployments typically use a **two-layer pull model**: a shared org base followed by team/user personalization. The pod's cloud IAM identity (no secrets needed) authenticates to object storage.

### Layering Model

```
┌─────────────────────────────────────────┐
│  Layer 2: Personal/Team artifacts       │  ← s3 sync / gsutil rsync / azcopy sync
│  (custom skills, steering, overrides)   │
├─────────────────────────────────────────┤
│  Layer 1: Org base tarball              │  ← s3 cp | tar xz (one-time)
│  (AGENTS.md, shared bins, common docs)  │
├─────────────────────────────────────────┤
│  Container image (openab + agent CLI)   │
└─────────────────────────────────────────┘
```

### Cloud-Agnostic Auth

| Cloud | Pod Identity Mechanism | CLI |
|-------|----------------------|-----|
| AWS | IRSA / EKS Pod Identity | `aws s3 cp`, `aws s3 sync` |
| GCP | Workload Identity | `gcloud storage cp`, `gcloud storage rsync` |
| Azure | Workload Identity Federation | `az storage blob download`, `azcopy sync` |

All three inject credentials via projected tokens or metadata — no secrets in config, no env vars to manage.

### CLI Tooling: Self-Bootstrapping

The base openab image does NOT include cloud CLIs. The `pre_boot` script self-bootstraps using `curl` (which is in the base image):

```bash
# Install AWS CLI to $HOME/bin (persists on PVC across restarts)
if [ ! -x "$HOME/bin/aws" ]; then
  mkdir -p "$HOME/bin"
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip
  unzip -q /tmp/awscli.zip -d /tmp
  /tmp/aws/install --install-dir "$HOME/aws-cli" --bin-dir "$HOME/bin"
  rm -rf /tmp/awscli.zip /tmp/aws
fi
export PATH="$HOME/bin:$PATH"
```

Since `$HOME` is on the PVC, the CLI install survives pod restarts — it only downloads once.

### Canonical Enterprise Workflow

The enterprise platform team maintains a single `values-prod.yaml`. No custom Dockerfile, no custom chart — stock image, stock chart, all customization in values:

```yaml
# values-prod.yaml
agents:
  kiro:
    hooks:
      preBoot:
        enabled: true
        timeoutSeconds: 120
        onFailure: abort
        env:
          BOOTSTRAP_BASE_URI: "s3://acme-openab/base.tar.gz"
          BOOTSTRAP_PERSONAL_URI: "s3://acme-openab/teams/platform/alice/"
        script: |
          #!/bin/sh
          set -e

          # Self-bootstrap AWS CLI (persists on PVC)
          if [ ! -x "$HOME/bin/aws" ]; then
            mkdir -p "$HOME/bin"
            curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip
            unzip -q /tmp/awscli.zip -d /tmp
            /tmp/aws/install --install-dir "$HOME/aws-cli" --bin-dir "$HOME/bin"
            rm -rf /tmp/awscli.zip /tmp/aws
          fi
          export PATH="$HOME/bin:$PATH"

          # Layer 1: org-wide base (one-time)
          if [ ! -f "$HOME/.bootstrapped" ]; then
            aws s3 cp "$BOOTSTRAP_BASE_URI" - | tar xz -C "$HOME"
            touch "$HOME/.bootstrapped"
          fi

          # Layer 2: team/user personalization (always fresh)
          aws s3 sync "$BOOTSTRAP_PERSONAL_URI" "$HOME/"
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/openab-agent
```

Deploy:
```bash
helm upgrade openab openab/openab -f values-prod.yaml
```

> **Note:** Alternatively, an initContainer can pre-install the cloud CLI into a shared `emptyDir` volume (mounted at `/tools` on both containers), eliminating the curl/install step from the script. This trades a simpler script for more Helm template complexity — use whichever fits your team's preference.

### Design Principle

openab is cloud-agnostic — it doesn't know or care about AWS/GCP/Azure. The `[hooks.pre_boot]` script is the abstraction boundary. The Helm chart is unopinionated — it provides plumbing (ConfigMap mount, env pass-through) without prescribing cloud-specific logic. Enterprises write their script once in values, and every agent pod gets it.

### ECS Fargate Workflow

ECS has no Helm, no ConfigMap, no initContainer. The `inline` option is the native fit — the script lives directly in `config.toml`, which is injected via environment variable, S3 fetch in entrypoint, or EFS mount.

**config.toml with inline pre_boot:**

```toml
[hooks.pre_boot]
timeout_seconds = 120
on_failure = "abort"
inline = '''
#!/bin/sh
set -e

# Self-bootstrap AWS CLI (EFS or ephemeral — re-installs each cold start)
if [ ! -x "$HOME/bin/aws" ]; then
  mkdir -p "$HOME/bin"
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip
  unzip -q /tmp/awscli.zip -d /tmp
  /tmp/aws/install --install-dir "$HOME/aws-cli" --bin-dir "$HOME/bin"
  rm -rf /tmp/awscli.zip /tmp/aws
fi
export PATH="$HOME/bin:$PATH"

# Pull artifacts (task role provides credentials automatically)
if [ ! -f "$HOME/.bootstrapped" ]; then
  aws s3 cp "$BOOTSTRAP_BASE_URI" - | tar xz -C "$HOME"
  touch "$HOME/.bootstrapped"
fi
aws s3 sync "$BOOTSTRAP_PERSONAL_URI" "$HOME/"
'''

[agent]
command = "kiro-cli"
args = ["acp", "--trust-all-tools"]
env = { BOOTSTRAP_BASE_URI = "s3://acme-openab/base.tar.gz", BOOTSTRAP_PERSONAL_URI = "s3://acme-openab/teams/platform/alice/" }
```

**ECS task definition highlights:**

- Task IAM role grants `s3:GetObject` / `s3:ListBucket` / `s3:PutObject` — no credentials in config
- Config.toml stored in S3 or Secrets Manager, fetched at container start via entrypoint wrapper

**Storage options:**

| Mode | Volume | CLI install | Artifacts | OAuth tokens |
|------|--------|-------------|-----------|--------------|
| EFS | EFS mount at `$HOME` | Persists (install once) | Persists | Persists |
| Ephemeral | Task ephemeral storage | Re-installs each cold start | Re-pulls each start | Requires S3 backup/restore |

**Ephemeral mode with S3 backup/restore:**

When no EFS is available, the `pre_boot` script restores state from S3 on start, and `pre_shutdown` backs up state before exit:

```toml
[hooks.pre_boot]
timeout_seconds = 120
on_failure = "abort"
inline = '''
#!/bin/sh
set -e

# Install AWS CLI
if [ ! -x "$HOME/bin/aws" ]; then
  mkdir -p "$HOME/bin"
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip
  unzip -q /tmp/awscli.zip -d /tmp
  /tmp/aws/install --install-dir "$HOME/aws-cli" --bin-dir "$HOME/bin"
  rm -rf /tmp/awscli.zip /tmp/aws
fi
export PATH="$HOME/bin:$PATH"

# Restore state from S3 (tokens, settings, cached artifacts)
aws s3 sync "$STATE_BUCKET/$TASK_FAMILY/" "$HOME/" --quiet

# Pull org artifacts (always, in case base tarball updated)
aws s3 cp "$BOOTSTRAP_BASE_URI" - | tar xz -C "$HOME"
aws s3 sync "$BOOTSTRAP_PERSONAL_URI" "$HOME/"
'''

[hooks.pre_shutdown]
timeout_seconds = 30
on_failure = "warn"
inline = '''
#!/bin/sh
export PATH="$HOME/bin:$PATH"
aws s3 sync "$HOME/" "s3://$STATE_BUCKET/$TASK_FAMILY/" \
  --exclude "aws-cli/*" --exclude "bin/*" --quiet
'''

[agent]
command = "kiro-cli"
args = ["acp", "--trust-all-tools"]
env = { BOOTSTRAP_BASE_URI = "s3://acme-openab/base.tar.gz", BOOTSTRAP_PERSONAL_URI = "s3://acme-openab/teams/platform/alice/", STATE_BUCKET = "s3://acme-openab/state", TASK_FAMILY = "openab-kiro" }
```

**Lifecycle on Fargate Spot:**

```
Task starts → pre_boot (restore from S3) → agent ready → ... running ...
Spot reclaim → SIGTERM → pre_shutdown (backup to S3) → exit
Task restarts → pre_boot (restore from S3) → agent ready (as if nothing happened)
```

- Fargate Spot works fine since `pre_boot` is idempotent — worst case is a re-download on spot reclaim
- Ephemeral mode trades startup latency for simpler infra (no EFS to manage)
- EFS mode is preferred when OAuth tokens must survive task replacement without re-auth

---

## 8. Migration

- **Existing deployments:** No change required. `[hooks]` is optional; omitting it preserves current behavior.
- **Operators with custom entrypoints:** Can migrate their entrypoint logic into a `pre_boot` script and use the stock image.

---

## 9. Future Considerations

- **Readiness integration:** Expose readiness only after `pre_boot` completes (useful for k8s readiness probes).
- **`post_boot` / `pre_session` / `post_session` hooks:** Natural extensions under the same `[hooks]` namespace — each gets its own sub-table with the same field schema.
- **Caching for `url` mode:** Cache the fetched script on PVC, only re-fetch if sha256 in config changes (avoids network dependency on routine restarts).
