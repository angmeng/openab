# Reference Architecture: GitHub App Token Vending Machine on AWS

> **This doc is meant to be used with Kiro or any coding CLI.** Prompt your AI agent with something like:
>
> ```
> per https://github.com/openabdev/openab/blob/main/docs/refarch/github-app-token-vending.md set up a GitHub App token vending machine in my AWS account
> ```
>
> and it will guide you through (or handle) the full setup on AWS.

Securely vend short-lived GitHub Installation Access Tokens via an AWS Lambda function, eliminating the need to store GitHub App PEM private keys in GitHub Secrets or CI/CD pipelines.

## Problem Statement

When CI/CD workflows (e.g. GitHub Actions) need to interact with GitHub across multiple organizations, developers face a dilemma:

| Approach | Limitation |
|----------|-----------|
| Fine-grained PAT | Bound to a single organization; cannot cross orgs |
| Classic PAT | Coarse permissions; deprecated path |
| PEM in GitHub Secrets | Workflow can exfiltrate the key; compromise = permanent impersonation |

**Core issue**: Any secret that a workflow can _read_ is a secret that a compromised workflow can _steal_.

## Design Goals

1. **OAB obtains short-lived PAT only** — The agent receives a 1-hour Installation Access Token, never a long-lived credential.
2. **No OAuth device flow required** — No interactive browser login, no token refresh dance. Token vending is fully automated and headless.
3. **No private key is ever exposed to the agent** — The PEM never leaves AWS. The agent cannot read, copy, or exfiltrate it.
4. **Private key always stays in AWS** — Stored in Secrets Manager, accessed only by the Lambda function within the same AWS account.
5. **Vending machine vends short-lived tokens only to OAB** — The Lambda is a controlled gate: it validates the caller and returns a scoped, time-limited token.
6. **IAM control with scoped permissions** — Access to the vending function is governed by IAM policies (Task Role, IRSA, or OIDC trust). Only explicitly authorized workloads can invoke it.

## Solution: Lambda Token Vending Machine

Move the PEM private key to AWS Secrets Manager and expose a Lambda "vending function" that:

1. Receives a request (installation ID, optional permission scopes)
2. Retrieves the PEM from Secrets Manager (never leaves AWS)
3. Signs a short-lived JWT (10 min)
4. Exchanges the JWT for a GitHub Installation Access Token (1 hour)
5. Returns only the token — caller never sees the PEM

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        GitHub Actions Workflow                           │
│                                                                         │
│  permissions:                                                           │
│    id-token: write    ← Enable OIDC                                     │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                          ① OIDC Token (JWT)
                          sts:AssumeRoleWithWebIdentity
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         AWS IAM (OIDC Trust)                             │
│                                                                         │
│  Condition:                                                             │
│    sub = "repo:myorg/myrepo:ref:refs/heads/main"                        │
│    aud = "sts.amazonaws.com"                                            │
│                                                                         │
│  → Only the specified repo + branch can assume this role                │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                    ② Temporary AWS Credentials (15 min)
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    AWS Lambda (Token Vending Function)                   │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  1. Validate installation_id against allowlist                     │  │
│  │  2. Retrieve PEM from Secrets Manager                             │  │
│  │  3. Sign JWT with PEM (RS256, valid 10 min)                       │  │
│  │  4. Call GitHub API to exchange JWT for Installation Token         │  │
│  │  5. Return token only — NEVER return PEM                          │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└───────────┬─────────────────────────────────┬───────────────────────────┘
            │                                 │
   ③ GetSecretValue                  ④ POST /installations/{id}/
            │                            access_tokens
            ▼                                 ▼
┌───────────────────────┐       ┌───────────────────────────┐
│  AWS Secrets Manager  │       │        GitHub API          │
│                       │       │                           │
│  ┌─────────────────┐  │       │  Verify JWT → issue token │
│  │  PEM Private Key │  │       │  (ghs_xxx, valid 1 hour)  │
│  │  🔒 Never leaves │  │       │                           │
│  │     AWS          │  │       └─────────────┬─────────────┘
│  └─────────────────┘  │                     │
└───────────────────────┘                     │
                                              │
                                 ⑤ Installation Access Token
                                    (1 hour, scoped)
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        GitHub Actions Workflow                           │
│                                                                         │
│  git clone https://x-access-token:TOKEN@github.com/org/repo.git        │
│  curl -H "Authorization: Bearer TOKEN" https://api.github.com/...      │
│                                                                         │
│  ✅ Cross-org access (wherever the App is installed)                    │
│  ✅ Workflow NEVER touches the PEM private key                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Variant: OpenAB on ECS Fargate / EKS Pod

When OpenAB runs as a container on AWS (instead of inside GitHub Actions), it uses the native ECS Task Role or EKS IRSA to invoke the vending Lambda directly — no OIDC needed.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              AWS Cloud                                        │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │              ECS Fargate Task / EKS Pod                                 │  │
│  │                                                                        │  │
│  │  ┌─────────────────────────────────────┐                               │  │
│  │  │          OpenAB Agent               │                               │  │
│  │  │                                     │                               │  │
│  │  │  • Discord bot (listens for cmds)   │                               │  │
│  │  │  • Kiro CLI / coding agent          │                               │  │
│  │  │  • PR review, code push, etc.       │                               │  │
│  │  └──────────────┬──────────────────────┘                               │  │
│  │                  │                                                     │  │
│  │                  │ Needs GitHub token                                   │  │
│  │                  │ (push code, create PR, post comments)               │  │
│  │                  ▼                                                     │  │
│  │  ┌─────────────────────────────────────┐                               │  │
│  │  │    Token Vending Client             │                               │  │
│  │  │                                     │                               │  │
│  │  │  1. Use Task Role / Pod IAM Role    │                               │  │
│  │  │  2. Invoke Lambda                   │                               │  │
│  │  │  3. Receive short-lived token       │                               │  │
│  │  └──────────────┬──────────────────────┘                               │  │
│  │                  │                                                     │  │
│  └──────────────────┼─────────────────────────────────────────────────────┘  │
│                     │                                                        │
│        ① lambda:InvokeFunction                                               │
│           (via ECS Task Role / EKS IRSA)                                     │
│                     │                                                        │
│                     ▼                                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │               AWS Lambda (Token Vending Function)                       │  │
│  │                                                                        │  │
│  │   ② GetSecretValue ──────────┐                                         │  │
│  │                               ▼                                        │  │
│  │                    ┌─────────────────────┐                             │  │
│  │                    │  Secrets Manager     │                             │  │
│  │                    │  ┌───────────────┐   │                             │  │
│  │                    │  │ PEM 🔒        │   │                             │  │
│  │                    │  │ Never exposed │   │                             │  │
│  │                    │  └───────────────┘   │                             │  │
│  │                    └─────────────────────┘                             │  │
│  │                                                                        │  │
│  │   ③ Sign JWT (RS256) → POST /installations/{id}/access_tokens          │  │
│  └────────────────────────────────────┬───────────────────────────────────┘  │
│                                       │                                      │
└───────────────────────────────────────┼──────────────────────────────────────┘
                                        │
                               ④ JWT (10 min)
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │         GitHub API           │
                         │                             │
                         │  Verify JWT                  │
                         │  Issue Installation Token    │
                         │  (ghs_xxx, 1 hour)          │
                         └──────────────┬──────────────┘
                                        │
                           ⑤ Installation Access Token
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              AWS Cloud                                        │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │              ECS Fargate Task / EKS Pod                                 │  │
│  │                                                                        │  │
│  │  ┌─────────────────────────────────────┐                               │  │
│  │  │          OpenAB Agent               │                               │  │
│  │  │                                     │    ⑥ Use token:               │  │
│  │  │  gh pr create ...                   │───────────────────────┐       │  │
│  │  │  git push                           │                       │       │  │
│  │  │  gh api /repos/.../comments         │                       │       │  │
│  │  └─────────────────────────────────────┘                       │       │  │
│  └────────────────────────────────────────────────────────────────┼───────┘  │
│                                                                   │          │
└───────────────────────────────────────────────────────────────────┼──────────┘
                                                                    │
                                                                    ▼
                                                     ┌──────────────────────────┐
                                                     │    GitHub Repos           │
                                                     │                          │
                                                     │  • openabdev/openab      │
                                                     │  • other-org/other-repo  │
                                                     │  • (cross-org OK ✅)      │
                                                     └──────────────────────────┘
```

| Step | What happens |
|------|-------------|
| ① | OAB container uses Task Role (ECS) or IRSA (EKS) to invoke Lambda |
| ② | Lambda retrieves PEM from Secrets Manager |
| ③ | Lambda signs JWT and calls GitHub API |
| ④⑤ | GitHub verifies JWT → returns 1-hour Installation Token |
| ⑥ | OAB uses token for git push / PR / comments (cross-org capable) |

> **Note**: Unlike the GitHub Actions flow (which uses OIDC), ECS/EKS workloads use native AWS IAM (Task Role / IRSA) to authenticate with Lambda. No OIDC configuration needed — just grant `lambda:InvokeFunction` permission to the Task Role.

## Security Model

| Layer | Protection |
|-------|-----------|
| GitHub OIDC | Only specified repo + branch can assume the IAM Role |
| IAM Role Trust Policy | Pinned to `repo:org/repo:ref:refs/heads/main` |
| Secrets Manager | PEM never leaves AWS; Lambda is the only consumer |
| Lambda | Can add allowlist checks (repo, org, IP) |
| Installation Token | Short-lived (1 hour); scoped to installed repos |
| CloudTrail | Full audit trail of every token vend |

### Threat Model Comparison

| Scenario | PEM in GitHub Secrets | Token Vending Machine |
|----------|----------------------|----------------------|
| Malicious workflow edit | PEM exfiltrated → permanent compromise | Attacker gets 1-hour token (if they can assume role) |
| Stolen token | N/A (they have the key) | Token expires in 1 hour |
| Revocation | Must rotate PEM + update all consumers | Revoke IAM Role instantly; PEM stays safe |
| Blast radius | All installations of the App | Only repos the assumed role can request |

## Cost

| Resource | Spec | Price/mo |
|----------|------|----------|
| Lambda | ~100 invocations × 128MB × 1s | ~$0 (free tier) |
| Secrets Manager | 1 secret | $0.40 |
| CloudWatch Logs | minimal | ~$0 |
| **Total** | | **< $1/month** |

## Prerequisites

1. **AWS account** with IAM admin access
2. **GitHub App** created in your org (or personal account)
3. **GitHub App Private Key** (`.pem` file)
4. **GitHub App Installation ID** (visible after installing the App on a repo/org)
5. **GitHub Actions OIDC provider** configured in AWS IAM

## Setup

### 1. Create the GitHub App

1. Go to **GitHub Settings → Developer settings → GitHub Apps → New GitHub App**
2. Set permissions (e.g., `Contents: Read & Write`, `Pull requests: Read & Write`)
3. Install the App on target repositories/organizations
4. Note down:
   - **App ID** (numeric)
   - **Installation ID** (from the App's installation page)
5. Generate and download the **Private Key** (`.pem`)

### 2. Store PEM in AWS Secrets Manager

```bash
aws secretsmanager create-secret \
  --name github-app/private-key \
  --secret-string file://your-app.pem \
  --description "GitHub App private key for token vending" \
  --region us-east-1
```

Delete the local `.pem` file after storing:

```bash
shred -u your-app.pem
```

### 3. Configure GitHub OIDC Provider in AWS

Skip if you already have the GitHub OIDC provider in your account.

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

> **Note**: The `--thumbprint-list` value is a required CLI parameter but AWS no longer validates it for GitHub's OIDC provider (as of [July 2023](https://github.blog/changelog/2023-07-13-github-actions-oidc-token-thumbprint-no-longer-needs-to-be-verified/)). AWS automatically fetches and verifies the provider's current certificate. You can use any valid thumbprint placeholder here.

### 4. Create IAM Role with OIDC Trust

```bash
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:ref:refs/heads/main"
        }
      }
    }
  ]
}
EOF

aws iam create-role \
  --role-name GitHubTokenVendingCaller \
  --assume-role-policy-document file://trust-policy.json
```

**Important**: Replace `YOUR_ORG/YOUR_REPO` with the exact repository allowed to call the vending function. Use `repo:YOUR_ORG/*:*` only if you trust ALL repos in the org.

Attach invoke permission:

```bash
cat > invoke-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:github-token-vending"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name GitHubTokenVendingCaller \
  --policy-name InvokeTokenVending \
  --policy-document file://invoke-policy.json
```

### 5. Create the Lambda Function

#### Lambda execution role

```bash
cat > lambda-trust.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
  --role-name GitHubTokenVendingLambda \
  --assume-role-policy-document file://lambda-trust.json

aws iam attach-role-policy \
  --role-name GitHubTokenVendingLambda \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

cat > secrets-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:us-east-1:ACCOUNT_ID:secret:github-app/private-key-*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name GitHubTokenVendingLambda \
  --policy-name ReadGitHubAppPEM \
  --policy-document file://secrets-policy.json
```

#### Lambda code (Python)

```python
# lambda_function.py
import json
import os
import time
import urllib.request
import boto3
import jwt  # PyJWT

secrets_client = boto3.client("secretsmanager")

# Cache PEM in memory across warm invocations
_cached_pem = None

GITHUB_APP_ID = os.environ["GITHUB_APP_ID"]  # Set via Lambda environment variable
ALLOWED_INSTALLATION_IDS = ["INSTALLATION_ID_1"]  # Allowlist


def get_pem():
    global _cached_pem
    if _cached_pem is None:
        resp = secrets_client.get_secret_value(SecretId="github-app/private-key")
        _cached_pem = resp["SecretString"]
    return _cached_pem


def create_jwt(app_id: str, pem: str) -> str:
    now = int(time.time())
    payload = {
        "iat": now - 60,       # Clock drift tolerance
        "exp": now + 600,      # 10 minutes max
        "iss": app_id,
    }
    return jwt.encode(payload, pem, algorithm="RS256")


def get_installation_token(jwt_token: str, installation_id: str) -> dict:
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def lambda_handler(event, context):
    installation_id = event.get("installation_id")

    # Validate input
    if not installation_id:
        return {"statusCode": 400, "body": "missing installation_id"}

    if installation_id not in ALLOWED_INSTALLATION_IDS:
        return {"statusCode": 403, "body": "installation_id not in allowlist"}

    try:
        pem = get_pem()
        jwt_token = create_jwt(GITHUB_APP_ID, pem)
        result = get_installation_token(jwt_token, installation_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "token": result["token"],
                "expires_at": result["expires_at"],
            }),
        }
    except Exception as e:
        print(f"Token vending error: {e}")  # Log to CloudWatch only
        return {"statusCode": 500, "body": "internal error"}
```

#### Package and deploy

```bash
mkdir -p /tmp/lambda-pkg && cd /tmp/lambda-pkg
pip install PyJWT cryptography -t .
cp lambda_function.py .
zip -r9 function.zip .

aws lambda create-function \
  --function-name github-token-vending \
  --runtime python3.12 \
  --role arn:aws:iam::ACCOUNT_ID:role/GitHubTokenVendingLambda \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --timeout 10 \
  --memory-size 128 \
  --environment "Variables={GITHUB_APP_ID=YOUR_APP_ID}" \
  --region us-east-1
```

### 6. GitHub Actions Workflow Usage

```yaml
name: Deploy with vended token

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write   # Required for OIDC
      contents: read

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::ACCOUNT_ID:role/GitHubTokenVendingCaller
          aws-region: us-east-1

      - name: Vend GitHub token
        id: vend
        run: |
          RESPONSE=$(aws lambda invoke \
            --function-name github-token-vending \
            --payload '{"installation_id": "YOUR_INSTALLATION_ID"}' \
            --cli-binary-format raw-in-base64-out \
            /dev/stdout 2>/dev/null)
          TOKEN=$(echo "$RESPONSE" | jq -r '.body' | jq -r '.token')
          echo "::add-mask::$TOKEN"
          echo "token=$TOKEN" >> "$GITHUB_OUTPUT"

      - name: Use the token
        run: |
          git clone https://x-access-token:${{ steps.vend.outputs.token }}@github.com/other-org/private-repo.git
          # Or use for API calls:
          # curl -H "Authorization: Bearer ${{ steps.vend.outputs.token }}" https://api.github.com/repos/...
```

## Hardening Checklist

- [ ] Pin OIDC trust to specific repository AND branch (`refs/heads/main`)
- [ ] Set `ALLOWED_INSTALLATION_IDS` in Lambda to restrict which orgs can be accessed
- [ ] Enable CloudTrail for Lambda invocations — alert on unusual patterns
- [ ] Rotate GitHub App Private Key every 90 days (App supports multiple active keys)
- [ ] Set Lambda concurrency limit to prevent abuse (e.g., `ReservedConcurrentExecutions: 10`)
- [ ] Use Lambda environment encryption with a customer-managed KMS key
- [ ] Enable AWS Config rule to detect IAM trust policy changes
- [ ] Review GitHub App installation permissions quarterly

## Key Rotation Procedure

GitHub Apps support multiple concurrent private keys, enabling zero-downtime rotation:

```bash
# 1. Generate new key in GitHub App settings (UI) → downloads new .pem

# 2. Store new key as a new version
aws secretsmanager put-secret-value \
  --secret-id github-app/private-key \
  --secret-string file://new-key.pem

# 3. Verify Lambda uses new key (invoke and check success)
aws lambda invoke \
  --function-name github-token-vending \
  --payload '{"installation_id": "YOUR_ID"}' \
  /tmp/test-output.json && cat /tmp/test-output.json

# 4. Revoke old key in GitHub App settings (UI)

# 5. Shred local copy
shred -u new-key.pem
```

## Comparison: Why Not Just Use `actions/create-github-app-token`?

| Aspect | `actions/create-github-app-token` | Token Vending Machine |
|--------|----------------------------------|----------------------|
| PEM location | GitHub Secrets (workflow can read) | AWS Secrets Manager (workflow cannot read) |
| Cross-org | Yes | Yes |
| PEM exfiltration risk | Possible via malicious workflow | Not possible — PEM never leaves AWS |
| Audit trail | GitHub audit log only | CloudTrail + CloudWatch |
| Setup complexity | Low (1 action) | Medium (Lambda + IAM + OIDC) |
| Best for | Trusted repos, small teams | High-security, multi-org, compliance |

## FAQ

**Q: Who can assume the IAM Role = who can get tokens?**
A: Yes. The OIDC trust policy is the critical control point. Pin it to specific repos and branches. Use `Condition` blocks aggressively.

**Q: Can an attacker replay a vended token?**
A: The token is valid for 1 hour and scoped to the App's installed repositories. After expiry, it's useless. Compared to a stolen PEM (permanent compromise until rotated), this is significantly better.

**Q: What if my Lambda is compromised?**
A: Attacker could vend tokens until discovered. Mitigation: CloudTrail alerts, concurrency limits, and VPC placement (optional) to restrict network access.

**Q: Can I use this without GitHub Actions (e.g., from ECS, EC2)?**
A: Yes. Any compute with IAM credentials that can invoke the Lambda can vend tokens. Replace the OIDC trust with an appropriate principal (ECS task role, EC2 instance profile, etc.).

## Related

- [GitHub: Creating an installation access token](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app)
- [GitHub: About OIDC for GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/about-security-hardening-with-openid-connect)
- [AWS: Configure OIDC for GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services)
- [`actions/create-github-app-token`](https://github.com/actions/create-github-app-token)
