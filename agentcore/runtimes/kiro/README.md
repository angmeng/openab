# Kiro Runtime for AgentCore

Ready-to-build container that runs Kiro CLI on Amazon Bedrock AgentCore Runtime.

## Build & Deploy

```bash
# Build (arm64 required)
docker buildx build --platform linux/arm64 -t <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/agentcore-kiro:latest . --push

# Create runtime
aws bedrock-agentcore-control create-agent-runtime \
  --agent-runtime-name kiro_agent \
  --agent-runtime-artifact '{"containerConfiguration":{"containerUri":"<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/agentcore-kiro:latest"}}' \
  --role-arn "arn:aws:iam::<ACCOUNT>:role/agentcore-execution-role" \
  --network-configuration '{"networkMode":"PUBLIC"}' \
  --protocol-configuration '{"serverProtocol":"HTTP"}' \
  --region <REGION>

# Store API key in Token Vault
aws bedrock-agentcore-control create-workload-identity --name kiro-coding-agent --region <REGION>
aws bedrock-agentcore-control create-api-key-credential-provider \
  --name kiro-api-key --api-key "$KIRO_API_KEY" --region <REGION>
```

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Container image (amazonlinux:2023 + kiro-cli) |
| `healthcheck.py` | HTTP server: `/ping` + `/invocations` → kiro-cli |
| `run.sh` | Auth (Token Vault) + launch kiro-cli with flags |
| `steering/agent.md` | Default instructions (customize as needed) |

## Authentication

The container fetches `KIRO_API_KEY` from AgentCore Identity Token Vault at each invocation. No plaintext secrets in env vars or config files.
