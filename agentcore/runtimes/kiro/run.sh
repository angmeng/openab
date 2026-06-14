#!/usr/bin/env bash
# Kiro CLI launcher for AgentCore Runtime.
# Fetches KIRO_API_KEY from AgentCore Identity Token Vault at each invocation.
set -euo pipefail

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-${AWS_REGION:-us-east-1}}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export HOME="/home/agent"

WORKLOAD_NAME="${AGENTCORE_WORKLOAD_NAME:-kiro-coding-agent}"
CREDENTIAL_PROVIDER="${AGENTCORE_CREDENTIAL_PROVIDER:-kiro-api-key}"

fetch_api_key() {
  python3 -W ignore -c "
import boto3, sys, warnings
warnings.filterwarnings('ignore')
from botocore.config import Config
config = Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 2})
client = boto3.client('bedrock-agentcore', region_name='${AWS_DEFAULT_REGION}', config=config)
try:
    token = client.get_workload_access_token(workloadName='${WORKLOAD_NAME}')['workloadAccessToken']
    key = client.get_resource_api_key(
        workloadIdentityToken=token,
        resourceCredentialProviderName='${CREDENTIAL_PROVIDER}'
    )['apiKey']
    print(key, end='')
except Exception as e:
    print(f'[identity] Failed to fetch key: {e}', file=sys.stderr)
"
}

echo "[auth] Fetching KIRO_API_KEY from AgentCore Identity Token Vault..."
KIRO_API_KEY="$(fetch_api_key)"
export KIRO_API_KEY

if [ -n "$KIRO_API_KEY" ]; then
  echo "[auth] KIRO_API_KEY retrieved successfully"
else
  echo "[auth] WARNING: Could not retrieve KIRO_API_KEY"
  exec kiro-cli login --use-device-flow
fi

# Parse args: ACTION "prompt"
# Note: Extra flags are sourced from KIRO_EXTRA_FLAGS env var (operator-controlled)
# rather than from argv, to prevent argument injection from untrusted prompts.
ACTION="${1:-interactive}"
shift 2>/dev/null || true
PROMPT="$*"

cd "$HOME"

case "$ACTION" in
  interactive) exec kiro-cli ;;
  chat) exec kiro-cli chat --no-interactive --trust-all-tools ${KIRO_EXTRA_FLAGS:-} -- "$PROMPT" ;;
  *) exec kiro-cli "$ACTION" ${KIRO_EXTRA_FLAGS:-} -- "$PROMPT" ;;
esac
