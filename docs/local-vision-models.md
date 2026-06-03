# Local OpenAI-Compatible Vision Models

OpenAB can pass inbound image attachments to ACP agents as image content blocks, but the downstream coding agent and selected local model must both support image input.

```text
User image
  → OpenAB inbound attachment pipeline
  → ACP ImageContent / ContentBlock::Image
  → Pi/OpenCode model config that declares image input support
  → local vision-capable inference endpoint
```

OpenAB can create the ACP image block, but it cannot make a text-only coding agent or text-only local model understand it. Use this guide for local OpenAI-compatible endpoints such as `llama-server`.

## Requirements

- The local inference server exposes an OpenAI-compatible chat completions endpoint that accepts `image_url` content parts with base64 data URLs.
- The selected model is a vision/multimodal model, not a text-only GGUF.
- The coding-agent model metadata declares image input support.
- The model can still emit reliable tool calls through the OpenAI-compatible endpoint if you want to use it as a coding agent.

## Start a vision-capable `llama-server`

Hosted GGUF from Hugging Face, when the repo includes a multimodal projector:

```bash
llama-server \
  -hf ggml-org/Qwen2.5-VL-7B-Instruct-GGUF:Q4_K_M \
  --alias local-vision \
  --host 0.0.0.0 \
  --port 8080 \
  -c 32768
```

In this mode, `llama.cpp` auto-loads the projector when the hosted GGUF repo provides one.

Local GGUF files require both the text model and the multimodal projector:

```bash
llama-server \
  -m /models/qwen2.5-vl-7b-instruct-q4_k_m.gguf \
  --mmproj /models/mmproj-qwen2.5-vl-7b-instruct-f16.gguf \
  --alias local-vision \
  --host 0.0.0.0 \
  --port 8080 \
  -c 32768
```

Do **not** add `--no-mmproj` for image input. If `llama-server` runs in the same pod as OpenAB and the coding agent, use `http://127.0.0.1:8080/v1`. If it runs as a separate Kubernetes Service, use that Service URL instead.

## Pi Configuration

Pi must declare image input support in `~/.pi/agent/models.json` with `input: ["text", "image"]`.

```bash
kubectl exec deployment/openab-pi -- mkdir -p /home/node/.pi/agent
kubectl exec -i deployment/openab-pi -- sh -c 'cat >/home/node/.pi/agent/models.json'<<'EOF'
{
  "providers": {
    "local-vision": {
      "baseUrl": "http://127.0.0.1:8080/v1",
      "api": "openai-completions",
      "apiKey": "not-needed",
      "compat": {
        "supportsDeveloperRole": false,
        "supportsReasoningEffort": false,
        "maxTokensField": "max_tokens"
      },
      "models": [
        {
          "id": "local-vision",
          "name": "Local Vision (llama.cpp)",
          "reasoning": false,
          "input": ["text", "image"],
          "contextWindow": 32768,
          "maxTokens": 4096,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        }
      ]
    }
  }
}
EOF
```

Then select `local-vision/local-vision` in Pi with `/model`, or verify it with:

```bash
kubectl exec deployment/openab-pi -- pi --list-models local-vision
```

For OpenAB deployments, persist the Pi config on the PVC before starting the bot.

## OpenCode Configuration

OpenCode must declare image input support in `opencode.json` with `modalities.input: ["text", "image"]`.

Create `opencode.json` in the working directory, usually `/home/node`:

```bash
kubectl exec -i deployment/openab-opencode -- sh -c 'cat >/home/node/opencode.json'<<'EOF'
{
  "$schema": "https://opencode.ai/config.json",
  "model": "local-vision/local-vision",
  "provider": {
    "local-vision": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Local llama.cpp vision",
      "options": {
        "baseURL": "http://127.0.0.1:8080/v1",
        "apiKey": "not-needed"
      },
      "models": {
        "local-vision": {
          "name": "Local Vision (llama.cpp)",
          "tool_call": true,
          "limit": {
            "context": 32768,
            "output": 4096
          },
          "modalities": {
            "input": ["text", "image"],
            "output": ["text"]
          }
        }
      }
    }
  }
}
EOF
```

Restart OpenAB/OpenCode after changing the config:

```bash
kubectl rollout restart deployment/openab-opencode
kubectl exec deployment/openab-opencode -- opencode models local-vision
```

`tool_call: true` tells OpenCode the model can be used for coding-agent tool calls. Keep it only if the selected local model and server can reliably emit tool calls through the OpenAI-compatible endpoint.

## Verify Image and Tool Support

Upload a screenshot and confirm the response references the image content. Then verify normal file-edit or shell-tool workflows still work. Image description alone is not enough for a coding agent.

## Local Vision Pitfalls

- Use a vision/multimodal GGUF from the [`llama.cpp` multimodal list](https://github.com/ggml-org/llama.cpp/blob/master/docs/multimodal.md), not a text-only model.
- With local files, always provide the matching `--mmproj` file.
- Do not use `--no-mmproj`; it disables the projector needed for image input.
- Use enough context for image tokens plus coding-agent tools and repository context. Start around 16k-32k if the model and hardware allow it.
- Smoke-test tool calls. Some local vision models can describe images but are not reliable enough for coding-agent tool use.

## Related

- [Inbound Attachments](inbound-attachments.md) — how OpenAB converts uploaded images into ACP image content
- [Pi](pi.md) — Pi coding agent setup
- [OpenCode](opencode.md) — OpenCode setup
