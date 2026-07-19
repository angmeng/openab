# Tool Display Configuration

Control how tool calls are rendered in chat messages during agent responses.

## Configuration

```toml
[reactions]
tool_display = "full"   # full | compact | none
```

### Helm

```yaml
agents:
  kiro:
    reactions:
      toolDisplay: "full"   # full | compact | none
```

## Modes

### `full` (default)

Shows each tool call with its complete title. **Consecutive** repeats of the same tool are collapsed into a single line with an `(×N)` suffix, so a burst like three back-to-back `ToolSearch` calls renders as one line — not three. A non-consecutive repeat (e.g. `curl → grep → curl`) still renders as three separate lines — grouping is adjacency-only, order-preserving. When the resulting **run count** exceeds 3 for either the finished or the still-running set mid-stream, that set collapses into a raw-count summary (`✅ 5 · ❌ 1 tool(s) completed` / `🔧 4 more running` + the trailing few grouped runs).

```
✅ `curl -s "https://ghcr.io/v2/openabdev/charts/openab/tags/list"`
✅ `grep -r "pattern" src/` (×2)
🔧 `npm install`...

Agent response text here...
```

Best for: debugging, understanding what the agent is doing step by step.

### `compact`

Shows a single-line count summary. No tool names, commands, or arguments are displayed.

```
✅ 3 · 🔧 1 tool(s)

Agent response text here...
```

Best for: everyday use, public channels, mobile.

### `none`

Hides tool lines entirely. Only the final agent response is shown. Reaction emojis (🔧→✅) still work, so you can tell the agent is busy.

```
Agent response text here...
```

Best for: clean output when you only care about the final answer.

## Icons

| Icon | Meaning |
|------|---------|
| 🔧 | Tool is running |
| ✅ | Tool completed successfully |
| ❌ | Tool failed |

## Notes

- **Default**: `full` shows complete tool titles. Use `tool_display = "compact"` for a cleaner count-only summary, or `"none"` to hide tools entirely.
- **Reaction emojis are independent**: The emoji reactions on messages (👀→🤔→🔧→🆗) work regardless of `tool_display` setting.
- **Streaming behavior**: In `compact` mode, the count updates in real-time as tools start and finish. In `full` mode, individual and grouped-repeat lines appear up to 3 runs per set (finished / running); once either set has more than 3 runs, that set switches to a raw-count summary (`✅ N · ❌ M tool(s) completed` / `🔧 N more running` + the trailing groups).
