# ADR: Telegram Rich Messages Support

**Status:** Accepted  
**Date:** 2026-06-14  
**Author:** 超渡法師  
**Bot API Version:** 10.1 (June 11, 2026)

## Context

Telegram Bot API 10.1 introduced **Rich Messages** — a structured formatting system that allows bots to send highly formatted content (headings, tables, code blocks, collages, math formulas, etc.) and **stream AI-generated replies** with seamless rich formatting.

Currently, the OAB Telegram adapter in `gateway/src/adapters/telegram.rs` uses `sendMessage` with `parse_mode: Markdown` for all replies. This limits formatting to basic inline styles and lacks support for tables and headings.

## Decision

Add Rich Message support to the Telegram gateway adapter with three capabilities:

### 1. `sendRichMessage` for structured replies

When the agent output contains complex formatting (tables, headings, long-form content), use `sendRichMessage` with `InputRichMessage.markdown` instead of `sendMessage`.

### 2. `sendRichMessageDraft` for AI streaming

Implement streaming UX via the draft → final pattern:

```
sendRichMessageDraft(draft_id=N, rich_message={markdown: partial})  // updates animate
   ... repeat as content grows ...
sendRichMessage(rich_message={markdown: final})                     // persist
```

### 3. `editMessageText` with `rich_message` param

For editing existing rich messages (e.g., updating a streaming reply).

## Architecture

```
Agent output (markdown)
       │
       ▼
┌─────────────────────────────┐
│  TG Adapter (gateway)       │
│                             │
│  classify_reply(content) →  │
│    Simple  → sendMessage    │
│    Complex → sendRichMessage│
│    Stream  → Draft → Final  │
└─────────────────────────────┘
```

### Classification heuristic

A reply is "complex" if it contains any of:
- Markdown table (`|---|`)
- ATX headings (`# `, `## `)
- Content > 4096 chars (sendMessage limit)

Code blocks (triple backtick) are intentionally **not** classified as complex — `sendMessage` preserves syntax highlighting with language headers and copy buttons, which `RichBlockPreformatted` currently lacks.

Otherwise, fall back to `sendMessage` with `Markdown` parse mode for maximum client compatibility.

### Rich Message Limits (from API docs)

| Limit | Value |
|-------|-------|
| Text length | 32,768 UTF-8 chars |
| Blocks | 500 |
| Nesting | 16 levels |
| Media | 50 attachments |
| Table columns | 20 |

## Implementation Plan

### Phase 1: `sendRichMessage` (basic)

- Add `send_rich_message()` function to `telegram.rs`
- Add classification logic in `handle_reply()`
- Pass markdown directly to `InputRichMessage.markdown` (no conversion needed — Rich Markdown is GitHub Flavored Markdown compatible)

### Phase 2: Streaming via `sendRichMessageDraft`

- Add `send_rich_message_draft()` function
- Integrate with existing gateway streaming infrastructure
- Use `RichBlockThinking` for "thinking" animation during agent processing

### Phase 3: Fallback & compatibility

- Detect API errors (old clients / bot API version mismatch) and fall back to `sendMessage`
- Feature-gate behind env var: `TELEGRAM_RICH_MESSAGES=true` (default on, set `=false` to opt out)

## Consequences

**Positive:**
- Much better formatting for PR reviews, code analysis, structured reports
- Native streaming UX for AI responses (no more "typing..." placeholder)
- No conversion layer needed — agent markdown passes through directly

**Negative:**
- Requires Bot API 10.1+ (June 2026) — older Telegram clients may not render correctly
- Two code paths to maintain (sendMessage vs sendRichMessage)

## References

- [Bot API 10.1 Changelog](https://core.telegram.org/bots/api#june-11-2026)
- [Rich Message Formatting Options](https://core.telegram.org/bots/api#rich-message-formatting-options)
- [sendRichMessage](https://core.telegram.org/bots/api#sendrichmessage)
- [sendRichMessageDraft](https://core.telegram.org/bots/api#sendrichmessagedraft)
