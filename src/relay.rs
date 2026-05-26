//! Cross-channel relay marker support.
//!
//! Lets agents emit a block in their response that gets forwarded to a different
//! platform's channel via a peer adapter. Spec: see
//! `AI-Memory/shared/proposals/2026-05-26-openab-cross-channel-relay.md`.
//!
//! Marker grammar (block-only, line-anchored — same approach as
//! `slack::extract_file_send_markers` post-2026-05-26):
//!
//! ```text
//! <<openab-relay-to discord:swat-team>>
//! body line 1
//! body line 2
//! <<openab-relay-end>>
//! ```
//!
//! Two parsing entry points:
//! - [`extract_relay_blocks`] — final-pass extractor. Returns residual text +
//!   parsed directives + parse errors.
//! - [`mask_streaming_relay_blocks`] — streaming-time mask. Hides in-progress
//!   blocks from the placeholder edit loop so bare markers never appear to the
//!   origin user.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use serde::Deserialize;

use crate::adapter::ChatAdapter;

// --- Marker constants ---

/// Opener prefix. Body of opener line is `<<openab-relay-to {platform}:{alias}>>`.
pub(crate) const OPENER_PREFIX: &str = "<<openab-relay-to ";
pub(crate) const OPENER_SUFFIX: &str = ">>";
pub(crate) const CLOSER_LINE: &str = "<<openab-relay-end>>";

/// File-send marker prefix — mirrored from `slack.rs:22`. Used by
/// [`strip_file_send_markers`] to neutralize file-send markers inside relay
/// bodies before peer dispatch (prevents Slack-targeted relay from triggering
/// unintended file uploads).
const FILE_SEND_MARKER_PREFIX: &str = "<<openab-send-file ";
const FILE_SEND_MARKER_SUFFIX: &str = ">>";

/// v4: max allowed prefix_template length. Well below smallest peer
/// message_limit (Discord 2000, Slack 4000). Prevents format::split_message
/// from char-by-char splitting an overlong template mid-string.
pub const PREFIX_TEMPLATE_MAX_LEN: usize = 200;

// --- Config types ---

#[derive(Clone, Debug, Deserialize)]
pub struct RelayConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_prefix_template")]
    pub prefix_template: String,
    #[serde(default)]
    pub aliases: HashMap<String, RelayAlias>,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prefix_template: default_prefix_template(),
            aliases: HashMap::new(),
        }
    }
}

fn default_prefix_template() -> String {
    "[via {sender_persona} from {sender_platform}]".to_string()
}

impl RelayConfig {
    /// Validate config at load time. Called from main.rs after deserialization.
    pub fn validate(&self) -> Result<(), String> {
        if self.prefix_template.len() > PREFIX_TEMPLATE_MAX_LEN {
            return Err(format!(
                "relay.prefix_template too long ({} > {} bytes)",
                self.prefix_template.len(),
                PREFIX_TEMPLATE_MAX_LEN,
            ));
        }
        for key in self.aliases.keys() {
            if !key.contains(':') {
                return Err(format!(
                    "relay.aliases key {key:?} missing 'platform:' prefix"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RelayAlias {
    pub channel_id: String,
}

// --- Parse results ---

#[derive(Clone, Debug)]
pub struct RelayDirective {
    pub target_platform: String,
    pub target_alias: String,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayParseError {
    UnterminatedOpener { line: usize, target: String },
    NestedOpener { line: usize, outer_target: String },
    MalformedOpener { line: usize, raw: String },
}

#[derive(Clone, Debug, Default)]
pub struct RelayExtractResult {
    pub residual: String,
    pub directives: Vec<RelayDirective>,
    pub errors: Vec<RelayParseError>,
}

// --- Runtime context ---

/// Built once in main.rs after both adapters are constructed. Peer refs are
/// `Weak` to avoid cross-adapter Arc cycles.
pub struct RelayContext {
    pub config: Arc<RelayConfig>,
    pub peers: HashMap<String, Weak<dyn ChatAdapter>>,
}

impl RelayContext {
    pub fn peer(&self, platform: &str) -> Option<Arc<dyn ChatAdapter>> {
        self.peers.get(platform).and_then(Weak::upgrade)
    }
}

// --- Parser helpers ---

/// Returns `Some((platform, alias))` if `trimmed_line` is a well-formed opener,
/// `None` if it's not an opener at all, or returns an error variant signalling
/// a malformed opener (prefix present but body doesn't parse).
fn parse_opener_line(trimmed: &str) -> OpenerKind {
    if !trimmed.starts_with(OPENER_PREFIX) || !trimmed.ends_with(OPENER_SUFFIX) {
        return OpenerKind::NotOpener;
    }
    let inner = &trimmed[OPENER_PREFIX.len()..trimmed.len() - OPENER_SUFFIX.len()];
    let inner = inner.trim();
    match inner.split_once(':') {
        Some((platform, alias)) => {
            let platform = platform.trim();
            let alias = alias.trim();
            if platform.is_empty() || alias.is_empty() {
                OpenerKind::Malformed(trimmed.to_string())
            } else {
                OpenerKind::Valid {
                    platform: platform.to_string(),
                    alias: alias.to_string(),
                }
            }
        }
        None => OpenerKind::Malformed(trimmed.to_string()),
    }
}

enum OpenerKind {
    NotOpener,
    Valid { platform: String, alias: String },
    Malformed(String),
}

fn is_closer_line(trimmed: &str) -> bool {
    trimmed == CLOSER_LINE
}

// --- Public API ---

/// Final-pass parser. Line-anchored block extraction.
///
/// Always returns `RelayExtractResult`. If `content` has no opener markers,
/// `residual == content` and both vecs are empty.
pub fn extract_relay_blocks(content: &str) -> RelayExtractResult {
    if !content.contains(OPENER_PREFIX) {
        return RelayExtractResult {
            residual: content.to_string(),
            directives: Vec::new(),
            errors: Vec::new(),
        };
    }

    let mut residual_lines: Vec<&str> = Vec::new();
    let mut directives: Vec<RelayDirective> = Vec::new();
    let mut errors: Vec<RelayParseError> = Vec::new();

    let lines: Vec<&str> = content.split('\n').collect();
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();
        match parse_opener_line(trimmed) {
            OpenerKind::NotOpener => {
                residual_lines.push(line);
                i += 1;
            }
            OpenerKind::Malformed(raw) => {
                errors.push(RelayParseError::MalformedOpener {
                    line: i + 1,
                    raw,
                });
                residual_lines.push(line);
                i += 1;
            }
            OpenerKind::Valid { platform, alias } => {
                let opener_line_no = i + 1;
                let mut body_lines: Vec<&str> = Vec::new();
                let mut j = i + 1;
                let mut closed = false;
                while j < lines.len() {
                    let inner_line = lines[j];
                    let inner_trimmed = inner_line.trim();
                    if is_closer_line(inner_trimmed) {
                        closed = true;
                        break;
                    }
                    if let OpenerKind::Valid { .. } = parse_opener_line(inner_trimmed) {
                        errors.push(RelayParseError::NestedOpener {
                            line: j + 1,
                            outer_target: format!("{platform}:{alias}"),
                        });
                        // Keep scanning until closer — body preserves the
                        // nested opener line verbatim (no double-extraction).
                    }
                    body_lines.push(inner_line);
                    j += 1;
                }
                if closed {
                    directives.push(RelayDirective {
                        target_platform: platform,
                        target_alias: alias,
                        body: body_lines.join("\n"),
                    });
                    i = j + 1; // skip past closer
                } else {
                    // Unterminated — preserve opener line as literal residual,
                    // record error, do NOT consume body lines (they go back
                    // into residual as normal text from i+1 onward).
                    errors.push(RelayParseError::UnterminatedOpener {
                        line: opener_line_no,
                        target: format!("{platform}:{alias}"),
                    });
                    residual_lines.push(line);
                    i += 1;
                }
            }
        }
    }

    RelayExtractResult {
        residual: residual_lines.join("\n"),
        directives,
        errors,
    }
}

/// Streaming-time mask. Hides in-progress relay blocks from the placeholder
/// edit loop. Called on every stream tick from `adapter.rs`.
///
/// Behaviour:
/// - No opener present → returns `Cow::Borrowed(content)` (fast path).
/// - Complete opener+closer pair → replaced with `[relaying to platform:alias…]`.
/// - Unterminated opener (still streaming) → truncated at opener line, status
///   line appended. The final dispatch happens post-stream.
///
/// Idempotent: `mask(mask(x)) == mask(x)` because the status line `[relaying
/// to ...]` does not start with `<<openab-relay-to ` so won't be re-parsed.
pub fn mask_streaming_relay_blocks(content: &str) -> Cow<'_, str> {
    if !content.contains(OPENER_PREFIX) {
        return Cow::Borrowed(content);
    }

    let lines: Vec<&str> = content.split('\n').collect();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();
        match parse_opener_line(trimmed) {
            OpenerKind::Valid { platform, alias } => {
                // Scan for closer.
                let mut j = i + 1;
                let mut closed = false;
                while j < lines.len() {
                    if is_closer_line(lines[j].trim()) {
                        closed = true;
                        break;
                    }
                    j += 1;
                }
                out.push(format!("[relaying to {platform}:{alias}…]"));
                if closed {
                    i = j + 1;
                } else {
                    // Unterminated — swallow everything after opener (it's
                    // still streaming). Mask runs again on next tick.
                    break;
                }
            }
            _ => {
                out.push(line.to_string());
                i += 1;
            }
        }
    }

    Cow::Owned(out.join("\n"))
}

/// Loop-guard: rejects if body still contains an opener marker line.
pub fn contains_relay_marker(body: &str) -> bool {
    if !body.contains(OPENER_PREFIX) {
        return false;
    }
    body.split('\n')
        .any(|l| matches!(parse_opener_line(l.trim()), OpenerKind::Valid { .. }))
}

/// Apply prefix template — substitutes {sender_persona}, {sender_platform}.
pub fn format_relay_body(template: &str, persona: &str, platform: &str, body: &str) -> String {
    let prefix = template
        .replace("{sender_persona}", persona)
        .replace("{sender_platform}", platform);
    if prefix.is_empty() {
        body.to_string()
    } else {
        format!("{prefix}\n{body}")
    }
}

/// Strip `<<openab-send-file PATH>>` marker lines from a relay body before
/// peer dispatch. Same line-anchored rules as the file-send extractor itself.
///
/// Prevents Slack-targeted relay bodies from triggering unintended file
/// uploads via `slack.rs:620` file-send extractor.
pub fn strip_file_send_markers(body: &str) -> String {
    if !body.contains(FILE_SEND_MARKER_PREFIX) {
        return body.to_string();
    }
    body.split('\n')
        .filter(|line| {
            let trimmed = line.trim();
            !(trimmed.starts_with(FILE_SEND_MARKER_PREFIX)
                && trimmed.ends_with(FILE_SEND_MARKER_SUFFIX))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- extract_relay_blocks: final-pass parser ---

    #[test]
    fn no_markers_returns_empty_result_with_residual_intact() {
        let input = "hello\nworld\n";
        let r = extract_relay_blocks(input);
        assert_eq!(r.residual, input);
        assert!(r.directives.is_empty());
        assert!(r.errors.is_empty());
    }

    #[test]
    fn parse_single_block() {
        let input = "before\n<<openab-relay-to discord:swat-team>>\nhello fern\n<<openab-relay-end>>\nafter";
        let r = extract_relay_blocks(input);
        assert_eq!(r.residual, "before\nafter");
        assert_eq!(r.directives.len(), 1);
        assert_eq!(r.directives[0].target_platform, "discord");
        assert_eq!(r.directives[0].target_alias, "swat-team");
        assert_eq!(r.directives[0].body, "hello fern");
        assert!(r.errors.is_empty());
    }

    #[test]
    fn parse_multiple_blocks() {
        let input = "<<openab-relay-to discord:a>>\none\n<<openab-relay-end>>\nmiddle\n<<openab-relay-to slack:b>>\ntwo\n<<openab-relay-end>>";
        let r = extract_relay_blocks(input);
        assert_eq!(r.residual, "middle");
        assert_eq!(r.directives.len(), 2);
        assert_eq!(r.directives[0].target_alias, "a");
        assert_eq!(r.directives[1].target_alias, "b");
        assert!(r.errors.is_empty());
    }

    #[test]
    fn parse_empty_body_block() {
        let input = "<<openab-relay-to discord:x>>\n<<openab-relay-end>>";
        let r = extract_relay_blocks(input);
        assert_eq!(r.directives.len(), 1);
        assert_eq!(r.directives[0].body, "");
    }

    #[test]
    fn unterminated_opener_records_error_and_preserves_line() {
        let input = "<<openab-relay-to discord:x>>\nstill streaming";
        let r = extract_relay_blocks(input);
        assert_eq!(r.directives.len(), 0);
        assert_eq!(r.errors.len(), 1);
        match &r.errors[0] {
            RelayParseError::UnterminatedOpener { target, .. } => {
                assert_eq!(target, "discord:x");
            }
            other => panic!("expected UnterminatedOpener, got {other:?}"),
        }
        assert!(r.residual.contains("<<openab-relay-to discord:x>>"));
    }

    #[test]
    fn nested_opener_records_error_emits_outer_directive() {
        let input = "<<openab-relay-to discord:a>>\n<<openab-relay-to slack:b>>\ninner\n<<openab-relay-end>>";
        let r = extract_relay_blocks(input);
        assert_eq!(r.directives.len(), 1);
        assert_eq!(r.directives[0].target_platform, "discord");
        assert_eq!(r.directives[0].target_alias, "a");
        assert!(r.directives[0].body.contains("<<openab-relay-to slack:b>>"));
        assert_eq!(r.errors.len(), 1);
        assert!(matches!(r.errors[0], RelayParseError::NestedOpener { .. }));
    }

    #[test]
    fn preserve_inline_marker_text() {
        let input = "see the marker `<<openab-relay-to discord:x>>` inline here";
        let r = extract_relay_blocks(input);
        assert_eq!(r.residual, input);
        assert!(r.directives.is_empty());
        assert!(r.errors.is_empty());
    }

    #[test]
    fn preserve_quoted_marker_in_code_fence() {
        let input = "```\n<<openab-relay-to discord:x>>\nfoo\n<<openab-relay-end>>\n```";
        let r = extract_relay_blocks(input);
        // Line-anchored: opener occupies its own line (after trim), so this
        // IS treated as a real block. Code fences don't protect content from
        // the parser — agents need to use a different format if they want to
        // demonstrate the marker without firing it (e.g. zero-width space).
        assert_eq!(r.directives.len(), 1);
    }

    #[test]
    fn body_preserves_blank_lines_verbatim() {
        let input = "<<openab-relay-to discord:x>>\nline1\n\nline3\n<<openab-relay-end>>";
        let r = extract_relay_blocks(input);
        assert_eq!(r.directives[0].body, "line1\n\nline3");
    }

    #[test]
    fn whitespace_trimmed_around_opener_and_closer() {
        let input = "  <<openab-relay-to discord:x>>  \nbody\n  <<openab-relay-end>>  ";
        let r = extract_relay_blocks(input);
        assert_eq!(r.directives.len(), 1);
        assert_eq!(r.directives[0].body, "body");
    }

    #[test]
    fn malformed_opener_no_colon() {
        let input = "<<openab-relay-to nocolon>>\nbody\n<<openab-relay-end>>";
        let r = extract_relay_blocks(input);
        assert_eq!(r.directives.len(), 0);
        assert_eq!(r.errors.len(), 1);
        assert!(matches!(r.errors[0], RelayParseError::MalformedOpener { .. }));
    }

    // --- mask_streaming_relay_blocks ---

    #[test]
    fn mask_no_marker_returns_borrowed_fast_path() {
        let input = "hello world";
        let r = mask_streaming_relay_blocks(input);
        assert!(matches!(r, Cow::Borrowed(_)));
        assert_eq!(r, "hello world");
    }

    #[test]
    fn mask_complete_block_replaced_with_status_line() {
        let input = "before\n<<openab-relay-to discord:x>>\nhello\n<<openab-relay-end>>\nafter";
        let r = mask_streaming_relay_blocks(input);
        assert!(!r.contains("<<openab-relay-to"));
        assert!(!r.contains("<<openab-relay-end"));
        assert!(!r.contains("hello"));
        assert!(r.contains("[relaying to discord:x…]"));
        assert!(r.contains("before"));
        assert!(r.contains("after"));
    }

    #[test]
    fn mask_unterminated_opener_truncates_at_opener() {
        let input = "before\n<<openab-relay-to discord:x>>\npartial body still streaming";
        let r = mask_streaming_relay_blocks(input);
        assert!(!r.contains("<<openab-relay-to"));
        assert!(!r.contains("partial body"));
        assert!(r.contains("[relaying to discord:x…]"));
        assert!(r.contains("before"));
    }

    #[test]
    fn mask_idempotent() {
        let input = "before\n<<openab-relay-to discord:x>>\nbody\n<<openab-relay-end>>\nafter";
        let once = mask_streaming_relay_blocks(input).into_owned();
        let twice = mask_streaming_relay_blocks(&once).into_owned();
        assert_eq!(once, twice);
    }

    #[test]
    fn mask_preserves_text_before_opener() {
        let input = "important context\n<<openab-relay-to discord:x>>\nbody\n<<openab-relay-end>>";
        let r = mask_streaming_relay_blocks(input);
        assert!(r.starts_with("important context"));
    }

    #[test]
    fn mask_multiple_complete_blocks_each_replaced() {
        let input = "<<openab-relay-to discord:a>>\n1\n<<openab-relay-end>>\n<<openab-relay-to slack:b>>\n2\n<<openab-relay-end>>";
        let r = mask_streaming_relay_blocks(input);
        assert!(r.contains("[relaying to discord:a…]"));
        assert!(r.contains("[relaying to slack:b…]"));
        assert!(!r.contains("<<openab-relay"));
    }

    // --- contains_relay_marker (loop guard) ---

    #[test]
    fn loop_guard_detects_nested_marker_in_body() {
        let body = "first paragraph\n<<openab-relay-to discord:x>>\nshould_not_relay";
        assert!(contains_relay_marker(body));
    }

    #[test]
    fn loop_guard_clean_body_passes() {
        let body = "first paragraph\nno markers here at all";
        assert!(!contains_relay_marker(body));
    }

    #[test]
    fn loop_guard_inline_marker_does_not_trigger() {
        let body = "see `<<openab-relay-to discord:x>>` example inline";
        assert!(!contains_relay_marker(body));
    }

    // --- format_relay_body ---

    #[test]
    fn prefix_template_substitution() {
        let formatted = format_relay_body(
            "[via {sender_persona} from {sender_platform}]",
            "tifa",
            "slack",
            "hello",
        );
        assert_eq!(formatted, "[via tifa from slack]\nhello");
    }

    #[test]
    fn prefix_template_empty_omits_prefix() {
        let formatted = format_relay_body("", "tifa", "slack", "hello");
        assert_eq!(formatted, "hello");
    }

    // --- strip_file_send_markers ---

    #[test]
    fn strip_removes_file_send_marker_lines() {
        let body = "hi\n<<openab-send-file /etc/passwd>>\nbye";
        let r = strip_file_send_markers(body);
        assert_eq!(r, "hi\nbye");
    }

    #[test]
    fn strip_preserves_inline_marker_text() {
        let body = "see `<<openab-send-file /x>>` inline";
        let r = strip_file_send_markers(body);
        assert_eq!(r, body);
    }

    #[test]
    fn strip_fast_path_no_marker() {
        let body = "plain text";
        let r = strip_file_send_markers(body);
        assert_eq!(r, body);
    }

    // --- RelayConfig::validate ---

    #[test]
    fn validate_rejects_overlong_prefix() {
        let cfg = RelayConfig {
            prefix_template: "x".repeat(PREFIX_TEMPLATE_MAX_LEN + 1),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_alias_without_colon() {
        let mut aliases = HashMap::new();
        aliases.insert(
            "bad-key".to_string(),
            RelayAlias {
                channel_id: "C1".into(),
            },
        );
        let cfg = RelayConfig {
            aliases,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_default() {
        let cfg = RelayConfig::default();
        assert!(cfg.validate().is_ok());
    }

    // --- mask + extract drift property ---

    #[test]
    fn mask_then_extract_consistent_residual_subset() {
        // For any text containing a complete relay block, the extractor's
        // residual must NOT contain the original body text (it should have
        // been routed to a directive). The mask MUST replace the body with
        // a status line that the extractor doesn't recognize. This ensures
        // the two passes can't drift on what counts as a marker.
        let input = "before\n<<openab-relay-to discord:x>>\nsecret\n<<openab-relay-end>>\nafter";
        let extracted = extract_relay_blocks(input);
        assert!(!extracted.residual.contains("secret"));
        assert_eq!(extracted.directives.len(), 1);

        let masked = mask_streaming_relay_blocks(input);
        assert!(!masked.contains("secret"));
        // Mask output run back through extractor: zero directives (status
        // line isn't a valid opener) and zero errors.
        let re_extracted = extract_relay_blocks(&masked);
        assert_eq!(re_extracted.directives.len(), 0);
        assert_eq!(re_extracted.errors.len(), 0);
    }
}
