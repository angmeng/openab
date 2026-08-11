//! File-send confinement — the security boundary for `<<openab-send-file>>`.
//!
//! The marker is agent *output text*, not a tool call, so no PreToolUse hook
//! on the agent side ever sees it: without a boundary here, an agent could
//! name any host path (`~/.openab/.env`, another lane's files) and have the
//! bridge upload it to a chat channel. Policy: a marker path must resolve —
//! after canonicalization, so a symlink inside the workspace can't smuggle a
//! file from outside it — to inside the agent's `working_dir`. Files that
//! legitimately live outside the root are delivered by copying them into the
//! workspace first, which turns the invisible marker into a tool call the
//! host's hooks can inspect.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{anyhow, Result};

static FILE_SEND_ROOT: OnceLock<PathBuf> = OnceLock::new();

/// Set the allowed root for file-send markers (the agent `working_dir`).
/// Called once at startup, right after config load; later calls are no-ops.
/// The root is canonicalized so the later prefix checks compare like with
/// like (macOS case differences, `/tmp` → `/private/tmp`, …).
pub fn set_file_send_root(root: &str) {
    let canonical = std::fs::canonicalize(root).unwrap_or_else(|_| PathBuf::from(root));
    let _ = FILE_SEND_ROOT.set(canonical);
}

/// Validate a marker path against the configured root and return the
/// canonicalized path callers must use for the actual read — checking one
/// path and reading another would reopen the symlink hole. Fail-closed: if
/// the root was never set, every send is refused; a wiring bug in a security
/// control must be loud, not fail-open.
pub fn confine_file_send_path(path: &str) -> Result<PathBuf> {
    let root = FILE_SEND_ROOT
        .get()
        .ok_or_else(|| anyhow!("file-send root not initialized — refusing to send: {path}"))?;
    confine(path, root)
}

/// Pure check: canonicalize `path` (resolving symlinks) and require it to sit
/// under `root`. `starts_with` compares whole components, so `/root-evil`
/// does not match root `/root`.
pub fn confine(path: &str, root: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|e| anyhow!("cannot resolve path {path}: {e}"))?;
    if !canonical.starts_with(root) {
        return Err(anyhow!(
            "path is outside the bridge working directory (allowed root: {}) — \
             copy the file into the workspace first: {path}",
            root.display()
        ));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fresh unique dir under the OS temp dir, canonicalized (macOS /tmp is a
    /// symlink to /private/tmp — tests must compare canonical to canonical,
    /// exactly like the production wiring does).
    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "openab-file-send-test-{}-{tag}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::canonicalize(&dir).unwrap()
    }

    #[test]
    fn inside_root_is_allowed_and_returns_canonical() {
        let root = scratch("inside");
        let file = root.join("report.mp3");
        std::fs::write(&file, b"audio").unwrap();
        let out = confine(file.to_str().unwrap(), &root).unwrap();
        assert_eq!(out, std::fs::canonicalize(&file).unwrap());
    }

    #[test]
    fn outside_root_is_denied() {
        let root = scratch("outside-root");
        let elsewhere = scratch("outside-elsewhere");
        let file = elsewhere.join("secrets.env");
        std::fs::write(&file, b"TOKEN=x").unwrap();
        let err = confine(file.to_str().unwrap(), &root).unwrap_err();
        assert!(err.to_string().contains("outside the bridge working directory"));
    }

    #[test]
    fn sibling_prefix_dir_is_denied() {
        // /…/root-evil must not pass a check against root /…/root.
        let root = scratch("prefix");
        let evil = PathBuf::from(format!("{}-evil", root.display()));
        std::fs::create_dir_all(&evil).unwrap();
        let file = evil.join("payload.txt");
        std::fs::write(&file, b"x").unwrap();
        assert!(confine(file.to_str().unwrap(), &root).is_err());
    }

    #[test]
    fn symlink_escaping_root_is_denied() {
        let root = scratch("symlink-root");
        let elsewhere = scratch("symlink-elsewhere");
        let target = elsewhere.join("real-secret.txt");
        std::fs::write(&target, b"secret").unwrap();
        let link = root.join("innocent-looking.txt");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = confine(link.to_str().unwrap(), &root).unwrap_err();
        assert!(err.to_string().contains("outside the bridge working directory"));
    }

    #[test]
    fn missing_file_is_denied() {
        let root = scratch("missing");
        let ghost = root.join("does-not-exist.pdf");
        assert!(confine(ghost.to_str().unwrap(), &root).is_err());
    }
}
