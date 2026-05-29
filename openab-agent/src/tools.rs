use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tracing::debug;

use crate::llm::ToolDef;

/// Validate that a path is within the allowed working directory.
/// This function has NO side-effects — it never creates directories or files.
fn validate_path(path: &str, working_dir: &Path) -> Result<PathBuf> {
    let target = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        working_dir.join(path)
    };

    // For existing paths, canonicalize directly
    if target.exists() {
        let canonical = target.canonicalize()?;
        let canonical_working = working_dir.canonicalize()?;
        if !canonical.starts_with(&canonical_working) {
            return Err(anyhow!(
                "path traversal denied: {} is outside working directory",
                path
            ));
        }
        return Ok(canonical);
    }

    // For non-existent paths, walk up to find the nearest existing ancestor
    let mut ancestor = target.parent();
    while let Some(p) = ancestor {
        if p.exists() {
            let canonical_ancestor = p.canonicalize()?;
            let canonical_working = working_dir.canonicalize()?;
            if !canonical_ancestor.starts_with(&canonical_working) {
                return Err(anyhow!(
                    "path traversal denied: {} is outside working directory",
                    path
                ));
            }
            // Reconstruct the full path relative to the canonicalized ancestor
            let remainder = target.strip_prefix(p).unwrap_or(target.as_path());
            return Ok(canonical_ancestor.join(remainder));
        }
        ancestor = p.parent();
    }

    Err(anyhow!(
        "path traversal denied: no valid ancestor for {}",
        path
    ))
}

/// Build a filtered environment for bash tool execution.
fn build_env(allow_list: &[String]) -> HashMap<String, String> {
    let mut env = HashMap::new();
    for key in &["PATH", "HOME", "USER", "LANG", "TERM", "SHELL"] {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }
    for key in allow_list {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }
    env
}

/// Execute a tool call and return the result as a string.
pub async fn execute_tool(name: &str, input: &Value, working_dir: &Path) -> Result<String> {
    match name {
        "read" => tool_read(input, working_dir),
        "write" => tool_write(input, working_dir),
        "edit" => tool_edit(input, working_dir),
        "bash" => tool_bash(input, working_dir).await,
        _ => Err(anyhow!("unknown tool: {name}")),
    }
}

/// Read file contents or list directory.
fn tool_read(input: &Value, working_dir: &Path) -> Result<String> {
    let path_str = input
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("read: missing 'path' parameter"))?;

    let path = validate_path(path_str, working_dir)?;

    if path.is_dir() {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                entries.push(format!("{name}/"));
            } else {
                entries.push(name);
            }
        }
        entries.sort();
        Ok(entries.join("\n"))
    } else {
        let content =
            std::fs::read_to_string(&path).map_err(|e| anyhow!("read {}: {e}", path.display()))?;

        // Apply optional line range
        let offset = input.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let limit = input.get("limit").and_then(|v| v.as_u64());

        let lines: Vec<&str> = content.lines().collect();
        let start = offset.min(lines.len());
        let end = match limit {
            Some(l) => (start + l as usize).min(lines.len()),
            None => lines.len(),
        };

        Ok(lines[start..end].join("\n"))
    }
}

/// Create or overwrite a file.
fn tool_write(input: &Value, working_dir: &Path) -> Result<String> {
    let path_str = input
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("write: missing 'path' parameter"))?;
    let content = input
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("write: missing 'content' parameter"))?;

    let path = validate_path(path_str, working_dir)?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, content)?;

    Ok(format!(
        "wrote {} bytes to {}",
        content.len(),
        path.display()
    ))
}

/// Replace an exact string in a file.
fn tool_edit(input: &Value, working_dir: &Path) -> Result<String> {
    let path_str = input
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("edit: missing 'path' parameter"))?;
    let old_str = input
        .get("old_str")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("edit: missing 'old_str' parameter"))?;
    let new_str = input
        .get("new_str")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("edit: missing 'new_str' parameter"))?;

    let path = validate_path(path_str, working_dir)?;
    let content = std::fs::read_to_string(&path)
        .map_err(|e| anyhow!("edit: cannot read {}: {e}", path.display()))?;

    let count = content.matches(old_str).count();
    if count == 0 {
        return Err(anyhow!("edit: old_str not found in {}", path.display()));
    }

    let new_content = content.replacen(old_str, new_str, 1);
    std::fs::write(&path, &new_content)?;

    Ok(format!(
        "replaced 1 occurrence in {} ({count} total matches)",
        path.display()
    ))
}

/// Execute a shell command with process group isolation and env filtering.
async fn tool_bash(input: &Value, working_dir: &Path) -> Result<String> {
    let command = input
        .get("command")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("bash: missing 'command' parameter"))?;

    let cmd_working_dir = input
        .get("working_dir")
        .and_then(|v| v.as_str())
        .map(|p| {
            if Path::new(p).is_absolute() {
                PathBuf::from(p)
            } else {
                working_dir.join(p)
            }
        })
        .unwrap_or_else(|| working_dir.to_path_buf());

    let timeout_secs = std::env::var("OPENAB_AGENT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120);

    let env_allow: Vec<String> = std::env::var("OPENAB_AGENT_BASH_ENV_ALLOW")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let env = build_env(&env_allow);

    debug!("bash: executing '{}' in {:?}", command, cmd_working_dir);

    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        .current_dir(&cmd_working_dir)
        .env_clear()
        .envs(&env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    // Create new process group on Unix for clean cleanup
    #[cfg(unix)]
    unsafe {
        #[allow(unused_imports)]
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let child = cmd
        .spawn()
        .map_err(|e| anyhow!("bash: spawn failed: {e}"))?;

    // Capture pid before wait_with_output takes ownership
    #[cfg(unix)]
    let child_pid = child.id();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        child.wait_with_output(),
    )
    .await;

    match result {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let code = output.status.code().unwrap_or(-1);

            let mut result = String::new();
            if !stdout.is_empty() {
                result.push_str(&stdout);
            }
            if !stderr.is_empty() {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str("[stderr]\n");
                result.push_str(&stderr);
            }
            if code != 0 {
                result.push_str(&format!("\n[exit code: {code}]"));
            }
            Ok(result)
        }
        Ok(Err(e)) => Err(anyhow!("bash: execution error: {e}")),
        Err(_) => {
            // Timeout — kill the process group
            #[cfg(unix)]
            if let Some(pid) = child_pid {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
            }
            Err(anyhow!("bash: command timed out after {timeout_secs}s"))
        }
    }
}

/// Return tool definitions for the LLM.
pub fn tool_definitions() -> Vec<ToolDef> {
    vec![
        ToolDef {
            name: "read".to_string(),
            description: "Read file contents or list a directory.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File or directory path" },
                    "offset": { "type": "integer", "description": "Line offset to start reading from (0-indexed)" },
                    "limit": { "type": "integer", "description": "Number of lines to read" }
                },
                "required": ["path"]
            }),
        },
        ToolDef {
            name: "write".to_string(),
            description: "Create or overwrite a file with the given content.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path to write" },
                    "content": { "type": "string", "description": "Content to write" }
                },
                "required": ["path", "content"]
            }),
        },
        ToolDef {
            name: "edit".to_string(),
            description: "Replace the first occurrence of old_str with new_str in a file."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path to edit" },
                    "old_str": { "type": "string", "description": "Exact string to find" },
                    "new_str": { "type": "string", "description": "Replacement string" }
                },
                "required": ["path", "old_str", "new_str"]
            }),
        },
        ToolDef {
            name: "bash".to_string(),
            description: "Execute a shell command and return stdout/stderr.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "Shell command to execute" },
                    "working_dir": { "type": "string", "description": "Working directory (optional, defaults to agent working dir)" }
                },
                "required": ["command"]
            }),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_validate_path_within_working_dir() {
        let tmp = TempDir::new().unwrap();
        let result = validate_path("test.txt", tmp.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_traversal_denied() {
        let tmp = TempDir::new().unwrap();
        let result = validate_path("../../etc/passwd", tmp.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path traversal"));
    }

    #[test]
    #[ignore] // Integration test: filesystem access
    fn test_tool_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let input = json!({ "path": "hello.txt", "content": "hello world" });
        let result = tool_write(&input, tmp.path()).unwrap();
        assert!(result.contains("11 bytes"));

        let read_input = json!({ "path": "hello.txt" });
        let content = tool_read(&read_input, tmp.path()).unwrap();
        assert_eq!(content, "hello world");
    }

    #[test]
    #[ignore] // Integration test: filesystem access
    fn test_tool_edit() {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("test.rs");
        std::fs::write(&file_path, "fn main() {\n    println!(\"old\");\n}\n").unwrap();

        let input = json!({
            "path": "test.rs",
            "old_str": "println!(\"old\")",
            "new_str": "println!(\"new\")"
        });
        let result = tool_edit(&input, tmp.path()).unwrap();
        assert!(result.contains("replaced 1 occurrence"));

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("println!(\"new\")"));
    }

    #[test]
    #[ignore] // Integration test: filesystem access
    fn test_tool_read_directory() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "").unwrap();
        std::fs::write(tmp.path().join("b.txt"), "").unwrap();
        std::fs::create_dir(tmp.path().join("subdir")).unwrap();

        let input = json!({ "path": "." });
        let result = tool_read(&input, tmp.path()).unwrap();
        assert!(result.contains("a.txt"));
        assert!(result.contains("b.txt"));
        assert!(result.contains("subdir/"));
    }

    #[tokio::test]
    #[ignore] // Integration test: subprocess execution
    async fn test_tool_bash_simple() {
        let tmp = TempDir::new().unwrap();
        let input = json!({ "command": "echo hello" });
        let result = tool_bash(&input, tmp.path()).await.unwrap();
        assert_eq!(result.trim(), "hello");
    }

    #[tokio::test]
    #[ignore] // Integration test: subprocess execution
    async fn test_tool_bash_env_filtered() {
        let tmp = TempDir::new().unwrap();
        // Verify that arbitrary env vars are NOT passed through (env is cleared)
        let input = json!({ "command": "env | grep -c ANTHROPIC || true" });
        let result = tool_bash(&input, tmp.path()).await.unwrap();
        // With env_clear(), no ANTHROPIC vars should exist in child
        assert!(result.trim() == "0" || result.trim().is_empty() || result.contains("[exit code:"));
    }
}
