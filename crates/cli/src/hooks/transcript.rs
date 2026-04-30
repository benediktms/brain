//! Claude Code transcript JSONL parser.
//!
//! Parses the transcript file provided by Claude Code hooks via
//! `transcript_path` in the hook input JSON. The transcript is a JSONL file
//! where each line is a JSON object representing one turn or tool event.
//!
//! This module is the single location for Claude Code transcript parsing logic.
//! Memory crate code must not import this — it would couple the persistence
//! layer to Claude Code's transcript format.

use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Transcript entry types
// ---------------------------------------------------------------------------

/// A single line in the transcript JSONL.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TranscriptEntry {
    /// A tool use invocation by the assistant.
    ToolUse(ToolUseEntry),
    /// The result returned for a tool use.
    ToolResult(ToolResultEntry),
    /// An assistant message (non-tool turn).
    #[serde(other)]
    Other,
}

/// A `tool_use` entry in the transcript.
#[derive(Debug, Deserialize)]
pub struct ToolUseEntry {
    /// Tool name (e.g. `Edit`, `Write`, `MultiEdit`, `Bash`, `Read`).
    pub name: String,
    /// Tool input fields (tool-specific).
    pub input: serde_json::Value,
}

/// A `tool_result` entry in the transcript.
#[derive(Debug, Deserialize)]
pub struct ToolResultEntry {
    /// The tool_use_id this result corresponds to.
    pub tool_use_id: String,
    /// Result content (may be a string or structured value).
    pub content: Option<serde_json::Value>,
    /// Whether the tool call reported an error.
    #[serde(default)]
    pub is_error: bool,
}

// ---------------------------------------------------------------------------
// Parsed transcript
// ---------------------------------------------------------------------------

/// Extracted data from a transcript JSONL file.
#[derive(Debug, Default)]
pub struct ParsedTranscript {
    /// File paths that were edited or written during the session.
    pub edited_files: Vec<String>,
    /// Error strings encountered in tool results.
    pub errors: Vec<String>,
    /// Total number of tool calls in the transcript.
    pub tool_call_count: usize,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse a transcript JSONL file at `path` and extract structured data.
///
/// Lines that fail to parse are silently skipped — partial transcripts
/// (truncated by compaction) must not abort the hook.
pub fn parse_transcript(path: &Path) -> Result<ParsedTranscript> {
    let content = std::fs::read_to_string(path)?;
    parse_transcript_str(&content)
}

/// Parse transcript JSONL from a string (used in tests).
pub fn parse_transcript_str(content: &str) -> Result<ParsedTranscript> {
    let mut result = ParsedTranscript::default();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<TranscriptEntry>(line) else {
            continue;
        };
        match entry {
            TranscriptEntry::ToolUse(tu) => {
                result.tool_call_count += 1;
                if let Some(path) = extract_file_path(&tu)
                    && !result.edited_files.contains(&path)
                {
                    result.edited_files.push(path);
                }
            }
            TranscriptEntry::ToolResult(tr) => {
                if tr.is_error
                    && let Some(msg) = extract_error_message(&tr)
                {
                    result.errors.push(msg);
                }
            }
            TranscriptEntry::Other => {}
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Field extraction helpers
// ---------------------------------------------------------------------------

/// Extract a file path from a tool use entry, if the tool is file-modifying.
///
/// Covers `Edit`, `Write`, and `MultiEdit` tools.
fn extract_file_path(tu: &ToolUseEntry) -> Option<String> {
    match tu.name.as_str() {
        "Edit" | "Write" => tu
            .input
            .get("file_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        "MultiEdit" => tu
            .input
            .get("file_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        _ => None,
    }
}

/// Extract a human-readable error message from a tool result.
fn extract_error_message(tr: &ToolResultEntry) -> Option<String> {
    let content = tr.content.as_ref()?;
    if let Some(s) = content.as_str()
        && !s.is_empty()
    {
        return Some(s.to_string());
    }
    // Array of content blocks
    if let Some(arr) = content.as_array() {
        for item in arr {
            if let Some(text) = item.get("text").and_then(|t| t.as_str())
                && !text.is_empty()
            {
                return Some(text.to_string());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn edit_entry(path: &str) -> String {
        format!(
            r#"{{"type":"tool_use","name":"Edit","input":{{"file_path":"{path}","old_string":"x","new_string":"y"}}}}"#
        )
    }

    fn write_entry(path: &str) -> String {
        format!(
            r#"{{"type":"tool_use","name":"Write","input":{{"file_path":"{path}","content":"data"}}}}"#
        )
    }

    fn bash_entry() -> &'static str {
        r#"{"type":"tool_use","name":"Bash","input":{"command":"cargo build"}}"#
    }

    fn error_result(tool_use_id: &str, msg: &str) -> String {
        format!(
            r#"{{"type":"tool_result","tool_use_id":"{tool_use_id}","is_error":true,"content":"{msg}"}}"#
        )
    }

    #[test]
    fn extracts_edit_file_paths() {
        let content = [edit_entry("src/main.rs"), edit_entry("src/lib.rs")].join("\n");
        let parsed = parse_transcript_str(&content).unwrap();
        assert_eq!(parsed.edited_files, ["src/main.rs", "src/lib.rs"]);
    }

    #[test]
    fn extracts_write_file_paths() {
        let content = write_entry("dist/output.js");
        let parsed = parse_transcript_str(&content).unwrap();
        assert_eq!(parsed.edited_files, ["dist/output.js"]);
    }

    #[test]
    fn deduplicates_file_paths() {
        let content = [edit_entry("src/main.rs"), edit_entry("src/main.rs")].join("\n");
        let parsed = parse_transcript_str(&content).unwrap();
        assert_eq!(parsed.edited_files.len(), 1);
    }

    #[test]
    fn bash_tool_does_not_add_file() {
        let content = bash_entry();
        let parsed = parse_transcript_str(content).unwrap();
        assert!(parsed.edited_files.is_empty());
    }

    #[test]
    fn counts_all_tool_calls() {
        let content = [
            edit_entry("a.rs"),
            bash_entry().to_string(),
            write_entry("b.rs"),
        ]
        .join("\n");
        let parsed = parse_transcript_str(&content).unwrap();
        assert_eq!(parsed.tool_call_count, 3);
    }

    #[test]
    fn extracts_error_from_tool_result() {
        let content = [
            bash_entry().to_string(),
            error_result("id-1", "build failed"),
        ]
        .join("\n");
        let parsed = parse_transcript_str(&content).unwrap();
        assert_eq!(parsed.errors, ["build failed"]);
    }

    #[test]
    fn skips_malformed_lines_gracefully() {
        let content = "not json at all\n".to_string() + &edit_entry("ok.rs");
        let parsed = parse_transcript_str(&content).unwrap();
        assert_eq!(parsed.edited_files, ["ok.rs"]);
    }

    #[test]
    fn empty_transcript_produces_zero_counts() {
        let parsed = parse_transcript_str("").unwrap();
        assert_eq!(parsed.tool_call_count, 0);
        assert!(parsed.edited_files.is_empty());
        assert!(parsed.errors.is_empty());
    }
}
