use std::fs;
use std::io::Read as _;
use std::path::Path;

use anyhow::{Context, Result};
use brain_persistence::db::summaries::Episode;
use serde_json::{Map, Value, json};

/// The hook entries brain installs into `.claude/settings.json`.
///
/// LEGACY: prefer `brain plugin install` (canonical plugin surface).
/// This path mutates the project's `.claude/settings.json` directly and is
/// retained for advanced / manual use only. New hooks ship via the canonical
/// brain plugin manifest.
///
/// Each entry carries `"_brain_managed": true` so `is_brain_hook` can detect
/// and upgrade entries without command-prefix matching.
fn brain_hooks() -> Value {
    json!({
        "UserPromptSubmit": [
            {
                "_brain_managed": true,
                "hooks": [
                    {
                        "type": "command",
                        "command": "brain tasks list --ready --output=hook-envelope 2>/dev/null"
                    }
                ]
            }
        ],
        "SessionStart": [
            {
                "_brain_managed": true,
                "hooks": [
                    {
                        "type": "command",
                        "command": "brain tasks stats --output=hook-envelope 2>/dev/null"
                    }
                ]
            }
        ]
    })
}

/// Sentinel field injected into every brain-managed hook entry.
///
/// Canonical detection uses this field rather than command-prefix matching so
/// new hook subcommands (`brain hooks pre-compact`, `brain hooks stop`, etc.)
/// are recognised automatically without updating a prefix allowlist.
///
/// LEGACY: command-prefix detection via `BRAIN_COMMAND_PREFIX` is retired.
/// The `_brain_managed` marker is spoof-resistant for the use cases brain
/// controls (plugin-installed hooks) — a foreign hook must opt in explicitly.
const BRAIN_MANAGED_MARKER: &str = "_brain_managed";

fn is_brain_hook(entry: &Value) -> bool {
    entry
        .get(BRAIN_MANAGED_MARKER)
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Merge brain hooks into an existing hooks object, preserving non-brain hooks.
fn merge_hooks(existing: &Value) -> Value {
    let brain = brain_hooks();
    let brain_obj = brain.as_object().unwrap();

    let mut merged = existing.as_object().cloned().unwrap_or_else(Map::new);

    for (event_name, brain_entries) in brain_obj {
        let brain_arr = brain_entries.as_array().unwrap();

        let existing_arr = merged
            .get(event_name)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Keep non-brain hooks, then append brain hooks
        let mut new_arr: Vec<Value> = existing_arr
            .into_iter()
            .filter(|entry| !is_brain_hook(entry))
            .collect();
        new_arr.extend(brain_arr.clone());

        merged.insert(event_name.clone(), Value::Array(new_arr));
    }

    Value::Object(merged)
}

/// Install brain hooks directly into `.claude/settings.json`.
///
/// LEGACY: use `brain plugin install` for the canonical plugin surface.
/// This command is retained for advanced / manual environments where the
/// Claude Code plugin marketplace is unavailable.
pub fn install(dry_run: bool) -> Result<()> {
    let hooks = brain_hooks();

    if dry_run {
        println!("Hook configuration that would be added to .claude/settings.json:\n");
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "hooks": hooks }))?
        );
        return Ok(());
    }

    let claude_dir = Path::new(".claude");
    let settings_path = claude_dir.join("settings.json");

    // Ensure .claude/ directory exists
    fs::create_dir_all(claude_dir).context("Failed to create .claude/ directory")?;

    // Read existing settings or start with empty object
    let mut settings: Value = if settings_path.exists() {
        let content =
            fs::read_to_string(&settings_path).context("Failed to read .claude/settings.json")?;
        serde_json::from_str(&content).context("Failed to parse .claude/settings.json")?
    } else {
        json!({})
    };

    // Merge hooks
    let existing_hooks = settings.get("hooks").cloned().unwrap_or(json!({}));
    let merged = merge_hooks(&existing_hooks);
    settings
        .as_object_mut()
        .unwrap()
        .insert("hooks".to_string(), merged);

    // Write back
    let output = serde_json::to_string_pretty(&settings)?;
    fs::write(&settings_path, format!("{output}\n"))
        .context("Failed to write .claude/settings.json")?;

    println!("Installed brain hooks into .claude/settings.json");
    println!();
    println!("Hooks added:");
    println!("  SessionStart     -> brain tasks stats --output=hook-envelope");
    println!("  UserPromptSubmit -> brain tasks list --ready --output=hook-envelope");

    Ok(())
}

pub fn status() -> Result<()> {
    let settings_path = Path::new(".claude/settings.json");

    if !settings_path.exists() {
        println!("Status: not installed");
        println!("  .claude/settings.json does not exist");
        println!("  Run `brain hooks install` to set up hooks");
        return Ok(());
    }

    let content =
        fs::read_to_string(settings_path).context("Failed to read .claude/settings.json")?;
    let settings: Value =
        serde_json::from_str(&content).context("Failed to parse .claude/settings.json")?;

    let hooks = settings.get("hooks");

    let has_session_start = hooks
        .and_then(|h| h.get("SessionStart"))
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(is_brain_hook));

    let has_prompt_submit = hooks
        .and_then(|h| h.get("UserPromptSubmit"))
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(is_brain_hook));

    if has_session_start && has_prompt_submit {
        println!("Status: installed");
        println!("  SessionStart:      active");
        println!("  UserPromptSubmit:  active");
    } else if has_session_start || has_prompt_submit {
        println!("Status: partially installed");
        println!(
            "  SessionStart:      {}",
            if has_session_start {
                "active"
            } else {
                "missing"
            }
        );
        println!(
            "  UserPromptSubmit:  {}",
            if has_prompt_submit {
                "active"
            } else {
                "missing"
            }
        );
        println!("  Run `brain hooks install` to fix");
    } else {
        println!("Status: not installed");
        println!("  .claude/settings.json exists but has no brain hooks");
        println!("  Run `brain hooks install` to set up hooks");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// PreCompact hook
// ---------------------------------------------------------------------------

/// Hook input JSON as received from Claude Code on stdin.
#[derive(Debug)]
struct PreCompactInput {
    transcript_path: Option<std::path::PathBuf>,
    session_id: Option<String>,
}

fn parse_pre_compact_input(raw: &str) -> PreCompactInput {
    let v: Value = serde_json::from_str(raw).unwrap_or(Value::Object(Map::new()));
    let transcript_path = v
        .get("transcript_path")
        .and_then(|p| p.as_str())
        .map(std::path::PathBuf::from);
    let session_id = v
        .get("session_id")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string());
    PreCompactInput {
        transcript_path,
        session_id,
    }
}

/// `brain hooks pre-compact` — invoked by the Claude Code PreCompact hook.
///
/// Reads hook input from stdin, parses the transcript JSONL, extracts the
/// set of files edited during the session, writes a snapshot episode tagged
/// `urgency:pre-compact` and `session:<id>`, then emits the standard hook
/// envelope so Claude Code injects a summary into the compacted context.
pub fn pre_compact() -> Result<()> {
    // Read hook input from stdin.
    let mut stdin_raw = String::new();
    std::io::stdin()
        .read_to_string(&mut stdin_raw)
        .context("failed to read hook input from stdin")?;

    let input = parse_pre_compact_input(&stdin_raw);

    // Parse transcript (gracefully handle missing path).
    let transcript = if let Some(ref path) = input.transcript_path {
        crate::hooks::transcript::parse_transcript(path).unwrap_or_default()
    } else {
        crate::hooks::transcript::ParsedTranscript::default()
    };

    let session_id = input
        .session_id
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    // Build summary content.
    let edited_list = if transcript.edited_files.is_empty() {
        "(none)".to_string()
    } else {
        transcript.edited_files.join(", ")
    };

    let goal = format!("PreCompact snapshot — session {session_id}");
    let actions = format!(
        "Edited files: {edited_list}. Tool calls: {}.",
        transcript.tool_call_count
    );
    let outcome = if transcript.errors.is_empty() {
        "Session ended without recorded errors.".to_string()
    } else {
        format!("Errors encountered: {}.", transcript.errors.join("; "))
    };

    // Determine tags.
    let mut tags = vec![
        "urgency:pre-compact".to_string(),
        format!("session:{session_id}"),
    ];
    for file in &transcript.edited_files {
        // Tag each edited file so retrieval can find this snapshot by file.
        let basename = std::path::Path::new(file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(file);
        tags.push(format!("file:{basename}"));
    }

    // Open BrainStores and write the episode.
    let stores = open_stores_for_cwd()?;

    let episode = Episode {
        brain_id: stores.brain_id.clone(),
        goal: goal.clone(),
        actions: actions.clone(),
        outcome: outcome.clone(),
        tags,
        importance: 0.8,
    };

    let summary_id = stores.store_episode(&episode)?;

    // Emit hook envelope.
    let context = format!(
        "PreCompact snapshot recorded (id: {summary_id}).\n\
         Edited files this session: {edited_list}\n\
         Tool calls: {}\n\
         {}",
        transcript.tool_call_count,
        if !transcript.errors.is_empty() {
            format!("Errors: {}", transcript.errors.join("; "))
        } else {
            String::new()
        }
    );

    let envelope = crate::hooks::build_hook_envelope("PreCompact", &context);
    println!("{envelope}");

    Ok(())
}

/// Open BrainStores for the current working directory.
///
/// Uses the brain marker file (`.brain/brain.toml`) to locate the registered
/// brain. Falls back to path-based resolution if no marker is found.
pub(crate) fn open_stores_for_cwd() -> Result<brain_lib::stores::BrainStores> {
    let cwd = std::env::current_dir()?;
    if let Some(root) = brain_lib::config::find_brain_root(&cwd) {
        let toml = brain_lib::config::load_brain_toml(&root.join(".brain"))?;
        return brain_lib::stores::BrainStores::from_brain(&toml.name).map_err(anyhow::Error::from);
    }
    // Fallback: derive sqlite_db path from the brain home.
    let brain_home = brain_lib::config::brain_home()?;
    let sqlite_db = brain_home.join("brain.db");
    brain_lib::stores::BrainStores::from_path(&sqlite_db, None).map_err(anyhow::Error::from)
}

// ---------------------------------------------------------------------------
// Stop hook (implemented in node .2)
// ---------------------------------------------------------------------------

/// `brain hooks stop` — invoked by the Claude Code Stop hook.
///
/// Reads transcript JSONL and writes 1–3 episodic memory entries covering
/// edited files, errors, and (for heavy sessions) a session summary.
pub fn stop() -> Result<()> {
    // Placeholder — full implementation added in the Stop hook commit.
    let mut stdin_raw = String::new();
    std::io::stdin().read_to_string(&mut stdin_raw).ok();
    // Emit empty envelope (suppress output; no context to inject on stop).
    println!("{}", crate::hooks::build_hook_envelope("Stop", ""));
    Ok(())
}

// ---------------------------------------------------------------------------
// PreToolUse hook (implemented in node .3)
// ---------------------------------------------------------------------------

/// `brain hooks pre-tool-use` — invoked by the Claude Code PreToolUse hook.
///
/// Retrieves file-scoped memory and injects it before Edit/Write/MultiEdit.
/// Opt-in: requires `auto_inject.pre_edit_recall = true` in brain config.
pub fn pre_tool_use() -> Result<()> {
    // Placeholder — full implementation added in the PreToolUse hook commit.
    let mut stdin_raw = String::new();
    std::io::stdin().read_to_string(&mut stdin_raw).ok();
    // Do nothing when not opted in; emit empty envelope.
    println!("{}", crate::hooks::build_hook_envelope("PreToolUse", ""));
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_pre_compact_input ─────────────────────────────────────────────

    #[test]
    fn parses_transcript_path_from_hook_input() {
        let raw = r#"{"transcript_path":"/tmp/t.jsonl","session_id":"sess-42","trigger":"manual"}"#;
        let input = parse_pre_compact_input(raw);
        assert_eq!(
            input.transcript_path.as_deref().unwrap(),
            std::path::Path::new("/tmp/t.jsonl")
        );
    }

    #[test]
    fn parses_session_id_from_hook_input() {
        let raw = r#"{"session_id":"abc-123","transcript_path":""}"#;
        let input = parse_pre_compact_input(raw);
        assert_eq!(input.session_id.unwrap(), "abc-123");
    }

    #[test]
    fn missing_fields_produce_none() {
        let input = parse_pre_compact_input("{}");
        assert!(input.transcript_path.is_none());
        assert!(input.session_id.is_none());
    }

    #[test]
    fn malformed_json_produces_none_fields() {
        let input = parse_pre_compact_input("not-json");
        assert!(input.transcript_path.is_none());
        assert!(input.session_id.is_none());
    }

    // ── transcript → episode integration ────────────────────────────────────

    /// Feed a synthetic transcript JSONL via a temp file, assert episode written.
    #[test]
    fn pre_compact_writes_episode_from_transcript() {
        use brain_persistence::db::Db;

        let transcript_content = [
            r#"{"type":"tool_use","name":"Edit","input":{"file_path":"src/main.rs","old_string":"a","new_string":"b"}}"#,
            r#"{"type":"tool_use","name":"Write","input":{"file_path":"src/lib.rs","content":"data"}}"#,
            r#"{"type":"tool_use","name":"Bash","input":{"command":"cargo build"}}"#,
        ]
        .join("\n");

        let dir = tempfile::tempdir().unwrap();
        let transcript_path = dir.path().join("transcript.jsonl");
        std::fs::write(&transcript_path, &transcript_content).unwrap();

        // Build hook input JSON.
        let hook_input = serde_json::json!({
            "transcript_path": transcript_path.to_str().unwrap(),
            "session_id": "test-session-1",
            "trigger": "manual"
        });
        let input = parse_pre_compact_input(&hook_input.to_string());

        // Parse transcript directly.
        let transcript =
            crate::hooks::transcript::parse_transcript(input.transcript_path.as_ref().unwrap())
                .unwrap();

        assert_eq!(transcript.edited_files.len(), 2);
        assert!(transcript.edited_files.contains(&"src/main.rs".to_string()));
        assert!(transcript.edited_files.contains(&"src/lib.rs".to_string()));
        assert_eq!(transcript.tool_call_count, 3);

        // Write the episode to an in-memory DB (verifies the write path).
        let db = Db::open_in_memory().unwrap();
        db.ensure_brain_registered("test-brain-id", "test-brain")
            .unwrap();

        let episode = brain_persistence::db::summaries::Episode {
            brain_id: "test-brain-id".to_string(),
            goal: "PreCompact snapshot — session test-session-1".to_string(),
            actions: format!(
                "Edited files: src/main.rs, src/lib.rs. Tool calls: {}.",
                transcript.tool_call_count
            ),
            outcome: "Session ended without recorded errors.".to_string(),
            tags: vec![
                "urgency:pre-compact".to_string(),
                "session:test-session-1".to_string(),
                "file:main.rs".to_string(),
                "file:lib.rs".to_string(),
            ],
            importance: 0.8,
        };

        let summary_id = db
            .with_write_conn(|conn| brain_persistence::db::summaries::store_episode(conn, &episode))
            .unwrap();

        assert!(!summary_id.is_empty());

        // Verify row exists.
        let count: i64 = db
            .with_read_conn(|conn| {
                Ok(conn
                    .query_row("SELECT COUNT(*) FROM summaries", [], |row| row.get(0))
                    .unwrap_or(0))
            })
            .unwrap();
        assert_eq!(count, 1);
    }
}
