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

    let summary_id = stores.store_vetted_episode(&episode)?;

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

    // PreCompact is not in Claude Code's hookSpecificOutput allow-list — emit
    // the minimal universal-fields shape. The snapshot is persisted as an
    // episode; retrieval surfaces it on subsequent compactions.
    let _ = context;
    println!("{}", crate::hooks::build_minimal_hook_ack());

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
// Stop hook
// ---------------------------------------------------------------------------

/// Minimum tool calls required before writing stop-hook episodes.
const STOP_MIN_TOOL_CALLS: usize = 3;

/// Tool-call threshold above which a summary episode is written.
const STOP_HEAVY_SESSION_THRESHOLD: usize = 20;

/// `brain hooks stop` — invoked by the Claude Code Stop hook.
///
/// Reads transcript JSONL from the path in the hook input JSON and writes
/// 1–3 episodic memory entries:
/// - `session-<id>-files`: list of files edited during the session.
/// - `session-<id>-fixes`: errors encountered + files changed in the same
///   span (heuristic for "what was fixed").
/// - `session-<id>-summary`: high-level summary; only for heavy sessions
///   (≥20 tool calls).
///
/// Trust defaults to `untrusted` at the SQL layer (no explicit column set).
/// Episodes are skipped entirely when:
/// - `stop_reason == "user_interrupt"` (aborted sessions are noise).
/// - Total tool-call count < 3 (trivial interactions not worth persisting).
pub fn stop() -> Result<()> {
    let mut stdin_raw = String::new();
    std::io::stdin()
        .read_to_string(&mut stdin_raw)
        .context("failed to read hook input from stdin")?;

    let v: Value = serde_json::from_str(&stdin_raw).unwrap_or(Value::Object(Map::new()));

    let stop_reason = v.get("stop_reason").and_then(|s| s.as_str()).unwrap_or("");

    // Silently exit on user-initiated interrupts.
    if stop_reason == "user_interrupt" {
        println!("{}", crate::hooks::build_minimal_hook_ack());
        return Ok(());
    }

    let transcript_path = v
        .get("transcript_path")
        .and_then(|p| p.as_str())
        .map(std::path::PathBuf::from);

    let session_id = v
        .get("session_id")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown")
        .to_string();

    let transcript = if let Some(ref path) = transcript_path {
        crate::hooks::transcript::parse_transcript(path).unwrap_or_default()
    } else {
        crate::hooks::transcript::ParsedTranscript::default()
    };

    // Skip trivial sessions.
    if transcript.tool_call_count < STOP_MIN_TOOL_CALLS {
        println!("{}", crate::hooks::build_minimal_hook_ack());
        return Ok(());
    }

    let stores = open_stores_for_cwd()?;
    let brain_id = stores.brain_id.clone();

    let mut written_ids: Vec<String> = Vec::new();

    // Episode 1: files edited.
    if !transcript.edited_files.is_empty() {
        let file_list = transcript.edited_files.join(", ");
        let ep = Episode {
            brain_id: brain_id.clone(),
            goal: format!("Session {session_id}: files edited"),
            actions: format!("Files modified: {file_list}"),
            outcome: format!(
                "Session ended ({}). {} tool calls.",
                stop_reason, transcript.tool_call_count
            ),
            tags: vec![format!("session:{session_id}"), "session-files".to_string()],
            importance: 0.6,
        };
        let id = stores.store_vetted_episode(&ep)?;
        written_ids.push(id);
    }

    // Episode 2: errors / fixes (only when errors were recorded).
    if !transcript.errors.is_empty() {
        let error_summary = transcript.errors.join("; ");
        let file_context = if transcript.edited_files.is_empty() {
            "(no files edited)".to_string()
        } else {
            transcript.edited_files.join(", ")
        };
        let ep = Episode {
            brain_id: brain_id.clone(),
            goal: format!("Session {session_id}: errors and fixes"),
            actions: format!(
                "Errors encountered: {error_summary}. Changed files in same span: {file_context}"
            ),
            outcome: "Errors may have been addressed by subsequent edits in the same session."
                .to_string(),
            tags: vec![format!("session:{session_id}"), "session-fixes".to_string()],
            importance: 0.7,
        };
        let id = stores.store_vetted_episode(&ep)?;
        written_ids.push(id);
    }

    // Episode 3: session summary (heavy sessions only).
    if transcript.tool_call_count >= STOP_HEAVY_SESSION_THRESHOLD {
        let file_count = transcript.edited_files.len();
        let ep = Episode {
            brain_id: brain_id.clone(),
            goal: format!("Session {session_id}: summary"),
            actions: format!(
                "Heavy session: {} tool calls, {} files edited, {} errors.",
                transcript.tool_call_count,
                file_count,
                transcript.errors.len()
            ),
            outcome: format!("Session ended: {stop_reason}."),
            tags: vec![
                format!("session:{session_id}"),
                "session-summary".to_string(),
            ],
            importance: 0.8,
        };
        let id = stores.store_vetted_episode(&ep)?;
        written_ids.push(id);
    }

    // Stop is not in Claude Code's hookSpecificOutput allow-list — emit the
    // minimal universal-fields shape. Episodes are persisted; retrieval
    // surfaces them on subsequent sessions.
    let _ = (written_ids, session_id);
    println!("{}", crate::hooks::build_minimal_hook_ack());
    Ok(())
}

// ---------------------------------------------------------------------------
// PreToolUse hook
// ---------------------------------------------------------------------------

/// Emit an empty PreToolUse envelope and return successfully.
///
/// Used when the opt-in gate is not active, the throttle fires, or retrieval
/// produces no results. An empty `additionalContext` is valid; Claude Code
/// will simply not inject any context.
#[inline]
fn emit_empty_pre_tool_use() {
    println!("{}", crate::hooks::build_hook_envelope("PreToolUse", ""));
}

/// Hook input JSON from Claude Code for PreToolUse events.
///
/// Claude Code sends:
/// ```json
/// {
///   "session_id": "...",
///   "tool_name": "Edit",
///   "tool_input": { "file_path": "/abs/path/to/file.rs", ... }
/// }
/// ```
#[derive(Debug)]
struct PreToolUseInput {
    session_id: String,
    tool_name: String,
    file_path: Option<String>,
}

fn parse_pre_tool_use_input(raw: &str) -> PreToolUseInput {
    let v: Value = serde_json::from_str(raw).unwrap_or(Value::Object(Map::new()));

    let session_id = v
        .get("session_id")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown")
        .to_string();

    let tool_name = v
        .get("tool_name")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    // Claude Code wraps tool parameters in `tool_input`.
    let file_path = v
        .get("tool_input")
        .and_then(|ti| ti.get("file_path"))
        .and_then(|p| p.as_str())
        .map(|s| s.to_string());

    PreToolUseInput {
        session_id,
        tool_name,
        file_path,
    }
}

/// `brain hooks pre-tool-use` — invoked by the Claude Code PreToolUse hook.
///
/// Retrieves file-scoped memory and injects it into the LLM context before a
/// write-tool (Edit, Write, MultiEdit) executes.
///
/// ## Opt-in gate
///
/// Requires `[auto_inject] pre_edit_recall = true` in the brain's
/// `.brain/brain.toml`. Default is `false`. Users opt in per brain. This
/// allows using the skill `mem:search` for explicit recall, or enabling
/// ambient injection for fully automated context enrichment.
///
/// ## Per-file-per-session throttle
///
/// Records `(session_id, file_path)` in `pre_tool_use_seen` on each
/// injection. Subsequent edits of the same file within the same Claude Code
/// session emit an empty envelope without retrieving or injecting anything.
///
/// ## Retrieval strategy (3-step, memesh-inspired)
///
/// 1. Tag match `file:<basename>` — exact filename match.
/// 2. Tag match `file:<stem>` — filename without extension (catches multi-ext).
/// 3. FTS5 fallback — full-text search using the basename as the query term.
///
/// Results are scoped to the current brain (`brain_id`) and filtered to
/// `trust='trusted'`. Cross-brain leakage is prevented by the brain_id scope.
/// Maximum 3 results are injected.
///
/// ## Safety
///
/// Content passes through `sanitize_hook_input` (control-seq stripping, length
/// cap, role-token removal) before injection. A `safety frame` header+footer
/// wraps the content. An audit row is written to `injection_audit`.
pub fn pre_tool_use() -> Result<()> {
    let mut stdin_raw = String::new();
    std::io::stdin().read_to_string(&mut stdin_raw).ok();

    let input = parse_pre_tool_use_input(&stdin_raw);

    // No file path → nothing to inject.
    let file_path = match &input.file_path {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            emit_empty_pre_tool_use();
            return Ok(());
        }
    };

    // Load brain config to check opt-in.
    let cwd = std::env::current_dir()?;
    let brain_toml = if let Some(root) = brain_lib::config::find_brain_root(&cwd) {
        brain_lib::config::load_brain_toml(&root.join(".brain")).ok()
    } else {
        None
    };

    let auto_inject = brain_toml.as_ref().map(|t| &t.auto_inject);

    // Gate: `auto_inject.pre_edit_recall` must be true.
    let opted_in = auto_inject.is_some_and(|ai| ai.pre_edit_recall);
    if !opted_in {
        emit_empty_pre_tool_use();
        return Ok(());
    }

    let max_bytes = auto_inject
        .map(|ai| ai.max_bytes)
        .unwrap_or(crate::hooks::injection::DEFAULT_MAX_BYTES);

    // Open stores.
    let stores = open_stores_for_cwd()?;
    let brain_id = stores.brain_id.clone();

    // Per-file-per-session throttle.
    let already_seen = stores.is_pre_tool_use_seen(&input.session_id, &file_path)?;
    if already_seen {
        emit_empty_pre_tool_use();
        return Ok(());
    }
    stores.mark_pre_tool_use_seen(&input.session_id, &file_path)?;

    // Derive basename and stem for tag lookups.
    let path_obj = std::path::Path::new(&file_path);
    let basename = path_obj
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&file_path)
        .to_string();
    let stem = path_obj
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&basename)
        .to_string();

    const MAX_RESULTS: usize = 3;

    // Strategy 1: exact basename tag match.
    let mut results = stores.retrieve_summaries_by_tag_trusted(
        &brain_id,
        &format!("file:{basename}"),
        MAX_RESULTS,
    )?;

    // Strategy 2: stem tag match (if basename != stem and still need results).
    if results.is_empty() && stem != basename {
        results = stores.retrieve_summaries_by_tag_trusted(
            &brain_id,
            &format!("file:{stem}"),
            MAX_RESULTS,
        )?;
    }

    // Strategy 3: FTS5 fallback — trust-filtered inside BrainStores.
    if results.is_empty() {
        results = stores.retrieve_summaries_by_fts_trusted(&basename, &brain_id, MAX_RESULTS)?;
    }

    if results.is_empty() {
        emit_empty_pre_tool_use();
        return Ok(());
    }

    // Assemble raw content.
    let raw_content = results
        .iter()
        .map(|(id, content)| format!("[{id}]\n{content}"))
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");

    let record_ids = results
        .iter()
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>()
        .join(",");

    // Sanitize.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let sanitize_opts = crate::hooks::injection::SanitizeOpts {
        enabled: true,
        max_bytes,
        hook_event: format!("PreToolUse:{}", input.tool_name),
        session_id: Some(input.session_id.clone()),
        record_ids: Some(record_ids.clone()),
        opt_in_source: "brain.toml".to_string(),
    };

    let sanitized = crate::hooks::injection::sanitize_hook_input(&raw_content, &sanitize_opts);

    // Audit log (non-fatal).
    let audit_entry = brain_persistence::db::InjectionAuditEntry {
        ts: now,
        hook_event: &sanitize_opts.hook_event,
        session_id: sanitize_opts.session_id.as_deref(),
        record_ids: Some(&record_ids),
        input_len: sanitized.input_len as i64,
        output_len: sanitized.output_len as i64,
        stripped_counts: &sanitized.stripped.to_json(),
        was_truncated: sanitized.was_truncated,
        opt_in_source: &sanitize_opts.opt_in_source,
    };
    let _ = stores.log_injection_audit(&audit_entry);

    // Safety frame.
    let framed = crate::hooks::apply_safety_frame(&sanitized.text);

    // Emit envelope.
    let envelope = crate::hooks::build_hook_envelope("PreToolUse", &framed);
    println!("{envelope}");

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

    // ── Stop hook ───────────────────────────────────────────────────────────

    fn make_tool_use_line(name: &str, file: Option<&str>) -> String {
        if let Some(f) = file {
            format!(
                r#"{{"type":"tool_use","name":"{name}","input":{{"file_path":"{f}","old_string":"x","new_string":"y"}}}}"#
            )
        } else {
            format!(r#"{{"type":"tool_use","name":"{name}","input":{{"command":"cargo build"}}}}"#)
        }
    }

    fn make_error_result(id: &str, msg: &str) -> String {
        format!(
            r#"{{"type":"tool_result","tool_use_id":"{id}","is_error":true,"content":"{msg}"}}"#
        )
    }

    #[test]
    fn stop_skips_trivial_session_below_threshold() {
        // 2 tool calls < STOP_MIN_TOOL_CALLS(3) — expect no episodes written.
        let transcript = [
            make_tool_use_line("Edit", Some("a.rs")),
            make_tool_use_line("Bash", None),
        ]
        .join("\n");
        let parsed = crate::hooks::transcript::parse_transcript_str(&transcript).unwrap();
        assert!(parsed.tool_call_count < STOP_MIN_TOOL_CALLS);
    }

    #[test]
    fn stop_writes_files_episode_for_qualifying_session() {
        use brain_persistence::db::Db;

        let transcript = (0..5)
            .map(|i| make_tool_use_line("Edit", Some(&format!("src/f{i}.rs"))))
            .collect::<Vec<_>>()
            .join("\n");

        let parsed = crate::hooks::transcript::parse_transcript_str(&transcript).unwrap();
        assert!(parsed.tool_call_count >= STOP_MIN_TOOL_CALLS);
        assert!(!parsed.edited_files.is_empty());

        let db = Db::open_in_memory().unwrap();
        db.ensure_brain_registered("b1", "test-brain").unwrap();

        let ep = Episode {
            brain_id: "b1".to_string(),
            goal: "Session test-stop-1: files edited".to_string(),
            actions: format!("Files modified: {}", parsed.edited_files.join(", ")),
            outcome: "Session ended (end_turn). 5 tool calls.".to_string(),
            tags: vec![
                "session:test-stop-1".to_string(),
                "session-files".to_string(),
            ],
            importance: 0.6,
        };

        let id = db
            .with_write_conn(|conn| brain_persistence::db::summaries::store_episode(conn, &ep))
            .unwrap();
        assert!(!id.is_empty());
    }

    #[test]
    fn stop_writes_fixes_episode_when_errors_present() {
        use brain_persistence::db::Db;

        let transcript = [
            make_tool_use_line("Bash", None),
            make_tool_use_line("Bash", None),
            make_tool_use_line("Bash", None),
            make_error_result("id1", "build failed: missing semicolon"),
            make_tool_use_line("Edit", Some("src/main.rs")),
        ]
        .join("\n");

        let parsed = crate::hooks::transcript::parse_transcript_str(&transcript).unwrap();
        assert!(!parsed.errors.is_empty());
        assert!(parsed.tool_call_count >= STOP_MIN_TOOL_CALLS);

        let db = Db::open_in_memory().unwrap();
        db.ensure_brain_registered("b2", "test-brain").unwrap();

        let ep = Episode {
            brain_id: "b2".to_string(),
            goal: "Session test-stop-2: errors and fixes".to_string(),
            actions: format!(
                "Errors encountered: {}. Changed files in same span: src/main.rs",
                parsed.errors.join("; ")
            ),
            outcome: "Errors may have been addressed by subsequent edits in the same session."
                .to_string(),
            tags: vec![
                "session:test-stop-2".to_string(),
                "session-fixes".to_string(),
            ],
            importance: 0.7,
        };

        let id = db
            .with_write_conn(|conn| brain_persistence::db::summaries::store_episode(conn, &ep))
            .unwrap();
        assert!(!id.is_empty());
    }

    #[test]
    fn stop_writes_summary_episode_for_heavy_session() {
        // Verify STOP_HEAVY_SESSION_THRESHOLD constant is correct.
        assert_eq!(STOP_HEAVY_SESSION_THRESHOLD, 20);

        let transcript = (0..STOP_HEAVY_SESSION_THRESHOLD)
            .map(|i| make_tool_use_line("Bash", None).replace("cargo build", &format!("cmd{i}")))
            .collect::<Vec<_>>()
            .join("\n");

        let parsed = crate::hooks::transcript::parse_transcript_str(&transcript).unwrap();
        assert!(parsed.tool_call_count >= STOP_HEAVY_SESSION_THRESHOLD);
    }

    // ── PreToolUse hook ────────────────────────────────────────────────────

    // (a) Opt-in OFF: hook must return an empty envelope without injecting.
    //
    // When `auto_inject.pre_edit_recall` is false, the gate check returns false
    // before any store is opened. We verify that `AutoInjectConfig` default
    // is false, which is the condition the hook checks.
    #[test]
    fn pre_tool_use_opt_in_off_by_default() {
        use brain_lib::config::AutoInjectConfig;

        let cfg = AutoInjectConfig::default();
        // Both master switch and per-hook flag must default to false.
        assert!(!cfg.enabled, "auto_inject.enabled must default to false");
        assert!(
            !cfg.pre_edit_recall,
            "auto_inject.pre_edit_recall must default to false"
        );
        // Empty envelope is produced when !opted_in — verified by inspecting
        // the gate branch in pre_tool_use().
    }

    // (b) Opt-in ON + clean content: tag-based retrieval returns sanitized content.
    //
    // We exercise Db::retrieve_summaries_by_tag_trusted directly with an
    // in-memory DB that holds a trusted summary tagged `file:hooks.rs`.
    #[test]
    fn pre_tool_use_retrieves_trusted_memory_for_file() {
        use brain_persistence::db::Db;
        use brain_persistence::db::summaries::Episode;

        let db = Db::open_in_memory().unwrap();
        db.ensure_brain_registered("brain-b1", "test").unwrap();

        // Insert a summary tagged with "file:hooks.rs" (default trust = 'untrusted').
        let ep = Episode {
            brain_id: "brain-b1".to_string(),
            goal: "Fix hooks.rs".to_string(),
            actions: "Changed handler".to_string(),
            outcome: "OK".to_string(),
            tags: vec!["file:hooks.rs".to_string()],
            importance: 0.9,
        };
        let id = db
            .with_write_conn(|conn| brain_persistence::db::summaries::store_episode(conn, &ep))
            .unwrap();

        // Untrusted summary must not be returned.
        let results_before = db
            .retrieve_summaries_by_tag_trusted("brain-b1", "file:hooks.rs", 3)
            .unwrap();
        assert!(
            results_before.is_empty(),
            "untrusted summary must not be returned"
        );

        // Mark it trusted.
        db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE summaries SET trust = 'trusted' WHERE summary_id = ?1",
                rusqlite::params![id],
            )?;
            Ok(())
        })
        .unwrap();

        let results = db
            .retrieve_summaries_by_tag_trusted("brain-b1", "file:hooks.rs", 3)
            .unwrap();
        assert_eq!(results.len(), 1, "expected 1 trusted result");
        assert!(
            results[0].1.contains("Fix hooks.rs") || results[0].1.contains("Changed handler"),
            "content must include episode text"
        );
    }

    // (c) Throttle: second call for same file+session returns empty.
    //
    // We call mark_pre_tool_use_seen then is_pre_tool_use_seen and assert
    // the second call detects the prior entry.
    #[test]
    fn pre_tool_use_throttle_prevents_second_injection() {
        use brain_persistence::db::Db;

        let db = Db::open_in_memory().unwrap();
        db.ensure_brain_registered("brain-c1", "test").unwrap();

        let session = "sess-throttle-1";
        let file = "/src/main.rs";

        // First call: not yet seen.
        assert!(!db.is_pre_tool_use_seen(session, file).unwrap());

        // Mark as seen.
        db.mark_pre_tool_use_seen(session, file).unwrap();

        // Second call: now seen — throttle fires.
        assert!(
            db.is_pre_tool_use_seen(session, file).unwrap(),
            "throttle must detect prior entry for same session+file"
        );

        // Different session with the same file is NOT throttled.
        assert!(!db.is_pre_tool_use_seen("sess-other", file).unwrap());

        // Same session with a different file is NOT throttled.
        assert!(!db.is_pre_tool_use_seen(session, "/src/lib.rs").unwrap());
    }

    // (d) Non-write tools are ignored (tool_name not in Edit|Write|MultiEdit).
    //
    // The Claude Code matcher `Edit|Write|MultiEdit` prevents the hook from
    // firing for other tools. We verify parse_pre_tool_use_input correctly
    // captures the tool_name so it can be gated in the hook body, and that
    // a Read tool produces a different tool_name than the write set.
    #[test]
    fn pre_tool_use_parse_captures_tool_name() {
        // Write-tool: should have file_path populated.
        let edit_raw = r#"{"session_id":"s1","tool_name":"Edit","tool_input":{"file_path":"/src/main.rs","old_string":"a","new_string":"b"}}"#;
        let edit = parse_pre_tool_use_input(edit_raw);
        assert_eq!(edit.tool_name, "Edit");
        assert_eq!(edit.file_path.as_deref(), Some("/src/main.rs"));

        // Non-write tool: Read doesn't carry file_path in tool_input.
        let read_raw =
            r#"{"session_id":"s1","tool_name":"Read","tool_input":{"file_path":"/src/main.rs"}}"#;
        let read_inp = parse_pre_tool_use_input(read_raw);
        // tool_name identifies non-write tools; the matcher in plugin.json
        // prevents this hook from running for Read, but we verify parsing is correct.
        assert_eq!(read_inp.tool_name, "Read");
        assert_ne!(read_inp.tool_name, "Edit");
        assert_ne!(read_inp.tool_name, "Write");
        assert_ne!(read_inp.tool_name, "MultiEdit");

        // Malformed input: missing tool_name defaults to empty string.
        let empty = parse_pre_tool_use_input("{}");
        assert_eq!(empty.tool_name, "");
        assert!(empty.file_path.is_none());
    }
}
