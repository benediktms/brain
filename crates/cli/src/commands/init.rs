use anyhow::{Context, Result, bail};
use brain_lib::config::{
    BrainEntry, BrainToml, brain_home, generate_brain_id, load_global_config,
    paths::normalize_note_paths, save_brain_toml, save_global_config,
};
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// Initialize a new brain in the current (or given) directory.
pub fn run(name: Option<String>, notes: Vec<PathBuf>, no_agents_md: bool) -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let brain_dir = cwd.join(".brain");
    let marker_path = brain_dir.join("brain.toml");

    if marker_path.exists() {
        bail!(
            "Brain already initialized: {} exists",
            marker_path.display()
        );
    }

    // Derive brain name from explicit flag or directory name.
    let brain_name = name.unwrap_or_else(|| {
        cwd.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "brain".into())
    });

    // Default note dirs to cwd if none specified.
    let note_dirs: Vec<PathBuf> = if notes.is_empty() {
        vec![PathBuf::from(".")]
    } else {
        notes
    };

    // 1. Create .brain/ in the project root.
    fs::create_dir_all(&brain_dir)?;

    // Generate a stable brain ID
    let brain_id = generate_brain_id();

    // 2. Write .brain/brain.toml
    let brain_toml = BrainToml {
        name: brain_name.clone(),
        notes: note_dirs.clone(),
        id: Some(brain_id.clone()),
    };
    save_brain_toml(&brain_dir, &brain_toml)?;

    // 3. Write .brain/.gitignore
    let gitignore_path = brain_dir.join(".gitignore");
    fs::write(
        &gitignore_path,
        "# Derived data — do not commit\nbrain.db*\nlancedb/\nmodels/\n",
    )?;

    // 4. Register in global config (~/.brain/config.toml)
    let mut global = load_global_config()?;

    let abs_notes = normalize_note_paths(&note_dirs, &cwd)?;

    global.brains.insert(
        brain_name.clone(),
        BrainEntry {
            root: cwd.clone(),
            notes: abs_notes,
            id: Some(brain_id.clone()),
        },
    );
    save_global_config(&global)?;

    // 5. Create ~/.brain/brains/<name>/ with restrictive permissions
    let home = brain_home()?;
    let brains_dir = home.join("brains").join(&brain_name);
    brain_lib::fs_permissions::ensure_private_dir(&brains_dir)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Seed project_prefix at init time so first task IDs are stable and derived
    // from init context (`--name` or current directory basename).
    seed_project_prefix_if_missing(&brains_dir.join("brain.db"), &brain_name)?;

    // 6. Upsert AGENTS.md (unless --no-agents-md)
    if !no_agents_md {
        upsert_agent_docs(&cwd, &brain_name)?;
    }

    // 7. Register brain MCP server in Claude Code (user scope)
    let brain_bin = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "brain".into());
    if let Err(e) = super::mcp_setup::register_claude(&brain_bin, false) {
        eprintln!("Warning: {e}");
    }

    // 8. Signal daemon to reload registry (best-effort)
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    // 9. Print success
    let display_notes: Vec<String> = note_dirs.iter().map(|p| p.display().to_string()).collect();
    println!(
        "Brain \"{brain_name}\" initialized (id: {brain_id}). Note directories: {:?}",
        display_notes
    );

    Ok(())
}

fn seed_project_prefix_if_missing(db_path: &Path, seed_name: &str) -> Result<()> {
    let db = brain_lib::db::Db::open(db_path)?;
    db.with_write_conn(|conn| {
        if brain_lib::db::meta::get_meta(conn, "project_prefix")?.is_none() {
            let prefix = brain_lib::db::meta::generate_prefix(seed_name);
            brain_lib::db::meta::set_meta(conn, "project_prefix", &prefix)?;
        }
        Ok(())
    })?;
    Ok(())
}

/// Generate or update AGENTS.md and a bridge CLAUDE.md in the given directory.
pub fn upsert_agent_docs(cwd: &std::path::Path, brain_name: &str) -> Result<()> {
    let agents_md_path = cwd.join("AGENTS.md");
    let build_section = detect_build_section(cwd);
    let brain_section = BRAIN_SECTION_TEMPLATE
        .replace("{brain_name}", brain_name)
        .replace("{build_section}", &build_section);

    if agents_md_path.exists() {
        let existing = fs::read_to_string(&agents_md_path)?;
        if let Some(start) = existing.find(BRAIN_SECTION_START) {
            // Replace existing brain section.
            let end = existing
                .find(BRAIN_SECTION_END)
                .map(|i| i + BRAIN_SECTION_END.len())
                .unwrap_or(existing.len());
            let mut updated = String::with_capacity(existing.len());
            updated.push_str(&existing[..start]);
            updated.push_str(&brain_section);
            // Skip any trailing newline after the old end marker.
            let rest = &existing[end..];
            let rest = rest.strip_prefix('\n').unwrap_or(rest);
            updated.push_str(rest);
            fs::write(&agents_md_path, updated)?;
            println!("Updated brain section in AGENTS.md");
        } else {
            // Append brain section.
            let mut content = existing;
            if !content.ends_with('\n') {
                content.push('\n');
            }
            content.push('\n');
            content.push_str(&brain_section);
            fs::write(&agents_md_path, content)?;
            println!("Appended brain section to AGENTS.md");
        }
    } else {
        let content = format!("# {brain_name}\n\n{brain_section}");
        fs::write(&agents_md_path, content)?;
        println!("Generated AGENTS.md");
    }

    // Also generate a thin CLAUDE.md bridge if it doesn't exist or has old brain content.
    let claude_md_path = cwd.join("CLAUDE.md");
    let bridge_ref = "Read [AGENTS.md](./AGENTS.md) for project instructions — it is the canonical reference for all AI agents.\n".to_string();
    if claude_md_path.exists() {
        let existing = fs::read_to_string(&claude_md_path)?;
        if let Some(start) = existing.find(BRAIN_SECTION_START) {
            // Replace the brain section with the bridge reference, preserving surrounding content.
            let end = existing
                .find(BRAIN_SECTION_END)
                .map(|i| i + BRAIN_SECTION_END.len())
                .unwrap_or(existing.len());
            let mut updated = String::with_capacity(existing.len());
            updated.push_str(&existing[..start]);
            updated.push_str(&bridge_ref);
            let rest = &existing[end..];
            let rest = rest.strip_prefix('\n').unwrap_or(rest);
            updated.push_str(rest);
            fs::write(&claude_md_path, updated)?;
            println!("Replaced brain section in CLAUDE.md with bridge to AGENTS.md");
        }
        // Otherwise leave existing CLAUDE.md untouched.
    } else {
        let content = format!(
            "# {brain_name}\n\n{bridge_ref}\n\
             <!-- Additional Claude Code-specific instructions below if needed -->\n"
        );
        fs::write(&claude_md_path, content)?;
        println!("Generated CLAUDE.md (bridge to AGENTS.md)");
    }

    Ok(())
}

fn detect_build_section(cwd: &std::path::Path) -> String {
    if cwd.join("Cargo.toml").exists() {
        return r#"## Build & Test

```bash
cargo build    # Build
cargo test     # Test
cargo clippy   # Lint
```

"#
        .to_string();
    }
    if cwd.join("package.json").exists() {
        return r#"## Build & Test

```bash
npm install    # Install dependencies
npm test       # Test
npm run build  # Build
```

"#
        .to_string();
    }
    if cwd.join("justfile").exists() {
        return r#"## Build & Test

```bash
just           # Run default recipe
just test      # Test
just build     # Build
```

"#
        .to_string();
    }
    if cwd.join("Makefile").exists() {
        return r#"## Build & Test

```bash
make           # Build
make test      # Test
```

"#
        .to_string();
    }
    String::new()
}

const BRAIN_SECTION_START: &str = "<!-- brain:start -->";
const BRAIN_SECTION_END: &str = "<!-- brain:end -->";

const BRAIN_SECTION_TEMPLATE: &str = r#"<!-- brain:start -->
{build_section}## Task Management

This project uses `brain` for task tracking. **Always use MCP tools for task operations** — they provide structured responses and are the canonical interface for AI agents. CLI commands exist for human terminal use only.

### MCP Tools (preferred for AI agents)

When running as an MCP server (`brain mcp`), these tools are available:

**Task tools:**
- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`, `cross_brain_ref_added`, `cross_brain_ref_removed`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`. Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description).
- `tasks_get` — Get full task details including relationships, comments, labels, linked notes, and cross-brain references (`cross_refs`). Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by priority then due date. Use for "what should I work on?" queries.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Note:** `tasks_apply_event` and `tasks_close` automatically generate and embed searchable capsules into LanceDB on every task create, update, or completion. Tasks become discoverable via `memory_search_minimal` without any extra steps.

**Cross-brain tools:**
- `brains.list` — List all brain projects registered in `~/.brain/config.toml`. Returns `name`, `id`, `root` (filesystem path), and `prefix` (task ID prefix) for each brain. Use this to discover available targets before calling `tasks.create` with a `brain` parameter.
- `tasks.create` — Create a task in the local brain or, when the `brain` parameter is provided, in another registered brain project. Required params: `title`. For cross-brain creation, also pass `brain` (registry name or 8-char brain ID). Optional params: `description`, `priority` (0–4, default 4), `task_type`, `assignee`, `parent` (remote task ID). When `link_from` is provided (a local task ID), a cross-brain ref is added to that local task. `link_type` controls the ref direction (depends_on|blocks|related, default related). Returns `task_id` for local creation, or `remote_task_id`, `remote_brain_name`, `remote_brain_id`, and `local_ref_created` for cross-brain creation.

**Cross-brain workflow:**
1. Call `brains.list` to discover registered brains and their prefixes.
2. Call `tasks.create` with the `brain` parameter set to the target brain name and your task details.
3. Optionally pass `link_from` (a local task ID) to auto-create a cross-brain reference on the local task.

**Memory tools:**
- `memory_search_minimal` — Semantic search across indexed notes and tasks. Returns compact stubs (title, summary, score, kind). The `kind` field is `"note"` for indexed documents, `"task"` for active task capsules, or `"task-outcome"` for completed task outcomes. Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

**Records tools:**
- `records.create_artifact` — Create a new artifact record with base64-encoded content.
- `records.save_snapshot` — Save an opaque state bundle as a snapshot record.
- `records.get` — Get a record by ID with full metadata, tags, and links (supports prefix resolution).
- `records.list` — List records with optional filters (kind, status, tag, task_id).
- `records.fetch_content` — Fetch raw content of a record as base64-encoded data.
- `records.archive` — Archive a record (metadata-only, payload preserved).
- `records.tag_add` — Add a tag to a record (idempotent).
- `records.tag_remove` — Remove a tag from a record (idempotent).
- `records.link_add` — Link a record to a task or note chunk.
- `records.link_remove` — Remove a link from a record.

**Other tools:**
- `status` — Health/status probe. Returns project name, brain ID, task counts, and index stats.

### CLI Commands (for human terminal use)

```bash
# Finding work
brain tasks ready              # Show tasks with no blockers
brain tasks list               # List all tasks
brain tasks list --status=open # Filter by status
brain tasks list --search "query" # Full-text search
brain tasks list --priority 1 --label urgent # Combined filters
brain tasks show <id>          # Detailed task view

# Creating & updating
brain tasks create --title="..." --description="..." --type=task --priority=2
brain tasks update <id> --status=in_progress
brain tasks comment <id> "comment text"

# Cross-brain task creation
brain tasks create --title="..." --brain=<NAME_OR_ID>          # Create in another brain
brain tasks create --title="..." --brain=infra --link-from=BRN-01X --link-type=related  # Create + auto-link

# Registry
brain list                     # List registered brains
brain list --json              # List as JSON (name, id, root, prefix)

# Dependencies
brain tasks dep add <task> <depends-on>
brain tasks dep add-chain BRN-01 BRN-02 BRN-03  # Sequential chain
brain tasks dep add-fan BRN-01 BRN-02,BRN-03    # Fan-out from source
brain tasks dep clear BRN-01                      # Remove all deps

# Labels
brain tasks labels                    # List all labels with counts
brain tasks list --group-by label     # List tasks grouped by label
brain tasks label batch-add --tasks BRN-01,BRN-02 my-label
brain tasks label rename old-label new-label
brain tasks label purge old-label

# Completing work
brain tasks close <id1> <id2>  # Close one or more tasks
brain tasks stats              # Project statistics

# Agent docs
brain docs                     # Regenerate AGENTS.md + bridge CLAUDE.md
brain agent schema             # Output JSON Schema for all MCP tools
brain agent schema --pretty    # Pretty-printed output
brain agent schema --tool tasks.apply_event --pretty  # Single tool
```

> **Tip:** Run `brain agent schema --pretty` to get the full JSON Schema for all MCP tools, including exact per-event-type payload definitions for `tasks_apply_event`. This is useful for validating payloads before sending them.

### Finding Work

When the user asks what to work on next (e.g., "what's next?", "what should I work on?", "next task", "any work?"), always check brain tasks first:
1. Use `tasks_next` MCP tool to get unblocked tasks sorted by priority
2. Present the top candidates with their ID, title, priority, and type
3. If a task has dependencies, briefly note what's blocking it

### Workflow

When working on tasks:
1. **Before starting**: Mark the task `in_progress` via `tasks_apply_event` (status_changed)
2. **While working**: Add comments via `tasks_apply_event` (comment_added) for significant decisions or blockers
3. **On completion**: Close the task via `tasks_close` (or `tasks_apply_event` with status_changed to `done`)

**Cross-task insights**: If you discover during work on one task that something affects or should be captured on a different task, immediately add a comment to that task with the relevant context. Don't defer — the insight is freshest now and costs seconds to capture vs. minutes to reconstruct later.

**Planning references**: When planning work, always reference the task ID(s) being planned for and any related tasks that may be affected. This creates a traceable link between plans and the work they address, and helps future agents (or humans) understand why decisions were made.

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic, spike
- **Statuses**: open, in_progress, blocked, done, cancelled
<!-- brain:end -->
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn agents_md_created_from_scratch() {
        let dir = tempdir().unwrap();
        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let content = fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert!(content.starts_with("# test-brain"));
        assert!(content.contains(BRAIN_SECTION_START));
        assert!(content.contains(BRAIN_SECTION_END));
        assert!(content.contains("## Task Management"));
    }

    #[test]
    fn agents_md_preserves_content_before_markers() {
        let dir = tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");

        let preamble = "# My Project\n\nCustom instructions here.\n\n";
        let old_brain = "<!-- brain:start -->\nold content\n<!-- brain:end -->\n";
        let initial = format!("{preamble}{old_brain}");
        fs::write(&agents_path, &initial).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&agents_path).unwrap();
        assert!(result.starts_with(preamble), "preamble must be preserved");
        assert!(result.contains(BRAIN_SECTION_START));
        assert!(
            result.contains("## Task Management"),
            "new content must be present"
        );
        assert!(
            !result.contains("old content"),
            "old brain content must be replaced"
        );
    }

    #[test]
    fn agents_md_preserves_content_after_markers() {
        let dir = tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");

        let old_brain = "<!-- brain:start -->\nold content\n<!-- brain:end -->\n";
        let suffix = "\n## My Custom Section\n\nDo not delete this.\n";
        let initial = format!("{old_brain}{suffix}");
        fs::write(&agents_path, &initial).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&agents_path).unwrap();
        assert!(
            result.contains("## My Custom Section"),
            "content after markers must be preserved"
        );
        assert!(
            result.contains("Do not delete this."),
            "content after markers must be preserved"
        );
        assert!(
            result.contains("## Task Management"),
            "new brain content must be present"
        );
    }

    #[test]
    fn agents_md_preserves_content_around_markers() {
        let dir = tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");

        let before = "# Project\n\nBefore brain section.\n\n";
        let old_brain = "<!-- brain:start -->\nold stuff\n<!-- brain:end -->\n";
        let after = "\n## After Section\n\nKeep this too.\n";
        fs::write(&agents_path, format!("{before}{old_brain}{after}")).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&agents_path).unwrap();
        assert!(
            result.starts_with(before),
            "content before markers must be preserved"
        );
        assert!(
            result.contains("## After Section"),
            "content after markers must be preserved"
        );
        assert!(
            result.contains("Keep this too."),
            "content after markers must be preserved"
        );
        assert!(
            result.contains("## Task Management"),
            "new brain content must be present"
        );
    }

    #[test]
    fn agents_md_appends_when_no_markers() {
        let dir = tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");

        let existing = "# Existing Project\n\nSome custom docs.\n";
        fs::write(&agents_path, existing).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&agents_path).unwrap();
        assert!(
            result.starts_with("# Existing Project"),
            "existing content must be preserved"
        );
        assert!(
            result.contains("Some custom docs."),
            "existing content must be preserved"
        );
        assert!(
            result.contains(BRAIN_SECTION_START),
            "brain section must be appended"
        );
    }

    #[test]
    fn claude_md_bridge_created_when_missing() {
        let dir = tempdir().unwrap();
        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let content = fs::read_to_string(dir.path().join("CLAUDE.md")).unwrap();
        assert!(
            content.contains("AGENTS.md"),
            "bridge must reference AGENTS.md"
        );
        assert!(
            !content.contains(BRAIN_SECTION_START),
            "bridge must not contain brain markers"
        );
    }

    #[test]
    fn claude_md_with_old_markers_replaced_by_bridge() {
        let dir = tempdir().unwrap();
        let claude_path = dir.path().join("CLAUDE.md");

        let old_content = "# brain\n\n<!-- brain:start -->\nold task docs\n<!-- brain:end -->\n";
        fs::write(&claude_path, old_content).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&claude_path).unwrap();
        assert!(
            result.contains("AGENTS.md"),
            "must contain bridge reference"
        );
        assert!(
            !result.contains("old task docs"),
            "old brain content must be removed"
        );
        assert!(
            result.contains("# brain"),
            "content before markers must be preserved"
        );
    }

    #[test]
    fn claude_md_preserves_custom_content_around_old_markers() {
        let dir = tempdir().unwrap();
        let claude_path = dir.path().join("CLAUDE.md");

        let before = "# My Project\n\nClaude-specific instructions.\n\n";
        let markers = "<!-- brain:start -->\nold brain stuff\n<!-- brain:end -->\n";
        let after = "\n## Custom Section\n\nKeep this Claude content.\n";
        fs::write(&claude_path, format!("{before}{markers}{after}")).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&claude_path).unwrap();
        assert!(
            result.contains("Claude-specific instructions."),
            "content before markers must be preserved"
        );
        assert!(
            result.contains("Keep this Claude content."),
            "content after markers must be preserved"
        );
        assert!(
            result.contains("AGENTS.md"),
            "bridge reference must be present"
        );
        assert!(
            !result.contains("old brain stuff"),
            "old brain content must be removed"
        );
    }

    #[test]
    fn claude_md_without_markers_left_untouched() {
        let dir = tempdir().unwrap();
        let claude_path = dir.path().join("CLAUDE.md");

        let custom = "# My Project\n\nCustom Claude-specific instructions.\n";
        fs::write(&claude_path, custom).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&claude_path).unwrap();
        assert_eq!(
            result, custom,
            "CLAUDE.md without markers must be left untouched"
        );
    }

    #[test]
    fn repeated_upsert_is_idempotent() {
        let dir = tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");

        let preamble = "# Project\n\nKeep this.\n\n";
        fs::write(&agents_path, preamble).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();
        let after_first = fs::read_to_string(&agents_path).unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();
        let after_second = fs::read_to_string(&agents_path).unwrap();

        assert_eq!(
            after_first, after_second,
            "repeated upsert must be idempotent"
        );
    }

    #[test]
    fn seed_project_prefix_handles_complex_name() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("brain.db");
        seed_project_prefix_if_missing(&db_path, "my-cool_project 2026").unwrap();

        let db = brain_lib::db::Db::open(&db_path).unwrap();
        let stored = db
            .with_read_conn(|conn| brain_lib::db::meta::get_meta(conn, "project_prefix"))
            .unwrap()
            .unwrap();
        assert_eq!(stored, "MCP");
    }

    #[test]
    fn seed_project_prefix_does_not_override_existing_value() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("brain.db");
        seed_project_prefix_if_missing(&db_path, "alpha-service").unwrap();

        let db = brain_lib::db::Db::open(&db_path).unwrap();
        db.with_write_conn(|conn| brain_lib::db::meta::set_meta(conn, "project_prefix", "XYZ"))
            .unwrap();

        seed_project_prefix_if_missing(&db_path, "beta-service").unwrap();

        let stored = db
            .with_read_conn(|conn| brain_lib::db::meta::get_meta(conn, "project_prefix"))
            .unwrap()
            .unwrap();
        assert_eq!(stored, "XYZ");
    }
}
