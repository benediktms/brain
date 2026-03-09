use anyhow::{Context, Result, bail};
use brain_lib::config::{
    BrainEntry, BrainToml, brain_home, load_global_config, paths::normalize_note_paths,
    save_brain_toml, save_global_config,
};
use std::fs;
use std::path::PathBuf;

/// Initialize a new brain in the current (or given) directory.
pub fn run(name: Option<String>, notes: Vec<PathBuf>, no_claude_md: bool) -> Result<()> {
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

    // 2. Write .brain/brain.toml
    let brain_toml = BrainToml {
        name: brain_name.clone(),
        notes: note_dirs.clone(),
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
        },
    );
    save_global_config(&global)?;

    // 5. Create ~/.brain/brains/<name>/ with restrictive permissions
    let home = brain_home()?;
    let brains_dir = home.join("brains").join(&brain_name);
    brain_lib::fs_permissions::ensure_private_dir(&brains_dir)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // 6. Upsert CLAUDE.md (unless --no-claude-md)
    if !no_claude_md {
        let claude_md_path = cwd.join("CLAUDE.md");
        let build_section = detect_build_section(&cwd);
        let brain_section = BRAIN_SECTION_TEMPLATE
            .replace("{brain_name}", &brain_name)
            .replace("{build_section}", &build_section);

        if claude_md_path.exists() {
            let existing = fs::read_to_string(&claude_md_path)?;
            if existing.contains(BRAIN_SECTION_START) {
                // Replace existing brain section.
                let start = existing.find(BRAIN_SECTION_START).unwrap();
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
                fs::write(&claude_md_path, updated)?;
                println!("Updated brain section in CLAUDE.md");
            } else {
                // Append brain section.
                let mut content = existing;
                if !content.ends_with('\n') {
                    content.push('\n');
                }
                content.push('\n');
                content.push_str(&brain_section);
                fs::write(&claude_md_path, content)?;
                println!("Appended brain section to CLAUDE.md");
            }
        } else {
            let content = format!("# {brain_name}\n\n{brain_section}");
            fs::write(&claude_md_path, content)?;
            println!("Generated CLAUDE.md");
        }
    }

    // 7. Register brain MCP server in Claude Code (user scope)
    let brain_bin = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "brain".into());
    if let Err(e) = super::mcp_setup::register_claude(&brain_bin, false) {
        eprintln!("Warning: {e}");
    }

    // 8. Print success
    let display_notes: Vec<String> = note_dirs.iter().map(|p| p.display().to_string()).collect();
    println!(
        "Brain \"{brain_name}\" initialized. Note directories: {:?}",
        display_notes
    );

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
- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`. Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description).
- `tasks_get` — Get full task details including relationships, comments, labels, and linked notes. Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by priority then due date. Use for "what should I work on?" queries.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Memory tools:**
- `memory_search_minimal` — Semantic search across indexed notes. Returns compact stubs (title, summary, score). Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

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
```

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

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic
- **Statuses**: open, in_progress, blocked, done, cancelled
<!-- brain:end -->
"#;
