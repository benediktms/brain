use anyhow::{Context, Result};
use brain_lib::config::{
    BrainEntry, BrainToml, brain_home, find_brain_by_id, find_brain_by_path, generate_brain_id,
    load_brain_toml, load_global_config, paths::normalize_note_paths, save_brain_toml,
    save_global_config,
};
use brain_persistence::db::schema::BrainUpsert;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// Initialize a new brain in the current (or given) directory.
pub fn run(name: Option<String>, notes: Vec<PathBuf>, no_agents_md: bool) -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let brain_dir = cwd.join(".brain");
    let marker_path = brain_dir.join("brain.toml");

    // Case A: .brain/brain.toml already exists locally.
    // Check if its brain ID is registered in the global config.
    // If found: add cwd to that brain's roots and return.
    // If not found: fall through to re-register using the existing brain ID.
    let existing_brain_id: Option<String> = if marker_path.exists() {
        let local_toml =
            load_brain_toml(&brain_dir).context("failed to read existing .brain/brain.toml")?;

        if let Some(ref local_id) = local_toml.id {
            let mut global = load_global_config()?;
            // find_brain_by_id returns a reference — collect the name, then mutate.
            let existing_name = find_brain_by_id(&global, local_id).map(|(n, _)| n);

            if let Some(brain_name) = existing_name {
                // Update DB (source of truth) with the new root.
                let home = brain_home()?;
                let db_path = home.join("brain.db");
                let db = brain_persistence::db::Db::open(&db_path)?;
                if let Ok(Some(brain_row)) = db.get_brain(local_id) {
                    let mut roots: Vec<std::path::PathBuf> = brain_row
                        .roots_json
                        .as_deref()
                        .and_then(|j| serde_json::from_str(j).ok())
                        .unwrap_or_default();
                    if roots.contains(&cwd) {
                        println!(
                            "Path already registered in brain \"{}\" ({} roots)",
                            brain_name,
                            roots.len()
                        );
                    } else {
                        roots.push(cwd.clone());
                        let roots_json = serde_json::to_string(&roots)?;
                        db.upsert_brain(&BrainUpsert {
                            brain_id: local_id,
                            name: &brain_name,
                            prefix: brain_row.prefix.as_deref().unwrap_or("BRN"),
                            roots_json: &roots_json,
                            notes_json: brain_row.notes_json.as_deref().unwrap_or("[]"),
                            aliases_json: brain_row.aliases_json.as_deref().unwrap_or("[]"),
                            archived: brain_row.archived,
                        })?;
                        // Project to state_projection.toml.
                        let entry = global.brains.get_mut(&brain_name).unwrap();
                        if !entry.roots.contains(&cwd) {
                            entry.roots.push(cwd.clone());
                        }
                        save_global_config(&global)?;
                        println!(
                            "Path added to existing brain \"{}\" (now has {} roots)",
                            brain_name,
                            roots.len()
                        );
                    }
                } else {
                    // Brain in config but not in DB yet — fall through to re-register.
                    // (backward compat with old installs)
                    let already_has_root = global
                        .brains
                        .get(&brain_name)
                        .map(|e| e.roots.contains(&cwd))
                        .unwrap_or(false);
                    if !already_has_root {
                        if let Some(entry) = global.brains.get_mut(&brain_name) {
                            entry.roots.push(cwd.clone());
                        }
                        let root_count = global.brains[&brain_name].roots.len();
                        save_global_config(&global)?;
                        println!(
                            "Path added to existing brain \"{}\" (now has {} roots)",
                            brain_name, root_count
                        );
                    } else {
                        let root_count = global.brains[&brain_name].roots.len();
                        println!(
                            "Path already registered in brain \"{}\" ({} roots)",
                            brain_name, root_count
                        );
                    }
                }
                return Ok(());
            }
        }

        // Local brain.toml exists but its ID is not in the global config.
        // Re-register as a new brain entry, preserving the existing brain ID and name.
        let local_toml =
            load_brain_toml(&brain_dir).context("failed to re-read existing .brain/brain.toml")?;
        Some(local_toml.id.unwrap_or_else(generate_brain_id))
    } else {
        // Case B: No local .brain/brain.toml, but cwd is already registered as a root
        // of an existing brain (e.g. a second clone or worktree that wasn't init'd yet).
        let mut global = load_global_config()?;
        let existing_name = find_brain_by_path(&global, &cwd).map(|(n, _)| n);

        if let Some(brain_name) = existing_name {
            let entry = global.brains.get_mut(&brain_name).unwrap();
            let brain_id = entry.id.clone().unwrap_or_default();
            // Write local brain.toml so the directory is fully initialised.
            fs::create_dir_all(&brain_dir)?;
            let brain_toml = BrainToml {
                name: brain_name.clone(),
                notes: vec![],
                id: Some(brain_id.clone()),
                prefix: None,
            };
            save_brain_toml(&brain_dir, &brain_toml)?;
            // cwd is already in roots — nothing to add, just report.
            println!(
                "Path already registered in brain \"{}\" ({} roots) — local brain.toml created",
                brain_name,
                entry.roots.len()
            );
            return Ok(());
        }

        None
    };

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

    // Use preserved brain ID (from re-register path) or generate a new one.
    let brain_id = existing_brain_id.unwrap_or_else(generate_brain_id);

    // 2. Write .brain/brain.toml
    let brain_toml = BrainToml {
        name: brain_name.clone(),
        notes: note_dirs.clone(),
        id: Some(brain_id.clone()),
        prefix: None,
    };
    save_brain_toml(&brain_dir, &brain_toml)?;

    // 3. Write .brain/.gitignore
    let gitignore_path = brain_dir.join(".gitignore");
    fs::write(
        &gitignore_path,
        "# Derived data — do not commit\nbrain.db*\nlancedb/\nmodels/\n",
    )?;

    // 4. Create ~/.brain/brains/<name>/ with restrictive permissions
    let home = brain_home()?;
    let brains_dir = home.join("brains").join(&brain_name);
    brain_lib::fs_permissions::ensure_private_dir(&brains_dir)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let abs_notes = normalize_note_paths(&note_dirs, &cwd)?;

    // 4b. Register brain in DB (source of truth) with roots, notes, prefix.
    let db_path = home.join("brain.db");
    seed_project_prefix_if_missing(&db_path, &brain_name)?;
    {
        let db = brain_persistence::db::Db::open(&db_path)?;
        let prefix = brain_persistence::db::meta::generate_prefix(&brain_name);
        let roots_json = serde_json::to_string(&vec![&cwd])?;
        let notes_json = serde_json::to_string(&abs_notes)?;
        let aliases_json = "[]".to_string();
        db.upsert_brain(&BrainUpsert {
            brain_id: &brain_id,
            name: &brain_name,
            prefix: &prefix,
            roots_json: &roots_json,
            notes_json: &notes_json,
            aliases_json: &aliases_json,
            archived: false,
        })?;
    }

    // 4c. Project to state_projection.toml (read-only projection for human readability).
    {
        let mut global = load_global_config()?;
        global.brains.insert(
            brain_name.clone(),
            BrainEntry {
                roots: vec![cwd.clone()],
                notes: abs_notes,
                id: Some(brain_id.clone()),
                aliases: vec![],
                prefix: None,
                archived: false,
            },
        );
        save_global_config(&global)?;
    }

    // 5b. One-time import: if the project already has a .brain/tasks/events.jsonl
    // (e.g. this is a cloned repo), replay those events into the unified SQLite.
    let project_jsonl = brain_dir.join("tasks").join("events.jsonl");
    if project_jsonl.exists() {
        import_project_jsonl(&db_path, &brain_id, &project_jsonl);
    }

    // 6. Upsert AGENTS.md (unless --no-agents-md)
    if !no_agents_md {
        upsert_agent_docs(&cwd, &brain_name)?;
    }

    // 7. Register brain MCP server in Claude Code (user scope)
    let brain_bin = super::mcp_setup::installed_brain_bin();
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

/// Import task events from a project-local JSONL file into the unified SQLite.
///
/// Used during `brain init` to replay events from a cloned repo's
/// `.brain/tasks/events.jsonl` into the unified database. Errors are logged
/// as warnings — init should not fail due to import issues.
fn import_project_jsonl(db_path: &Path, brain_id: &str, jsonl_path: &Path) {
    let result = (|| -> Result<usize> {
        let db = brain_persistence::db::Db::open(db_path)?;
        let store = brain_lib::tasks::TaskStore::with_brain_id(db, brain_id, brain_id)?;
        Ok(store.import_from_jsonl(jsonl_path)?)
    })();
    match result {
        Ok(n) => {
            if n > 0 {
                println!("Imported {n} task events from {}", jsonl_path.display());
            }
        }
        Err(e) => {
            eprintln!(
                "Warning: failed to import task events from {}: {e}",
                jsonl_path.display()
            );
        }
    }
}

fn seed_project_prefix_if_missing(db_path: &Path, seed_name: &str) -> Result<()> {
    let db = brain_persistence::db::Db::open(db_path)?;
    db.with_write_conn(|conn| {
        if brain_persistence::db::meta::get_meta(conn, "project_prefix")?.is_none() {
            let prefix = brain_persistence::db::meta::generate_prefix(seed_name);
            brain_persistence::db::meta::set_meta(conn, "project_prefix", &prefix)?;
        }
        Ok(())
    })?;
    Ok(())
}

/// Generate or update AGENTS.md and a bridge CLAUDE.md in the given directory.
pub fn upsert_agent_docs(cwd: &std::path::Path, brain_name: &str) -> Result<()> {
    let agents_md_path = cwd.join("AGENTS.md");
    let build_section = detect_build_section(cwd);
    let brain_section = render_brain_section(brain_name, &build_section);

    if agents_md_path.exists() {
        let existing = fs::read_to_string(&agents_md_path)?;
        if let Some(first_pos) = existing.find(BRAIN_SECTION_START_PREFIX) {
            // Remove ALL brain:start...brain:end blocks, then insert once at the
            // position of the first removed block. This cleans up stale duplicates.
            let mut cleaned = existing.clone();
            let mut insert_pos = first_pos;
            let mut first_removal = true;
            while let Some(start) = cleaned.find(BRAIN_SECTION_START_PREFIX) {
                let end = cleaned[start..]
                    .find(BRAIN_SECTION_END)
                    .map(|i| start + i + BRAIN_SECTION_END.len())
                    .unwrap_or(cleaned.len());
                // Strip trailing newline after the end marker.
                let end = if cleaned.as_bytes().get(end) == Some(&b'\n') {
                    end + 1
                } else {
                    end
                };
                cleaned.replace_range(start..end, "");
                if first_removal {
                    insert_pos = start;
                    first_removal = false;
                }
            }
            cleaned.insert_str(insert_pos, &brain_section);
            fs::write(&agents_md_path, cleaned)?;
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
        if existing.contains(BRAIN_SECTION_START_PREFIX) {
            // Remove ALL brain:start...brain:end blocks, insert bridge at first position.
            let mut cleaned = existing.clone();
            let mut insert_pos = cleaned.find(BRAIN_SECTION_START_PREFIX).unwrap();
            let mut first_removal = true;
            while let Some(start) = cleaned.find(BRAIN_SECTION_START_PREFIX) {
                let end = cleaned[start..]
                    .find(BRAIN_SECTION_END)
                    .map(|i| start + i + BRAIN_SECTION_END.len())
                    .unwrap_or(cleaned.len());
                let end = if cleaned.as_bytes().get(end) == Some(&b'\n') {
                    end + 1
                } else {
                    end
                };
                cleaned.replace_range(start..end, "");
                if first_removal {
                    insert_pos = start;
                    first_removal = false;
                }
            }
            cleaned.insert_str(insert_pos, &bridge_ref);
            fs::write(&claude_md_path, cleaned)?;
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

const BRAIN_SECTION_START_PREFIX: &str = "<!-- brain:start";
const BRAIN_SECTION_END: &str = "<!-- brain:end -->";

/// Template body WITHOUT markers. Markers are added by `render_brain_section()`.
const BRAIN_SECTION_BODY: &str = r#"{build_section}## Task Management

This project uses `brain` for task tracking. **Always use MCP tools for task operations** — they provide structured responses and are the canonical interface for AI agents. CLI commands exist for human terminal use only.

> **Brain task IDs are local-only.** Task IDs like `brn-6f4`, `BRN-01JPH...`, or `BRX-01K...` live in the local brain DB and have no meaning outside this machine. Never include them in artifacts visible to others — pull request descriptions, commit subjects intended for code review, public docs, GitHub issues/comments, Slack messages, or anything that crosses the workstation boundary. They are fine inside commit body text purely as an internal cross-reference, but they are NOT a substitute for a real change description: write the PR/commit so a reader who has never seen the brain DB still understands the change. If a follow-up needs to be tracked publicly, file a GitHub issue and reference that instead.

### MCP Tools (preferred for AI agents)

When running as an MCP server (`brain mcp`), these tools are available:

**Task tools:**
- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`, `external_id_added`, `external_id_removed`, `external_blocker_added`, `external_blocker_resolved`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`). External blockers (added via `external_blocker_added`) gate readiness until resolved.
- `tasks_create` — Create a task with a flat schema (no event envelope). Required param: `title`. Optional: `description`, `priority` (0-4, default 4), `task_type` (task|bug|feature|epic|spike), `assignee`, `parent` (task ID prefix), `due_ts` (ISO 8601), `defer_until` (ISO 8601), `actor` (default: mcp). For remote creation: add `brain` (target brain name or ID from registry); optionally `link_from` (local task ID) and `link_type` (depends_on|blocks|related, default: related). Returns `{task_id, task, unblocked_task_ids}` for local creation, or `{remote_task_id, remote_brain_name, remote_brain_id, local_ref_created}` for remote creation.
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`, `in_progress` (exact match), `cancelled` (exact match). Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description). Optional `brains` array to query across multiple brain projects (e.g. `["work", "personal"]`); use `["all"]` (or `["*"]`) to query every registered brain. Each task is tagged with its source `brain`; federated responses include a top-level `brains` array. Singular `brain` is accepted as a deprecated alias for `brains: [name]`.
- `tasks_get` — Get full task details including relationships, comments, labels, linked notes, external IDs (`external_ids`), and external blockers (`external_blockers` — the subset that gate readiness). Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by status (in-progress first), then priority, then due date. Use for "what should I work on?" queries. Optional `brains` array to query across multiple brains (e.g. `["work", "personal"]`); use `["all"]` to merge ready tasks from every registered brain and sort by priority globally. Each task is tagged with its source `brain`; federated responses include a top-level `brains` array.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Supports `brain` param for cross-brain label management. Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Note:** `tasks_apply_event` and `tasks_close` automatically generate and embed searchable capsules into LanceDB on every task create, update, or completion. Tasks become discoverable via `memory_search_minimal` without any extra steps.

**Brain tools:**
- `brains.list` — List all brain projects registered in `~/.brain/state_projection.toml`. Returns `name`, `id`, `root` (filesystem path), and `prefix` (task ID prefix) for each brain. Also callable as `brains_list`.

**Memory tools:**
- `memory_search_minimal` — Semantic search across indexed notes and tasks. Returns compact stubs (title, summary, score, kind). The `kind` field is one of: `"note"`, `"episode"`, `"reflection"`, `"procedure"`, `"task"`, `"task-outcome"`, `"record"`. Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`). Optional `brains` array to search across multiple brain projects (e.g. `["work", "personal"]`); use `["all"]` to search all registered brains. Results include a `brain_name` field indicating the source brain. Supports metadata filters: `kinds` (array of kind strings to include), `time_after`/`time_before` (Unix timestamps), `tags_require` (AND — all must match), `tags_exclude` (NOR — any match excludes).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

**Records tools:**
- `records.create_document` — Create a document record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- `records.create_analysis` — Create an analysis record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- `records.create_plan` — Create a plan record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- `records.save_snapshot` — Save a snapshot record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- Per-kind policy: document/analysis/plan/summary are embedded and summarized; implementation/review/custom are embedded and searchable; snapshots are stored without embedding or summarization.
- `records.get` — Get a record by ID with full metadata, tags, and links (supports prefix resolution). Supports `brain` param for cross-brain access.
- `records.list` — List records with optional filters (kind, status, tag, task_id). Optional `brains` array to query across multiple brains (e.g. `["work", "personal"]`); use `["all"]` to query every registered brain. Each record is tagged with its source `brain`; federated responses include a top-level `brains` array. Singular `brain` is accepted as a deprecated alias for `brains: [name]`.
- `records.fetch_content` — Fetch raw content of a record. Text content (text/*, application/json, application/toml, application/yaml) is auto-decoded as UTF-8 and returned in a `text` field; binary content is returned as base64 in `data`. Response includes `encoding` ('utf-8' or 'base64'), `title`, and `kind` metadata. Supports `brain` param for cross-brain access.
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
brain tasks next               # Top 10 actionable tasks (priority-sorted)
brain tasks next -k 3          # Top 3 actionable tasks

# Creating & updating
brain tasks create --title="..." --description="..." --task-type=task --priority=2
brain tasks update <id> --status=in_progress
brain tasks comment <id> "comment text"

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

# Memory (semantic search & episodes)
brain memory search "query"                  # Search notes/tasks (compact stubs)
brain memory search -i lookup "exact term"   # Keyword-heavy search
brain memory search --brain all "patterns"   # Search all registered brains
brain memory expand <id1> <id2>              # Expand stubs to full content
brain memory write-episode --goal "..." --actions "..." --outcome "..."
brain memory reflect --topic "architecture"  # Prepare: get source material
brain memory reflect --commit --title "..." --content "..." --source-ids ep1,ep2

# Records (cross-brain writes supported via --brain)
brain documents create --title "Report" --file report.md
brain analyses create --title "Investigation" --brain other-brain --stdin
brain plans create --title "Rollout" --file rollout.md
brain snapshots save --title "State" --file state.json
brain snapshots save --title "State" --brain other-brain --stdin
brain artifacts restore <id>          # Print artifact content to stdout
brain artifacts restore <id> -o file  # Write artifact content to file
brain snapshots restore <id>          # Print snapshot content to stdout

# Status
brain status                  # Brain health check (task counts, index stats)
brain status --json           # Machine-readable JSON output

# Setup & management
brain init                     # Initialize a new brain in cwd
brain link <name>              # Link cwd as additional root for brain
brain alias add <alias> <name> # Add alias for a brain
brain alias remove <alias>     # Remove alias
brain alias list               # List aliases
brain config set <key> <val>   # Set brain config value
brain config get <key>         # Get brain config value
brain remove <name>            # Remove a brain from registry (alias: rm)
brain id                       # Show brain ID for current directory

# Daemon
brain daemon start [notes]     # Start background daemon
brain daemon stop              # Stop daemon
brain daemon status            # Check daemon status
brain daemon install           # Install launchd/systemd service
brain daemon uninstall         # Uninstall service

# Indexing & maintenance
brain reindex --full <path>    # Full reindex of notes
brain reindex --file <file>    # Reindex single file
brain vacuum                   # Clean stale data (default: >30 days)

# MCP server
brain mcp                      # Start MCP server (stdio)
brain mcp setup claude         # Auto-configure Claude Code MCP
brain mcp setup cursor         # Auto-configure Cursor MCP
brain mcp setup vscode         # Auto-configure VS Code MCP
brain hooks install            # Install git hooks

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

### Recording Context as Memory

When the user shares critical context that is not derivable from the current codebase, **proactively record it** using `memory_write_episode`. This preserves knowledge that would otherwise be lost between conversations.

**Record an episode when the user shares:**
- How an external API or service behaves (rate limits, quirks, undocumented behavior)
- Architecture or conventions of a different codebase that this project interacts with
- Business logic, domain rules, or constraints not captured in code
- Deployment topology, infrastructure details, or environment-specific behavior
- Historical context about why something was built a certain way
- Gotchas, workarounds, or lessons learned from past incidents

**How to record:** Use `memory_write_episode` with:
- `goal`: What the user was explaining or what prompted the context
- `actions`: The key facts, rules, or details shared
- `outcome`: How this knowledge should influence future work
- `tags`: Relevant topic tags for later retrieval (e.g. `["external-api", "payments"]`)

**Do not record:** Information already in the codebase, git history, or existing notes. Check `memory_search_minimal` first to avoid duplicates.

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic, spike
- **Statuses**: open, in_progress, blocked, done, cancelled
"#;

/// Render the full brain section with versioned start marker and end marker.
fn render_brain_section(brain_name: &str, build_section: &str) -> String {
    let body = BRAIN_SECTION_BODY
        .replace("{brain_name}", brain_name)
        .replace("{build_section}", build_section);
    let hex = blake3::hash(body.as_bytes()).to_hex();
    let hash = &hex.as_str()[..8];
    format!("<!-- brain:start:{hash} -->\n{body}{BRAIN_SECTION_END}\n")
}

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
        assert!(content.contains(BRAIN_SECTION_START_PREFIX));
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
        assert!(result.contains(BRAIN_SECTION_START_PREFIX));
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
            result.contains(BRAIN_SECTION_START_PREFIX),
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
            !content.contains(BRAIN_SECTION_START_PREFIX),
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
    fn duplicate_brain_sections_are_collapsed() {
        let dir = tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");

        let before = "# Project\n\nPreamble.\n\n";
        let block1 = "<!-- brain:start:aaaa1111 -->\nfirst block\n<!-- brain:end -->\n";
        let middle = "\n## Middle Section\n\nKeep this.\n\n";
        let block2 = "<!-- brain:start -->\nsecond block\n<!-- brain:end -->\n";
        let after = "\n## After Section\n\nAlso keep.\n";
        fs::write(
            &agents_path,
            format!("{before}{block1}{middle}{block2}{after}"),
        )
        .unwrap();

        upsert_agent_docs(dir.path(), "test-brain").unwrap();

        let result = fs::read_to_string(&agents_path).unwrap();
        // Exactly one brain section remains.
        assert_eq!(
            result.matches(BRAIN_SECTION_START_PREFIX).count(),
            1,
            "must have exactly one brain:start marker"
        );
        assert_eq!(
            result.matches(BRAIN_SECTION_END).count(),
            1,
            "must have exactly one brain:end marker"
        );
        // Old block contents removed.
        assert!(!result.contains("first block"), "stale block 1 removed");
        assert!(!result.contains("second block"), "stale block 2 removed");
        // Content before first block preserved.
        assert!(result.starts_with(before), "preamble preserved");
        // Content between and after blocks preserved.
        assert!(result.contains("## Middle Section"), "middle preserved");
        assert!(result.contains("## After Section"), "after preserved");
        // New brain content present.
        assert!(result.contains("## Task Management"), "new content present");
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

        let db = brain_persistence::db::Db::open(&db_path).unwrap();
        let stored = db
            .with_read_conn(|conn| brain_persistence::db::meta::get_meta(conn, "project_prefix"))
            .unwrap()
            .unwrap();
        assert_eq!(stored, "MCP");
    }

    #[test]
    fn seed_project_prefix_does_not_override_existing_value() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("brain.db");
        seed_project_prefix_if_missing(&db_path, "alpha-service").unwrap();

        let db = brain_persistence::db::Db::open(&db_path).unwrap();
        db.with_write_conn(|conn| {
            brain_persistence::db::meta::set_meta(conn, "project_prefix", "XYZ")
        })
        .unwrap();

        seed_project_prefix_if_missing(&db_path, "beta-service").unwrap();

        let stored = db
            .with_read_conn(|conn| brain_persistence::db::meta::get_meta(conn, "project_prefix"))
            .unwrap()
            .unwrap();
        assert_eq!(stored, "XYZ");
    }

    #[test]
    fn render_brain_section_embeds_correct_hash() {
        let section = render_brain_section("test", "");
        // Extract hash from marker: <!-- brain:start:XXXXXXXX -->
        let marker_end = section.find(" -->\n").unwrap();
        let hash_in_marker = &section["<!-- brain:start:".len()..marker_end];
        assert_eq!(hash_in_marker.len(), 8, "hash must be 8 hex chars");

        // Recompute: body is everything between the start marker line and the end marker.
        let body_start = section.find('\n').unwrap() + 1;
        let body_end = section.find(BRAIN_SECTION_END).unwrap();
        let body = &section[body_start..body_end];

        let hex = blake3::hash(body.as_bytes()).to_hex();
        let expected_hash = &hex.as_str()[..8];

        assert_eq!(
            hash_in_marker, expected_hash,
            "hash in marker must match blake3 of rendered body"
        );
    }

    #[test]
    fn render_brain_section_is_deterministic() {
        let a = render_brain_section("test", "## Build\n\n");
        let b = render_brain_section("test", "## Build\n\n");
        assert_eq!(a, b, "same inputs must produce identical output");
    }

    #[test]
    fn render_brain_section_hash_changes_on_body_change() {
        let a = render_brain_section("test", "## Build A\n\n");
        let b = render_brain_section("test", "## Build B\n\n");
        let hash_a = &a["<!-- brain:start:".len()..a.find(" -->\n").unwrap()];
        let hash_b = &b["<!-- brain:start:".len()..b.find(" -->\n").unwrap()];
        assert_ne!(hash_a, hash_b, "different body must produce different hash");
    }
}
