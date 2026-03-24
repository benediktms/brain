use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use brain_lib::config::{brain_home, load_global_config};
use rusqlite::Connection;

/// Arguments for `brain migrate`.
pub struct MigrateArgs {
    /// Skip the confirmation prompt.
    pub yes: bool,
    /// Remove per-brain brain.db files after successful migration.
    pub cleanup: bool,
    /// Migrate a specific brain (name, ID, or alias). `None` = all brains.
    pub brain: Option<String>,
}

/// Entry point for `brain migrate`.
///
/// Replays per-brain JSONL event logs into the unified `~/.brain/brain.db`.
/// Idempotent — safe to re-run. Events already present are skipped.
pub fn run(args: MigrateArgs) -> Result<()> {
    let home = brain_home()?;
    let unified_db_path = home.join("brain.db");

    // ── Pre-flight ─────────────────────────────────────────────────────────

    let config = load_global_config()?;

    if config.brains.is_empty() {
        println!("No brains registered in ~/.brain/config.toml. Nothing to migrate.");
        return Ok(());
    }

    // If a specific brain was requested, resolve it (by name, ID, or alias).
    let entries: Vec<(String, brain_lib::config::BrainEntry)> = if let Some(ref target) = args.brain
    {
        let (name, entry) = brain_lib::config::resolve_brain_entry_from_config(target, &config)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        vec![(name, entry)]
    } else {
        let mut all: Vec<(String, brain_lib::config::BrainEntry)> =
            config.brains.into_iter().collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    };

    // Collect brains eligible for JSONL migration.
    let mut to_migrate: Vec<(&str, &brain_lib::config::BrainEntry)> = Vec::new();
    let mut skipped: Vec<&str> = Vec::new();

    for (name, entry) in &entries {
        if entry.id.is_none() {
            skipped.push(name.as_str());
            continue;
        }
        // Eligible if any JSONL source exists (per-brain or project-level).
        let jsonl_paths = collect_jsonl_paths(name, entry, &home);
        if jsonl_paths.task_sources.is_empty() && jsonl_paths.record_sources.is_empty() {
            skipped.push(name.as_str());
            continue;
        }
        to_migrate.push((name.as_str(), entry));
    }

    println!("Brain migration — JSONL replay into unified storage");
    println!("====================================================");
    println!();
    println!("The following brains will be migrated into ~/.brain/brain.db:");
    println!();
    for (name, entry) in &to_migrate {
        let jsonl = collect_jsonl_paths(name, entry, &home);
        println!("  {} ({})", name, entry.id.as_deref().unwrap_or("no-id"));
        println!(
            "    JSONL sources: {} task, {} record",
            jsonl.task_sources.len(),
            jsonl.record_sources.len()
        );
    }

    if !skipped.is_empty() {
        println!();
        println!(
            "Skipped (no brain_id or no JSONL files): {}",
            skipped.join(", ")
        );
    }

    if to_migrate.is_empty() {
        println!();
        println!("No brains eligible for migration. Nothing to do.");
        return Ok(());
    }

    let backup_path = home.join("brain.db.pre-workspace-migration");
    println!();
    if unified_db_path.exists() && !backup_path.exists() {
        println!("A backup will be created at:");
        println!("  {}", backup_path.display());
    } else if backup_path.exists() {
        println!(
            "Backup already exists at {} — will be kept as-is.",
            backup_path.display()
        );
    } else {
        println!("The unified DB will be created fresh at:");
        println!("  {}", unified_db_path.display());
    }

    if args.cleanup {
        println!();
        println!("--cleanup: per-brain brain.db files will be removed after migration.");
    }

    // ── Confirmation ───────────────────────────────────────────────────────

    if !args.yes {
        println!();
        print!("Proceed with migration? [y/N] ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim().to_lowercase();
        if trimmed != "y" && trimmed != "yes" {
            println!("Migration cancelled.");
            return Ok(());
        }
    }

    println!();

    // ── Backup ─────────────────────────────────────────────────────────────

    if unified_db_path.exists() && !backup_path.exists() {
        std::fs::copy(&unified_db_path, &backup_path)
            .with_context(|| format!("failed to create backup at {}", backup_path.display()))?;
        println!("Backup created: {}", backup_path.display());
    } else if backup_path.exists() {
        println!(
            "Backup already exists at {} — skipping.",
            backup_path.display()
        );
    }

    // ── Execute migration ──────────────────────────────────────────────────

    // Ensure unified DB exists with full schema.
    let db = brain_lib::db::Db::open(&unified_db_path).with_context(|| {
        format!(
            "failed to initialise unified DB at {}",
            unified_db_path.display()
        )
    })?;

    // Migrate per-brain JSONL events.
    for (name, entry) in &to_migrate {
        let brain_id = entry.id.as_deref().unwrap();
        let jsonl_paths = collect_jsonl_paths(name, entry, &home);
        migrate_one_brain(&db, brain_id, name, &jsonl_paths)?;
    }

    // Migrate content-addressed object blobs.
    migrate_objects(&home, &entries)?;

    // ── Post-migration verification ─────────────────────────────────────────

    println!();
    println!("Verifying migration…");
    {
        let conn = Connection::open(&unified_db_path)
            .context("failed to open unified DB for verification")?;

        let brain_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM brains", [], |r| r.get(0))
            .context("verification: failed to count brains")?;

        let task_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks WHERE brain_id != ''", [], |r| {
                r.get(0)
            })
            .context("verification: failed to count tasks")?;

        let record_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM records WHERE brain_id != ''",
                [],
                |r| r.get(0),
            )
            .context("verification: failed to count records")?;

        let orphaned_tasks: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks WHERE brain_id = ''", [], |r| {
                r.get(0)
            })
            .context("verification: failed to count orphaned tasks")?;

        let orphaned_records: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM records WHERE brain_id = ''",
                [],
                |r| r.get(0),
            )
            .context("verification: failed to count orphaned records")?;

        println!("  Brains registered:  {}", brain_count);
        println!(
            "  Tasks migrated:     {} (orphaned: {})",
            task_count, orphaned_tasks
        );
        println!(
            "  Records migrated:   {} (orphaned: {})",
            record_count, orphaned_records
        );

        if orphaned_tasks > 0 || orphaned_records > 0 {
            eprintln!(
                "Warning: {} orphaned task(s) and {} orphaned record(s) detected.",
                orphaned_tasks, orphaned_records
            );
            eprintln!("These rows have no brain_id set. Inspect manually.");
        }
    }

    // ── Optional cleanup ───────────────────────────────────────────────────

    if args.cleanup {
        println!();
        println!("Cleanup: removing per-brain brain.db files…");
        for (name, _entry) in &to_migrate {
            // Compute per-brain path explicitly — paths.sqlite_db now points to
            // the unified ~/.brain/brain.db and must not be removed here.
            let per_brain_db = home.join("brains").join(name).join("brain.db");
            if per_brain_db.exists() {
                match std::fs::remove_file(&per_brain_db) {
                    Ok(_) => println!("  Removed: {}", per_brain_db.display()),
                    Err(e) => eprintln!(
                        "  Warning: could not remove {}: {}",
                        per_brain_db.display(),
                        e
                    ),
                }
            }
        }
    }

    println!();
    println!("Migration complete. Run `brain list` to verify registered brains.");

    Ok(())
}

// ── JSONL path collection ──────────────────────────────────────────────────

/// All JSONL event log paths for a single brain, deduplicated.
struct JsonlPaths {
    task_sources: Vec<PathBuf>,
    record_sources: Vec<PathBuf>,
}

/// Collect all JSONL event log paths for a brain from both per-brain data dir
/// and project roots.
fn collect_jsonl_paths(
    name: &str,
    entry: &brain_lib::config::BrainEntry,
    home: &Path,
) -> JsonlPaths {
    let brain_data = home.join("brains").join(name);
    let mut task_sources: Vec<PathBuf> = Vec::new();
    let mut record_sources: Vec<PathBuf> = Vec::new();

    // Per-brain data dir: ~/.brain/brains/<name>/tasks/events.jsonl
    let per_brain_tasks = brain_data.join("tasks").join("events.jsonl");
    if per_brain_tasks.exists() {
        task_sources.push(per_brain_tasks);
    }

    let per_brain_records = brain_data.join("records").join("events.jsonl");
    if per_brain_records.exists() {
        record_sources.push(per_brain_records);
    }

    // Project roots: <root>/.brain/tasks/events.jsonl
    for root in &entry.roots {
        let project_tasks = root.join(".brain").join("tasks").join("events.jsonl");
        if project_tasks.exists() && !task_sources.contains(&project_tasks) {
            task_sources.push(project_tasks);
        }
        // Records are only in per-brain dir, not project roots.
    }

    JsonlPaths {
        task_sources,
        record_sources,
    }
}

// ── Per-brain JSONL replay ─────────────────────────────────────────────────

/// Migrate a single brain by replaying JSONL event logs into the unified DB.
///
/// Idempotent: events that already exist are skipped.
fn migrate_one_brain(
    db: &brain_lib::db::Db,
    brain_id: &str,
    name: &str,
    jsonl: &JsonlPaths,
) -> Result<()> {
    // Register brain (idempotent).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    db.with_write_conn(|conn| {
        conn.execute(
            "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![brain_id, name, now],
        )?;
        Ok(())
    })
    .with_context(|| format!("failed to register brain '{name}' ({brain_id})"))?;

    // Replay task JSONL sources.
    let task_store = brain_lib::tasks::TaskStore::with_brain_id(db.clone(), brain_id, name)?;

    let mut total_tasks = 0usize;
    for path in &jsonl.task_sources {
        let count = task_store
            .import_from_jsonl(path)
            .with_context(|| format!("failed to import task events from {}", path.display()))?;
        total_tasks += count;
    }

    // Replay record JSONL sources.
    let record_store = brain_lib::records::RecordStore::with_brain_id(db.clone(), brain_id, name)?;

    let mut total_records = 0usize;
    for path in &jsonl.record_sources {
        let count = record_store
            .import_from_jsonl(path)
            .with_context(|| format!("failed to import record events from {}", path.display()))?;
        total_records += count;
    }

    println!(
        "  Migrated brain '{}' ({}): {} task events, {} record events.",
        name, brain_id, total_tasks, total_records
    );

    Ok(())
}

// ── Object blob migration (unchanged) ──────────────────────────────────────

/// Migrate per-brain object blobs into the unified `~/.brain/objects/` store.
fn migrate_objects(home: &Path, entries: &[(String, brain_lib::config::BrainEntry)]) -> Result<()> {
    let unified_objects = home.join("objects");
    std::fs::create_dir_all(&unified_objects).with_context(|| {
        format!(
            "failed to create unified objects dir {}",
            unified_objects.display()
        )
    })?;

    let mut total_moved = 0u64;
    let mut total_skipped = 0u64;

    for (name, entry) in entries {
        if entry.id.is_none() {
            continue;
        }
        // Per-brain data dir derived from home + name — sqlite_db now points to
        // the unified DB so we cannot use paths.sqlite_db.parent() here.
        let brain_data_dir = home.join("brains").join(name);
        let brain_objects = brain_data_dir.join("objects");

        if !brain_objects.exists() {
            continue;
        }

        let (moved, skipped) = migrate_one_objects_dir(&brain_objects, &unified_objects, name)?;
        total_moved += moved;
        total_skipped += skipped;
    }

    println!(
        "  Objects: {} moved, {} already present (dedup).",
        total_moved, total_skipped
    );

    Ok(())
}

/// Move/hard-link all blobs from `src_objects` into `dst_objects`.
fn migrate_one_objects_dir(
    src_objects: &Path,
    dst_objects: &Path,
    brain_name: &str,
) -> Result<(u64, u64)> {
    let mut moved = 0u64;
    let mut skipped = 0u64;

    let prefix_entries = match std::fs::read_dir(src_objects) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", src_objects.display()));
        }
    };

    for prefix_entry in prefix_entries {
        let prefix_entry = prefix_entry?;
        let prefix_path = prefix_entry.path();

        if !prefix_path.is_dir() {
            continue;
        }
        let prefix_name = match prefix_path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit()) => n.to_string(),
            _ => continue,
        };

        let dst_prefix_dir = dst_objects.join(&prefix_name);
        std::fs::create_dir_all(&dst_prefix_dir)
            .with_context(|| format!("failed to create prefix dir {}", dst_prefix_dir.display()))?;

        let blob_entries = std::fs::read_dir(&prefix_path)
            .with_context(|| format!("failed to read prefix dir {}", prefix_path.display()))?;

        for blob_entry in blob_entries {
            let blob_entry = blob_entry?;
            let blob_name = match blob_entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };

            if blob_name.ends_with(".tmp") {
                continue;
            }
            if blob_name.len() != 64 || !blob_name.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            let dst_blob = dst_prefix_dir.join(&blob_name);
            if dst_blob.exists() {
                skipped += 1;
                continue;
            }

            let src_blob = blob_entry.path();

            let link_result = std::fs::hard_link(&src_blob, &dst_blob);
            if let Err(ref e) = link_result {
                if e.kind() == std::io::ErrorKind::CrossesDevices || e.raw_os_error() == Some(18) {
                    std::fs::copy(&src_blob, &dst_blob).with_context(|| {
                        format!("failed to copy blob {} (brain '{}')", blob_name, brain_name)
                    })?;
                } else {
                    link_result.with_context(|| {
                        format!(
                            "failed to hard-link blob {} (brain '{}')",
                            blob_name, brain_name
                        )
                    })?;
                }
            }

            moved += 1;
        }
    }

    Ok((moved, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_lib::db::Db;
    use brain_lib::records::events::{ContentRefPayload, RecordCreatedPayload, RecordEvent};
    use brain_lib::tasks::events::TaskCreatedPayload;

    /// Write task events to a JSONL file and migrate via JSONL replay.
    #[test]
    fn test_migrate_one_brain_jsonl_basic() {
        let tmp = tempfile::TempDir::new().unwrap();
        let unified_db_path = tmp.path().join("brain.db");
        let db = Db::open(&unified_db_path).unwrap();

        // Create task JSONL
        let tasks_dir = tmp.path().join("tasks");
        std::fs::create_dir_all(&tasks_dir).unwrap();
        let tasks_jsonl = tasks_dir.join("events.jsonl");

        let task_event = brain_lib::tasks::events::TaskEvent::from_payload(
            "TST-01AAAA",
            "test",
            TaskCreatedPayload {
                title: "Test Task".to_string(),
                description: None,
                status: Default::default(),
                priority: 2,
                task_type: None,
                assignee: None,
                due_ts: None,
                defer_until: None,
                parent_task_id: None,
                display_id: None,
            },
        );
        brain_lib::tasks::events::append_event(&tasks_jsonl, &task_event).unwrap();

        // Create record JSONL
        let records_dir = tmp.path().join("records");
        std::fs::create_dir_all(&records_dir).unwrap();
        let records_jsonl = records_dir.join("events.jsonl");

        let record_event = RecordEvent::from_payload(
            "REC-01BBBB",
            "test",
            RecordCreatedPayload {
                title: "Test Record".to_string(),
                kind: "snapshot".to_string(),
                content_ref: ContentRefPayload::new("ab".repeat(32), 42, None),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        brain_lib::records::events::append_event(&records_jsonl, &record_event).unwrap();

        let jsonl = JsonlPaths {
            task_sources: vec![tasks_jsonl],
            record_sources: vec![records_jsonl],
        };

        migrate_one_brain(&db, "test-brain-id", "test-brain", &jsonl).unwrap();

        // Verify brain registered
        let count: i64 = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM brains WHERE brain_id = 'test-brain-id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(count, 1);

        // Verify task imported
        let task_count: i64 = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE brain_id = 'test-brain-id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(task_count, 1);

        // Verify record imported
        let rec_count: i64 = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM records WHERE brain_id = 'test-brain-id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(rec_count, 1);
    }

    /// Migration is idempotent — running twice produces no duplicates.
    #[test]
    fn test_migrate_one_brain_jsonl_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let unified_db_path = tmp.path().join("brain.db");
        let db = Db::open(&unified_db_path).unwrap();

        let tasks_dir = tmp.path().join("tasks");
        std::fs::create_dir_all(&tasks_dir).unwrap();
        let tasks_jsonl = tasks_dir.join("events.jsonl");

        let task_event = brain_lib::tasks::events::TaskEvent::from_payload(
            "TST-02CCCC",
            "test",
            TaskCreatedPayload {
                title: "Idempotent Task".to_string(),
                description: None,
                status: Default::default(),
                priority: 3,
                task_type: None,
                assignee: None,
                due_ts: None,
                defer_until: None,
                parent_task_id: None,
                display_id: None,
            },
        );
        brain_lib::tasks::events::append_event(&tasks_jsonl, &task_event).unwrap();

        let jsonl = JsonlPaths {
            task_sources: vec![tasks_jsonl.clone()],
            record_sources: vec![],
        };

        // First migration
        migrate_one_brain(&db, "idem-id", "idem-brain", &jsonl).unwrap();

        // Second migration — no failure, no duplicates
        migrate_one_brain(&db, "idem-id", "idem-brain", &jsonl).unwrap();

        let brain_count: i64 = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM brains WHERE brain_id = 'idem-id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(brain_count, 1, "brain should appear exactly once");

        let task_count: i64 = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE brain_id = 'idem-id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(task_count, 1, "task should appear exactly once");
    }

    /// Migration works even without a per-brain brain.db (the gateway scenario).
    #[test]
    fn test_migrate_no_source_db_jsonl_only() {
        let tmp = tempfile::TempDir::new().unwrap();
        let unified_db_path = tmp.path().join("brain.db");
        let db = Db::open(&unified_db_path).unwrap();

        // Only JSONL, no per-brain brain.db
        let tasks_dir = tmp.path().join("tasks");
        std::fs::create_dir_all(&tasks_dir).unwrap();
        let tasks_jsonl = tasks_dir.join("events.jsonl");

        let task_event = brain_lib::tasks::events::TaskEvent::from_payload(
            "GW-01DDDD",
            "test",
            TaskCreatedPayload {
                title: "Gateway Task".to_string(),
                description: None,
                status: Default::default(),
                priority: 1,
                task_type: None,
                assignee: None,
                due_ts: None,
                defer_until: None,
                parent_task_id: None,
                display_id: None,
            },
        );
        brain_lib::tasks::events::append_event(&tasks_jsonl, &task_event).unwrap();

        let jsonl = JsonlPaths {
            task_sources: vec![tasks_jsonl],
            record_sources: vec![],
        };

        // No brain.db exists — should work fine via JSONL replay
        migrate_one_brain(&db, "gateway-id", "gateway", &jsonl).unwrap();

        let task_count: i64 = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE brain_id = 'gateway-id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(task_count, 1, "gateway task should be imported from JSONL");
    }
}
