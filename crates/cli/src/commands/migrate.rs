use std::io::{self, Write as _};
use std::path::Path;

use anyhow::{Context, Result};
use brain_lib::config::{brain_home, load_global_config, resolve_paths_for_brain_with_home};
use rusqlite::Connection;

/// Arguments for the polished `brain migrate` command.
pub struct MigrateArgs {
    /// Skip the confirmation prompt.
    pub yes: bool,
    /// Remove per-brain brain.db files after successful migration.
    pub cleanup: bool,
}

/// Entry point for `brain migrate`.
///
/// Guides the user through pre-flight checks, backup, migration, and optional
/// cleanup.  Delegates actual data movement to `migrate_workspace`.
pub fn run(args: MigrateArgs) -> Result<()> {
    let home = brain_home()?;
    let unified_db_path = home.join("brain.db");

    // ── Pre-flight ─────────────────────────────────────────────────────────

    let config = load_global_config()?;

    if config.brains.is_empty() {
        println!("No brains registered in ~/.brain/config.toml. Nothing to migrate.");
        return Ok(());
    }

    // Detect already-migrated state: unified DB exists and brains table is populated.
    if unified_db_path.exists()
        && let Ok(conn) = Connection::open(&unified_db_path)
    {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM brains", [], |row| row.get(0))
            .unwrap_or(0);
        if count > 0 {
            println!(
                "Migration already complete: {} brain(s) registered in the unified DB.",
                count
            );
            println!("Run `brain list` to verify. Nothing to do.");
            return Ok(());
        }
    }

    // Sort for deterministic output.
    let mut entries: Vec<(String, brain_lib::config::BrainEntry)> =
        config.brains.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Collect brains that have a source DB to migrate.
    let mut to_migrate: Vec<(&str, &brain_lib::config::BrainEntry)> = Vec::new();
    let mut skipped: Vec<&str> = Vec::new();

    for (name, entry) in &entries {
        if entry.id.is_none() {
            skipped.push(name.as_str());
            continue;
        }
        let paths = resolve_paths_for_brain_with_home(name, &home);
        if !paths.sqlite_db.exists() {
            skipped.push(name.as_str());
            continue;
        }
        to_migrate.push((name.as_str(), entry));
    }

    println!("Brain migration — unified storage");
    println!("==================================");
    println!();
    println!("The following brains will be migrated into ~/.brain/brain.db:");
    println!();
    for (name, entry) in &to_migrate {
        let paths = resolve_paths_for_brain_with_home(name, &home);
        println!("  {} ({})", name, entry.id.as_deref().unwrap_or("no-id"));
        println!("    source: {}", paths.sqlite_db.display());
    }

    if !skipped.is_empty() {
        println!();
        println!(
            "Skipped (no brain_id or source DB not found): {}",
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

    // Ensure unified DB exists with full v18 schema.
    {
        let _db = brain_lib::db::Db::open(&unified_db_path).with_context(|| {
            format!(
                "failed to initialise unified DB at {}",
                unified_db_path.display()
            )
        })?;
    }

    // Migrate per-brain SQLite data.
    for (name, entry) in &to_migrate {
        let brain_id = entry.id.as_deref().unwrap();
        let paths = resolve_paths_for_brain_with_home(name, &home);
        migrate_one_brain(&unified_db_path, &paths.sqlite_db, brain_id, name)?;
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
            let paths = resolve_paths_for_brain_with_home(name, &home);
            if paths.sqlite_db.exists() {
                match std::fs::remove_file(&paths.sqlite_db) {
                    Ok(_) => println!("  Removed: {}", paths.sqlite_db.display()),
                    Err(e) => eprintln!(
                        "  Warning: could not remove {}: {}",
                        paths.sqlite_db.display(),
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
        let paths = resolve_paths_for_brain_with_home(name, home);
        let brain_data_dir = paths
            .sqlite_db
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| home.join("brains").join(name));
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

/// Migrate a single brain's data into the unified DB using SQLite ATTACH.
fn migrate_one_brain(
    unified_path: &Path,
    source_path: &Path,
    brain_id: &str,
    name: &str,
) -> Result<()> {
    let mut conn = Connection::open(unified_path)
        .with_context(|| format!("failed to open unified DB at {}", unified_path.display()))?;

    conn.pragma_update(None, "foreign_keys", "OFF")?;

    let already_migrated: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM brains WHERE brain_id = ?1",
            rusqlite::params![brain_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|c| c > 0)
        .context("failed to query brains table")?;

    if already_migrated {
        println!(
            "  Brain '{}' ({}): already migrated — skipped.",
            name, brain_id
        );
        return Ok(());
    }

    let source_path_str = source_path
        .to_str()
        .context("source DB path contains non-UTF8 characters")?;

    conn.execute_batch(&format!(
        "ATTACH DATABASE '{}' AS src",
        source_path_str.replace('\'', "''")
    ))?;

    // Check task_id collisions.
    {
        let mut stmt = conn.prepare(
            "SELECT s.task_id FROM src.tasks s
             INNER JOIN tasks t ON s.task_id = t.task_id
             WHERE t.brain_id != ?1
             LIMIT 1",
        )?;
        let collision: Option<String> = stmt
            .query_row(rusqlite::params![brain_id], |row| row.get(0))
            .ok();
        if let Some(task_id) = collision {
            anyhow::bail!(
                "task_id collision: '{}' from brain '{}' ({}) already exists in unified DB \
                 under a different brain. Aborting.",
                task_id,
                name,
                brain_id
            );
        }
    }

    {
        let tx = conn.transaction()?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        tx.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![brain_id, name, now],
        )?;

        let task_count: i64 =
            tx.query_row("SELECT COUNT(*) FROM src.tasks", [], |row| row.get(0))?;

        tx.execute_batch(&format!(
            "INSERT INTO tasks
                 (task_id, title, description, status, priority,
                  blocked_reason, due_ts, task_type, assignee, defer_until,
                  parent_task_id, child_seq, created_at, updated_at, brain_id)
             SELECT
                 task_id, title, description, status, priority,
                 blocked_reason, due_ts, task_type, assignee, defer_until,
                 parent_task_id, child_seq, created_at, updated_at, '{brain_id}'
             FROM src.tasks"
        ))?;

        tx.execute_batch(
            "INSERT OR IGNORE INTO task_deps (task_id, depends_on)
             SELECT task_id, depends_on FROM src.task_deps",
        )?;
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_labels (task_id, label)
             SELECT task_id, label FROM src.task_labels",
        )?;
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_comments (comment_id, task_id, author, body, created_at)
             SELECT comment_id, task_id, author, body, created_at FROM src.task_comments",
        )?;
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_external_ids (task_id, source, external_id, imported_at, external_url)
             SELECT task_id, source, external_id, imported_at, external_url FROM src.task_external_ids",
        )?;

        let record_count: i64 =
            tx.query_row("SELECT COUNT(*) FROM src.records", [], |row| row.get(0))?;

        tx.execute_batch(&format!(
            "INSERT OR IGNORE INTO records
                 (record_id, title, kind, status, description,
                  content_hash, content_size, media_type, task_id, actor,
                  created_at, updated_at, retention_class, pinned,
                  payload_available, content_encoding, original_size, brain_id)
             SELECT
                 record_id, title, kind, status, description,
                 content_hash, content_size, media_type, task_id, actor,
                 created_at, updated_at, retention_class, pinned,
                 payload_available, content_encoding, original_size, '{brain_id}'
             FROM src.records"
        ))?;

        tx.execute_batch(
            "INSERT OR IGNORE INTO record_tags (record_id, tag)
             SELECT record_id, tag FROM src.record_tags",
        )?;
        tx.execute_batch(
            "INSERT OR IGNORE INTO record_links (record_id, task_id, chunk_id, created_at)
             SELECT record_id, task_id, chunk_id, created_at FROM src.record_links",
        )?;
        tx.execute_batch(&format!(
            "INSERT OR IGNORE INTO record_events (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
             SELECT event_id, record_id, event_type, timestamp, actor, payload, '{brain_id}'
             FROM src.record_events"
        ))?;

        tx.commit()?;

        println!(
            "  Migrated brain '{}' ({}): {} tasks, {} records.",
            name, brain_id, task_count, record_count
        );
    }

    conn.execute_batch("DETACH DATABASE src")?;

    Ok(())
}
