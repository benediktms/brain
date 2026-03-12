use std::path::Path;

use anyhow::{Context, Result, bail};
use brain_lib::config::{brain_home, load_global_config, resolve_paths_for_brain_with_home};
use rusqlite::Connection;

/// Migrate all per-brain SQLite databases into the unified `~/.brain/brain.db`.
///
/// Idempotent: brains already present in the `brains` table are skipped.
/// Atomic per-brain: each brain's data is inserted in a single transaction.
/// Collision-safe: task_id conflicts across brains abort with a clear error.
pub fn run() -> Result<()> {
    let home = brain_home()?;
    let unified_db_path = home.join("brain.db");
    let backup_path = home.join("brain.db.pre-workspace-migration");

    // ── Load registry ──────────────────────────────────────────────────────

    let config = load_global_config()?;

    if config.brains.is_empty() {
        println!("No brains registered. Nothing to migrate.");
        return Ok(());
    }

    // ── Backup ─────────────────────────────────────────────────────────────

    if unified_db_path.exists() && !backup_path.exists() {
        std::fs::copy(&unified_db_path, &backup_path).with_context(|| {
            format!(
                "failed to create backup at {}",
                backup_path.display()
            )
        })?;
        println!("Backup created: {}", backup_path.display());
    } else if backup_path.exists() {
        println!(
            "Backup already exists at {} — skipping backup step.",
            backup_path.display()
        );
    }

    // ── Open unified DB (creates + migrates schema to v18 if absent) ───────

    {
        // Open via brain_lib Db to run schema init (creates brain.db + runs all migrations).
        let _db = brain_lib::db::Db::open(&unified_db_path)
            .with_context(|| format!("failed to open unified DB at {}", unified_db_path.display()))?;
        // _db dropped here; we use raw rusqlite below for ATTACH-based migration.
    }

    // ── Migrate each brain ─────────────────────────────────────────────────

    // Sort for deterministic output.
    let mut entries: Vec<(String, brain_lib::config::BrainEntry)> =
        config.brains.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    for (name, entry) in &entries {
        let brain_id = match &entry.id {
            Some(id) => id.clone(),
            None => {
                println!("  Skipping brain '{}': no brain_id in registry.", name);
                continue;
            }
        };

        let source_paths = resolve_paths_for_brain_with_home(name, &home);
        let source_db_path = source_paths.sqlite_db;

        if !source_db_path.exists() {
            println!(
                "  Skipping brain '{}': source DB not found at {}.",
                name,
                source_db_path.display()
            );
            continue;
        }

        migrate_one_brain(&unified_db_path, &source_db_path, &brain_id, name)?;
    }

    // ── Migrate object stores ───────────────────────────────────────────────

    migrate_objects(&home, &entries)?;

    println!("\nWorkspace migration complete.");
    Ok(())
}

/// Migrate per-brain object blobs into the unified `~/.brain/objects/` store.
///
/// Content-addressed: identical hashes (already present) are skipped.
/// Different hashes are hard-linked or copied into the unified store.
/// Per-brain `objects/` directories are left in place.
fn migrate_objects(
    home: &std::path::Path,
    entries: &[(String, brain_lib::config::BrainEntry)],
) -> Result<()> {
    let unified_objects = home.join("objects");
    std::fs::create_dir_all(&unified_objects)
        .with_context(|| format!("failed to create unified objects dir {}", unified_objects.display()))?;

    let mut total_moved = 0u64;
    let mut total_skipped = 0u64;

    for (name, entry) in entries {
        if entry.id.is_none() {
            continue;
        }

        let paths = brain_lib::config::resolve_paths_for_brain_with_home(name, home);
        let brain_data_dir = paths.sqlite_db
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
///
/// Returns `(moved, skipped)` counts.
fn migrate_one_objects_dir(
    src_objects: &std::path::Path,
    dst_objects: &std::path::Path,
    brain_name: &str,
) -> Result<(u64, u64)> {
    let mut moved = 0u64;
    let mut skipped = 0u64;

    let prefix_entries = match std::fs::read_dir(src_objects) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", src_objects.display())),
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

            // Skip temp files and non-hash filenames.
            if blob_name.ends_with(".tmp") {
                continue;
            }
            if blob_name.len() != 64 || !blob_name.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            let dst_blob = dst_prefix_dir.join(&blob_name);
            if dst_blob.exists() {
                // Content-addressed dedup: same hash → already present, skip.
                skipped += 1;
                continue;
            }

            let src_blob = blob_entry.path();

            // Try hard-link first (zero copy). Fall back to copy if cross-device.
            let link_result = std::fs::hard_link(&src_blob, &dst_blob);
            if let Err(ref e) = link_result {
                if e.kind() == std::io::ErrorKind::CrossesDevices
                    || e.raw_os_error() == Some(18) // EXDEV
                {
                    std::fs::copy(&src_blob, &dst_blob)
                        .with_context(|| format!("failed to copy blob {} (brain '{}')", blob_name, brain_name))?;
                } else {
                    link_result.with_context(|| {
                        format!("failed to hard-link blob {} (brain '{}')", blob_name, brain_name)
                    })?;
                }
            }

            moved += 1;
        }
    }

    Ok((moved, skipped))
}

/// Migrate a single brain's data into the unified DB.
///
/// Uses SQLite ATTACH to perform cross-database INSERT in a single transaction.
/// Idempotent: skips if brain_id already registered.
fn migrate_one_brain(
    unified_path: &Path,
    source_path: &Path,
    brain_id: &str,
    name: &str,
) -> Result<()> {
    let mut conn = Connection::open(unified_path)
        .with_context(|| format!("failed to open unified DB at {}", unified_path.display()))?;

    conn.pragma_update(None, "foreign_keys", "OFF")?;

    // Check idempotency: skip if already migrated.
    let already_migrated: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM brains WHERE brain_id = ?1",
            rusqlite::params![brain_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|c| c > 0)
        .with_context(|| "failed to query brains table")?;

    if already_migrated {
        println!("  Brain '{}' ({}): already migrated — skipped.", name, brain_id);
        return Ok(());
    }

    // Attach source DB BEFORE the transaction to avoid WAL locking issues.
    let source_path_str = source_path
        .to_str()
        .with_context(|| "source DB path contains non-UTF8 characters")?;

    conn.execute_batch(&format!(
        "ATTACH DATABASE '{}' AS src",
        source_path_str.replace('\'', "''")
    ))?;

    // Check task_id collisions using the attached source (no separate connection needed).
    check_task_id_collisions(&conn, brain_id, name)?;

    // Perform the migration in a single transaction.
    {
        let tx = conn.transaction()?;

        // Register brain.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        tx.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![brain_id, name, now],
        )?;

        // ── Tasks ──────────────────────────────────────────────────────────

        let task_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM src.tasks",
            [],
            |row| row.get(0),
        )?;

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

        // task_deps
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_deps (task_id, depends_on)
             SELECT task_id, depends_on FROM src.task_deps",
        )?;

        // task_labels
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_labels (task_id, label)
             SELECT task_id, label FROM src.task_labels",
        )?;

        // task_comments
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_comments (comment_id, task_id, author, body, created_at)
             SELECT comment_id, task_id, author, body, created_at FROM src.task_comments",
        )?;

        // task_external_ids
        tx.execute_batch(
            "INSERT OR IGNORE INTO task_external_ids (task_id, source, external_id, imported_at, external_url)
             SELECT task_id, source, external_id, imported_at, external_url FROM src.task_external_ids",
        )?;

        // ── Records ────────────────────────────────────────────────────────

        let record_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM src.records",
            [],
            |row| row.get(0),
        )?;

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

        // record_tags
        tx.execute_batch(
            "INSERT OR IGNORE INTO record_tags (record_id, tag)
             SELECT record_id, tag FROM src.record_tags",
        )?;

        // record_links
        tx.execute_batch(
            "INSERT OR IGNORE INTO record_links (record_id, task_id, chunk_id, created_at)
             SELECT record_id, task_id, chunk_id, created_at FROM src.record_links",
        )?;

        // record_events
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

/// Check for task_id collisions between the attached source DB (`src`) and
/// the unified DB. Requires `ATTACH DATABASE ... AS src` to have been called.
///
/// Aborts with an error if any task_id in `src.tasks` already exists in
/// the unified `tasks` table under a different brain_id.
fn check_task_id_collisions(
    conn: &Connection,
    brain_id: &str,
    name: &str,
) -> Result<()> {
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
        bail!(
            "task_id collision: '{}' from brain '{}' ({}) already exists in unified DB under a different brain. \
             Aborting — no partial state written.",
            task_id,
            name,
            brain_id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn setup_v18_db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS tasks (
                 task_id        TEXT PRIMARY KEY,
                 title          TEXT NOT NULL,
                 description    TEXT,
                 status         TEXT NOT NULL DEFAULT 'open',
                 priority       INTEGER NOT NULL DEFAULT 4,
                 blocked_reason TEXT,
                 due_ts         INTEGER,
                 task_type      TEXT NOT NULL DEFAULT 'task',
                 assignee       TEXT,
                 defer_until    INTEGER,
                 parent_task_id TEXT,
                 child_seq      INTEGER,
                 created_at     INTEGER NOT NULL,
                 updated_at     INTEGER NOT NULL,
                 brain_id       TEXT NOT NULL DEFAULT ''
             );
             CREATE TABLE IF NOT EXISTS task_deps (task_id TEXT NOT NULL, depends_on TEXT NOT NULL, PRIMARY KEY(task_id, depends_on));
             CREATE TABLE IF NOT EXISTS task_labels (task_id TEXT NOT NULL, label TEXT NOT NULL, PRIMARY KEY(task_id, label));
             CREATE TABLE IF NOT EXISTS task_comments (comment_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, author TEXT NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS task_external_ids (task_id TEXT NOT NULL, source TEXT NOT NULL, external_id TEXT NOT NULL, imported_at INTEGER, external_url TEXT, PRIMARY KEY(task_id, source, external_id));
             CREATE TABLE IF NOT EXISTS records (
                 record_id         TEXT PRIMARY KEY,
                 title             TEXT NOT NULL,
                 kind              TEXT NOT NULL,
                 status            TEXT NOT NULL DEFAULT 'active',
                 description       TEXT,
                 content_hash      TEXT NOT NULL,
                 content_size      INTEGER NOT NULL,
                 media_type        TEXT,
                 task_id           TEXT,
                 actor             TEXT NOT NULL,
                 created_at        INTEGER NOT NULL,
                 updated_at        INTEGER NOT NULL,
                 retention_class   TEXT,
                 pinned            INTEGER NOT NULL DEFAULT 0,
                 payload_available INTEGER NOT NULL DEFAULT 1,
                 content_encoding  TEXT NOT NULL DEFAULT 'identity',
                 original_size     INTEGER,
                 brain_id          TEXT NOT NULL DEFAULT ''
             );
             CREATE TABLE IF NOT EXISTS record_tags (record_id TEXT NOT NULL, tag TEXT NOT NULL, PRIMARY KEY(record_id, tag));
             CREATE TABLE IF NOT EXISTS record_links (record_id TEXT NOT NULL, task_id TEXT, chunk_id TEXT, created_at INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL,
                 brain_id   TEXT NOT NULL DEFAULT ''
             );
             PRAGMA user_version = 18;",
        )
        .unwrap();
        conn
    }

    fn insert_test_task(conn: &Connection, task_id: &str) {
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES (?1, 'Test', 0, 0)",
            rusqlite::params![task_id],
        )
        .unwrap();
    }

    fn insert_test_record(conn: &Connection, record_id: &str) {
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES (?1, 'Test', 'snapshot', 'abc', 0, 'agent', 0, 0)",
            rusqlite::params![record_id],
        )
        .unwrap();
    }

    #[test]
    fn test_migrate_one_brain_basic() {
        let tmp = TempDir::new().unwrap();
        let unified = tmp.path().join("brain.db");
        let source = tmp.path().join("source.db");

        {
            let u = setup_v18_db(&unified);
            drop(u);
        }
        {
            let s = setup_v18_db(&source);
            insert_test_task(&s, "BRN-T1");
            insert_test_record(&s, "REC-R1");
            drop(s);
        }

        migrate_one_brain(&unified, &source, "brain1id", "testbrain").unwrap();

        let conn = Connection::open(&unified).unwrap();
        let brain_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM brains WHERE brain_id = 'brain1id'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(brain_count, 1);

        let task_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks WHERE brain_id = 'brain1id'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(task_count, 1);

        let rec_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM records WHERE brain_id = 'brain1id'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rec_count, 1);
    }

    #[test]
    fn test_migrate_idempotent() {
        let tmp = TempDir::new().unwrap();
        let unified = tmp.path().join("brain.db");
        let source = tmp.path().join("source.db");

        {
            let u = setup_v18_db(&unified);
            drop(u);
        }
        {
            let s = setup_v18_db(&source);
            insert_test_task(&s, "BRN-T2");
            drop(s);
        }

        migrate_one_brain(&unified, &source, "brain2id", "brain2").unwrap();
        // Second run must not fail.
        migrate_one_brain(&unified, &source, "brain2id", "brain2").unwrap();

        let conn = Connection::open(&unified).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM brains WHERE brain_id = 'brain2id'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "brain should appear exactly once");
    }

    #[test]
    fn test_migrate_collision_aborts() {
        let tmp = TempDir::new().unwrap();
        let unified = tmp.path().join("brain.db");
        let source1 = tmp.path().join("source1.db");
        let source2 = tmp.path().join("source2.db");

        {
            let u = setup_v18_db(&unified);
            drop(u);
        }
        // Both sources have the same task_id.
        {
            let s = setup_v18_db(&source1);
            insert_test_task(&s, "COLLISION-T1");
            drop(s);
        }
        {
            let s = setup_v18_db(&source2);
            insert_test_task(&s, "COLLISION-T1");
            drop(s);
        }

        migrate_one_brain(&unified, &source1, "bid1", "brain-a").unwrap();
        let result = migrate_one_brain(&unified, &source2, "bid2", "brain-b");
        assert!(result.is_err(), "collision should abort migration");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("COLLISION-T1"), "error should mention the colliding task_id");
    }
}
