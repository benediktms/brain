use std::collections::{HashMap, HashSet};

use rusqlite::Connection;

use crate::db::tasks::display_id::pick_unique_prefix;
use crate::db::tasks::queries::blake3_short_hex;
use crate::error::Result;

/// v30 → v31: Add `id` column to tasks for stable, hash-based display IDs.
///
/// ULIDs are time-based, so tasks created in quick succession share long common
/// prefixes. This makes compact ULID-prefix display IDs fragile — they become
/// ambiguous as new tasks appear. The `id` column stores a BLAKE3 hash of the
/// full `task_id`, truncated to the shortest unique hex string (min 3 chars)
/// within each brain.
///
/// The column stores only the hex portion (e.g., `a3f`). The project prefix is
/// applied at runtime during display: `{prefix_lower}-{id}` (e.g., `brn-a3f`).
///
/// NOTE: JSONL event patching (injecting `id` into existing TaskCreated payloads)
/// is handled separately in the TaskStore initialization, since this migration
/// only receives a `&Connection` and has no access to filesystem paths.
pub fn migrate_v30_to_v31(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "ALTER TABLE tasks ADD COLUMN id TEXT;
         CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_brain_short_id ON tasks(brain_id, id);",
    )?;

    // Backfill: group by brain_id, compute shortest unique hash per brain
    let mut stmt =
        conn.prepare("SELECT task_id, brain_id FROM tasks ORDER BY brain_id, task_id")?;
    let rows: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if !rows.is_empty() {
        // Group by brain_id
        let mut by_brain: HashMap<String, Vec<String>> = HashMap::new();
        for (task_id, brain_id) in &rows {
            by_brain
                .entry(brain_id.clone())
                .or_default()
                .push(task_id.clone());
        }

        let mut update_stmt = conn.prepare("UPDATE tasks SET id = ?1 WHERE task_id = ?2")?;

        for task_ids in by_brain.values() {
            let mut used: HashSet<String> = HashSet::new();

            for task_id in task_ids {
                let full_hex = blake3_short_hex(task_id);
                let hash = pick_unique_prefix(&full_hex, &used);
                update_stmt.execute(rusqlite::params![hash, task_id])?;
                used.insert(hash);
            }
        }
    }

    conn.execute_batch("PRAGMA user_version = 31;")?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;

    fn setup_v30(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 prefix     TEXT,
                 created_at INTEGER NOT NULL,
                 archived   INTEGER NOT NULL DEFAULT 0,
                 roots      TEXT,
                 aliases    TEXT,
                 notes      TEXT,
                 projected  INTEGER NOT NULL DEFAULT 0
             );

             CREATE TABLE tasks (
                 task_id        TEXT PRIMARY KEY,
                 brain_id       TEXT NOT NULL DEFAULT '',
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
                 updated_at     INTEGER NOT NULL
             );

             PRAGMA user_version = 30;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 31);
    }

    #[test]
    fn test_id_column_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let result: Option<String> = conn
            .query_row("SELECT id FROM tasks LIMIT 1", [], |row| row.get(0))
            .optional()
            .unwrap()
            .flatten();
        assert_eq!(result, None);
    }

    #[test]
    fn test_unique_index_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_tasks_brain_short_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_backfill_existing_tasks() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);

        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at)
             VALUES ('BRN-01KMAXNT4Y4FJD1N8QF4MN5VE9', 'brain-1', 'Task one', 'open', 1, 1000, 1000)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at)
             VALUES ('BRN-01KMAXNT4Y4FJD1N8QF4MN5VEA', 'brain-1', 'Task two', 'open', 2, 1001, 1001)",
            [],
        ).unwrap();

        migrate_v30_to_v31(&conn).unwrap();

        let ids: Vec<String> = conn
            .prepare("SELECT id FROM tasks ORDER BY task_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert!(ids[0].len() >= MIN_SHORT_HASH_LEN);
        assert!(ids[1].len() >= MIN_SHORT_HASH_LEN);
    }

    #[test]
    fn test_cross_brain_isolation() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);

        // Two tasks in different brains with different task_ids.
        // The UNIQUE index on (brain_id, id) allows same hash across brains.
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at)
             VALUES ('AAA-01XYZABC', 'brain-a', 'Task A', 'open', 1, 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at)
             VALUES ('BBB-01XYZDEF', 'brain-b', 'Task B', 'open', 1, 1000, 1000)",
            [],
        )
        .unwrap();

        migrate_v30_to_v31(&conn).unwrap();

        // Both should have hash-based IDs, each unique within their brain
        let ids: Vec<(String, String)> = conn
            .prepare("SELECT brain_id, id FROM tasks ORDER BY brain_id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(ids.len(), 2);
        assert!(ids[0].1.len() >= MIN_SHORT_HASH_LEN);
        assert!(ids[1].1.len() >= MIN_SHORT_HASH_LEN);
    }

    #[test]
    fn test_pick_unique_prefix_no_collision() {
        let used = HashSet::new();
        let hex = "abcdef1234567890abcdef1234567890";
        let result = pick_unique_prefix(hex, &used);
        assert_eq!(result, "abc");
    }

    #[test]
    fn test_pick_unique_prefix_with_collision() {
        let mut used = HashSet::new();
        used.insert("abc".to_string());
        let hex = "abcdef1234567890abcdef1234567890";
        let result = pick_unique_prefix(hex, &used);
        assert_eq!(result, "abcd");
    }
}
