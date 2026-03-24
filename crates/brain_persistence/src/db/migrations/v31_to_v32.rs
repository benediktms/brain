use rusqlite::Connection;

use crate::error::Result;

/// v31 → v32: Rename `id` column to `display_id` for clarity.
///
/// The `id` column (added in v31) stores a BLAKE3 hash-based short display ID.
/// The name `id` is ambiguous with `task_id` (the ULID primary key). Renaming
/// to `display_id` clarifies intent. The JSON wire format keeps `"id"` as the
/// key via serde rename — no API break.
pub fn migrate_v31_to_v32(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "ALTER TABLE tasks RENAME COLUMN id TO display_id;
         DROP INDEX IF EXISTS idx_tasks_brain_short_id;
         CREATE UNIQUE INDEX idx_tasks_brain_display_id ON tasks(brain_id, display_id);
         PRAGMA user_version = 32;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v31 database by running v30→v31 on a minimal v30 schema.
    fn setup_v31(conn: &Connection) {
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
                 updated_at     INTEGER NOT NULL,
                 id             TEXT
             );

             CREATE UNIQUE INDEX idx_tasks_brain_short_id ON tasks(brain_id, id);
             PRAGMA user_version = 31;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 32);
    }

    #[test]
    fn test_column_renamed() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);

        // Insert a task before migration
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, id, created_at, updated_at)
             VALUES ('T1', 'b1', 'Test', 'open', 1, 'abc', 1000, 1000)",
            [],
        )
        .unwrap();

        migrate_v31_to_v32(&conn).unwrap();

        // Old column name should fail
        let err = conn.query_row("SELECT id FROM tasks LIMIT 1", [], |_| Ok(()));
        assert!(err.is_err());

        // New column name should work and preserve data
        let val: String = conn
            .query_row("SELECT display_id FROM tasks LIMIT 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "abc");
    }

    #[test]
    fn test_new_index_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        let new_idx: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_tasks_brain_display_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(new_idx, 1);

        let old_idx: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_tasks_brain_short_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(old_idx, 0);
    }
}
