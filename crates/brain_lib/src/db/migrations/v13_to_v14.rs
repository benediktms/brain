use rusqlite::Connection;

use crate::error::Result;

/// v13 → v14: Add cross-brain task references table.
///
/// Creates `task_cross_refs` for advisory references to tasks in other brain
/// instances. These references are metadata-only — they do NOT affect readiness,
/// blocking, cycle detection, or priority sorting.
pub fn migrate_v13_to_v14(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE IF NOT EXISTS task_cross_refs (
            task_id      TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            brain_id     TEXT NOT NULL,
            remote_task  TEXT NOT NULL,
            ref_type     TEXT NOT NULL DEFAULT 'related'
                         CHECK(ref_type IN ('depends_on','blocks','related')),
            note         TEXT,
            created_at   INTEGER NOT NULL,
            PRIMARY KEY (task_id, brain_id, remote_task)
        );

        CREATE INDEX IF NOT EXISTS idx_cross_refs_brain
            ON task_cross_refs (brain_id, remote_task);

        PRAGMA user_version = 14;

        COMMIT;
    ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v13(conn: &Connection) {
        // Minimal v13 schema with just the tasks table needed for FK
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE tasks (
                 task_id      TEXT PRIMARY KEY,
                 title        TEXT NOT NULL,
                 description  TEXT,
                 status       TEXT NOT NULL DEFAULT 'open',
                 priority     INTEGER NOT NULL DEFAULT 4,
                 blocked_reason TEXT,
                 due_ts       INTEGER,
                 task_type    TEXT NOT NULL DEFAULT 'task',
                 assignee     TEXT,
                 defer_until  INTEGER,
                 parent_task_id TEXT,
                 child_seq    INTEGER,
                 created_at   INTEGER NOT NULL,
                 updated_at   INTEGER NOT NULL
             );
             PRAGMA user_version = 13;",
        )
        .unwrap();
    }

    #[test]
    fn test_migration_creates_table_and_index() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v13(&conn);

        migrate_v13_to_v14(&conn).unwrap();

        // Verify schema version
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 14);

        // Verify table exists by inserting a task and a cross ref
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'Test', 0, 0)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO task_cross_refs (task_id, brain_id, remote_task, ref_type, created_at)
             VALUES ('t1', 'abc12345', 'INF-01KK7', 'depends_on', 1000)",
            [],
        )
        .unwrap();

        // Verify the row was inserted
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM task_cross_refs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_check_constraint_rejects_invalid_ref_type() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v13(&conn);
        migrate_v13_to_v14(&conn).unwrap();

        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'Test', 0, 0)",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO task_cross_refs (task_id, brain_id, remote_task, ref_type, created_at)
             VALUES ('t1', 'abc12345', 'INF-01KK7', 'invalid_type', 1000)",
            [],
        );
        assert!(
            result.is_err(),
            "invalid ref_type should be rejected by CHECK constraint"
        );
    }

    #[test]
    fn test_cascade_delete_on_task_removal() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        setup_v13(&conn);
        migrate_v13_to_v14(&conn).unwrap();

        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'Test', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_cross_refs (task_id, brain_id, remote_task, ref_type, created_at)
             VALUES ('t1', 'abc12345', 'INF-01KK7', 'related', 1000)",
            [],
        )
        .unwrap();

        // Delete the parent task
        conn.execute("DELETE FROM tasks WHERE task_id = 't1'", [])
            .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM task_cross_refs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "cross refs should be cascade-deleted");
    }

    #[test]
    fn test_primary_key_prevents_duplicates() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v13(&conn);
        migrate_v13_to_v14(&conn).unwrap();

        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'Test', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_cross_refs (task_id, brain_id, remote_task, ref_type, created_at)
             VALUES ('t1', 'abc12345', 'INF-01KK7', 'related', 1000)",
            [],
        )
        .unwrap();

        // Same PK should fail
        let result = conn.execute(
            "INSERT INTO task_cross_refs (task_id, brain_id, remote_task, ref_type, created_at)
             VALUES ('t1', 'abc12345', 'INF-01KK7', 'depends_on', 2000)",
            [],
        );
        assert!(result.is_err(), "duplicate PK should be rejected");
    }
}
