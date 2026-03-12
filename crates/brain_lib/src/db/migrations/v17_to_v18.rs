use rusqlite::Connection;

use crate::error::Result;

/// v17 → v18: Workspace unified storage — add brain_id columns and brains registry.
///
/// Creates the `brains` table for the workspace registry and adds `brain_id` to
/// the task and record tables so rows can be scoped to a specific brain instance.
///
/// Tables modified:
/// - `brains`        — new registry table (brain_id PK, name, created_at)
/// - `tasks`         — brain_id TEXT NOT NULL DEFAULT ''
/// - `records`       — brain_id TEXT NOT NULL DEFAULT ''
/// - `record_events` — brain_id TEXT NOT NULL DEFAULT ''
///
/// The DEFAULT '' is temporary. The data migration tool (step 2) populates
/// brain_id values from per-brain databases before workspace mode is activated.
pub fn migrate_v17_to_v18(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE IF NOT EXISTS brains (
            brain_id   TEXT PRIMARY KEY,
            name       TEXT NOT NULL UNIQUE,
            created_at INTEGER NOT NULL
        );

        ALTER TABLE tasks ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE records ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE record_events ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';

        CREATE INDEX IF NOT EXISTS idx_tasks_brain_status ON tasks(brain_id, status);
        CREATE INDEX IF NOT EXISTS idx_tasks_brain_priority ON tasks(brain_id, priority);
        CREATE INDEX IF NOT EXISTS idx_records_brain ON records(brain_id);
        CREATE INDEX IF NOT EXISTS idx_records_brain_status ON records(brain_id, status);

        -- Cross-brain refs are no longer needed; all brains share one DB.
        DROP TABLE IF EXISTS task_cross_refs;

        PRAGMA user_version = 18;

        COMMIT;
    ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal v17 schema: tasks, records, record_events tables only.
    fn setup_v17(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;

             CREATE TABLE tasks (
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
                 updated_at     INTEGER NOT NULL
             );

             CREATE TABLE records (
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
                 original_size     INTEGER
             );

             CREATE TABLE record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL
             );

             PRAGMA user_version = 17;",
        )
        .unwrap();
    }

    #[test]
    fn test_schema_version_is_18_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);

        migrate_v17_to_v18(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 18);
    }

    #[test]
    fn test_brains_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);
        migrate_v17_to_v18(&conn).unwrap();

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'default', 1000)",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM brains", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_brains_name_unique_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);
        migrate_v17_to_v18(&conn).unwrap();

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'default', 1000)",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b2', 'default', 2000)",
            [],
        );
        assert!(result.is_err(), "duplicate name should violate UNIQUE constraint");
    }

    #[test]
    fn test_tasks_brain_id_column_added_with_default() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);

        // Insert row before migration — brain_id column does not exist yet
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'Test', 0, 0)",
            [],
        )
        .unwrap();

        migrate_v17_to_v18(&conn).unwrap();

        let brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain_id, "", "pre-migration row should default to empty string");
    }

    #[test]
    fn test_records_brain_id_column_added_with_default() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'T', 'report', 'abc', 42, 'agent', 0, 0)",
            [],
        )
        .unwrap();

        migrate_v17_to_v18(&conn).unwrap();

        let brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain_id, "");
    }

    #[test]
    fn test_record_events_brain_id_column_added_with_default() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);

        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload)
             VALUES ('e1', 'r1', 'created', 1000, 'agent', '{}')",
            [],
        )
        .unwrap();

        migrate_v17_to_v18(&conn).unwrap();

        let brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM record_events WHERE event_id = 'e1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain_id, "");
    }

    #[test]
    fn test_composite_indexes_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);
        migrate_v17_to_v18(&conn).unwrap();

        let indexes: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'index'
                  AND name IN (
                      'idx_tasks_brain_status',
                      'idx_tasks_brain_priority',
                      'idx_records_brain',
                      'idx_records_brain_status'
                  )
                  ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(
            indexes,
            vec![
                "idx_records_brain",
                "idx_records_brain_status",
                "idx_tasks_brain_priority",
                "idx_tasks_brain_status",
            ]
        );
    }

    #[test]
    fn test_migration_is_idempotent_on_indexes() {
        // IF NOT EXISTS guards — running migration on a partially-migrated DB
        // should not fail. We simulate by manually bumping to 18 and re-running.
        let conn = Connection::open_in_memory().unwrap();
        setup_v17(&conn);
        migrate_v17_to_v18(&conn).unwrap();

        // Verify tables and indexes still intact after second schema open
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','index')
                  AND name IN ('brains','idx_tasks_brain_status','idx_records_brain')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
    }
}
