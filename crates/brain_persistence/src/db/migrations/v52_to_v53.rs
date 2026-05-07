//! v52 → v53: introduce saga tables (`sagas`, `saga_tasks`, `saga_events`).

use rusqlite::Connection;

use crate::error::Result;

pub fn migrate_v52_to_v53(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sagas (
            saga_id     TEXT PRIMARY KEY,
            title       TEXT NOT NULL,
            description TEXT,
            status      TEXT NOT NULL DEFAULT 'planning'
                CHECK (status IN ('planning','open','closed','cancelled')),
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL,
            closed_at   INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_sagas_status ON sagas(status);

        CREATE TABLE IF NOT EXISTS saga_tasks (
            saga_id   TEXT NOT NULL,
            task_id   TEXT NOT NULL,
            added_at  INTEGER NOT NULL,
            PRIMARY KEY (saga_id, task_id),
            FOREIGN KEY (saga_id) REFERENCES sagas(saga_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_saga_tasks_task_id ON saga_tasks(task_id);

        CREATE TABLE IF NOT EXISTS saga_events (
            event_id    TEXT PRIMARY KEY,
            saga_id     TEXT NOT NULL,
            event_type  TEXT NOT NULL
                CHECK (event_type IN (
                    'saga_created','saga_updated','saga_started','saga_closed',
                    'saga_cancelled','saga_reopened','saga_task_added',
                    'saga_task_removed','saga_task_cascaded'
                )),
            timestamp   INTEGER NOT NULL,
            actor       TEXT NOT NULL,
            payload     TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_saga_events_saga_id ON saga_events(saga_id);
        ",
    )?;

    tx.pragma_update(None, "user_version", 53i32)?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn fresh_v52(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA user_version = 52;",
        )
        .unwrap();
    }

    #[test]
    fn test_migration_creates_tables_and_indexes() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v52(&conn);

        migrate_v52_to_v53(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 53);

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(tables.contains(&"sagas".to_string()));
        assert!(tables.contains(&"saga_tasks".to_string()));
        assert!(tables.contains(&"saga_events".to_string()));

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(indexes.contains(&"idx_sagas_status".to_string()));
        assert!(indexes.contains(&"idx_saga_tasks_task_id".to_string()));
        assert!(indexes.contains(&"idx_saga_events_saga_id".to_string()));
    }

    #[test]
    fn test_saga_tasks_fk_cascade_on_saga_delete() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        fresh_v52(&conn);
        migrate_v52_to_v53(&conn).unwrap();

        conn.execute(
            "INSERT INTO sagas (saga_id, title, status, created_at, updated_at)
             VALUES ('saga1', 'Test Saga', 'planning', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO saga_tasks (saga_id, task_id, added_at)
             VALUES ('saga1', 'task1', 1000)",
            [],
        )
        .unwrap();

        let count_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM saga_tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count_before, 1);

        conn.execute("DELETE FROM sagas WHERE saga_id = 'saga1'", [])
            .unwrap();

        let count_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM saga_tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count_after, 0, "saga_tasks should cascade-delete with saga");
    }

    #[test]
    fn test_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v52(&conn);
        migrate_v52_to_v53(&conn).unwrap();

        // Re-run: IF NOT EXISTS guards make it a no-op
        conn.pragma_update(None, "user_version", 52i32).unwrap();
        migrate_v52_to_v53(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 53);
    }

    #[test]
    fn test_sagas_default_status_is_planning() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v52(&conn);
        migrate_v52_to_v53(&conn).unwrap();

        conn.execute(
            "INSERT INTO sagas (saga_id, title, created_at, updated_at)
             VALUES ('s1', 'My Saga', 1000, 1000)",
            [],
        )
        .unwrap();

        let status: String = conn
            .query_row("SELECT status FROM sagas WHERE saga_id = 's1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "planning");
    }
}
