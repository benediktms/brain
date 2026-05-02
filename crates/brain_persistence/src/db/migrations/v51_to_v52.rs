//! v51 → v52: add `updated_at` column to `task_comments`.
//!
//! Idempotent: safe to re-run after partial failure or when the column
//! already exists (e.g., after a rebuild that included the column).

use rusqlite::Connection;

use crate::error::Result;

pub fn migrate_v51_to_v52(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    // Idempotent: only add if the column does not already exist.
    // Uses pragma_table_xinfo (not pragma_table_info) so we see columns
    // added via ALTER TABLE in prior partial runs.
    let column_exists: bool = conn
        .query_row(
            "SELECT 1 FROM pragma_table_xinfo('task_comments') WHERE name = 'updated_at' LIMIT 1",
            [],
            |_row| Ok(true),
        )
        .ok()
        .unwrap_or(false);

    if !column_exists {
        tx.execute(
            "ALTER TABLE task_comments ADD COLUMN updated_at INTEGER",
            [],
        )?;

        // Backfill existing rows: set updated_at = created_at
        tx.execute(
            "UPDATE task_comments SET updated_at = created_at WHERE updated_at IS NULL",
            [],
        )?;
    }

    conn.pragma_update(None, "user_version", 52i32)?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn minimal_v51_schema(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE tasks (
                 task_id        TEXT PRIMARY KEY,
                 parent_task_id TEXT
             );
             CREATE TABLE task_comments (
                 comment_id TEXT PRIMARY KEY,
                 task_id    TEXT NOT NULL,
                 author     TEXT NOT NULL,
                 body       TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             PRAGMA user_version = 51;",
        )
        .unwrap();
    }

    #[test]
    fn test_adds_updated_at_column() {
        let conn = Connection::open_in_memory().unwrap();
        minimal_v51_schema(&conn);

        migrate_v51_to_v52(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 52);

        // Column exists and backfill worked
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_comments WHERE updated_at = created_at",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0); // no rows in empty schema
    }

    #[test]
    fn test_backfills_existing_comments() {
        let conn = Connection::open_in_memory().unwrap();
        minimal_v51_schema(&conn);

        conn.execute(
            "INSERT INTO task_comments (comment_id, task_id, author, body, created_at) \
             VALUES ('c1', 't1', 'alice', 'hello', 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_comments (comment_id, task_id, author, body, created_at) \
             VALUES ('c2', 't1', 'bob', 'world', 2000)",
            [],
        )
        .unwrap();

        migrate_v51_to_v52(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_comments WHERE updated_at = created_at",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        minimal_v51_schema(&conn);
        migrate_v51_to_v52(&conn).unwrap();

        conn.pragma_update(None, "user_version", 51i32).unwrap();
        migrate_v51_to_v52(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 52);
    }
}
