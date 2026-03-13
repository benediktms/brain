use rusqlite::Connection;

use crate::error::Result;

/// v20 → v21: Add `embedded_at` column to `tasks` and `chunks` tables.
///
/// Both columns are nullable `INTEGER` (Unix timestamp, seconds since epoch).
/// All existing rows start as NULL — the daemon will embed them on first run
/// after this migration.
pub fn migrate_v20_to_v21(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;

        BEGIN;

        ALTER TABLE tasks ADD COLUMN embedded_at INTEGER;
        ALTER TABLE chunks ADD COLUMN embedded_at INTEGER;

        PRAGMA user_version = 21;

        COMMIT;

        PRAGMA foreign_keys = ON;
    ",
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v20(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE files (
                 file_id         TEXT PRIMARY KEY,
                 path            TEXT UNIQUE NOT NULL,
                 content_hash    TEXT,
                 last_indexed_at INTEGER,
                 deleted_at      INTEGER,
                 indexing_state  TEXT NOT NULL DEFAULT 'idle'
                                 CHECK (indexing_state IN ('idle', 'indexing_started', 'indexed')),
                 chunker_version INTEGER,
                 pagerank_score  REAL
             );

             CREATE TABLE chunks (
                 chunk_id        TEXT PRIMARY KEY,
                 file_id         TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
                 chunk_ord       INTEGER NOT NULL,
                 chunk_hash      TEXT NOT NULL,
                 content         TEXT NOT NULL DEFAULT '',
                 chunker_version INTEGER NOT NULL DEFAULT 1,
                 heading_path    TEXT NOT NULL DEFAULT '',
                 byte_start      INTEGER NOT NULL DEFAULT 0,
                 byte_end        INTEGER NOT NULL DEFAULT 0,
                 token_estimate  INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON chunks(file_id);

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
                 updated_at     INTEGER NOT NULL,
                 brain_id       TEXT NOT NULL DEFAULT ''
             );

             PRAGMA user_version = 20;",
        )
        .unwrap();
    }

    #[test]
    fn test_columns_added() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v20(&conn);

        migrate_v20_to_v21(&conn).unwrap();

        // Version stamped
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 21);

        // tasks.embedded_at column exists and is NULL for existing rows
        let embedded_at: Option<i64> = conn
            .query_row("SELECT embedded_at FROM tasks WHERE 1=0", [], |row| {
                row.get(0)
            })
            .unwrap_or(None);
        // Column present — we can insert and read it
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T', 0, 0)",
            [],
        )
        .unwrap();
        let embedded_at: Option<i64> = conn
            .query_row(
                "SELECT embedded_at FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(embedded_at, None);

        // chunks.embedded_at column exists and is NULL for existing rows
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/a.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:0', 'f1', 0, 'h0', 'hello')",
            [],
        )
        .unwrap();
        let chunk_embedded_at: Option<i64> = conn
            .query_row(
                "SELECT embedded_at FROM chunks WHERE chunk_id = 'f1:0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chunk_embedded_at, None);
    }

    #[test]
    fn test_embedded_at_writable() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v20(&conn);
        migrate_v20_to_v21(&conn).unwrap();

        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at, embedded_at)
             VALUES ('t1', 'T', 0, 0, 1234567890)",
            [],
        )
        .unwrap();

        let ts: i64 = conn
            .query_row(
                "SELECT embedded_at FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ts, 1234567890);

        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/a.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, embedded_at)
             VALUES ('f1:0', 'f1', 0, 'h0', 'hello', 9876543210)",
            [],
        )
        .unwrap();

        let chunk_ts: i64 = conn
            .query_row(
                "SELECT embedded_at FROM chunks WHERE chunk_id = 'f1:0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chunk_ts, 9876543210);
    }

    #[test]
    fn test_existing_rows_have_null_embedded_at() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v20(&conn);

        // Insert rows before migration
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/a.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:0', 'f1', 0, 'h0', 'hello')",
            [],
        )
        .unwrap();

        migrate_v20_to_v21(&conn).unwrap();

        // Pre-migration rows must have NULL embedded_at
        let task_ea: Option<i64> = conn
            .query_row(
                "SELECT embedded_at FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_ea, None);

        let chunk_ea: Option<i64> = conn
            .query_row(
                "SELECT embedded_at FROM chunks WHERE chunk_id = 'f1:0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chunk_ea, None);
    }
}
