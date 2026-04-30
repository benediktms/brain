//! v47 → v48: `pre_tool_use_seen` table for per-file-per-session throttle.
//!
//! ## Purpose
//!
//! The PreToolUse hook (`brain hooks pre-tool-use`) injects file-scoped memory
//! before Claude edits a file. To prevent redundant injections for the same
//! file within a single Claude Code session, we record `(session_id, file_path)`
//! pairs here. The check is a simple point-lookup on the composite primary key.
//!
//! ## Schema
//!
//! | Column | Type | Notes |
//! |---|---|---|
//! | `session_id` | TEXT NOT NULL | Claude Code session ID. |
//! | `file_path`  | TEXT NOT NULL | Absolute or relative file path. |
//! | `ts`         | INTEGER NOT NULL | Unix seconds (insertion time). |
//!
//! ## Retention
//!
//! Rows accumulate indefinitely. A future `brain doctor --prune-hook-throttle`
//! command may purge rows older than a configurable TTL, but that is not part
//! of this migration. The table is expected to stay small: one row per edited
//! file per session, and Claude Code sessions are short-lived.

use rusqlite::Connection;

use crate::error::Result;

/// Create the `pre_tool_use_seen` table and stamp version 48.
pub fn migrate_v47_to_v48(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS pre_tool_use_seen (
             session_id  TEXT    NOT NULL,
             file_path   TEXT    NOT NULL,
             ts          INTEGER NOT NULL,
             PRIMARY KEY (session_id, file_path)
         );

         CREATE INDEX IF NOT EXISTS idx_pre_tool_use_seen_session
             ON pre_tool_use_seen (session_id);",
    )?;

    tx.execute_batch("PRAGMA user_version = 48;")?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::v46_to_v47::migrate_v46_to_v47;
    use super::*;

    fn setup_v47() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        super::super::migrate_v0_to_v1(&conn).unwrap();
        // Apply migrations up to v47 using the migration harness approach.
        // We call the full sequence via schema::run_migrations for convenience.
        crate::db::schema::run_migrations(&conn, 0).unwrap();
        conn
    }

    #[test]
    fn table_created_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        // Fresh DB at v0, run all migrations up to v48.
        crate::db::schema::run_migrations(&conn, 0).unwrap();

        // Table must exist.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='pre_tool_use_seen'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "pre_tool_use_seen table must exist after migration"
        );
    }

    #[test]
    fn composite_pk_prevents_duplicates() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        crate::db::schema::run_migrations(&conn, 0).unwrap();

        let now = 1_700_000_000i64;
        conn.execute(
            "INSERT INTO pre_tool_use_seen (session_id, file_path, ts) VALUES (?1, ?2, ?3)",
            rusqlite::params!["sess-1", "/path/to/file.rs", now],
        )
        .unwrap();

        // Duplicate insert must fail with UNIQUE violation.
        let result = conn.execute(
            "INSERT INTO pre_tool_use_seen (session_id, file_path, ts) VALUES (?1, ?2, ?3)",
            rusqlite::params!["sess-1", "/path/to/file.rs", now + 1],
        );
        assert!(
            result.is_err(),
            "duplicate (session_id, file_path) must be rejected"
        );
    }

    #[test]
    fn different_session_same_file_allowed() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        crate::db::schema::run_migrations(&conn, 0).unwrap();

        let now = 1_700_000_000i64;
        conn.execute(
            "INSERT INTO pre_tool_use_seen (session_id, file_path, ts) VALUES (?1, ?2, ?3)",
            rusqlite::params!["sess-1", "/path/to/file.rs", now],
        )
        .unwrap();
        // Different session — must succeed.
        conn.execute(
            "INSERT INTO pre_tool_use_seen (session_id, file_path, ts) VALUES (?1, ?2, ?3)",
            rusqlite::params!["sess-2", "/path/to/file.rs", now],
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM pre_tool_use_seen", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn user_version_stamped_48() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        crate::db::schema::run_migrations(&conn, 0).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 48);
    }
}
