use rusqlite::Connection;

use super::migrations::{
    migrate_v0_to_v1, migrate_v1_to_v2, migrate_v2_to_v3, migrate_v3_to_v4, migrate_v4_to_v5,
};
use crate::error::{BrainCoreError, Result};

/// Bump this when the schema changes after release.
/// Each bump requires a corresponding `migrate_vN_to_vN+1` function.
const SCHEMA_VERSION: i32 = 5;

/// Initialize the database schema: WAL mode, foreign keys, and all tables.
///
/// Uses a version-aware migration dispatch loop so that each migration
/// stamps its own version inside a transaction. This prevents the bug
/// where bumping `SCHEMA_VERSION` would silently stamp a new version
/// without running any migration DDL.
pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    let current: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;

    if current > SCHEMA_VERSION {
        return Err(BrainCoreError::SchemaVersion(format!(
            "database schema version {current} is newer than supported version {SCHEMA_VERSION}"
        )));
    }

    if current < SCHEMA_VERSION {
        run_migrations(conn, current)?;
    }

    // Always ensure FTS5 + triggers exist (idempotent, handles partial init)
    ensure_fts5(conn)?;

    Ok(())
}

/// Run migrations sequentially from `from_version` up to `SCHEMA_VERSION`.
fn run_migrations(conn: &Connection, from_version: i32) -> Result<()> {
    let mut version = from_version;
    while version < SCHEMA_VERSION {
        match version {
            0 => migrate_v0_to_v1(conn)?,
            1 => migrate_v1_to_v2(conn)?,
            2 => migrate_v2_to_v3(conn)?,
            3 => migrate_v3_to_v4(conn)?,
            4 => migrate_v4_to_v5(conn)?,
            other => {
                return Err(BrainCoreError::SchemaVersion(format!(
                    "no migration defined from version {other} to {}",
                    other + 1
                )));
            }
        }
        version += 1;
    }
    Ok(())
}

/// Ensure FTS5 virtual table and sync triggers exist (idempotent).
///
/// Called on every `init_schema` open, outside the migration transaction,
/// because FTS5 DDL has SQLite transaction limitations.
fn ensure_fts5(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_chunks USING fts5(
            content,
            content=chunks,
            content_rowid=rowid
        )",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS chunks_fts_insert AFTER INSERT ON chunks BEGIN
            INSERT INTO fts_chunks(rowid, content) VALUES (new.rowid, new.content);
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS chunks_fts_delete AFTER DELETE ON chunks BEGIN
            INSERT INTO fts_chunks(fts_chunks, rowid, content) VALUES('delete', old.rowid, old.content);
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS chunks_fts_update AFTER UPDATE OF content ON chunks BEGIN
            INSERT INTO fts_chunks(fts_chunks, rowid, content) VALUES('delete', old.rowid, old.content);
            INSERT INTO fts_chunks(rowid, content) VALUES (new.rowid, new.content);
        END",
        [],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_init_schema_creates_tables() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Verify WAL mode
        let journal_mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert!(
            journal_mode == "wal" || journal_mode == "memory",
            "expected wal or memory, got {journal_mode}"
        );

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"files".to_string()));
        assert!(tables.contains(&"chunks".to_string()));
        assert!(tables.contains(&"links".to_string()));
        assert!(tables.contains(&"summaries".to_string()));
        assert!(tables.contains(&"reflection_sources".to_string()));
    }

    #[test]
    fn test_init_schema_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        init_schema(&conn).unwrap(); // second call should not fail
    }

    #[test]
    fn test_fts5_table_exists() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_chunks'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count > 0, "fts_chunks table should exist");
    }

    #[test]
    fn test_fts5_triggers_exist() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let triggers: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'chunks_fts_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(
            triggers,
            vec![
                "chunks_fts_delete",
                "chunks_fts_insert",
                "chunks_fts_update"
            ]
        );
    }

    #[test]
    fn test_fts5_sync_with_chunks() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/test.md', 'idle')",
            [],
        )
        .unwrap();

        // Insert a chunk with content — trigger should populate FTS5
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:0', 'f1', 0, 'hash0', 'hello world full text search')",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Delete the chunk — FTS5 should be cleaned up
        conn.execute("DELETE FROM chunks WHERE chunk_id = 'f1:0'", [])
            .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_summaries_table_constraints() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Valid episode without file_id
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s1', 'episode', 'test content', 1000, 1000)",
            [],
        )
        .unwrap();

        // Invalid kind should fail
        let result = conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s2', 'invalid', 'content', 1000, 1000)",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_fresh_db_migrates_from_v0() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn test_already_current_is_noop() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Count objects before second init
        let count_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();

        init_schema(&conn).unwrap();

        let count_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count_before, count_after);

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn test_future_version_rejected() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();

        let result = init_schema(&conn);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("newer"),
            "error should mention 'newer', got: {err_msg}"
        );
    }

    #[test]
    fn test_version_not_stamped_without_migration() {
        let conn = Connection::open_in_memory().unwrap();

        // Bootstrap a real v1 database first
        init_schema(&conn).unwrap();

        // Simulate a hypothetical SCHEMA_VERSION bump by setting a future
        // version that no migration handles. If init_schema unconditionally
        // stamped the version, it would silently overwrite this.
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 99)
            .unwrap();

        // Re-opening should reject the future version, NOT silently stamp it
        let result = init_schema(&conn);
        assert!(result.is_err());

        // Version must remain untouched
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION + 99);
    }
}
