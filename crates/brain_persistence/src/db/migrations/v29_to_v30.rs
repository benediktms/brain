use rusqlite::Connection;

use crate::error::Result;

/// v29 → v30: Add `derived_summaries` table for directory and tag scope aggregation.
///
/// New table:
/// - `derived_summaries` — stores extractive summaries for directory or tag scopes.
///   Each row represents a cached aggregation of chunk content for a given scope.
///
/// Schema:
/// ```sql
/// CREATE TABLE derived_summaries (
///     id           TEXT    PRIMARY KEY,
///     scope_type   TEXT    NOT NULL CHECK(scope_type IN ('directory', 'tag')),
///     scope_value  TEXT    NOT NULL,
///     content      TEXT    NOT NULL DEFAULT '',
///     stale        INTEGER NOT NULL DEFAULT 0,
///     generated_at INTEGER NOT NULL DEFAULT 0,
///     UNIQUE(scope_type, scope_value)
/// );
/// CREATE INDEX idx_derived_scope ON derived_summaries(scope_type, scope_value);
/// ```
pub fn migrate_v29_to_v30(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS derived_summaries (
             id           TEXT    PRIMARY KEY,
             scope_type   TEXT    NOT NULL
                                  CHECK(scope_type IN ('directory', 'tag')),
             scope_value  TEXT    NOT NULL,
             content      TEXT    NOT NULL DEFAULT '',
             stale        INTEGER NOT NULL DEFAULT 0,
             generated_at INTEGER NOT NULL DEFAULT 0,
             UNIQUE(scope_type, scope_value)
         );
         CREATE INDEX IF NOT EXISTS idx_derived_scope ON derived_summaries(scope_type, scope_value);
         PRAGMA user_version = 30;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v29(conn: &Connection) {
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

             PRAGMA user_version = 29;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v29(&conn);
        migrate_v29_to_v30(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 30);
    }

    #[test]
    fn test_derived_summaries_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v29(&conn);
        migrate_v29_to_v30(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='derived_summaries'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "derived_summaries table should exist");
    }

    #[test]
    fn test_scope_index_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v29(&conn);
        migrate_v29_to_v30(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_derived_scope'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_derived_scope index should exist");
    }

    #[test]
    fn test_insert_and_query_row() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v29(&conn);
        migrate_v29_to_v30(&conn).unwrap();

        conn.execute(
            "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
             VALUES ('test-id-001', 'directory', 'src/auth/', 'Auth summary content', 0, 1000)",
            [],
        )
        .unwrap();

        let content: String = conn
            .query_row(
                "SELECT content FROM derived_summaries WHERE scope_value = 'src/auth/'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(content, "Auth summary content");
    }

    #[test]
    fn test_unique_constraint_on_scope() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v29(&conn);
        migrate_v29_to_v30(&conn).unwrap();

        conn.execute(
            "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
             VALUES ('id-001', 'directory', 'src/auth/', 'first', 0, 1000)",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
             VALUES ('id-002', 'directory', 'src/auth/', 'second', 0, 2000)",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate (scope_type, scope_value) should fail"
        );
    }
}
