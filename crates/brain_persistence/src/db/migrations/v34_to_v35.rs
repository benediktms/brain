use rusqlite::Connection;

use crate::error::Result;

/// Migration v34 → v35: source lineage + staleness detection.
///
/// 1. `summary_sources` — general-purpose join table linking any summary to
///    its source records (chunks, episodes, or other summaries). Enables
///    reverse lookup: "this chunk changed → which summaries are affected?"
///
/// 2. `summaries.consolidated_by` — tracks which reflection an episode was
///    rolled into, preventing re-consolidation of already-processed episodes.
///
/// 3. `derived_summaries.source_content_hash` — blake3 hash of source content
///    at generation time. Enables cheap "has content actually changed?" check
///    during the stale sweep, avoiding unnecessary LLM calls.
pub fn migrate_v34_to_v35(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;
        BEGIN;

        CREATE TABLE IF NOT EXISTS summary_sources (
            summary_id   TEXT NOT NULL,
            source_id    TEXT NOT NULL,
            source_type  TEXT NOT NULL CHECK(source_type IN ('chunk', 'episode', 'scope')),
            created_at   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (summary_id, source_id)
        );
        CREATE INDEX IF NOT EXISTS idx_summary_sources_source
            ON summary_sources(source_id);

        ALTER TABLE summaries ADD COLUMN consolidated_by TEXT DEFAULT NULL;

        ALTER TABLE derived_summaries ADD COLUMN source_content_hash TEXT DEFAULT NULL;

        COMMIT;
        PRAGMA foreign_keys = ON;
        PRAGMA user_version = 35;
        ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::db::migrations::{migrate_v0_to_v1, migrate_v29_to_v30};

    fn setup_v34() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        // Need base tables for ALTER TABLE to work.
        migrate_v0_to_v1(&conn).unwrap();
        migrate_v29_to_v30(&conn).unwrap();
        conn.execute_batch("PRAGMA user_version = 34;").unwrap();
        conn
    }

    #[test]
    fn test_migration_stamps_version_35() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 35);
    }

    #[test]
    fn test_summary_sources_table_exists() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='summary_sources'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_summary_sources_composite_pk() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();

        conn.execute(
            "INSERT INTO summary_sources (summary_id, source_id, source_type, created_at)
             VALUES ('sum-1', 'chunk-a', 'chunk', 1000)",
            [],
        )
        .unwrap();

        // Duplicate (summary_id, source_id) should fail.
        let result = conn.execute(
            "INSERT INTO summary_sources (summary_id, source_id, source_type, created_at)
             VALUES ('sum-1', 'chunk-a', 'chunk', 2000)",
            [],
        );
        assert!(result.is_err(), "duplicate PK should fail");
    }

    #[test]
    fn test_source_type_check_constraint() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO summary_sources (summary_id, source_id, source_type, created_at)
             VALUES ('sum-1', 'src-1', 'invalid', 1000)",
            [],
        );
        assert!(result.is_err(), "invalid source_type should fail CHECK");
    }

    #[test]
    fn test_consolidated_by_column_exists() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();

        // Insert an episode and verify consolidated_by defaults to NULL.
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('ep-1', 'episode', 'test', 1000, 1000)",
            [],
        )
        .unwrap();

        let val: Option<String> = conn
            .query_row(
                "SELECT consolidated_by FROM summaries WHERE summary_id = 'ep-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn test_source_content_hash_column_exists() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();

        conn.execute(
            "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
             VALUES ('ds-1', 'directory', '/src', '', 0, 1000)",
            [],
        )
        .unwrap();

        let val: Option<String> = conn
            .query_row(
                "SELECT source_content_hash FROM derived_summaries WHERE id = 'ds-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn test_reverse_lookup_index() {
        let conn = setup_v34();
        migrate_v34_to_v35(&conn).unwrap();

        // Insert multiple summaries referencing the same source.
        conn.execute_batch(
            "INSERT INTO summary_sources VALUES ('sum-1', 'chunk-x', 'chunk', 1000);
             INSERT INTO summary_sources VALUES ('sum-2', 'chunk-x', 'chunk', 1000);
             INSERT INTO summary_sources VALUES ('sum-3', 'chunk-y', 'chunk', 1000);",
        )
        .unwrap();

        // Reverse lookup: which summaries reference chunk-x?
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summary_sources WHERE source_id = 'chunk-x'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }
}
