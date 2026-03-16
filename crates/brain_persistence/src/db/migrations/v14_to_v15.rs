use rusqlite::Connection;

use crate::error::Result;

/// v14 → v15: Add `summarizer` and `chunk_id` columns to the `summaries` table.
///
/// - `summarizer TEXT NOT NULL DEFAULT 'rule-based'`: tracks which backend produced
///   each summary for invalidation when the model changes.
/// - `chunk_id TEXT`: foreign key to the chunk that was summarized (for kind='summary').
/// - Partial unique index on `(chunk_id, summarizer) WHERE kind = 'summary'` to prevent
///   duplicate ML summaries per chunk per backend.
pub fn migrate_v14_to_v15(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute(
        "ALTER TABLE summaries ADD COLUMN summarizer TEXT NOT NULL DEFAULT 'rule-based'",
        [],
    )?;
    tx.execute("ALTER TABLE summaries ADD COLUMN chunk_id TEXT", [])?;
    tx.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_summaries_chunk_summarizer
             ON summaries(chunk_id, summarizer) WHERE kind = 'summary'",
        [],
    )?;

    tx.execute("PRAGMA user_version = 15", [])?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v14(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE files (
                 file_id       TEXT PRIMARY KEY,
                 path          TEXT NOT NULL UNIQUE,
                 indexing_state TEXT NOT NULL DEFAULT 'idle'
             );
             CREATE TABLE summaries (
                 summary_id  TEXT PRIMARY KEY,
                 file_id     TEXT REFERENCES files(file_id) ON DELETE SET NULL,
                 kind        TEXT NOT NULL CHECK(kind IN ('episode', 'reflection', 'summary')),
                 title       TEXT,
                 content     TEXT NOT NULL,
                 tags        TEXT NOT NULL DEFAULT '[]',
                 importance  REAL NOT NULL DEFAULT 1.0,
                 created_at  INTEGER NOT NULL,
                 updated_at  INTEGER NOT NULL,
                 valid_from  INTEGER,
                 valid_to    INTEGER
             );
             PRAGMA user_version = 14;",
        )
        .unwrap();
    }

    #[test]
    fn test_migration_bumps_version() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);

        migrate_v14_to_v15(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 15);
    }

    #[test]
    fn test_summarizer_column_has_default() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        // Insert a row without specifying summarizer — should use default
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s1', 'episode', 'test content', 1000, 1000)",
            [],
        )
        .unwrap();

        let summarizer: String = conn
            .query_row(
                "SELECT summarizer FROM summaries WHERE summary_id = 's1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(summarizer, "rule-based");
    }

    #[test]
    fn test_chunk_id_column_is_nullable() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s1', 'episode', 'test content', 1000, 1000)",
            [],
        )
        .unwrap();

        let chunk_id: Option<String> = conn
            .query_row(
                "SELECT chunk_id FROM summaries WHERE summary_id = 's1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(chunk_id.is_none());
    }

    #[test]
    fn test_unique_index_prevents_duplicate_summaries_per_chunk() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        // First ML summary for chunk c1 with backend 'flan-t5-small'
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, chunk_id, summarizer, created_at, updated_at)
             VALUES ('s1', 'summary', 'first summary', 'c1', 'flan-t5-small', 1000, 1000)",
            [],
        )
        .unwrap();

        // Duplicate (same chunk_id + summarizer + kind='summary') should fail
        let result = conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, chunk_id, summarizer, created_at, updated_at)
             VALUES ('s2', 'summary', 'duplicate summary', 'c1', 'flan-t5-small', 2000, 2000)",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate (chunk_id, summarizer) for kind='summary' should be rejected"
        );
    }

    #[test]
    fn test_unique_index_allows_different_backends_same_chunk() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, chunk_id, summarizer, created_at, updated_at)
             VALUES ('s1', 'summary', 'flan summary', 'c1', 'flan-t5-small', 1000, 1000)",
            [],
        )
        .unwrap();

        // Different backend for the same chunk — should succeed
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, chunk_id, summarizer, created_at, updated_at)
             VALUES ('s2', 'summary', 'remote summary', 'c1', 'remote-llm', 1000, 1000)",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summaries WHERE chunk_id = 'c1' AND kind = 'summary'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_unique_index_is_partial_episodes_not_affected() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        // Two episodes can share chunk_id (index only applies WHERE kind='summary')
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, chunk_id, summarizer, created_at, updated_at)
             VALUES ('s1', 'episode', 'episode 1', 'c1', 'rule-based', 1000, 1000)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, chunk_id, summarizer, created_at, updated_at)
             VALUES ('s2', 'episode', 'episode 2', 'c1', 'rule-based', 2000, 2000)",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summaries WHERE chunk_id = 'c1' AND kind = 'episode'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 2,
            "partial index should not block duplicate episodes"
        );
    }
}
