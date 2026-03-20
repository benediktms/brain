use rusqlite::Connection;

use crate::error::Result;

/// v27 → v28: Loosen `summaries.kind` CHECK constraint to allow 'procedure'.
///
/// SQLite does not support `ALTER TABLE ... ALTER COLUMN`, so changing a CHECK
/// constraint requires full table recreation:
/// 1. Rename existing table to a temporary name.
/// 2. Create new table with the updated CHECK.
/// 3. Copy all rows.
/// 4. Drop the temporary table.
/// 5. Recreate all indexes.
///
/// New CHECK: `kind IN ('episode','reflection','summary','procedure')`
pub fn migrate_v27_to_v28(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;

         ALTER TABLE summaries RENAME TO summaries_v27;

         CREATE TABLE summaries (
             summary_id  TEXT PRIMARY KEY,
             file_id     TEXT REFERENCES files(file_id) ON DELETE SET NULL,
             kind        TEXT NOT NULL CHECK(kind IN ('episode','reflection','summary','procedure')),
             title       TEXT,
             content     TEXT NOT NULL DEFAULT '',
             tags        TEXT NOT NULL DEFAULT '[]',
             importance  REAL NOT NULL DEFAULT 1.0,
             created_at  INTEGER NOT NULL DEFAULT 0,
             updated_at  INTEGER NOT NULL DEFAULT 0,
             valid_from  INTEGER,
             valid_to    INTEGER,
             summarizer  TEXT,
             chunk_id    TEXT,
             brain_id    TEXT NOT NULL DEFAULT '',
             parent_id   TEXT REFERENCES summaries(summary_id),
             source_hash TEXT,
             confidence  REAL NOT NULL DEFAULT 1.0
         );

         INSERT INTO summaries
             SELECT summary_id, file_id, kind, title, content, tags, importance,
                    created_at, updated_at, valid_from, valid_to, summarizer, chunk_id,
                    brain_id, parent_id, source_hash, confidence
             FROM summaries_v27;

         DROP TABLE summaries_v27;

         CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind);
         CREATE INDEX IF NOT EXISTS idx_summaries_brain_id ON summaries(brain_id);
         CREATE INDEX IF NOT EXISTS idx_summaries_kind_brain_created ON summaries(kind, brain_id, created_at DESC);

         PRAGMA user_version = 28;

         PRAGMA foreign_keys = ON;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v27 schema — summaries table with the old CHECK constraint.
    fn setup_v27(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE files (
                 file_id        TEXT PRIMARY KEY,
                 path           TEXT NOT NULL UNIQUE,
                 indexing_state TEXT NOT NULL DEFAULT 'idle'
             );

             CREATE TABLE summaries (
                 summary_id  TEXT PRIMARY KEY,
                 file_id     TEXT REFERENCES files(file_id) ON DELETE SET NULL,
                 kind        TEXT NOT NULL CHECK(kind IN ('episode','reflection','summary')),
                 title       TEXT,
                 content     TEXT NOT NULL DEFAULT '',
                 tags        TEXT NOT NULL DEFAULT '[]',
                 importance  REAL NOT NULL DEFAULT 1.0,
                 created_at  INTEGER NOT NULL DEFAULT 0,
                 updated_at  INTEGER NOT NULL DEFAULT 0,
                 valid_from  INTEGER,
                 valid_to    INTEGER,
                 summarizer  TEXT,
                 chunk_id    TEXT,
                 brain_id    TEXT NOT NULL DEFAULT '',
                 parent_id   TEXT REFERENCES summaries(summary_id),
                 source_hash TEXT,
                 confidence  REAL NOT NULL DEFAULT 1.0
             );

             CREATE TABLE reflection_sources (
                 reflection_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 source_id     TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 PRIMARY KEY (reflection_id, source_id)
             );

             CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind);
             CREATE INDEX IF NOT EXISTS idx_summaries_brain_id ON summaries(brain_id);
             CREATE INDEX IF NOT EXISTS idx_summaries_kind_brain_created
                 ON summaries(kind, brain_id, created_at DESC);

             PRAGMA user_version = 27;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);

        migrate_v27_to_v28(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 28);
    }

    #[test]
    fn test_procedure_kind_accepted_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);
        migrate_v27_to_v28(&conn).unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('p1', 'procedure', 'step-by-step guide', 'brain-abc', 1000, 1000)",
            [],
        )
        .unwrap();

        let kind: String = conn
            .query_row(
                "SELECT kind FROM summaries WHERE summary_id = 'p1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(kind, "procedure");
    }

    #[test]
    fn test_existing_kinds_still_accepted() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);
        migrate_v27_to_v28(&conn).unwrap();

        for (id, kind) in [("e1", "episode"), ("r1", "reflection"), ("s1", "summary")] {
            conn.execute(
                "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
                 VALUES (?1, ?2, 'content', '', 1000, 1000)",
                rusqlite::params![id, kind],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM summaries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3, "all three legacy kinds should insert cleanly");
    }

    #[test]
    fn test_invalid_kind_still_rejected() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);
        migrate_v27_to_v28(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('x1', 'bogus', 'content', '', 1000, 1000)",
            [],
        );
        assert!(result.is_err(), "invalid kind should be rejected by CHECK");
    }

    #[test]
    fn test_existing_rows_preserved_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, confidence, created_at, updated_at)
             VALUES ('pre1', 'episode', 'pre-existing content', 'brain-x', 0.9, 5000, 5000)",
            [],
        )
        .unwrap();

        migrate_v27_to_v28(&conn).unwrap();

        let (kind, content, confidence): (String, String, f64) = conn
            .query_row(
                "SELECT kind, content, confidence FROM summaries WHERE summary_id = 'pre1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(kind, "episode");
        assert_eq!(content, "pre-existing content");
        assert!((confidence - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn test_empty_table_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);
        migrate_v27_to_v28(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 28);
    }

    #[test]
    fn test_indexes_exist_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);
        migrate_v27_to_v28(&conn).unwrap();

        for index_name in [
            "idx_summaries_kind",
            "idx_summaries_brain_id",
            "idx_summaries_kind_brain_created",
        ] {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [index_name],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "index {index_name} should exist after migration");
        }
    }

    #[test]
    fn test_all_columns_preserved() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v27(&conn);
        migrate_v27_to_v28(&conn).unwrap();

        // Insert a row exercising all non-null columns
        conn.execute(
            "INSERT INTO summaries
                 (summary_id, kind, content, tags, importance, created_at, updated_at,
                  valid_from, valid_to, summarizer, chunk_id, brain_id, source_hash, confidence)
             VALUES
                 ('full1', 'procedure', 'body', '[\"tag1\"]', 0.7, 2000, 2001,
                  1999, 3000, 'llm', 'chunk-abc', 'brain-z', 'sha256-xyz', 0.75)",
            [],
        )
        .unwrap();

        let (tags, importance, summarizer, source_hash): (String, f64, String, String) = conn
            .query_row(
                "SELECT tags, importance, summarizer, source_hash FROM summaries WHERE summary_id = 'full1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(tags, "[\"tag1\"]");
        assert!((importance - 0.7).abs() < f64::EPSILON);
        assert_eq!(summarizer, "llm");
        assert_eq!(source_hash, "sha256-xyz");
    }
}
