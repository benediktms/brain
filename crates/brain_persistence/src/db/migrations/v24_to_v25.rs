use rusqlite::Connection;

use crate::error::Result;

/// v24 → v25: Add `brain_id` column to `summaries` table plus Phase 4 foundations.
///
/// New columns:
/// - `brain_id TEXT NOT NULL DEFAULT ''` — brain scoping (backfilled post-migration via `ensure_brain_registered`)
/// - `parent_id TEXT REFERENCES summaries(summary_id)` — versioned learning chain (Phase 4, inert until activated)
/// - `source_hash TEXT` — staleness tracking hash (Phase 4, inert until activated)
/// - `confidence REAL NOT NULL DEFAULT 1.0` — confidence decay (Phase 4, inert until activated)
///
/// Also activates the dormant `valid_from` column by backfilling it from `created_at`.
pub fn migrate_v24_to_v25(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;

         ALTER TABLE summaries ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';
         CREATE INDEX idx_summaries_brain_id ON summaries(brain_id);

         ALTER TABLE summaries ADD COLUMN parent_id TEXT REFERENCES summaries(summary_id);
         ALTER TABLE summaries ADD COLUMN source_hash TEXT;
         ALTER TABLE summaries ADD COLUMN confidence REAL NOT NULL DEFAULT 1.0;

         UPDATE summaries SET valid_from = created_at WHERE valid_from IS NULL OR valid_from = 0;

         PRAGMA user_version = 25;

         PRAGMA foreign_keys = ON;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v24 schema — summaries table without brain_id.
    fn setup_v24(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE summaries (
                 summary_id  TEXT PRIMARY KEY,
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
                 chunk_id    TEXT
             );

             CREATE TABLE reflection_sources (
                 reflection_id TEXT NOT NULL,
                 source_id     TEXT NOT NULL,
                 PRIMARY KEY (reflection_id, source_id)
             );

             PRAGMA user_version = 24;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v24(&conn);

        migrate_v24_to_v25(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 25);
    }

    #[test]
    fn test_brain_id_column_exists_and_defaults_to_empty() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v24(&conn);

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s1', 'episode', 'test', 1000, 1000)",
            [],
        )
        .unwrap();

        migrate_v24_to_v25(&conn).unwrap();

        let brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM summaries WHERE summary_id = 's1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            brain_id, "",
            "existing row should default brain_id to empty string"
        );
    }

    #[test]
    fn test_phase4_columns_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v24(&conn);
        migrate_v24_to_v25(&conn).unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, confidence, created_at, updated_at)
             VALUES ('s2', 'episode', 'test', 'brain-abc', 0.8, 2000, 2000)",
            [],
        )
        .unwrap();

        let confidence: f64 = conn
            .query_row(
                "SELECT confidence FROM summaries WHERE summary_id = 's2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!((confidence - 0.8).abs() < f64::EPSILON);

        // parent_id defaults to NULL
        let parent_id: Option<String> = conn
            .query_row(
                "SELECT parent_id FROM summaries WHERE summary_id = 's2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(parent_id.is_none());
    }

    #[test]
    fn test_valid_from_backfilled_from_created_at() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v24(&conn);

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s3', 'episode', 'test', 5000, 5000)",
            [],
        )
        .unwrap();

        migrate_v24_to_v25(&conn).unwrap();

        let valid_from: i64 = conn
            .query_row(
                "SELECT valid_from FROM summaries WHERE summary_id = 's3'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            valid_from, 5000,
            "valid_from should be backfilled from created_at"
        );
    }

    #[test]
    fn test_brain_id_index_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v24(&conn);
        migrate_v24_to_v25(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_summaries_brain_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_summaries_brain_id index should exist");
    }

    #[test]
    fn test_empty_table_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v24(&conn);
        migrate_v24_to_v25(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 25);
    }
}
