use rusqlite::Connection;

use crate::error::Result;

/// v26 → v27: Add `searchable` and `embedded_at` columns to `records` table.
///
/// New columns:
/// - `searchable INTEGER NOT NULL DEFAULT 1` — whether this record should be indexed
///   for hybrid search. Callers opt out at creation time for operational debris.
/// - `embedded_at INTEGER` — nullable Unix timestamp; NULL means the record has not
///   yet been embedded into LanceDB. Daemon poll uses this for stale detection.
///
/// Backfill:
/// - Existing snapshot and dispatch-brief records are set to `searchable = 0`
///   (operational debris, not knowledge artifacts).
pub fn migrate_v26_to_v27(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "ALTER TABLE records ADD COLUMN searchable INTEGER NOT NULL DEFAULT 1;
         ALTER TABLE records ADD COLUMN embedded_at INTEGER;
         UPDATE records SET searchable = 0 WHERE kind IN ('snapshot', 'dispatch-brief');
         PRAGMA user_version = 27;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v26 records table for testing.
    fn setup_v26(conn: &Connection) {
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

             INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 1000);

             CREATE TABLE records (
                 record_id         TEXT PRIMARY KEY,
                 title             TEXT NOT NULL,
                 kind              TEXT NOT NULL,
                 status            TEXT NOT NULL DEFAULT 'active',
                 description       TEXT,
                 content_hash      TEXT NOT NULL,
                 content_size      INTEGER NOT NULL,
                 media_type        TEXT,
                 task_id           TEXT,
                 actor             TEXT NOT NULL,
                 created_at        INTEGER NOT NULL,
                 updated_at        INTEGER NOT NULL,
                 retention_class   TEXT,
                 pinned            INTEGER NOT NULL DEFAULT 0,
                 payload_available INTEGER NOT NULL DEFAULT 1,
                 content_encoding  TEXT NOT NULL DEFAULT 'identity',
                 original_size     INTEGER,
                 brain_id          TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id)
             );

             PRAGMA user_version = 26;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    fn insert_record(conn: &Connection, id: &str, kind: &str) {
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at, brain_id)
             VALUES (?1, 'Test', ?2, 'hash123', 100, 'agent', 1000, 1000, 'b1')",
            rusqlite::params![id, kind],
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v26(&conn);

        migrate_v26_to_v27(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 27);
    }

    #[test]
    fn test_searchable_defaults_to_1() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v26(&conn);
        migrate_v26_to_v27(&conn).unwrap();

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at, brain_id)
             VALUES ('r-new', 'New Record', 'analysis', 'hash', 10, 'agent', 2000, 2000, 'b1')",
            [],
        )
        .unwrap();

        let searchable: i32 = conn
            .query_row(
                "SELECT searchable FROM records WHERE record_id = 'r-new'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(searchable, 1);
    }

    #[test]
    fn test_embedded_at_defaults_to_null() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v26(&conn);
        migrate_v26_to_v27(&conn).unwrap();

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at, brain_id)
             VALUES ('r-new', 'New Record', 'analysis', 'hash', 10, 'agent', 2000, 2000, 'b1')",
            [],
        )
        .unwrap();

        let embedded_at: Option<i64> = conn
            .query_row(
                "SELECT embedded_at FROM records WHERE record_id = 'r-new'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(embedded_at.is_none());
    }

    #[test]
    fn test_backfill_snapshot_searchable_0() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v26(&conn);

        insert_record(&conn, "r-snap", "snapshot");
        insert_record(&conn, "r-disp", "dispatch-brief");
        insert_record(&conn, "r-analysis", "analysis");
        insert_record(&conn, "r-plan", "plan");
        insert_record(&conn, "r-review", "review");
        insert_record(&conn, "r-impl", "implementation");

        migrate_v26_to_v27(&conn).unwrap();

        let get_searchable = |id: &str| -> i32 {
            conn.query_row(
                "SELECT searchable FROM records WHERE record_id = ?1",
                [id],
                |row| row.get(0),
            )
            .unwrap()
        };

        assert_eq!(
            get_searchable("r-snap"),
            0,
            "snapshot should be unsearchable"
        );
        assert_eq!(
            get_searchable("r-disp"),
            0,
            "dispatch-brief should be unsearchable"
        );
        assert_eq!(
            get_searchable("r-analysis"),
            1,
            "analysis should be searchable"
        );
        assert_eq!(get_searchable("r-plan"), 1, "plan should be searchable");
        assert_eq!(get_searchable("r-review"), 1, "review should be searchable");
        assert_eq!(
            get_searchable("r-impl"),
            1,
            "implementation should be searchable"
        );
    }

    #[test]
    fn test_empty_table_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v26(&conn);
        migrate_v26_to_v27(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 27);
    }
}
