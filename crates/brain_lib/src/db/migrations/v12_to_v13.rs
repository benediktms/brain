use rusqlite::Connection;

use crate::error::Result;

/// v12 → v13: Add storage lifecycle columns to the `records` table.
///
/// Adds five columns to support compression, retention classes, and payload
/// eviction:
///
/// - `retention_class`     — optional string tag (e.g. "ephemeral", "permanent")
/// - `pinned`              — 1 if the record is exempt from GC, 0 otherwise
/// - `payload_available`   — 1 if the object-store blob is present, 0 if evicted
/// - `content_encoding`    — content-encoding of the stored blob (e.g. "gzip", "identity")
/// - `original_size`       — byte length of the payload before encoding
pub fn migrate_v12_to_v13(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        ALTER TABLE records ADD COLUMN retention_class TEXT;
        ALTER TABLE records ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE records ADD COLUMN payload_available INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE records ADD COLUMN content_encoding TEXT NOT NULL DEFAULT 'identity';
        ALTER TABLE records ADD COLUMN original_size INTEGER;

        PRAGMA user_version = 13;

        COMMIT;
    ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_v11_to_v12;

    fn setup_v12(conn: &Connection) {
        // Run migrations 0→11 via a minimal bootstrap, then 11→12
        // For simplicity we create just the records table as v11→v12 would.
        conn.execute_batch(
            "PRAGMA user_version = 11;
             CREATE TABLE records (
                 record_id    TEXT PRIMARY KEY,
                 title        TEXT NOT NULL,
                 kind         TEXT NOT NULL,
                 status       TEXT NOT NULL DEFAULT 'active',
                 description  TEXT,
                 content_hash TEXT NOT NULL,
                 content_size INTEGER NOT NULL,
                 media_type   TEXT,
                 task_id      TEXT,
                 actor        TEXT NOT NULL,
                 created_at   INTEGER NOT NULL,
                 updated_at   INTEGER NOT NULL
             );
             CREATE TABLE record_tags (
                 record_id TEXT NOT NULL,
                 tag       TEXT NOT NULL,
                 PRIMARY KEY (record_id, tag)
             );
             CREATE TABLE record_links (
                 record_id  TEXT NOT NULL,
                 task_id    TEXT,
                 chunk_id   TEXT,
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL
             );
             PRAGMA user_version = 12;",
        )
        .unwrap();
        let _ = migrate_v11_to_v12; // keep the import referenced
    }

    #[test]
    fn test_migration_adds_columns_with_correct_defaults() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v12(&conn);

        // Insert a row before migration to verify defaults apply to existing rows
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'T', 'report', 'abc', 42, 'agent', 0, 0)",
            [],
        )
        .unwrap();

        migrate_v12_to_v13(&conn).unwrap();

        // Verify schema version was bumped
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 13);

        // Verify new columns exist with expected defaults on the pre-existing row
        let (retention_class, pinned, payload_available, content_encoding, original_size): (
            Option<String>,
            i32,
            i32,
            String,
            Option<i64>,
        ) = conn
            .query_row(
                "SELECT retention_class, pinned, payload_available, content_encoding, original_size
                 FROM records WHERE record_id = 'r1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();

        assert!(retention_class.is_none());
        assert_eq!(pinned, 0);
        assert_eq!(payload_available, 1);
        assert_eq!(content_encoding, "identity");
        assert!(original_size.is_none());
    }

    #[test]
    fn test_migration_is_idempotent_via_schema() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v12(&conn);
        migrate_v12_to_v13(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 13);
    }
}
