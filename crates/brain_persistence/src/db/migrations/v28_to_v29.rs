use rusqlite::Connection;

use crate::error::Result;

/// v28 → v29: Add `object_links` table for URI-based cross-domain linking.
///
/// New table:
/// - `object_links` — stores directional links between synapse:// URIs.
///   Supports cross-domain linking (task ↔ record, procedure ↔ episode, etc.)
///
/// Schema:
/// ```sql
/// CREATE TABLE object_links (
///     source_uri TEXT NOT NULL,  -- synapse://brain/domain/id
///     target_uri TEXT NOT NULL,  -- synapse://brain/domain/id
///     link_type  TEXT NOT NULL DEFAULT 'related',
///     created_at INTEGER NOT NULL,
///     PRIMARY KEY (source_uri, target_uri)
/// );
/// CREATE INDEX idx_object_links_target ON object_links(target_uri);
/// ```
pub fn migrate_v28_to_v29(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS object_links (
             source_uri TEXT NOT NULL,
             target_uri TEXT NOT NULL,
             link_type  TEXT NOT NULL DEFAULT 'related',
             created_at INTEGER NOT NULL,
             PRIMARY KEY (source_uri, target_uri)
         );
         CREATE INDEX IF NOT EXISTS idx_object_links_target ON object_links(target_uri);
         PRAGMA user_version = 29;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v28 database for testing (no object_links table yet).
    fn setup_v28(conn: &Connection) {
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

             PRAGMA user_version = 28;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v28(&conn);

        migrate_v28_to_v29(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 29);
    }

    #[test]
    fn test_object_links_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v28(&conn);
        migrate_v28_to_v29(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='object_links'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "object_links table should exist");
    }

    #[test]
    fn test_target_index_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v28(&conn);
        migrate_v28_to_v29(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_object_links_target'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_object_links_target index should exist");
    }

    #[test]
    fn test_insert_and_query_link() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v28(&conn);
        migrate_v28_to_v29(&conn).unwrap();

        conn.execute(
            "INSERT INTO object_links (source_uri, target_uri, link_type, created_at)
             VALUES ('synapse://b1/tasks/t1', 'synapse://b1/records/r1', 'related', 1000)",
            [],
        )
        .unwrap();

        let link_type: String = conn
            .query_row(
                "SELECT link_type FROM object_links WHERE source_uri = 'synapse://b1/tasks/t1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(link_type, "related");
    }

    #[test]
    fn test_primary_key_prevents_duplicate() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v28(&conn);
        migrate_v28_to_v29(&conn).unwrap();

        conn.execute(
            "INSERT INTO object_links (source_uri, target_uri, link_type, created_at)
             VALUES ('synapse://b1/tasks/t1', 'synapse://b1/records/r1', 'related', 1000)",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO object_links (source_uri, target_uri, link_type, created_at)
             VALUES ('synapse://b1/tasks/t1', 'synapse://b1/records/r1', 'derived_from', 2000)",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate (source_uri, target_uri) should fail"
        );
    }

    #[test]
    fn test_empty_table_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v28(&conn);
        migrate_v28_to_v29(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 29);
    }
}
