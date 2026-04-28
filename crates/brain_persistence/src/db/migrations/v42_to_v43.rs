use rusqlite::Connection;

use crate::error::Result;

/// v42 → v43: Add `tag_cluster_runs` and `tag_aliases` tables for synonym
/// clustering & label alias resolution (parent task brn-83a.7.2).
///
/// Pure additive DDL: no existing rows are touched. Embedding population,
/// clustering, and query-path use are deferred to sibling tasks.
///
/// Timestamps use ISO 8601 TEXT to align with the convention in `lod_chunks`
/// and the direction of brn-83a.13 (which separately migrates existing
/// INTEGER timestamp columns).
pub fn migrate_v42_to_v43(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "BEGIN;

         CREATE TABLE IF NOT EXISTS tag_cluster_runs (
             run_id           TEXT PRIMARY KEY,
             started_at       TEXT NOT NULL,
             finished_at      TEXT,
             source_count     INTEGER,
             cluster_count    INTEGER,
             embedder_version TEXT NOT NULL,
             threshold        REAL NOT NULL,
             triggered_by     TEXT NOT NULL,
             notes            TEXT
         );

         CREATE TABLE IF NOT EXISTS tag_aliases (
             raw_tag          TEXT PRIMARY KEY,
             canonical_tag    TEXT NOT NULL,
             cluster_id       TEXT NOT NULL,
             last_run_id      TEXT NOT NULL REFERENCES tag_cluster_runs(run_id),
             embedding        BLOB,
             embedder_version TEXT,
             updated_at       TEXT NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_tag_aliases_canonical ON tag_aliases(canonical_tag);
         CREATE INDEX IF NOT EXISTS idx_tag_aliases_cluster   ON tag_aliases(cluster_id);

         PRAGMA user_version = 43;
         COMMIT;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v42 schema. This migration is pure additive DDL with no
    /// dependency on existing tables, so an empty schema is sufficient.
    fn setup_v42() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.pragma_update(None, "user_version", 42).unwrap();
        conn
    }

    fn table_exists(conn: &Connection, name: &str) -> bool {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [name],
                |row| row.get(0),
            )
            .unwrap();
        count == 1
    }

    fn index_exists(conn: &Connection, name: &str) -> bool {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [name],
                |row| row.get(0),
            )
            .unwrap();
        count == 1
    }

    #[test]
    fn test_version_stamp() {
        let conn = setup_v42();
        migrate_v42_to_v43(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 43);
    }

    #[test]
    fn test_tables_and_indexes_exist() {
        let conn = setup_v42();
        migrate_v42_to_v43(&conn).unwrap();

        assert!(table_exists(&conn, "tag_cluster_runs"));
        assert!(table_exists(&conn, "tag_aliases"));
        assert!(index_exists(&conn, "idx_tag_aliases_canonical"));
        assert!(index_exists(&conn, "idx_tag_aliases_cluster"));
    }

    #[test]
    fn test_idempotent() {
        let conn = setup_v42();
        migrate_v42_to_v43(&conn).unwrap();
        // Re-running stamps version 43 over 43 and the IF NOT EXISTS guards
        // make the DDL a no-op.
        migrate_v42_to_v43(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 43);
        assert!(table_exists(&conn, "tag_cluster_runs"));
        assert!(table_exists(&conn, "tag_aliases"));
    }

    #[test]
    fn test_fk_enforced() {
        let conn = setup_v42();
        migrate_v42_to_v43(&conn).unwrap();

        // Inserting a tag_aliases row referencing a non-existent run_id must fail.
        let result = conn.execute(
            "INSERT INTO tag_aliases
                 (raw_tag, canonical_tag, cluster_id, last_run_id, updated_at)
             VALUES ('auth', 'auth', 'c1', 'no-such-run', '2026-04-27T00:00:00Z')",
            [],
        );
        assert!(result.is_err(), "FK to tag_cluster_runs should be enforced");
    }
}
