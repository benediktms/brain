use rusqlite::Connection;

use crate::error::Result;

/// v43 → v44: Add `brain_id TEXT NOT NULL` to `tag_cluster_runs` and rebuild
/// `tag_aliases` under composite PK `(brain_id, raw_tag)`.
///
/// SQLite cannot `ALTER TABLE` a `PRIMARY KEY`, so the natural-key change on
/// `tag_aliases` from `raw_tag` to `(brain_id, raw_tag)` requires a recreate.
/// Both tables are empty in this single-user deployment (no production
/// caller has ever invoked `run_recluster`), so the migration is the
/// simplest possible sequence: DROP both, CREATE both. No row migration.
///
/// DROP order respects FK direction: `tag_aliases` (FK child) before
/// `tag_cluster_runs` (FK parent). CREATE order is reversed.
pub fn migrate_v43_to_v44(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "BEGIN;

         DROP INDEX IF EXISTS idx_tag_aliases_canonical;
         DROP INDEX IF EXISTS idx_tag_aliases_cluster;
         DROP TABLE IF EXISTS tag_aliases;
         DROP TABLE IF EXISTS tag_cluster_runs;

         CREATE TABLE tag_cluster_runs (
             run_id           TEXT PRIMARY KEY,
             brain_id         TEXT NOT NULL,
             started_at       TEXT NOT NULL,
             finished_at      TEXT,
             source_count     INTEGER,
             cluster_count    INTEGER,
             embedder_version TEXT NOT NULL,
             threshold        REAL NOT NULL,
             triggered_by     TEXT NOT NULL,
             notes            TEXT
         );

         CREATE TABLE tag_aliases (
             brain_id         TEXT NOT NULL,
             raw_tag          TEXT NOT NULL,
             canonical_tag    TEXT NOT NULL,
             cluster_id       TEXT NOT NULL,
             last_run_id      TEXT NOT NULL REFERENCES tag_cluster_runs(run_id),
             embedding        BLOB,
             embedder_version TEXT,
             updated_at       TEXT NOT NULL,
             PRIMARY KEY (brain_id, raw_tag)
         );

         CREATE INDEX idx_tag_aliases_canonical ON tag_aliases(canonical_tag);
         CREATE INDEX idx_tag_aliases_cluster   ON tag_aliases(cluster_id);

         PRAGMA user_version = 44;
         COMMIT;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::v42_to_v43::migrate_v42_to_v43;
    use super::*;

    /// Build a v43 fixture: empty schema with the v43-shape `tag_cluster_runs`
    /// and `tag_aliases` tables in place. Built by running the v42→v43
    /// migration on an empty in-memory DB.
    fn setup_v43() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.pragma_update(None, "user_version", 42).unwrap();
        migrate_v42_to_v43(&conn).unwrap();
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

    fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .any(|r| r.unwrap() == column)
    }

    #[test]
    fn test_version_stamp() {
        let conn = setup_v43();
        migrate_v43_to_v44(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 44);
    }

    #[test]
    fn test_tables_and_indexes_exist() {
        let conn = setup_v43();
        migrate_v43_to_v44(&conn).unwrap();

        assert!(table_exists(&conn, "tag_cluster_runs"));
        assert!(table_exists(&conn, "tag_aliases"));
        assert!(index_exists(&conn, "idx_tag_aliases_canonical"));
        assert!(index_exists(&conn, "idx_tag_aliases_cluster"));

        // brain_id column on both tables.
        assert!(column_exists(&conn, "tag_cluster_runs", "brain_id"));
        assert!(column_exists(&conn, "tag_aliases", "brain_id"));
    }

    #[test]
    fn test_idempotent() {
        let conn = setup_v43();
        migrate_v43_to_v44(&conn).unwrap();
        // Re-running on a v44 connection drops the empty tables and recreates
        // them. Tables are empty by design, so the second pass is safe.
        migrate_v43_to_v44(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 44);
        assert!(table_exists(&conn, "tag_cluster_runs"));
        assert!(table_exists(&conn, "tag_aliases"));
    }

    #[test]
    fn test_fk_enforced() {
        let conn = setup_v43();
        migrate_v43_to_v44(&conn).unwrap();

        // tag_aliases.last_run_id FK points at tag_cluster_runs.run_id.
        // Inserting an alias row that references a non-existent run_id
        // must fail.
        let result = conn.execute(
            "INSERT INTO tag_aliases
                 (brain_id, raw_tag, canonical_tag, cluster_id, last_run_id, updated_at)
             VALUES ('brain-a', 'auth', 'auth', 'c1', 'no-such-run', '2026-04-28T00:00:00Z')",
            [],
        );
        assert!(result.is_err(), "FK to tag_cluster_runs should be enforced");
    }

    #[test]
    fn test_composite_pk_enforces_per_brain_uniqueness() {
        let conn = setup_v43();
        migrate_v43_to_v44(&conn).unwrap();

        // Seed a run row so the FK resolves.
        conn.execute(
            "INSERT INTO tag_cluster_runs
                 (run_id, brain_id, started_at, embedder_version, threshold, triggered_by)
             VALUES ('r1', 'brain-a', '2026-04-28T00:00:00Z', 'bge', 0.85, 'manual')",
            [],
        )
        .unwrap();

        let insert = |brain_id: &str, raw_tag: &str| {
            conn.execute(
                "INSERT INTO tag_aliases
                     (brain_id, raw_tag, canonical_tag, cluster_id, last_run_id, updated_at)
                 VALUES (?1, ?2, ?2, 'c1', 'r1', '2026-04-28T00:00:00Z')",
                rusqlite::params![brain_id, raw_tag],
            )
        };

        // Two brains, same raw_tag — must succeed under composite PK.
        insert("brain-a", "shared").unwrap();
        insert("brain-b", "shared").unwrap();

        // Same brain, same raw_tag — must fail (PK collision).
        let dup = insert("brain-a", "shared");
        assert!(
            dup.is_err(),
            "composite PK should reject (brain-a, shared) twice"
        );
    }
}
