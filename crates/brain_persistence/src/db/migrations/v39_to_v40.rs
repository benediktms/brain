use rusqlite::Connection;

use crate::error::Result;

/// v39 → v40: Create the `lod_chunks` table for Level-of-Detail storage.
///
/// LOD chunks are pre-computed representations of source objects at different
/// fidelity levels:
/// - **L0**: extractive/deterministic (~100 tokens)
/// - **L1**: LLM-summarized (~2000 tokens)
/// - **L2**: passthrough from source (never stored in this table)
///
/// Keyed on `(object_uri, lod_level)` so each object has at most one
/// representation per level.  `source_hash` (BLAKE3) enables staleness
/// detection by comparing against the current source content.
///
/// Part of the Retrieve+ initiative (ADR-001, brn-83a.5.1).
pub fn migrate_v39_to_v40(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "BEGIN;

         CREATE TABLE lod_chunks (
             id          TEXT    NOT NULL,
             object_uri  TEXT    NOT NULL,
             brain_id    TEXT    NOT NULL,
             lod_level   TEXT    NOT NULL CHECK(lod_level IN ('L0','L1')),
             content     TEXT    NOT NULL,
             token_est   INTEGER,
             method      TEXT    NOT NULL CHECK(method IN ('extractive','llm')),
             model_id    TEXT,
             source_hash TEXT    NOT NULL,
             created_at  TEXT    NOT NULL,
             expires_at  TEXT,
             job_id      TEXT,
             PRIMARY KEY (id),
             UNIQUE (object_uri, lod_level)
         );

         CREATE INDEX idx_lod_chunks_brain ON lod_chunks (brain_id, created_at DESC);
         CREATE INDEX idx_lod_chunks_exp   ON lod_chunks (expires_at) WHERE expires_at IS NOT NULL;

         PRAGMA user_version = 40;

         COMMIT;",
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v39(conn: &Connection) {
        conn.execute_batch("PRAGMA user_version = 39;").unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v39(&conn);

        migrate_v39_to_v40(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 40);
    }

    #[test]
    fn test_table_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v39(&conn);
        migrate_v39_to_v40(&conn).unwrap();

        conn.execute(
            "INSERT INTO lod_chunks (id, object_uri, brain_id, lod_level, content, token_est,
                 method, source_hash, created_at)
             VALUES ('01ABC', 'synapse://b/memory/c1', 'brain1', 'L0', 'hello', 10,
                 'extractive', 'hash1', '2026-03-30T00:00:00Z')",
            [],
        )
        .unwrap();

        let content: String = conn
            .query_row(
                "SELECT content FROM lod_chunks WHERE id = '01ABC'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(content, "hello");
    }

    #[test]
    fn test_indexes_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v39(&conn);
        migrate_v39_to_v40(&conn).unwrap();

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='lod_chunks'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();

        assert!(
            indexes.contains(&"idx_lod_chunks_brain".to_string()),
            "missing idx_lod_chunks_brain, got: {indexes:?}"
        );
        assert!(
            indexes.contains(&"idx_lod_chunks_exp".to_string()),
            "missing idx_lod_chunks_exp, got: {indexes:?}"
        );
    }

    #[test]
    fn test_unique_constraint_enforced() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v39(&conn);
        migrate_v39_to_v40(&conn).unwrap();

        conn.execute(
            "INSERT INTO lod_chunks (id, object_uri, brain_id, lod_level, content,
                 method, source_hash, created_at)
             VALUES ('01A', 'synapse://b/memory/c1', 'b', 'L0', 'first',
                 'extractive', 'h1', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO lod_chunks (id, object_uri, brain_id, lod_level, content,
                 method, source_hash, created_at)
             VALUES ('01B', 'synapse://b/memory/c1', 'b', 'L0', 'duplicate',
                 'extractive', 'h2', '2026-01-01T00:00:00Z')",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate (object_uri, lod_level) should fail"
        );
    }

    #[test]
    fn test_check_constraint_rejects_l2() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v39(&conn);
        migrate_v39_to_v40(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO lod_chunks (id, object_uri, brain_id, lod_level, content,
                 method, source_hash, created_at)
             VALUES ('01A', 'synapse://b/memory/c1', 'b', 'L2', 'nope',
                 'passthrough', 'h1', '2026-01-01T00:00:00Z')",
            [],
        );
        assert!(result.is_err(), "L2 should be rejected by CHECK constraint");
    }

    #[test]
    fn test_check_constraint_rejects_invalid_method() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v39(&conn);
        migrate_v39_to_v40(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO lod_chunks (id, object_uri, brain_id, lod_level, content,
                 method, source_hash, created_at)
             VALUES ('01A', 'synapse://b/memory/c1', 'b', 'L0', 'nope',
                 'passthrough', 'h1', '2026-01-01T00:00:00Z')",
            [],
        );
        assert!(
            result.is_err(),
            "method 'passthrough' should be rejected by CHECK constraint"
        );
    }
}
