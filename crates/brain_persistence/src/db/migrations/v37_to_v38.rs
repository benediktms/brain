use rusqlite::Connection;

use crate::error::Result;

/// v37 → v38: Fix `reflection_sources` FK references pointing to stale
/// `summaries_v27` table.
///
/// The v27→v28 migration renamed `summaries` to `summaries_v27` as a
/// temporary step while recreating the table with a looser CHECK constraint.
/// On some databases, SQLite's `ALTER TABLE … RENAME` propagated the renamed
/// table name into the FK definitions of `reflection_sources` before the
/// migration could drop and recreate it.  This left `reflection_sources` with
/// `REFERENCES "summaries_v27"(summary_id)` — a table that no longer exists —
/// causing all inserts to fail with:
///
///     no such table: main.summaries_v27
///
/// Fix: detect and recreate `reflection_sources` with correct FK references
/// pointing to `summaries`.  This is a no-op on databases where the FKs are
/// already correct.
pub fn migrate_v37_to_v38(conn: &Connection) -> Result<()> {
    // Check whether remediation is needed by inspecting the stored DDL.
    let needs_fix: bool = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='reflection_sources'",
            [],
            |row| row.get::<_, String>(0),
        )
        .map(|sql| sql.contains("summaries_v27"))
        .unwrap_or(false);

    if needs_fix {
        conn.execute("PRAGMA foreign_keys = OFF", [])?;

        conn.execute_batch(
            "CREATE TABLE reflection_sources_fix AS
                 SELECT reflection_id, source_id FROM reflection_sources;

             DROP TABLE reflection_sources;

             CREATE TABLE reflection_sources (
                 reflection_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 source_id     TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 PRIMARY KEY (reflection_id, source_id)
             );

             INSERT OR IGNORE INTO reflection_sources
                 SELECT reflection_id, source_id FROM reflection_sources_fix;

             DROP TABLE reflection_sources_fix;",
        )?;

        conn.execute("PRAGMA foreign_keys = ON", [])?;
    }

    conn.execute("PRAGMA user_version = 38", [])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Build a minimal v37 schema with the broken FK reference.
    fn setup_v37_broken(conn: &Connection) {
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
                 confidence  REAL NOT NULL DEFAULT 1.0,
                 consolidated_by TEXT DEFAULT NULL,
                 embedded_at INTEGER
             );

             -- Simulate the broken state: FKs point to summaries_v27
             CREATE TABLE reflection_sources (
                 reflection_id TEXT NOT NULL REFERENCES summaries_v27(summary_id) ON DELETE CASCADE,
                 source_id     TEXT NOT NULL REFERENCES summaries_v27(summary_id) ON DELETE CASCADE,
                 PRIMARY KEY (reflection_id, source_id)
             );

             PRAGMA user_version = 37;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    /// Build a v37 schema where FKs are already correct (no-op case).
    fn setup_v37_correct(conn: &Connection) {
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
                 confidence  REAL NOT NULL DEFAULT 1.0,
                 consolidated_by TEXT DEFAULT NULL,
                 embedded_at INTEGER
             );

             CREATE TABLE reflection_sources (
                 reflection_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 source_id     TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 PRIMARY KEY (reflection_id, source_id)
             );

             PRAGMA user_version = 37;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v37_broken(&conn);

        migrate_v37_to_v38(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 38);
    }

    #[test]
    fn test_fixes_broken_fk_references() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v37_broken(&conn);

        migrate_v37_to_v38(&conn).unwrap();

        // The stored DDL should now reference summaries, not summaries_v27
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='reflection_sources'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !sql.contains("summaries_v27"),
            "FK should no longer reference summaries_v27, got: {sql}"
        );
        assert!(
            sql.contains("summaries"),
            "FK should reference summaries, got: {sql}"
        );
    }

    #[test]
    fn test_preserves_existing_data() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v37_broken(&conn);

        // Insert test data (FKs are not enforced since summaries_v27 doesn't exist
        // and we need foreign_keys OFF to insert)
        conn.execute("PRAGMA foreign_keys = OFF", []).unwrap();
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('ep1', 'episode', 'content', '', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('ref1', 'reflection', 'reflection', '', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO reflection_sources (reflection_id, source_id) VALUES ('ref1', 'ep1')",
            [],
        )
        .unwrap();
        conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

        migrate_v37_to_v38(&conn).unwrap();

        // Verify data survived
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM reflection_sources WHERE reflection_id = 'ref1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "existing reflection_sources data should survive");
    }

    #[test]
    fn test_noop_when_fks_already_correct() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v37_correct(&conn);

        // Insert test data
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('ep1', 'episode', 'content', '', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('ref1', 'reflection', 'reflection', '', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO reflection_sources (reflection_id, source_id) VALUES ('ref1', 'ep1')",
            [],
        )
        .unwrap();

        migrate_v37_to_v38(&conn).unwrap();

        // Version should be stamped
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 38);

        // Data should be intact
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM reflection_sources", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);

        // FKs should still reference summaries
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='reflection_sources'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!sql.contains("summaries_v27"));
    }

    #[test]
    fn test_fk_enforcement_works_after_fix() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v37_broken(&conn);

        // Insert a valid episode
        conn.execute("PRAGMA foreign_keys = OFF", []).unwrap();
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('ep1', 'episode', 'content', '', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, brain_id, created_at, updated_at)
             VALUES ('ref1', 'reflection', 'my reflection', '', 1000, 1000)",
            [],
        )
        .unwrap();
        conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

        migrate_v37_to_v38(&conn).unwrap();

        // Valid insert should work
        let result = conn.execute(
            "INSERT INTO reflection_sources (reflection_id, source_id) VALUES ('ref1', 'ep1')",
            [],
        );
        assert!(result.is_ok(), "valid FK insert should succeed after fix");

        // Invalid insert should fail (FK enforcement)
        let result = conn.execute(
            "INSERT INTO reflection_sources (reflection_id, source_id) VALUES ('ref1', 'nonexistent')",
            [],
        );
        assert!(result.is_err(), "invalid FK insert should fail after fix");
    }
}
