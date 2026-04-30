//! v45 → v46: Trust/provenance schema for hook-ingestable records.
//!
//! ## Design: first-class TEXT column, NOT NULL
//!
//! `trust` is stored as `TEXT NOT NULL DEFAULT 'untrusted'` on both `records`
//! and `summaries`. Three values are defined (see [`Trust`]):
//!
//! - `untrusted` — hook-injected, attacker-controlled, opaque provenance.
//! - `vetted` — intermediate band: tool-derived but reviewed/curated by a
//!   human-in-the-loop pass (e.g. explicit `brain memory write` invoked on
//!   hook output after review).
//! - `trusted` — user-authored or explicitly marked safe (e.g. the existing
//!   `brain memory write` CLI path used before hook integration).
//!
//! Choosing TEXT over INTEGER:
//! - Readable in SQL queries and log output without a lookup table.
//! - Forward-compatible: adding a new band does not shift existing integer values.
//! - SQLite does not enforce CHECK constraints on ALTER TABLE ADD COLUMN without
//!   a full table recreate; a CHECK is added inline for new tables. For ALTERs,
//!   constraint compliance is enforced at the Rust layer via [`Trust`].
//!
//! ## `source_tool` column
//!
//! `source_tool TEXT` (NULL-able) records the originating tool for a row:
//! `transcript`, `git`, `web_fetch`, `bash`, `read`, `user`, or NULL.
//! NULL means "system-internal" (e.g. consolidation jobs).
//! Hook-driven importers set a specific value; user-invoked paths set `user`.
//!
//! ## Backfill stance
//!
//! Existing rows in `records` and `summaries` are backfilled to `trusted`.
//! Rationale: pre-migration rows were all user-authored (no hook integration
//! existed before this migration). Treating them as `untrusted` would break
//! every existing retrieval path. A one-time log warning is emitted at
//! runtime by the migration caller when the row count is non-zero.
//!
//! Hook-driven importers added after this migration MUST pass `trust='untrusted'`
//! explicitly. The SQL DEFAULT ensures that any INSERT that omits the column
//! also lands as `untrusted` — the safe-by-default stance the sentinel requires.

use rusqlite::Connection;

use crate::error::Result;

/// Add `trust` and `source_tool` columns to `records` and `summaries`.
/// Backfill existing rows to `trusted`. Stamp version 46.
pub fn migrate_v45_to_v46(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    // ── records ──────────────────────────────────────────────────────────────
    // Idempotent: skip if already present (re-run safety).
    let has_records_trust = column_exists(&tx, "records", "trust")?;
    let has_records_source_tool = column_exists(&tx, "records", "source_tool")?;

    if !has_records_trust {
        tx.execute_batch(
            "ALTER TABLE records
             ADD COLUMN trust TEXT NOT NULL DEFAULT 'untrusted';",
        )?;
        // Backfill legacy rows to 'trusted' — these predate hook integration.
        let backfilled = tx.execute(
            "UPDATE records SET trust = 'trusted' WHERE trust = 'untrusted'",
            [],
        )?;
        if backfilled > 0 {
            tracing::warn!(
                count = backfilled,
                "v45→v46: backfilled {backfilled} existing records rows to trust='trusted' (legacy honor)"
            );
        }
    }

    if !has_records_source_tool {
        tx.execute_batch(
            "ALTER TABLE records
             ADD COLUMN source_tool TEXT;",
        )?;
        // Existing rows get NULL source_tool — origin unknown / system-internal.
    }

    // ── summaries ────────────────────────────────────────────────────────────
    let has_summaries_trust = column_exists(&tx, "summaries", "trust")?;
    let has_summaries_source_tool = column_exists(&tx, "summaries", "source_tool")?;

    if !has_summaries_trust {
        tx.execute_batch(
            "ALTER TABLE summaries
             ADD COLUMN trust TEXT NOT NULL DEFAULT 'untrusted';",
        )?;
        // Backfill existing episodes/reflections to 'trusted'.
        let backfilled = tx.execute(
            "UPDATE summaries SET trust = 'trusted' WHERE trust = 'untrusted'",
            [],
        )?;
        if backfilled > 0 {
            tracing::warn!(
                count = backfilled,
                "v45→v46: backfilled {backfilled} existing summaries rows to trust='trusted' (legacy honor)"
            );
        }
    }

    if !has_summaries_source_tool {
        tx.execute_batch(
            "ALTER TABLE summaries
             ADD COLUMN source_tool TEXT;",
        )?;
    }

    // ── indexes ──────────────────────────────────────────────────────────────
    // Partial index on untrusted rows for fast retrieval-side filtering.
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_records_untrusted
         ON records (brain_id, created_at)
         WHERE trust = 'untrusted';

         CREATE INDEX IF NOT EXISTS idx_summaries_untrusted
         ON summaries (brain_id, created_at)
         WHERE trust = 'untrusted';",
    )?;

    tx.execute_batch("PRAGMA user_version = 46;")?;
    tx.commit()?;
    Ok(())
}

/// Check whether `table.column` exists in the given connection.
fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .any(|name| name == column);
    Ok(exists)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;

    fn setup_v45() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        // Run all migrations up to v45 via init_schema (which runs to SCHEMA_VERSION).
        // At this point SCHEMA_VERSION is still 45 in the binary being compiled; after
        // the bump it will be 46 — so we drive migrations manually for the test fixture.
        use crate::db::migrations::*;
        migrate_v0_to_v1(&conn).unwrap();
        migrate_v1_to_v2(&conn).unwrap();
        migrate_v2_to_v3(&conn).unwrap();
        migrate_v3_to_v4(&conn).unwrap();
        migrate_v4_to_v5(&conn).unwrap();
        migrate_v5_to_v6(&conn).unwrap();
        migrate_v6_to_v7(&conn).unwrap();
        migrate_v7_to_v8(&conn).unwrap();
        migrate_v8_to_v9(&conn).unwrap();
        migrate_v9_to_v10(&conn).unwrap();
        migrate_v10_to_v11(&conn).unwrap();
        migrate_v11_to_v12(&conn).unwrap();
        migrate_v12_to_v13(&conn).unwrap();
        migrate_v13_to_v14(&conn).unwrap();
        migrate_v14_to_v15(&conn).unwrap();
        migrate_v15_to_v16(&conn).unwrap();
        migrate_v16_to_v17(&conn).unwrap();
        migrate_v17_to_v18(&conn).unwrap();
        migrate_v18_to_v19(&conn).unwrap();
        migrate_v19_to_v20(&conn).unwrap();
        migrate_v20_to_v21(&conn).unwrap();
        migrate_v21_to_v22(&conn).unwrap();
        migrate_v22_to_v23(&conn).unwrap();
        migrate_v23_to_v24(&conn).unwrap();
        migrate_v24_to_v25(&conn).unwrap();
        migrate_v25_to_v26(&conn).unwrap();
        migrate_v26_to_v27(&conn).unwrap();
        migrate_v27_to_v28(&conn).unwrap();
        migrate_v28_to_v29(&conn).unwrap();
        migrate_v29_to_v30(&conn).unwrap();
        migrate_v30_to_v31(&conn).unwrap();
        migrate_v31_to_v32(&conn).unwrap();
        migrate_v32_to_v33(&conn).unwrap();
        migrate_v33_to_v34(&conn).unwrap();
        migrate_v34_to_v35(&conn).unwrap();
        migrate_v35_to_v36(&conn).unwrap();
        migrate_v36_to_v37(&conn).unwrap();
        migrate_v37_to_v38(&conn).unwrap();
        migrate_v38_to_v39(&conn).unwrap();
        migrate_v39_to_v40(&conn).unwrap();
        migrate_v40_to_v41(&conn).unwrap();
        migrate_v41_to_v42(&conn).unwrap();
        migrate_v42_to_v43(&conn).unwrap();
        migrate_v43_to_v44(&conn).unwrap();
        migrate_v44_to_v45(&conn).unwrap();
        conn
    }

    #[test]
    fn test_version_stamp() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 46);
    }

    #[test]
    fn test_trust_column_on_records() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();
        assert!(column_exists(&conn, "records", "trust").unwrap());
        assert!(column_exists(&conn, "records", "source_tool").unwrap());
    }

    #[test]
    fn test_trust_column_on_summaries() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();
        assert!(column_exists(&conn, "summaries", "trust").unwrap());
        assert!(column_exists(&conn, "summaries", "source_tool").unwrap());
    }

    #[test]
    fn test_idempotent() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();
        migrate_v45_to_v46(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 46);
    }

    #[test]
    fn test_default_trust_for_new_hook_insert() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();

        // Disable FK for test isolation (no parent brain row needed).
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();

        // Insert a record WITHOUT specifying trust — must land as 'untrusted'.
        conn.execute(
            "INSERT INTO records
                 (record_id, brain_id, title, kind, status, content_hash,
                  content_size, actor, created_at, updated_at,
                  pinned, payload_available, content_encoding, original_size, searchable)
             VALUES ('r-hook', 'b1', 'Hook Record', 'snapshot', 'active',
                     'deadbeef', 0, 'hook', 1000, 1000, 0, 1, 'none', 0, 0)",
            [],
        )
        .unwrap();

        let trust: String = conn
            .query_row(
                "SELECT trust FROM records WHERE record_id = 'r-hook'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            trust, "untrusted",
            "omitted trust must default to 'untrusted'"
        );
    }

    #[test]
    fn test_existing_records_backfilled_to_trusted() {
        let conn = setup_v45();
        // Plant a pre-migration record row (no trust column yet at v45).
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT INTO records
                 (record_id, brain_id, title, kind, status, content_hash,
                  content_size, actor, created_at, updated_at,
                  pinned, payload_available, content_encoding, original_size, searchable)
             VALUES ('r-legacy', 'b1', 'Legacy', 'document', 'active',
                     'deadbeef', 0, 'user', 500, 500, 0, 1, 'none', 0, 1)",
            [],
        )
        .unwrap();
        migrate_v45_to_v46(&conn).unwrap();

        let trust: String = conn
            .query_row(
                "SELECT trust FROM records WHERE record_id = 'r-legacy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            trust, "trusted",
            "legacy rows must be backfilled to 'trusted'"
        );
    }

    #[test]
    fn test_new_insert_with_explicit_trust() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();

        conn.execute(
            "INSERT INTO records
                 (record_id, brain_id, title, kind, status, content_hash,
                  content_size, actor, created_at, updated_at,
                  pinned, payload_available, content_encoding, original_size, searchable,
                  trust, source_tool)
             VALUES ('r-trusted', 'b1', 'User Record', 'document', 'active',
                     'deadbeef', 0, 'user', 1000, 1000, 0, 1, 'none', 0, 1,
                     'trusted', 'user')",
            [],
        )
        .unwrap();

        let (trust, source_tool): (String, Option<String>) = conn
            .query_row(
                "SELECT trust, source_tool FROM records WHERE record_id = 'r-trusted'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(trust, "trusted");
        assert_eq!(source_tool.as_deref(), Some("user"));
    }

    #[test]
    fn test_summaries_default_trust() {
        let conn = setup_v45();
        migrate_v45_to_v46(&conn).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();

        conn.execute(
            "INSERT INTO summaries
                 (summary_id, kind, content, created_at, updated_at)
             VALUES ('s-hook', 'episode', 'hook content', 1000, 1000)",
            [],
        )
        .unwrap();

        let trust: String = conn
            .query_row(
                "SELECT trust FROM summaries WHERE summary_id = 's-hook'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            trust, "untrusted",
            "new summaries must default to 'untrusted'"
        );
    }
}
