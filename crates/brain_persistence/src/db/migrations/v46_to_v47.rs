//! v46 → v47: `injection_audit` table for hook-sanitization audit log.
//!
//! ## Purpose
//!
//! Every call to `sanitize_hook_input` records one row here so that users can
//! audit what content was injected into the LLM context, when, and how much
//! was stripped. The table is append-only at the API level — no update or
//! delete paths are exposed from Rust code.
//!
//! ## Schema
//!
//! | Column | Type | Notes |
//! |---|---|---|
//! | `id` | INTEGER PRIMARY KEY | Auto-increment rowid. |
//! | `ts` | INTEGER NOT NULL | Unix seconds (UTC). |
//! | `hook_event` | TEXT NOT NULL | Hook name, e.g. `"PreToolUse:Edit"`. |
//! | `session_id` | TEXT | Claude Code session ID, if available. |
//! | `record_ids` | TEXT | Comma-separated record IDs injected. |
//! | `input_len` | INTEGER NOT NULL | Byte length before sanitization. |
//! | `output_len` | INTEGER NOT NULL | Byte length after sanitization and cap. |
//! | `stripped_counts` | TEXT NOT NULL | JSON: per-category strip counts. |
//! | `was_truncated` | INTEGER NOT NULL | 1 if output was length-capped. |
//! | `opt_in_source` | TEXT NOT NULL | Where opt-in came from, e.g. `"brain.toml"`. |
//!
//! ## Retention
//!
//! The table is never vacuumed automatically. Future tooling may add a
//! configurable retention purge, but that must be an explicit user action
//! with confirmation — not an implicit daemon cleanup.

use rusqlite::Connection;

use crate::error::Result;

/// Create the `injection_audit` table and stamp version 47.
pub fn migrate_v46_to_v47(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS injection_audit (
             id              INTEGER PRIMARY KEY,
             ts              INTEGER NOT NULL,
             hook_event      TEXT    NOT NULL,
             session_id      TEXT,
             record_ids      TEXT,
             input_len       INTEGER NOT NULL,
             output_len      INTEGER NOT NULL,
             stripped_counts TEXT    NOT NULL,
             was_truncated   INTEGER NOT NULL DEFAULT 0,
             opt_in_source   TEXT    NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_injection_audit_ts
             ON injection_audit (ts);

         CREATE INDEX IF NOT EXISTS idx_injection_audit_session
             ON injection_audit (session_id)
             WHERE session_id IS NOT NULL;",
    )?;

    tx.execute_batch("PRAGMA user_version = 47;")?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::v44_to_v45::migrate_v44_to_v45;
    use super::*;

    /// Build an in-memory database at v46 for use in v46→v47 migration tests.
    ///
    /// v45→v46 (trust-schema columns) is owned by a separate migration branch.
    /// Until this branch is rebased on top of that work, we stamp v46 directly
    /// here as a no-op placeholder. The canonical migration replaces this on rebase.
    fn setup_v46() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        super::super::migrate_v0_to_v1(&conn).unwrap();
        super::super::migrate_v1_to_v2(&conn).unwrap();
        super::super::migrate_v2_to_v3(&conn).unwrap();
        super::super::migrate_v3_to_v4(&conn).unwrap();
        super::super::migrate_v4_to_v5(&conn).unwrap();
        super::super::migrate_v5_to_v6(&conn).unwrap();
        super::super::migrate_v6_to_v7(&conn).unwrap();
        super::super::migrate_v7_to_v8(&conn).unwrap();
        super::super::migrate_v8_to_v9(&conn).unwrap();
        super::super::migrate_v9_to_v10(&conn).unwrap();
        super::super::migrate_v10_to_v11(&conn).unwrap();
        super::super::migrate_v11_to_v12(&conn).unwrap();
        super::super::migrate_v12_to_v13(&conn).unwrap();
        super::super::migrate_v13_to_v14(&conn).unwrap();
        super::super::migrate_v14_to_v15(&conn).unwrap();
        super::super::migrate_v15_to_v16(&conn).unwrap();
        super::super::migrate_v16_to_v17(&conn).unwrap();
        super::super::migrate_v17_to_v18(&conn).unwrap();
        super::super::migrate_v18_to_v19(&conn).unwrap();
        super::super::migrate_v19_to_v20(&conn).unwrap();
        super::super::migrate_v20_to_v21(&conn).unwrap();
        super::super::migrate_v21_to_v22(&conn).unwrap();
        super::super::migrate_v22_to_v23(&conn).unwrap();
        super::super::migrate_v23_to_v24(&conn).unwrap();
        super::super::migrate_v24_to_v25(&conn).unwrap();
        super::super::migrate_v25_to_v26(&conn).unwrap();
        super::super::migrate_v26_to_v27(&conn).unwrap();
        super::super::migrate_v27_to_v28(&conn).unwrap();
        super::super::migrate_v28_to_v29(&conn).unwrap();
        super::super::migrate_v29_to_v30(&conn).unwrap();
        super::super::migrate_v30_to_v31(&conn).unwrap();
        super::super::migrate_v31_to_v32(&conn).unwrap();
        super::super::migrate_v32_to_v33(&conn).unwrap();
        super::super::migrate_v33_to_v34(&conn).unwrap();
        super::super::migrate_v34_to_v35(&conn).unwrap();
        super::super::migrate_v35_to_v36(&conn).unwrap();
        super::super::migrate_v36_to_v37(&conn).unwrap();
        super::super::migrate_v37_to_v38(&conn).unwrap();
        super::super::migrate_v38_to_v39(&conn).unwrap();
        super::super::migrate_v39_to_v40(&conn).unwrap();
        super::super::migrate_v40_to_v41(&conn).unwrap();
        super::super::migrate_v41_to_v42(&conn).unwrap();
        super::super::migrate_v42_to_v43(&conn).unwrap();
        super::super::migrate_v43_to_v44(&conn).unwrap();
        migrate_v44_to_v45(&conn).unwrap();
        // No-op stamp for v45→v46 until rebased on the trust-schema migration branch.
        conn.pragma_update(None, "user_version", 46).unwrap();
        conn
    }

    #[test]
    fn version_stamped_47() {
        let conn = setup_v46();
        migrate_v46_to_v47(&conn).unwrap();
        let v: i32 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, 47);
    }

    #[test]
    fn table_exists_and_insertable() {
        let conn = setup_v46();
        migrate_v46_to_v47(&conn).unwrap();

        conn.execute(
            "INSERT INTO injection_audit
                 (ts, hook_event, session_id, record_ids, input_len, output_len,
                  stripped_counts, was_truncated, opt_in_source)
             VALUES (1000, 'PreToolUse:Edit', 'sess-1', 'rec-1', 500, 490,
                     '{\"ansi\":1}', 0, 'brain.toml')",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM injection_audit", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn idempotent() {
        let conn = setup_v46();
        migrate_v46_to_v47(&conn).unwrap();
        migrate_v46_to_v47(&conn).unwrap();
        let v: i32 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, 47);
    }

    #[test]
    fn ts_index_exists() {
        let conn = setup_v46();
        migrate_v46_to_v47(&conn).unwrap();
        let has: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master
                 WHERE type='index' AND name='idx_injection_audit_ts'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(has, "idx_injection_audit_ts must exist");
    }
}
