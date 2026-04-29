//! v44 → v45: Promote `task_external_ids` rows to first-class blockers.
//!
//! ## Design choice — extend vs sibling table
//!
//! Two new NULL-safe columns are added to the existing `task_external_ids`
//! table rather than introducing a sibling `task_external_blockers` table:
//!
//! - `blocking BOOLEAN NOT NULL DEFAULT 0` — flag distinguishing pure-metadata
//!   external IDs (`blocking=0`, the historical behavior) from real blockers
//!   (`blocking=1`).
//! - `resolved_at INTEGER NULL` — Unix-seconds timestamp matching the column
//!   shape used elsewhere in the schema (e.g. `imported_at`, `created_at`).
//!   `NULL` means "still blocking"; non-NULL means "resolved at this time"
//!   (history preserved).
//!
//! Extending the existing table keeps the pre-migration semantics for callers
//! that only use `external_id_added` / `external_id_removed` events — those
//! rows land with the column DEFAULT of `0`, never gating readiness. Callers
//! that opt in via the new `external_blocker_added` event get `blocking=1`.
//! A sibling table would have required a parallel insert path, double-source
//! lookups in `tasks.get`, and a more complex query change.
//!
//! ## Backfill stance
//!
//! No row migration is needed. The `NOT NULL DEFAULT 0` clause on `blocking`
//! handles every pre-existing row in a single ALTER TABLE — SQLite stamps
//! the default into existing rows automatically. `resolved_at` is `NULL` by
//! default for every row (new or existing).
//!
//! ## Rollback
//!
//! SQLite cannot `DROP COLUMN` on a table with foreign-key inbound references
//! without a full table-recreate. If a rollback is ever required: drop FKs,
//! rebuild `task_external_ids` from a SELECT that omits the two columns, and
//! restore FKs. No down migration is shipped here because the new columns are
//! additive and ignorable by readers that don't know about them.

use rusqlite::Connection;

use crate::error::Result;

/// Add `blocking` and `resolved_at` columns to `task_external_ids` so that
/// rows can act as first-class blockers when callers opt in. Stamp version 45.
pub fn migrate_v44_to_v45(conn: &Connection) -> Result<()> {
    // Idempotent: check whether the columns already exist before adding.
    let has_blocking = column_exists(conn, "task_external_ids", "blocking")?;
    let has_resolved_at = column_exists(conn, "task_external_ids", "resolved_at")?;

    let tx = conn.unchecked_transaction()?;

    if !has_blocking {
        tx.execute_batch(
            "ALTER TABLE task_external_ids
             ADD COLUMN blocking INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !has_resolved_at {
        tx.execute_batch(
            "ALTER TABLE task_external_ids
             ADD COLUMN resolved_at INTEGER;",
        )?;
    }

    tx.execute_batch("PRAGMA user_version = 45;")?;
    tx.commit()?;
    Ok(())
}

/// Helper: check whether `table.column` exists.
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
    use super::super::v43_to_v44::migrate_v43_to_v44;
    use super::*;

    /// Build a v44 fixture: empty schema with `task_external_ids` table in its
    /// pre-migration shape (no `blocking` / `resolved_at` columns).
    fn setup_v44() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        // We need the `tasks` and `task_external_ids` tables for the test —
        // initialize via the cumulative migration chain up to v44.
        super::super::migrate_v0_to_v1(&conn).unwrap();
        super::super::migrate_v1_to_v2(&conn).unwrap();
        super::super::migrate_v2_to_v3(&conn).unwrap();
        super::super::migrate_v3_to_v4(&conn).unwrap();
        super::super::migrate_v4_to_v5(&conn).unwrap();
        super::super::migrate_v5_to_v6(&conn).unwrap();
        super::super::migrate_v6_to_v7(&conn).unwrap();
        for n in 7..43 {
            apply_n_to_n_plus_1(&conn, n);
        }
        migrate_v43_to_v44(&conn).unwrap();
        conn
    }

    /// Apply migration `n -> n+1` by name. Used to drive the fixture chain
    /// without re-listing every migration in a giant match.
    fn apply_n_to_n_plus_1(conn: &Connection, n: i32) {
        use super::super::*;
        match n {
            7 => migrate_v7_to_v8(conn).unwrap(),
            8 => migrate_v8_to_v9(conn).unwrap(),
            9 => migrate_v9_to_v10(conn).unwrap(),
            10 => migrate_v10_to_v11(conn).unwrap(),
            11 => migrate_v11_to_v12(conn).unwrap(),
            12 => migrate_v12_to_v13(conn).unwrap(),
            13 => migrate_v13_to_v14(conn).unwrap(),
            14 => migrate_v14_to_v15(conn).unwrap(),
            15 => migrate_v15_to_v16(conn).unwrap(),
            16 => migrate_v16_to_v17(conn).unwrap(),
            17 => migrate_v17_to_v18(conn).unwrap(),
            18 => migrate_v18_to_v19(conn).unwrap(),
            19 => migrate_v19_to_v20(conn).unwrap(),
            20 => migrate_v20_to_v21(conn).unwrap(),
            21 => migrate_v21_to_v22(conn).unwrap(),
            22 => migrate_v22_to_v23(conn).unwrap(),
            23 => migrate_v23_to_v24(conn).unwrap(),
            24 => migrate_v24_to_v25(conn).unwrap(),
            25 => migrate_v25_to_v26(conn).unwrap(),
            26 => migrate_v26_to_v27(conn).unwrap(),
            27 => migrate_v27_to_v28(conn).unwrap(),
            28 => migrate_v28_to_v29(conn).unwrap(),
            29 => migrate_v29_to_v30(conn).unwrap(),
            30 => migrate_v30_to_v31(conn).unwrap(),
            31 => migrate_v31_to_v32(conn).unwrap(),
            32 => migrate_v32_to_v33(conn).unwrap(),
            33 => migrate_v33_to_v34(conn).unwrap(),
            34 => migrate_v34_to_v35(conn).unwrap(),
            35 => migrate_v35_to_v36(conn).unwrap(),
            36 => migrate_v36_to_v37(conn).unwrap(),
            37 => migrate_v37_to_v38(conn).unwrap(),
            38 => migrate_v38_to_v39(conn).unwrap(),
            39 => migrate_v39_to_v40(conn).unwrap(),
            40 => migrate_v40_to_v41(conn).unwrap(),
            41 => migrate_v41_to_v42(conn).unwrap(),
            42 => migrate_v42_to_v43(conn).unwrap(),
            other => panic!("apply_n_to_n_plus_1: no migration for {other}"),
        }
    }

    #[test]
    fn test_version_stamp() {
        let conn = setup_v44();
        migrate_v44_to_v45(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 45);
    }

    #[test]
    fn test_columns_added() {
        let conn = setup_v44();
        migrate_v44_to_v45(&conn).unwrap();
        assert!(column_exists(&conn, "task_external_ids", "blocking").unwrap());
        assert!(column_exists(&conn, "task_external_ids", "resolved_at").unwrap());
    }

    #[test]
    fn test_idempotent() {
        let conn = setup_v44();
        migrate_v44_to_v45(&conn).unwrap();
        // Re-running on a v45 connection must succeed without error.
        migrate_v44_to_v45(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 45);
    }

    #[test]
    fn test_existing_rows_default_blocking_false() {
        let conn = setup_v44();
        // Insert a pre-migration row (the v6→v7 shape: no blocking/resolved_at).
        conn.execute(
            "INSERT INTO task_external_ids (task_id, source, external_id, imported_at)
             VALUES ('legacy-task', 'github', 'GH-1', 1000)",
            [],
        )
        // The FK on task_id requires the parent row — it's NOT enforced when
        // inserting into a pre-existing row (no parent task), so we expect a
        // violation here. Disable FK to plant the test row.
        .unwrap_or_else(|_| {
            conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
            let n = conn
                .execute(
                    "INSERT INTO task_external_ids (task_id, source, external_id, imported_at)
                     VALUES ('legacy-task', 'github', 'GH-1', 1000)",
                    [],
                )
                .unwrap();
            conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
            n
        });

        migrate_v44_to_v45(&conn).unwrap();

        let blocking: i64 = conn
            .query_row(
                "SELECT blocking FROM task_external_ids WHERE task_id = 'legacy-task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(blocking, 0, "pre-existing rows must default to blocking=0");

        let resolved_at: Option<i64> = conn
            .query_row(
                "SELECT resolved_at FROM task_external_ids WHERE task_id = 'legacy-task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(resolved_at, None, "resolved_at must default to NULL");
    }

    #[test]
    fn test_can_insert_blocking_row() {
        let conn = setup_v44();
        migrate_v44_to_v45(&conn).unwrap();
        // Disable FK to skip parent-task check; we only care about column shape.
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT INTO task_external_ids
                 (task_id, source, external_id, imported_at, blocking, resolved_at)
             VALUES ('t1', 'jira', 'PROJ-1', 1000, 1, NULL)",
            [],
        )
        .unwrap();

        let (blocking, resolved_at): (i64, Option<i64>) = conn
            .query_row(
                "SELECT blocking, resolved_at FROM task_external_ids
                 WHERE task_id = 't1' AND source = 'jira' AND external_id = 'PROJ-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(blocking, 1);
        assert!(resolved_at.is_none());
    }
}
