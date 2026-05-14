//! v54 → v55: enforce uniqueness of `brains.prefix` via a partial UNIQUE index.
//!
//! Duplicate prefixes silently misroute `resolve_brain_from_prefix` (the
//! resolver does `LIMIT 1` and picks an arbitrary row), which surfaces as
//! the misleading `"prefix too short: need at least 4 characters after 'X-'"`
//! error on every mutation API when the picked brain's tasks don't contain
//! the requested display_id. The structural fix: make duplicate prefixes
//! impossible at the schema level.
//!
//! The index is partial (`WHERE prefix IS NOT NULL`) because some legacy
//! brain rows have NULL prefixes — those are not in the routing path and
//! should not be constrained.
//!
//! Migration aborts with a clear error if duplicates exist in the data,
//! pointing the operator at the rows that need reconciliation before the
//! migration can run.

use rusqlite::Connection;

use crate::error::BrainCoreError;
use crate::sql::{SqlError, SqlResult};

pub fn migrate_v54_to_v55(conn: &Connection) -> SqlResult<()> {
    let tx = conn.unchecked_transaction()?;

    // Refuse to run if data has duplicates — caller must reconcile first.
    // Collects all duplicate prefixes (with the brain ids that share them)
    // into the error so the operator gets actionable detail.
    let duplicates: Vec<(String, i64)> = {
        let mut stmt = tx.prepare(
            "SELECT prefix, COUNT(*) AS cnt FROM brains
             WHERE prefix IS NOT NULL
             GROUP BY prefix
             HAVING cnt > 1
             ORDER BY prefix",
        )?;
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };

    if !duplicates.is_empty() {
        let mut detail = String::from(
            "v54→v55 cannot enforce UNIQUE(prefix) — these prefixes are used by multiple brains:\n",
        );
        for (prefix, cnt) in &duplicates {
            let names: Vec<String> = {
                let mut stmt = tx.prepare(
                    "SELECT brain_id || ' (' || name || ')' FROM brains WHERE prefix = ?1",
                )?;
                stmt.query_map([prefix], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            detail.push_str(&format!("  {prefix} × {cnt}: {}\n", names.join(", ")));
        }
        detail.push_str(
            "Reconcile by renaming the prefix of all but one brain, then retry the migration.",
        );
        return Err(SqlError::Domain(BrainCoreError::Migration(detail)));
    }

    // Idempotent: `IF NOT EXISTS` handles fixture replays that bump
    // user_version without the index actually existing yet.
    tx.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_brains_prefix
         ON brains(prefix) WHERE prefix IS NOT NULL;",
    )?;

    tx.pragma_update(None, "user_version", 55i32)?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OptionalExtension};

    /// Convenience accessor used in tests below; keeps the test code readable.
    fn index_exists(conn: &Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?1",
            [name],
            |_| Ok(()),
        )
        .optional()
        .unwrap()
        .is_some()
    }

    fn fresh_v54(conn: &Connection) {
        // Walk the schema up to current head (v55) via init_schema, then
        // tear down what v54→v55 created and roll the version back to 54
        // so this migration is the next one applied. The brains table and
        // all its dependencies were already created by earlier migrations.
        crate::db::schema::init_schema(conn).unwrap();
        conn.execute_batch("DROP INDEX IF EXISTS idx_brains_prefix;")
            .unwrap();
        conn.pragma_update(None, "user_version", 54i32).unwrap();
    }

    fn insert_brain(conn: &Connection, brain_id: &str, name: &str, prefix: Option<&str>) {
        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES (?1, ?2, ?3, 1000)",
            rusqlite::params![brain_id, name, prefix],
        )
        .unwrap();
    }

    #[test]
    fn empty_table_creates_index_and_bumps_version() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);

        migrate_v54_to_v55(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 55);
        assert!(index_exists(&conn, "idx_brains_prefix"));
    }

    #[test]
    fn unique_prefixes_pass_migration() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_brain(&conn, "b1", "alpha", Some("ALP"));
        insert_brain(&conn, "b2", "beta", Some("BET"));
        insert_brain(&conn, "b3", "gamma", Some("GAM"));

        migrate_v54_to_v55(&conn).unwrap();

        assert!(index_exists(&conn, "idx_brains_prefix"));
    }

    #[test]
    fn null_prefix_rows_do_not_block_migration() {
        // Legacy brains with NULL prefix must not trip the partial UNIQUE check.
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_brain(&conn, "b1", "alpha", None);
        insert_brain(&conn, "b2", "beta", None);
        insert_brain(&conn, "b3", "gamma", Some("GAM"));

        migrate_v54_to_v55(&conn).unwrap();

        assert!(index_exists(&conn, "idx_brains_prefix"));
    }

    #[test]
    fn duplicate_prefixes_abort_migration_with_detail() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_brain(&conn, "b1", "alpha", Some("DUP"));
        insert_brain(&conn, "b2", "beta", Some("DUP"));

        let err = migrate_v54_to_v55(&conn).expect_err("duplicates must abort migration");
        let msg = err.to_string();
        assert!(
            msg.contains("DUP"),
            "error must name the duplicate prefix; got: {msg}"
        );
        assert!(
            msg.contains("b1"),
            "error must list offending brain_ids; got: {msg}"
        );
        assert!(
            msg.contains("b2"),
            "error must list offending brain_ids; got: {msg}"
        );
        // Migration did not run — version stays at 54 and the index is absent.
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 54);
        assert!(!index_exists(&conn, "idx_brains_prefix"));
    }

    #[test]
    fn post_migration_duplicate_insert_is_rejected() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_brain(&conn, "b1", "alpha", Some("ALP"));

        migrate_v54_to_v55(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('b2', 'duplicate', 'ALP', 2000)",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate prefix INSERT must fail the UNIQUE constraint"
        );
    }

    #[test]
    fn idempotent_replay() {
        // Re-running the migration after success must not error (fixture replays
        // that bump user_version then re-run init_schema rely on this).
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_brain(&conn, "b1", "alpha", Some("ALP"));

        migrate_v54_to_v55(&conn).unwrap();
        // Roll version back and re-run.
        conn.pragma_update(None, "user_version", 54i32).unwrap();
        migrate_v54_to_v55(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 55);
    }
}
