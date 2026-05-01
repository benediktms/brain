//! v50 → v51: backfill polymorphic edges from legacy structures.
//!
//! Reads `tasks.parent_task_id`, `task_deps`, `record_links` and emits
//! corresponding `entity_links` rows via `INSERT OR IGNORE` on the unique
//! tuple `(from_type, from_id, to_type, to_id, edge_kind)`. The migration is
//! idempotent: safe to re-run after partial failure.
//!
//! ## Column mapping
//!
//! Source tables lack `brain_scope` — that column exists only on `entity_links`.
//! Backfilled rows carry `brain_scope = NULL` (nullable per schema). `task_deps`
//! also lacks `created_at`; backfilled blocks edges receive the migration
//! timestamp via `strftime`.
//!
//! ## Audit-trail carve-out
//!
//! This backfill bypasses the event-sourcing path — rows go directly into
//! `entity_links` without emitting `LinkCreated` events into the unified
//! event log. This is operationally correct (the rows pre-exist in the
//! legacy structures) but is a deliberate carve-out. After this migration,
//! `entity_links` contains a mix of:
//!
//! 1. Backfilled rows: id from `randomblob(16)`, no event-log lineage.
//! 2. Forward rows from Wave 4 dual-write: each has a corresponding
//!    `LinkCreated` event in the unified event log.
//!
//! Future audit tooling must distinguish these classes — backfilled rows
//! have no event-log preimage and are NOT replayable from event-source state.

use rusqlite::Connection;

use crate::error::Result;

pub fn migrate_v50_to_v51(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "
        -- parent_of edges (tasks.parent_task_id → entity_links)
        INSERT OR IGNORE INTO entity_links(id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
        SELECT lower(hex(randomblob(16))),
               'TASK', parent_task_id,
               'TASK', task_id,
               'parent_of',
               strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
               NULL
        FROM tasks
        WHERE parent_task_id IS NOT NULL;

        -- blocks edges (task_deps → entity_links)
        -- task_deps has no created_at or brain_scope; use migration timestamp and NULL scope.
        INSERT OR IGNORE INTO entity_links(id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
        SELECT lower(hex(randomblob(16))),
               'TASK', task_id,
               'TASK', depends_on,
               'blocks',
               strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
               NULL
        FROM task_deps;

        -- covers edges record → task (record_links with task_id NOT NULL)
        INSERT OR IGNORE INTO entity_links(id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
        SELECT lower(hex(randomblob(16))),
               'RECORD', record_id,
               'TASK', task_id,
               'covers',
               strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
               NULL
        FROM record_links
        WHERE task_id IS NOT NULL;

        -- covers edges record → chunk (record_links with chunk_id NOT NULL)
        INSERT OR IGNORE INTO entity_links(id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
        SELECT lower(hex(randomblob(16))),
               'RECORD', record_id,
               'CHUNK', chunk_id,
               'covers',
               strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
               NULL
        FROM record_links
        WHERE chunk_id IS NOT NULL;

        PRAGMA user_version = 51;
        ",
    )?;

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // Minimal smoke tests; bulk fixture coverage lives in
    // tests/migration_v51_fixture_test.rs
    use super::*;
    use rusqlite::Connection;

    fn minimal_v50_schema(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE tasks (
                 task_id        TEXT PRIMARY KEY,
                 parent_task_id TEXT
             );
             CREATE TABLE task_deps (
                 task_id    TEXT NOT NULL,
                 depends_on TEXT NOT NULL,
                 PRIMARY KEY (task_id, depends_on)
             );
             CREATE TABLE record_links (
                 record_id  TEXT NOT NULL,
                 task_id    TEXT,
                 chunk_id   TEXT,
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE entity_links (
                 id         TEXT PRIMARY KEY,
                 from_type  TEXT NOT NULL,
                 from_id    TEXT NOT NULL,
                 to_type    TEXT NOT NULL,
                 to_id      TEXT NOT NULL,
                 edge_kind  TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 brain_scope TEXT,
                 CHECK(NOT (from_type = to_type AND from_id = to_id))
             );
             CREATE UNIQUE INDEX idx_entity_links_unique
                 ON entity_links(from_type, from_id, to_type, to_id, edge_kind);
             PRAGMA user_version = 50;",
        )
        .unwrap();
    }

    #[test]
    fn test_empty_tables_migrates_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        minimal_v50_schema(&conn);

        migrate_v50_to_v51(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 51);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_links", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_idempotent_on_empty() {
        let conn = Connection::open_in_memory().unwrap();
        minimal_v50_schema(&conn);
        migrate_v50_to_v51(&conn).unwrap();

        conn.pragma_update(None, "user_version", 50i32).unwrap();
        migrate_v50_to_v51(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_links", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
