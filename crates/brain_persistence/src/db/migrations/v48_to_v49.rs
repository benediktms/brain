//! v48 → v49: polymorphic `entity_links` table with full index set.
//!
//! **Note:** The two partial indexes introduced here (`idx_entity_links_blocks_partial`
//! and `idx_entity_links_parent_of_partial`) were subsequently dropped in v50.
//! The SQLite query planner never selected them — the covering composite
//! `idx_entity_links_unique` suffices for all hot-path queries.
//!
//! ## Purpose
//!
//! Introduces a single edge table that unifies all cross-entity relationships
//! (task dependencies, parent/child hierarchy, record-to-record links, etc.)
//! under one polymorphic graph. The table is named `entity_links` to avoid
//! collision with the existing `links` table (note/wiki file-linking, v0).
//! Readers and writers are wired in downstream tasks — this migration is
//! schema-only.
//!
//! ## Schema
//!
//! | Column       | Type          | Notes                                            |
//! |---|---|---|
//! | `id`         | TEXT NOT NULL | ULID primary key.                                |
//! | `from_type`  | TEXT NOT NULL | Source entity type (TASK, RECORD, EPISODE, …).   |
//! | `from_id`    | TEXT NOT NULL | Source entity ID.                                |
//! | `to_type`    | TEXT NOT NULL | Target entity type.                              |
//! | `to_id`      | TEXT NOT NULL | Target entity ID.                                |
//! | `edge_kind`  | TEXT NOT NULL | Relationship kind (blocks, parent_of, covers, …).|
//! | `created_at` | TEXT NOT NULL | ISO 8601 timestamp.                              |
//! | `brain_scope`| TEXT          | Nullable; cross-brain edges stored unscoped.     |
//!
//! ## Indexes
//!
//! - `idx_entity_links_unique` — unique composite on (from_type, from_id, to_type, to_id, edge_kind).
//! - `idx_entity_links_outgoing` — outgoing traversal by (from_type, from_id, edge_kind).
//! - `idx_entity_links_incoming` — incoming traversal by (to_type, to_id, edge_kind).
//! - `idx_entity_links_blocks_partial` — partial index for TASK→TASK `blocks` edges (hot path).
//! - `idx_entity_links_parent_of_partial` — partial index for TASK→TASK `parent_of` edges (hot path).

use rusqlite::Connection;

use crate::error::Result;

/// Create the `entity_links` polymorphic edge table and its indexes, then stamp version 49.
pub fn migrate_v48_to_v49(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS entity_links (
             id          TEXT PRIMARY KEY,
             from_type   TEXT NOT NULL,
             from_id     TEXT NOT NULL,
             to_type     TEXT NOT NULL,
             to_id       TEXT NOT NULL,
             edge_kind   TEXT NOT NULL,
             created_at  TEXT NOT NULL,
             brain_scope TEXT,
             CHECK (NOT (from_type = to_type AND from_id = to_id))
         );

         CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_links_unique
             ON entity_links(from_type, from_id, to_type, to_id, edge_kind);

         CREATE INDEX IF NOT EXISTS idx_entity_links_outgoing
             ON entity_links(from_type, from_id, edge_kind);

         CREATE INDEX IF NOT EXISTS idx_entity_links_incoming
             ON entity_links(to_type, to_id, edge_kind);

         CREATE INDEX IF NOT EXISTS idx_entity_links_blocks_partial
             ON entity_links(from_id, to_id)
             WHERE from_type = 'TASK' AND to_type = 'TASK' AND edge_kind = 'blocks';

         CREATE INDEX IF NOT EXISTS idx_entity_links_parent_of_partial
             ON entity_links(from_id, to_id)
             WHERE from_type = 'TASK' AND to_type = 'TASK' AND edge_kind = 'parent_of';",
    )?;

    tx.execute_batch("PRAGMA user_version = 49;")?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Per-version invariants previously asserted here have been moved to
    // dedicated fixture tests. The v49-shape table and indexes are covered by
    // `tests/migration_v49_fixture_test.rs`; the v50 amendment that drops the
    // partial indexes is covered by `tests/migration_v50_fixture_test.rs`.
    // Re-running migrations to current and asserting v49 specifics here became
    // brittle once v50 shipped.

    #[test]
    fn self_loop_check_constraint_fires() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        crate::db::schema::run_migrations(&conn, 0).unwrap();

        let result = conn.execute(
            "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at)
             VALUES ('01JSELF', 'TASK', 'task-1', 'TASK', 'task-1', 'blocks', '2026-05-01T00:00:00Z')",
            [],
        );
        match result {
            Err(rusqlite::Error::SqliteFailure(ffi_err, _))
                if ffi_err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK => {}
            other => panic!("expected SQLITE_CONSTRAINT_CHECK (275); got {other:?}"),
        }
    }
}
