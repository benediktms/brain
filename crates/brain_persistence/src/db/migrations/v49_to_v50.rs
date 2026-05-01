//! v49 → v50: drop the two partial indexes on `entity_links` that the SQLite
//! query planner never selects.
//!
//! ## Rationale
//!
//! Sentinel D empirically confirmed (on populated tables, post-ANALYZE) that
//! `idx_entity_links_blocks_partial` and `idx_entity_links_parent_of_partial`
//! are never chosen by the planner. The covering composite index
//! `idx_entity_links_unique` wins every hot-path query. Dead indexes impose
//! write-amplification and page-cache pressure with zero read benefit. They
//! are dropped here.
//!
//! The remaining indexes (`idx_entity_links_unique`, `idx_entity_links_outgoing`,
//! `idx_entity_links_incoming`) are unaffected.

use rusqlite::Connection;

use crate::error::Result;

/// Drop the two dead partial indexes on `entity_links` and stamp version 50.
pub fn migrate_v49_to_v50(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "DROP INDEX IF EXISTS idx_entity_links_blocks_partial;
         DROP INDEX IF EXISTS idx_entity_links_parent_of_partial;
         PRAGMA user_version = 50;",
    )?;

    tx.commit()?;
    Ok(())
}
