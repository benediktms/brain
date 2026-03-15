use rusqlite::Connection;

use crate::error::Result;

/// v7 → v8: Add child_seq column for hierarchical display IDs.
///
/// - `child_seq` stores the 1-based ordinal among siblings (NULL for root tasks).
///   Enables display IDs like `PARENT_PREFIX.1`, `PARENT_PREFIX.2`, etc.
pub fn migrate_v7_to_v8(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;
        ALTER TABLE tasks ADD COLUMN child_seq INTEGER;
        PRAGMA user_version = 8;
        COMMIT;
    ",
    )?;
    Ok(())
}
