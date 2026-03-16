use rusqlite::Connection;

use crate::error::Result;

/// v8 → v9: Add chunker_version to files table.
///
/// Tracks which chunker algorithm version was used to chunk each file.
/// NULL means "unknown" → forces re-chunking on next scan.
pub fn migrate_v8_to_v9(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;
        ALTER TABLE files ADD COLUMN chunker_version INTEGER;
        PRAGMA user_version = 9;
        COMMIT;
    ",
    )?;
    Ok(())
}
