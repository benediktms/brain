use rusqlite::Connection;

use crate::error::Result;

/// Add `brain_meta` key-value table for project configuration.
/// Stamp version 6.
pub fn migrate_v5_to_v6(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE IF NOT EXISTS brain_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        PRAGMA user_version = 6;

        COMMIT;
        ",
    )?;
    Ok(())
}
