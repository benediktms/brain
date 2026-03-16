use rusqlite::Connection;

use crate::error::Result;

/// Add `task_external_ids` table for multi-source import tracking.
/// Stamp version 7.
pub fn migrate_v6_to_v7(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE IF NOT EXISTS task_external_ids (
            task_id      TEXT NOT NULL REFERENCES tasks(task_id),
            source       TEXT NOT NULL,
            external_id  TEXT NOT NULL,
            external_url TEXT,
            imported_at  INTEGER NOT NULL,
            PRIMARY KEY (task_id, source, external_id)
        );

        CREATE INDEX IF NOT EXISTS idx_external_lookup
            ON task_external_ids (source, external_id);

        PRAGMA user_version = 7;

        COMMIT;
        ",
    )?;
    Ok(())
}
