use rusqlite::Connection;

use crate::error::Result;

/// Add parent_task_id column for task hierarchy (parent-child relationships).
/// Stamp version 4.
pub fn migrate_v3_to_v4(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        ALTER TABLE tasks ADD COLUMN parent_task_id TEXT REFERENCES tasks(task_id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id);

        PRAGMA user_version = 4;

        COMMIT;
        ",
    )?;
    Ok(())
}
