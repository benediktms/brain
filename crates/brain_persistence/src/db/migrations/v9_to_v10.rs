use rusqlite::Connection;

use crate::error::Result;

/// v9 → v10: Add composite index on tasks(status, priority, task_type).
///
/// Speeds up all filtered+sorted list queries that filter by status
/// and ORDER BY priority with epic CASE expression.
pub fn migrate_v9_to_v10(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;
        CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
            ON tasks(status, priority, task_type);
        PRAGMA user_version = 10;
        COMMIT;
    ",
    )?;
    Ok(())
}
