use rusqlite::Connection;

use crate::error::Result;

/// Add performance indexes for common task queries.
/// Stamp version 5.
pub fn migrate_v4_to_v5(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_defer_until ON tasks(defer_until);
        CREATE INDEX IF NOT EXISTS idx_task_deps_depends_on ON task_deps(depends_on);
        CREATE INDEX IF NOT EXISTS idx_task_comments_task_id ON task_comments(task_id);
        CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

        PRAGMA user_version = 5;

        COMMIT;
        ",
    )?;
    Ok(())
}
