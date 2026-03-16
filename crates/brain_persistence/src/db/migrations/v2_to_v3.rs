use rusqlite::Connection;

use crate::error::Result;

/// Add task fields (type, assignee, defer_until) and tables (labels, comments).
/// Stamp version 3.
pub fn migrate_v2_to_v3(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        -- New scalar columns on tasks
        -- task_type already defined in v1 CREATE TABLE
        ALTER TABLE tasks ADD COLUMN assignee TEXT;
        ALTER TABLE tasks ADD COLUMN defer_until INTEGER;

        -- Labels (many-to-many)
        CREATE TABLE IF NOT EXISTS task_labels (
            task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            label   TEXT NOT NULL,
            PRIMARY KEY (task_id, label)
        );

        -- Comments (append-only)
        CREATE TABLE IF NOT EXISTS task_comments (
            comment_id TEXT PRIMARY KEY,
            task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            author     TEXT NOT NULL,
            body       TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );

        PRAGMA user_version = 3;

        COMMIT;
        ",
    )?;
    Ok(())
}
