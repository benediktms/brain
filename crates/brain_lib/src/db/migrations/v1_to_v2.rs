use rusqlite::Connection;

use crate::error::Result;

/// Add task tables and stamp version 2.
pub fn migrate_v1_to_v2(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE IF NOT EXISTS tasks (
            task_id        TEXT PRIMARY KEY,
            title          TEXT NOT NULL,
            description    TEXT,
            status         TEXT NOT NULL DEFAULT 'open'
                           CHECK(status IN ('open','in_progress','blocked','done','cancelled')),
            priority       INTEGER NOT NULL DEFAULT 4 CHECK(priority BETWEEN 0 AND 4),
            blocked_reason TEXT,
            due_ts         INTEGER,
            created_at     INTEGER NOT NULL,
            updated_at     INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_deps (
            task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            depends_on TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            PRIMARY KEY (task_id, depends_on),
            CHECK(task_id != depends_on)
        );

        CREATE TABLE IF NOT EXISTS task_note_links (
            task_id  TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            chunk_id TEXT NOT NULL,
            PRIMARY KEY (task_id, chunk_id)
        );

        CREATE TABLE IF NOT EXISTS task_events (
            event_id   TEXT PRIMARY KEY,
            task_id    TEXT NOT NULL,
            event_type TEXT NOT NULL,
            timestamp  INTEGER NOT NULL,
            actor      TEXT NOT NULL,
            payload    TEXT NOT NULL DEFAULT '{}'
        );

        PRAGMA user_version = 2;

        COMMIT;
        ",
    )?;
    Ok(())
}
