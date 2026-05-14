//! Writers for the `task_note_links` table.

use rusqlite::Connection;

use crate::sql::SqlResult;

/// INSERT OR IGNORE into task_note_links.
pub fn link_note(conn: &Connection, task_id: &str, chunk_id: &str) -> SqlResult<()> {
    conn.execute(
        "INSERT OR IGNORE INTO task_note_links (task_id, chunk_id) VALUES (?1, ?2)",
        rusqlite::params![task_id, chunk_id],
    )?;
    Ok(())
}

/// DELETE from task_note_links.
pub fn unlink_note(conn: &Connection, task_id: &str, chunk_id: &str) -> SqlResult<()> {
    conn.execute(
        "DELETE FROM task_note_links WHERE task_id = ?1 AND chunk_id = ?2",
        rusqlite::params![task_id, chunk_id],
    )?;
    Ok(())
}
