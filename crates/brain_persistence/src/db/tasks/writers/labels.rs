//! Writers for the `task_labels` table.

use rusqlite::Connection;

use crate::sql::SqlResult;

/// INSERT OR IGNORE into task_labels.
pub fn add_label(conn: &Connection, task_id: &str, label: &str) -> SqlResult<()> {
    conn.execute(
        "INSERT OR IGNORE INTO task_labels (task_id, label) VALUES (?1, ?2)",
        rusqlite::params![task_id, label],
    )?;
    Ok(())
}

/// DELETE from task_labels.
pub fn remove_label(conn: &Connection, task_id: &str, label: &str) -> SqlResult<()> {
    conn.execute(
        "DELETE FROM task_labels WHERE task_id = ?1 AND label = ?2",
        rusqlite::params![task_id, label],
    )?;
    Ok(())
}
