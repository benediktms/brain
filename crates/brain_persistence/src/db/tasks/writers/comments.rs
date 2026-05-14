//! Writers for the `task_comments` table.

use rusqlite::Connection;

use crate::error::BrainCoreError;
use crate::sql::{SqlError, SqlResult};

/// INSERT into task_comments.
pub fn add_comment(
    conn: &Connection,
    comment_id: &str,
    task_id: &str,
    author: &str,
    body: &str,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO task_comments (comment_id, task_id, author, body, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![comment_id, task_id, author, body, ts],
    )?;
    Ok(())
}

/// UPDATE task_comments SET body, updated_at WHERE comment_id AND task_id.
///
/// Returns `Err(SqlError::Domain(...))` if no row was affected (comment not found).
pub fn update_comment(
    conn: &Connection,
    comment_id: &str,
    task_id: &str,
    body: &str,
    ts: i64,
) -> SqlResult<()> {
    let rows_affected = conn.execute(
        "UPDATE task_comments SET body = ?1, updated_at = ?2
         WHERE comment_id = ?3 AND task_id = ?4",
        rusqlite::params![body, ts, comment_id, task_id],
    )?;
    if rows_affected == 0 {
        return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
            "comment_id '{comment_id}' not found for task '{task_id}'"
        ))));
    }
    Ok(())
}
