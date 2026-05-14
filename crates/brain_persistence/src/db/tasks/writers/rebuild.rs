//! Writers for bulk-rebuild operations (FTS triggers, table clearing).

use rusqlite::Connection;

use crate::sql::SqlResult;

/// Drop the three FTS content-sync triggers before a bulk rebuild.
///
/// Called before clearing projection tables to avoid content-sync deletes
/// on a potentially corrupt FTS index.
pub fn drop_fts_triggers(conn: &Connection) -> SqlResult<()> {
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS tasks_fts_insert;
         DROP TRIGGER IF EXISTS tasks_fts_delete;
         DROP TRIGGER IF EXISTS tasks_fts_update;",
    )?;
    Ok(())
}

/// DELETE all rows from projection tables in FK-safe order.
///
/// Must be called after `drop_fts_triggers` (no FTS triggers fire during the
/// bulk delete). Accepts a `&Connection` which may be a transaction — callers
/// pass the transaction handle directly.
pub fn rebuild_clear_all(conn: &Connection) -> SqlResult<()> {
    conn.execute_batch(
        "DELETE FROM task_events;
         DELETE FROM task_comments;
         DELETE FROM task_labels;
         DELETE FROM task_note_links;
         DELETE FROM task_external_ids;
         DELETE FROM task_deps;
         DELETE FROM tasks;",
    )?;
    Ok(())
}

/// Trigger an FTS index rebuild from the content table.
///
/// Must be called outside any open transaction.
pub fn rebuild_fts_index(conn: &Connection) -> SqlResult<()> {
    conn.execute("INSERT INTO fts_tasks(fts_tasks) VALUES('rebuild')", [])?;
    Ok(())
}
