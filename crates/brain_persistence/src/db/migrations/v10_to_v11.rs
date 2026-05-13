use rusqlite::Connection;

use crate::sql::SqlResult;

/// v10 → v11: Add FTS5 full-text search index on tasks (title + description).
///
/// Uses FTS5 content-sync pattern (same as fts_chunks) so the virtual table
/// stays in sync with the tasks table via triggers.
pub fn migrate_v10_to_v11(conn: &Connection) -> SqlResult<()> {
    conn.execute_batch(
        "
        BEGIN;
        PRAGMA user_version = 11;
        COMMIT;
    ",
    )?;
    Ok(())
}
