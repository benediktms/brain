use std::collections::HashSet;

use rusqlite::Connection;

use crate::db::short_id::{blake3_short_hex, pick_unique_prefix};
use crate::sql::SqlResult;

/// Compute a collision-safe `display_id` for `task_id` in `target_brain_id`.
///
/// Reads all existing `display_id` values for the target brain inside the
/// provided connection (must be called within a transaction that already holds
/// the write lock), then delegates to [`pick_unique_prefix`].
pub fn compute_display_id_for_target(
    conn: &Connection,
    task_id: &str,
    target_brain_id: &str,
) -> SqlResult<String> {
    let used: HashSet<String> = {
        let mut stmt = conn.prepare_cached(
            "SELECT display_id FROM tasks WHERE brain_id = ?1 AND display_id IS NOT NULL",
        )?;
        stmt.query_map(rusqlite::params![target_brain_id], |row| row.get(0))?
            .collect::<std::result::Result<HashSet<_>, _>>()?
    };

    let full_hex = blake3_short_hex(task_id);
    Ok(pick_unique_prefix(&full_hex, &used))
}
