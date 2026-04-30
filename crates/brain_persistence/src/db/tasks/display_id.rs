use std::collections::HashSet;

use rusqlite::Connection;

use crate::error::Result;

use super::queries::{MIN_SHORT_HASH_LEN, blake3_short_hex};

/// Pick the shortest unique prefix of `full_hex` not already present in `used`.
///
/// Starts at `MIN_SHORT_HASH_LEN` and extends until a free slot is found.
/// If all positions are exhausted (extremely unlikely with 64 hex chars), the
/// full hex string is returned as-is — the caller's UNIQUE constraint INSERT
/// will surface any remaining collision.
pub fn pick_unique_prefix(full_hex: &str, used: &HashSet<String>) -> String {
    for len in MIN_SHORT_HASH_LEN..=full_hex.len() {
        let candidate = &full_hex[..len];
        if !used.contains(candidate) {
            return candidate.to_string();
        }
    }
    full_hex.to_string()
}

/// Compute a collision-safe `display_id` for `task_id` in `target_brain_id`.
///
/// Reads all existing `display_id` values for the target brain inside the
/// provided connection (must be called within a transaction that already holds
/// the write lock), then delegates to [`pick_unique_prefix`].
pub fn compute_display_id_for_target(
    conn: &Connection,
    task_id: &str,
    target_brain_id: &str,
) -> Result<String> {
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
