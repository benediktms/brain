use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use super::listing::{get_task, task_exists};
use crate::db::meta;
use crate::error::{BrainCoreError, Result};

/// Minimum ULID prefix length (after project prefix + separator).
const MIN_ULID_PREFIX_LEN: usize = 4;

/// Minimum display prefix: "BRN-" (4) + 4 ULID chars = 8.
pub(crate) const MIN_DISPLAY_PREFIX_LEN: usize = 8;

/// Get the next child_seq for a parent task (max existing + 1, or 1 if no children).
pub fn next_child_seq(conn: &Connection, parent_task_id: &str) -> Result<i64> {
    let max: Option<i64> = conn
        .query_row(
            "SELECT MAX(child_seq) FROM tasks WHERE parent_task_id = ?1",
            [parent_task_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    Ok(max.unwrap_or(0) + 1)
}

pub fn resolve_task_id(conn: &Connection, input: &str) -> Result<String> {
    // Fast path: exact match
    if task_exists(conn, input)? {
        return Ok(input.to_string());
    }

    // Check for hierarchical display ID: "PREFIX.N" where N is child_seq
    if let Some(dot_pos) = input.rfind('.') {
        let parent_part = &input[..dot_pos];
        let seq_part = &input[dot_pos + 1..];
        if let Ok(seq) = seq_part.parse::<i64>() {
            // Resolve the parent prefix first (recursive)
            if let Ok(parent_id) = resolve_task_id(conn, parent_part) {
                let child: Option<String> = conn
                    .query_row(
                        "SELECT task_id FROM tasks WHERE parent_task_id = ?1 AND child_seq = ?2",
                        rusqlite::params![parent_id, seq],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(child_id) = child {
                    return Ok(child_id);
                }
            }
        }
    }

    let normalized = input.to_ascii_uppercase();

    // Determine if this looks like a prefixed ID (has a dash after position 0)
    // or a bare ULID prefix. Legacy UUIDs also have dashes but at position 8.
    let search_prefix = match normalized.find('-') {
        Some(dash_pos) if dash_pos <= 4 => {
            // Looks like a project prefix (1-4 chars before dash), e.g. "BRN-01JPH..."
            let ulid_part = &normalized[dash_pos + 1..];
            if ulid_part.len() < MIN_ULID_PREFIX_LEN {
                return Err(BrainCoreError::TaskEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters after '{}'",
                    &normalized[..=dash_pos]
                )));
            }
            normalized
        }
        Some(_) => {
            // Legacy UUID format (dash at position 8, e.g. "019571A8-...") — search as-is
            normalized
        }
        None => {
            // No dash — bare ULID prefix, auto-prepend project prefix
            if normalized.len() < MIN_ULID_PREFIX_LEN {
                return Err(BrainCoreError::TaskEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters, got {}",
                    normalized.len()
                )));
            }
            let prefix =
                meta::get_meta(conn, "project_prefix")?.unwrap_or_else(|| "BRN".to_string());
            format!("{prefix}-{normalized}")
        }
    };

    // Range scan on PRIMARY KEY B-tree
    let upper_bound = increment_string(&search_prefix);
    let mut stmt =
        conn.prepare("SELECT task_id, title FROM tasks WHERE task_id >= ?1 AND task_id < ?2")?;
    let matches: Vec<(String, String)> = stmt
        .query_map(rusqlite::params![search_prefix, upper_bound], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    match matches.len() {
        0 => Err(BrainCoreError::TaskEvent(format!(
            "no task found matching prefix: {input}"
        ))),
        1 => Ok(matches.into_iter().next().unwrap().0),
        n => {
            let candidates: Vec<String> = matches
                .iter()
                .map(|(id, title)| format!("  {id} — {title}"))
                .collect();
            Err(BrainCoreError::TaskEvent(format!(
                "ambiguous prefix '{input}': matches {n} tasks:\n{}",
                candidates.join("\n")
            )))
        }
    }
}

/// Core ULID-based prefix computation without dot notation.
fn compact_id_ulid(conn: &Connection, task_id: &str) -> Result<String> {
    let prev: Option<String> = conn
        .query_row(
            "SELECT task_id FROM tasks WHERE task_id < ?1 ORDER BY task_id DESC LIMIT 1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;
    let next: Option<String> = conn
        .query_row(
            "SELECT task_id FROM tasks WHERE task_id > ?1 ORDER BY task_id ASC LIMIT 1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;

    let min_prev = prev
        .as_deref()
        .map(|p| common_prefix_len(task_id, p) + 1)
        .unwrap_or(1);
    let min_next = next
        .as_deref()
        .map(|n| common_prefix_len(task_id, n) + 1)
        .unwrap_or(1);

    let min_len = min_prev
        .max(min_next)
        .max(MIN_DISPLAY_PREFIX_LEN)
        .min(task_id.len());

    Ok(task_id[..min_len].to_string())
}

/// Compute a compact display ID for a single task.
///
/// For tasks with a parent and `child_seq`, always returns dot notation
/// (e.g. "BRN-01KK7NY.3"). Recurses through the parent chain so
/// grandchildren get "BRN-XXX.1.2". For root tasks, uses the shortest
/// unique ULID prefix via O(log n) index seeks.
pub fn compact_id(conn: &Connection, task_id: &str) -> Result<String> {
    // Dot notation for any child with parent + child_seq
    if let Some(task) = get_task(conn, task_id)?
        && let (Some(parent_id), Some(seq)) = (&task.parent_task_id, task.child_seq)
    {
        let parent_compact = compact_id(conn, parent_id)?;
        return Ok(format!("{parent_compact}.{seq}"));
    }

    compact_id_ulid(conn, task_id)
}

/// Compute compact display IDs for all tasks (batch, for list display).
///
/// Loads all IDs sorted, compares neighbors for ULID prefixes. Then applies
/// dot notation for every child with `parent_task_id` + `child_seq`,
/// processing parents before children so grandchild IDs resolve correctly.
pub fn compact_ids(conn: &Connection) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT task_id FROM tasks ORDER BY task_id")?;
    let ids: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut result = HashMap::new();
    let n = ids.len();

    for i in 0..n {
        let id = &ids[i];
        let prev = if i > 0 {
            Some(ids[i - 1].as_str())
        } else {
            None
        };
        let next = if i + 1 < n {
            Some(ids[i + 1].as_str())
        } else {
            None
        };

        // Find the minimum length to distinguish from both neighbors
        let min_len_prev = prev.map(|p| common_prefix_len(id, p) + 1).unwrap_or(1);
        let min_len_next = next.map(|nx| common_prefix_len(id, nx) + 1).unwrap_or(1);

        let min_len = min_len_prev.max(min_len_next).max(MIN_DISPLAY_PREFIX_LEN);
        let prefix_len = min_len.min(id.len());

        result.insert(id.clone(), id[..prefix_len].to_string());
    }

    // Apply dot notation for all children with parent + child_seq.
    // Process in parent-first order so transitive chains resolve correctly.
    let mut child_stmt = conn.prepare(
        "SELECT task_id, parent_task_id, child_seq
         FROM tasks
         WHERE parent_task_id IS NOT NULL AND child_seq IS NOT NULL
         ORDER BY parent_task_id, child_seq",
    )?;
    let children: Vec<(String, String, i64)> = child_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Multiple passes to handle transitive chains (parent → child → grandchild).
    // Each pass resolves one level of nesting; stop when no changes occur.
    let mut changed = true;
    while changed {
        changed = false;
        for (child_id, parent_id, seq) in &children {
            if let Some(parent_compact) = result.get(parent_id).cloned() {
                let dot_form = format!("{parent_compact}.{seq}");
                if result.get(child_id) != Some(&dot_form) {
                    result.insert(child_id.clone(), dot_form);
                    changed = true;
                }
            }
        }
    }

    Ok(result)
}

/// Increment the last byte of a string for exclusive upper bounds in range scans.
///
/// Example: `"BRN-01JP"` → `"BRN-01JQ"`
///
/// Precondition: `s` must be ASCII (ULID chars are Crockford Base32 `0-9A-Z`,
/// project prefixes are `A-Z`, and legacy UUIDs are `0-9a-f-`). All bytes are
/// in the `0x00..0x7E` range so incrementing always produces valid UTF-8.
/// If a non-ASCII byte is encountered, the fallback appends `\u{FFFF}`.
fn increment_string(s: &str) -> String {
    debug_assert!(s.is_ascii(), "increment_string expects ASCII input");
    let mut bytes = s.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            return String::from_utf8(bytes).unwrap_or_else(|_| format!("{s}\u{FFFF}"));
        }
        bytes[i] = 0;
    }
    // All 0xFF — append a high character as upper bound
    format!("{s}\u{FFFF}")
}

/// Length of the common byte prefix between two strings.
///
/// Uses byte comparison, which is correct and safe for ASCII strings (ULIDs,
/// project prefixes, UUIDs). For non-ASCII task IDs, this still returns a
/// valid byte offset since it only counts matching bytes.
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes()
        .zip(b.bytes())
        .take_while(|(ba, bb)| ba == bb)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_string_basic() {
        assert_eq!(increment_string("BRN-01JP"), "BRN-01JQ");
        assert_eq!(increment_string("A"), "B");
        assert_eq!(increment_string("Z"), "["); // Z (0x5A) + 1 = [ (0x5B)
    }

    #[test]
    fn test_increment_string_carry() {
        // 0xFF bytes carry over to the next position
        let result = increment_string("\u{7f}"); // DEL (0x7F) → 0x80 (invalid UTF-8)
        // Falls back to appending \u{FFFF} since 0x80 is invalid UTF-8
        assert!(result.starts_with('\u{7f}'));
    }

    #[test]
    fn test_common_prefix_len_basic() {
        assert_eq!(common_prefix_len("BRN-01JPHA", "BRN-01JPHB"), 9);
        assert_eq!(common_prefix_len("abc", "abd"), 2);
        assert_eq!(common_prefix_len("abc", "xyz"), 0);
        assert_eq!(common_prefix_len("abc", "abc"), 3);
        assert_eq!(common_prefix_len("", "abc"), 0);
    }
}
