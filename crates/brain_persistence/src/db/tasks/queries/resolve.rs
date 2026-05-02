use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use super::listing::{get_task, task_exists};
use crate::db::meta;
use crate::error::{BrainCoreError, Result};

/// Minimum ULID prefix length (after project prefix + separator).
const MIN_ULID_PREFIX_LEN: usize = 4;

/// Minimum display prefix: "BRN-" (4) + 4 ULID chars = 8.
pub(crate) const MIN_DISPLAY_PREFIX_LEN: usize = 8;

/// Minimum length for the hex portion of a hash-based short ID.
pub const MIN_SHORT_HASH_LEN: usize = 3;

/// BLAKE3 hash of a task_id → full 64-char lowercase hex string.
///
/// Pure function, no DB access. Used by migration backfill and projection.
pub fn blake3_short_hex(task_id: &str) -> String {
    blake3::hash(task_id.as_bytes()).to_hex().to_string()
}

/// Get the next child_seq for a parent task (max existing + 1, or 1 if no children).
pub fn next_child_seq(conn: &Connection, parent_task_id: &str) -> Result<i64> {
    let max: Option<i64> = conn
        .query_row(
            "SELECT MAX(t.child_seq) FROM tasks t
             JOIN entity_links el ON el.to_id = t.task_id
             WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='parent_of'
               AND el.from_id = ?1",
            [parent_task_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    Ok(max.unwrap_or(0) + 1)
}

pub fn resolve_task_id(conn: &Connection, input: &str) -> Result<String> {
    resolve_task_id_scoped(conn, input, None)
}

/// Resolve a brain_id from a task ID prefix (e.g. "ckt-ebd" → brain_id for CKT).
///
/// Returns `Some(brain_id)` if the input has a short prefix (1-4 chars before dash)
/// that matches a registered brain's prefix. Returns `None` otherwise.
pub fn resolve_brain_from_prefix(conn: &Connection, input: &str) -> Result<Option<String>> {
    match input.find('-') {
        Some(dash_pos) if dash_pos > 0 && dash_pos <= 4 => {
            let prefix = input[..dash_pos].to_ascii_uppercase();
            let brain_id: Option<String> = conn
                .query_row(
                    "SELECT brain_id FROM brains WHERE UPPER(prefix) = ?1",
                    [&prefix],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(brain_id)
        }
        _ => Ok(None),
    }
}

/// Resolve a task ID with optional brain_id scoping.
///
/// When `brain_id` is `Some(id)`, all lookups are filtered to tasks belonging
/// to that brain, preventing cross-brain collisions on short hashes / prefixes.
/// When `None`, resolves globally (legacy / single-brain mode).
pub fn resolve_task_id_scoped(
    conn: &Connection,
    input: &str,
    brain_id: Option<&str>,
) -> Result<String> {
    // Fast path: exact match
    if task_exists(conn, input)? {
        return Ok(input.to_string());
    }

    // Defense-in-depth: if the input has a prefix like "ckt-ebd", derive
    // the brain_id from the prefix when no explicit scope was provided.
    // This ensures the prefix in the ID itself always provides scoping.
    let derived_brain_id: Option<String> = if brain_id.is_none() {
        match input.find('-') {
            Some(dash_pos) if dash_pos <= 4 && dash_pos > 0 => {
                let prefix = input[..dash_pos].to_ascii_uppercase();
                conn.query_row(
                    "SELECT brain_id FROM brains WHERE UPPER(prefix) = ?1",
                    [&prefix],
                    |row| row.get(0),
                )
                .optional()?
            }
            _ => None,
        }
    } else {
        None
    };
    let effective_brain_id = brain_id.or(derived_brain_id.as_deref());

    let brain_clause = match effective_brain_id {
        Some(_) => " AND brain_id = ?",
        None => "",
    };

    // Check for hierarchical display ID: "PREFIX.N" where N is child_seq
    if let Some(dot_pos) = input.rfind('.') {
        let parent_part = &input[..dot_pos];
        let seq_part = &input[dot_pos + 1..];
        if let Ok(seq) = seq_part.parse::<i64>() {
            // Resolve the parent prefix first (recursive)
            if let Ok(parent_id) = resolve_task_id_scoped(conn, parent_part, effective_brain_id) {
                let child: Option<String> = conn
                    .query_row(
                        "SELECT t.task_id FROM tasks t
                         JOIN entity_links el ON el.to_id = t.task_id
                         WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='parent_of'
                           AND el.from_id = ?1 AND t.child_seq = ?2",
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

    // Try hash-based short ID resolution before ULID prefix matching.
    // Strip prefix if present (e.g., "brn-a3f" → "a3f"), then query id column.
    // TODO: consider dot-suffix stripping for robustness if display IDs are
    // accidentally passed where root IDs are expected.
    {
        let hex_part = match input.find('-') {
            Some(dash_pos) if dash_pos <= 4 => &input[dash_pos + 1..],
            _ => input,
        };
        // Only attempt if it looks like hex (lowercase, 0-9a-f)
        if !hex_part.is_empty()
            && hex_part.len() >= MIN_SHORT_HASH_LEN
            && hex_part
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        {
            // Exact match on display_id column — must also scope by brain
            // to avoid silently picking one row when multiple brains share the same display_id.
            let sql =
                format!("SELECT task_id, title FROM tasks WHERE display_id = ?1{brain_clause}");
            let exact_matches: Vec<(String, String)> = match effective_brain_id {
                Some(bid) => {
                    let mut stmt = conn.prepare(&sql)?;
                    stmt.query_map(rusqlite::params![hex_part, bid], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
                }
                None => {
                    let mut stmt = conn.prepare(&sql)?;
                    stmt.query_map(rusqlite::params![hex_part], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
                }
            };
            match exact_matches.len() {
                1 => return Ok(exact_matches.into_iter().next().unwrap().0),
                n if n > 1 => {
                    let candidates: Vec<String> = exact_matches
                        .iter()
                        .map(|(id, title)| format!("  {id} — {title}"))
                        .collect();
                    return Err(BrainCoreError::TaskEvent(format!(
                        "ambiguous short hash '{input}': matches {n} tasks:\n{}",
                        candidates.join("\n")
                    )));
                }
                _ => {} // 0 matches — fall through to prefix path
            }

            // Prefix match on display_id column (range scan)
            let upper = increment_string(hex_part);
            let sql = format!(
                "SELECT task_id, title FROM tasks WHERE display_id >= ?1 AND display_id < ?2{brain_clause}"
            );
            let matches: Vec<(String, String)> = match effective_brain_id {
                Some(bid) => {
                    let mut stmt = conn.prepare(&sql)?;
                    stmt.query_map(rusqlite::params![hex_part, upper, bid], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
                }
                None => {
                    let mut stmt = conn.prepare(&sql)?;
                    stmt.query_map(rusqlite::params![hex_part, upper], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
                }
            };

            match matches.len() {
                1 => return Ok(matches.into_iter().next().unwrap().0),
                n if n > 1 => {
                    let candidates: Vec<String> = matches
                        .iter()
                        .map(|(id, title)| format!("  {id} — {title}"))
                        .collect();
                    return Err(BrainCoreError::TaskEvent(format!(
                        "ambiguous short hash '{input}': matches {n} tasks:\n{}",
                        candidates.join("\n")
                    )));
                }
                _ => {} // 0 matches — fall through to ULID path
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
    let sql = format!(
        "SELECT task_id, title FROM tasks WHERE task_id >= ?1 AND task_id < ?2{brain_clause}"
    );
    let matches: Vec<(String, String)> = match effective_brain_id {
        Some(bid) => {
            let mut stmt = conn.prepare(&sql)?;
            stmt.query_map(rusqlite::params![search_prefix, upper_bound, bid], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        }
        None => {
            let mut stmt = conn.prepare(&sql)?;
            stmt.query_map(rusqlite::params![search_prefix, upper_bound], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        }
    };

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
/// Uses hash-based short IDs when available: `{prefix_lower}-{id}` (e.g., `brn-a3f`).
/// For children with `parent_task_id` + `child_seq`, returns dot notation
/// (e.g., `brn-a3f.1`). Recurses through the parent chain so grandchildren
/// get `brn-a3f.1.2`. Falls back to ULID prefix for pre-migration tasks.
///
/// If `task_id` does not exist in the `tasks` table (orphan dep, deleted target),
/// returns the raw ID unchanged — the ULID-prefix fallback is only valid for
/// tasks that exist but lack a `display_id`, not for missing tasks.
pub fn compact_id(conn: &Connection, task_id: &str) -> Result<String> {
    // Orphan / non-existent task: no compaction possible, return raw.
    let task = match get_task(conn, task_id)? {
        Some(t) => t,
        None => return Ok(task_id.to_string()),
    };

    // Dot notation for any child with parent + child_seq
    if let (Some(parent_id), Some(seq)) = (&task.parent_task_id, task.child_seq) {
        let parent_compact = compact_id(conn, parent_id)?;
        return Ok(format!("{parent_compact}.{seq}"));
    }

    // Try hash-based short ID
    if let Some(display) = short_id_display(conn, task_id)? {
        return Ok(display);
    }

    // Fallback: ULID prefix computation (pre-migration tasks)
    compact_id_ulid(conn, task_id)
}

/// Build display ID from `id` column + brain prefix: `{prefix_lower}-{id}`.
/// Returns `None` if the task has no `id` value (pre-migration).
fn short_id_display(conn: &Connection, task_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT t.display_id, COALESCE(LOWER(b.prefix), 'brx')
             FROM tasks t
             LEFT JOIN brains b ON b.brain_id = t.brain_id
             WHERE t.task_id = ?1",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    match row {
        Some((Some(id), prefix)) => Ok(Some(format!("{prefix}-{id}"))),
        _ => Ok(None),
    }
}

/// Compute compact display IDs for all tasks (batch, for list display).
///
/// Uses hash-based short IDs (`{prefix_lower}-{id}`) when available.
/// Falls back to ULID prefix computation for pre-migration tasks.
/// Applies dot notation for children with `parent_task_id` + `child_seq`.
pub fn compact_ids(conn: &Connection) -> Result<HashMap<String, String>> {
    // Load all tasks with their id and brain prefix
    let mut stmt = conn.prepare(
        "SELECT t.task_id, t.display_id, COALESCE(LOWER(b.prefix), 'brx')
         FROM tasks t
         LEFT JOIN brains b ON b.brain_id = t.brain_id
         ORDER BY t.task_id",
    )?;
    let rows: Vec<(String, Option<String>, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut result = HashMap::new();

    // First pass: hash-based IDs for tasks that have them
    let mut ulid_fallback_ids: Vec<String> = Vec::new();
    for (task_id, id, prefix) in &rows {
        if let Some(hash_id) = id {
            result.insert(task_id.clone(), format!("{prefix}-{hash_id}"));
        } else {
            ulid_fallback_ids.push(task_id.clone());
        }
    }

    // ULID prefix fallback for pre-migration tasks
    if !ulid_fallback_ids.is_empty() {
        let all_ids: Vec<&str> = rows.iter().map(|(tid, _, _)| tid.as_str()).collect();
        let n = all_ids.len();
        for i in 0..n {
            let id = all_ids[i];
            if result.contains_key(id) {
                continue; // already has hash-based ID
            }
            let prev = if i > 0 { Some(all_ids[i - 1]) } else { None };
            let next = if i + 1 < n {
                Some(all_ids[i + 1])
            } else {
                None
            };
            let min_len_prev = prev.map(|p| common_prefix_len(id, p) + 1).unwrap_or(1);
            let min_len_next = next.map(|nx| common_prefix_len(id, nx) + 1).unwrap_or(1);
            let min_len = min_len_prev.max(min_len_next).max(MIN_DISPLAY_PREFIX_LEN);
            let prefix_len = min_len.min(id.len());
            result.insert(id.to_string(), id[..prefix_len].to_string());
        }
    }

    // Apply dot notation for all children with a parent_of edge + child_seq.
    let mut child_stmt = conn.prepare(
        "SELECT t.task_id, el.from_id AS parent_task_id, t.child_seq
         FROM tasks t
         JOIN entity_links el
           ON el.to_type='TASK' AND el.to_id=t.task_id
          AND el.from_type='TASK' AND el.edge_kind='parent_of'
         WHERE t.child_seq IS NOT NULL
         ORDER BY el.from_id, t.child_seq",
    )?;
    let children: Vec<(String, String, i64)> = child_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Multiple passes for transitive chains (parent → child → grandchild).
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
    use crate::db::schema::{ensure_brain_registered, init_schema};

    /// Set up an in-memory DB with two brains and insert tasks with controlled display_ids.
    /// Returns (conn, task_id_brain_a, task_id_brain_b) where both tasks share the same display_id.
    fn setup_cross_brain(display_id: &str) -> (Connection, String, String) {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();
        ensure_brain_registered(&conn, "brain-bbb", "bravo").unwrap();

        let tid_a = "ALP-01JTEST00000000000000AA";
        let tid_b = "BRV-01JTEST00000000000000BB";

        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Alpha task', 'open', 2, strftime('%s','now'), strftime('%s','now'), ?2)",
            rusqlite::params![tid_a, display_id],
        ).unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-bbb', 'Bravo task', 'open', 2, strftime('%s','now'), strftime('%s','now'), ?2)",
            rusqlite::params![tid_b, display_id],
        ).unwrap();

        (conn, tid_a.to_string(), tid_b.to_string())
    }

    #[test]
    fn test_resolve_scoped_exact_hash_match() {
        let (conn, tid_a, tid_b) = setup_cross_brain("ebd");

        // Unscoped: ambiguous (two tasks with same display_id across brains)
        let err = resolve_task_id_scoped(&conn, "ebd", None).unwrap_err();
        assert!(
            err.to_string().contains("ambiguous"),
            "expected ambiguous error, got: {err}"
        );

        // Scoped to brain-aaa: resolves to alpha task
        let resolved = resolve_task_id_scoped(&conn, "ebd", Some("brain-aaa")).unwrap();
        assert_eq!(resolved, tid_a);

        // Scoped to brain-bbb: resolves to bravo task
        let resolved = resolve_task_id_scoped(&conn, "ebd", Some("brain-bbb")).unwrap();
        assert_eq!(resolved, tid_b);
    }

    #[test]
    fn test_resolve_scoped_prefix_hash_match() {
        let (conn, tid_a, tid_b) = setup_cross_brain("ebd42f");

        // Prefix "ebd" should be ambiguous unscoped (matches both brains)
        let err = resolve_task_id_scoped(&conn, "ebd", None).unwrap_err();
        assert!(
            err.to_string().contains("ambiguous"),
            "expected ambiguous error, got: {err}"
        );

        // Scoped: unique within each brain
        let resolved = resolve_task_id_scoped(&conn, "ebd", Some("brain-aaa")).unwrap();
        assert_eq!(resolved, tid_a);

        let resolved = resolve_task_id_scoped(&conn, "ebd", Some("brain-bbb")).unwrap();
        assert_eq!(resolved, tid_b);
    }

    #[test]
    fn test_resolve_scoped_with_prefix_stripped() {
        // Simulates "alp-ebd" where "alp-" is stripped to get "ebd"
        let (conn, tid_a, _tid_b) = setup_cross_brain("ebd");

        let resolved = resolve_task_id_scoped(&conn, "alp-ebd", Some("brain-aaa")).unwrap();
        assert_eq!(resolved, tid_a);
    }

    #[test]
    fn test_resolve_scoped_not_found() {
        let (conn, _tid_a, _tid_b) = setup_cross_brain("ebd42f");

        // Non-existent brain should find nothing via hash path,
        // then fall through to ULID path and fail
        let err = resolve_task_id_scoped(&conn, "alp-ebd42f", Some("brain-zzz")).unwrap_err();
        assert!(
            err.to_string().contains("no task found"),
            "expected not-found error, got: {err}"
        );
    }

    #[test]
    fn test_resolve_unscoped_backwards_compatible() {
        // Single task, no collision — unscoped should still work
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();

        let tid = "ALP-01JTEST00000000000000AA";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Solo task', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'abc')",
            [tid],
        ).unwrap();

        // Both scoped and unscoped should resolve
        let resolved = resolve_task_id_scoped(&conn, "abc", None).unwrap();
        assert_eq!(resolved, tid);

        let resolved = resolve_task_id_scoped(&conn, "abc", Some("brain-aaa")).unwrap();
        assert_eq!(resolved, tid);
    }

    #[test]
    fn test_resolve_prefix_derived_brain_scoping() {
        // Even without explicit brain_id, the prefix in the input ("alp-" / "brv-")
        // should derive the correct brain and scope resolution automatically.
        let (conn, tid_a, tid_b) = setup_cross_brain("ebd");

        // "alp-ebd" → derives brain-aaa from "ALP" prefix → resolves to alpha task
        let resolved = resolve_task_id_scoped(&conn, "alp-ebd", None).unwrap();
        assert_eq!(resolved, tid_a);

        // "brv-ebd" → derives brain-bbb from "BRV" prefix → resolves to bravo task
        let resolved = resolve_task_id_scoped(&conn, "brv-ebd", None).unwrap();
        assert_eq!(resolved, tid_b);
    }

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
