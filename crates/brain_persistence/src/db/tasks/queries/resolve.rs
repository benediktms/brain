use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use super::listing::{get_task, task_exists};
use super::{MatchSource, TaskResolutionResult};
use crate::db::meta;
use crate::error::BrainCoreError;
use crate::sql::{SqlError, SqlResult};

/// Minimum ULID prefix length (after project prefix + separator).
const MIN_ULID_PREFIX_LEN: usize = 4;

/// Minimum display prefix: "BRN-" (4) + 4 ULID chars = 8.
pub(crate) const MIN_DISPLAY_PREFIX_LEN: usize = 8;

// Short-ID helpers live in the shared `crate::db::short_id` module so sagas
// can reuse them; re-exported here to preserve the existing `super::queries::*`
// import paths used by `tasks::projections` and friends.
pub use crate::db::short_id::{MIN_SHORT_HASH_LEN, blake3_short_hex};

/// Get the next child_seq for a parent task (max existing + 1, or 1 if no children).
pub fn next_child_seq(conn: &Connection, parent_task_id: &str) -> SqlResult<i64> {
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

pub fn resolve_task_id(conn: &Connection, input: &str) -> SqlResult<TaskResolutionResult> {
    resolve_task_id_scoped(conn, input, None)
}

/// Resolve a brain_id from a task ID prefix (e.g. "ckt-ebd" → brain_id for CKT).
///
/// Returns `Some(brain_id)` if the input has a short prefix (1-4 chars before dash)
/// that matches a registered brain's prefix. Returns `None` otherwise.
///
/// Fails explicitly if multiple brains share the same prefix. The v54→v55 schema
/// migration enforces uniqueness, but this check is a defense-in-depth against
/// stale state (e.g. a fixture that bypassed the migration, or a hand-edited DB).
/// Silently picking the first match is the bug `brn-37e` ultimately diagnosed,
/// so this resolver refuses to do it.
pub fn resolve_brain_from_prefix(conn: &Connection, input: &str) -> SqlResult<Option<String>> {
    match input.find('-') {
        Some(dash_pos) if dash_pos > 0 && dash_pos <= 4 => {
            let prefix = input[..dash_pos].to_ascii_uppercase();
            let matches: Vec<String> = {
                let mut stmt =
                    conn.prepare("SELECT brain_id FROM brains WHERE UPPER(prefix) = ?1")?;
                stmt.query_map([&prefix], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            match matches.as_slice() {
                [] => Ok(None),
                [only] => Ok(Some(only.clone())),
                multiple => Err(SqlError::Domain(BrainCoreError::BrainRegistry(format!(
                    "ambiguous brain prefix '{prefix}': {} brains share it: {}",
                    multiple.len(),
                    multiple.join(", ")
                )))),
            }
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
) -> SqlResult<TaskResolutionResult> {
    // Fast path: exact match

    // Fast path: exact match on tasks table
    if task_exists(conn, input)? {
        return Ok(TaskResolutionResult {
            task_id: input.to_string(),
            match_source: MatchSource::Live,
            redirected_from: None,
        });
    }

    // Defense-in-depth: if the input has a prefix like "ckt-ebd", derive
    // the brain_id from the prefix when no explicit scope was provided.
    // Routes through `resolve_brain_from_prefix` so the ambiguity check
    // (multiple brains with the same prefix → error) applies uniformly.
    let derived_brain_id: Option<String> = if brain_id.is_none() {
        resolve_brain_from_prefix(conn, input)?
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
            if let Ok(parent_result) = resolve_task_id_scoped(conn, parent_part, effective_brain_id)
            {
                let child: Option<String> = conn
                    .query_row(
                        "SELECT t.task_id FROM tasks t
                         JOIN entity_links el ON el.to_id = t.task_id
                         WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='parent_of'
                           AND el.from_id = ?1 AND t.child_seq = ?2",
                        rusqlite::params![parent_result.task_id, seq],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(child_id) = child {
                    return Ok(TaskResolutionResult {
                        task_id: child_id,
                        match_source: MatchSource::Live,
                        redirected_from: None,
                    });
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
                1 => {
                    let (task_id, _) = exact_matches.into_iter().next().unwrap();
                    return Ok(TaskResolutionResult {
                        task_id,
                        match_source: MatchSource::Live,
                        redirected_from: None,
                    });
                }
                n if n > 1 => {
                    let candidates: Vec<String> = exact_matches
                        .iter()
                        .map(|(id, title)| format!("  {id} — {title}"))
                        .collect();
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "ambiguous short hash '{input}': matches {n} tasks:\n{}",
                        candidates.join("\n")
                    ))));
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
                1 => {
                    let (task_id, _) = matches.into_iter().next().unwrap();
                    return Ok(TaskResolutionResult {
                        task_id,
                        match_source: MatchSource::Live,
                        redirected_from: None,
                    });
                }
                n if n > 1 => {
                    let candidates: Vec<String> = matches
                        .iter()
                        .map(|(id, title)| format!("  {id} — {title}"))
                        .collect();
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "ambiguous short hash '{input}': matches {n} tasks:\n{}",
                        candidates.join("\n")
                    ))));
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
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters after '{}'",
                    &normalized[..=dash_pos]
                ))));
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
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters, got {}",
                    normalized.len()
                ))));
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
        0 => {
            // Final fallback: search task_aliases (task_external_ids) for any source.
            // This handles previous-ID aliases (source='previous') as well as
            // any other external ID that was written before this feature existed.
            let alias_row: Option<(String, String)> = conn
                .query_row(
                    "SELECT task_id, external_id FROM task_external_ids WHERE external_id = ?1 LIMIT 1",
                    [input],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((aliased_task_id, external_id)) = alias_row {
                return Ok(TaskResolutionResult {
                    task_id: aliased_task_id,
                    match_source: MatchSource::Alias,
                    redirected_from: Some(external_id),
                });
            }
            Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "no task found matching prefix: {input}"
            ))))
        }
        1 => {
            let (task_id, _) = matches.into_iter().next().unwrap();
            Ok(TaskResolutionResult {
                task_id,
                match_source: MatchSource::Live,
                redirected_from: None,
            })
        }
        n => {
            let candidates: Vec<String> = matches
                .iter()
                .map(|(id, title)| format!("  {id} — {title}"))
                .collect();
            Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "ambiguous prefix '{input}': matches {n} tasks:\n{}",
                candidates.join("\n")
            ))))
        }
    }
}

/// Core ULID-based prefix computation without dot notation.
fn compact_id_ulid(conn: &Connection, task_id: &str) -> SqlResult<String> {
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
pub fn compact_id(conn: &Connection, task_id: &str) -> SqlResult<String> {
    // Orphan / non-existent task: no compaction possible, return raw.
    let task = match get_task(conn, task_id)? {
        Some(t) => t,
        None => return Ok(task_id.to_string()),
    };

    // Dot notation for any child with parent + child_seq.
    // Use the child's own brain prefix (via short_id_display), not the
    // parent's compact form — a child in a different brain than its parent
    // must not inherit the parent's brain prefix (brn-6710).
    if let (Some(_parent_id), Some(seq)) = (&task.parent_task_id, task.child_seq)
        && let Some(child_display) = short_id_display(conn, task_id)?
    {
        return Ok(format!("{child_display}.{seq}"));
        // No hash-based display_id: fall through to ULID prefix computation.
        // (We don't recurse to parent since that would give the wrong prefix.)
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
fn short_id_display(conn: &Connection, task_id: &str) -> SqlResult<Option<String>> {
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
pub fn compact_ids(conn: &Connection) -> SqlResult<HashMap<String, String>> {
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

    // Apply dot notation for children using their own pre-computed display ID.
    // Each child has its own brain prefix already stored in `result` from the
    // hash-based ID pass (or ULID fallback pass). We use that directly rather
    // than chaining through parent_compact — which would give grandchildren the
    // grandparent's brain prefix when brains differ (brn-6710).
    let mut child_stmt = conn.prepare(
        "SELECT t.task_id, t.child_seq
         FROM tasks t
         JOIN entity_links el
           ON el.to_type='TASK' AND el.to_id=t.task_id
          AND el.from_type='TASK' AND el.edge_kind='parent_of'
         WHERE t.child_seq IS NOT NULL",
    )?;
    let children: Vec<(String, i64)> = child_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    for (child_id, seq) in &children {
        if let Some(child_compact) = result.get(child_id).cloned() {
            result.insert(child_id.clone(), format!("{child_compact}.{seq}"));
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

    // ─── alias / task_external_ids fallback tests ──────────────────────────
    //
    // When no live task matches an input string, the resolver falls back to
    // task_external_ids so that legacy / imported external IDs can still resolve.

    /// Helper: insert a task_external_ids row with FK off.
    fn insert_external_id(conn: &Connection, task_id: &str, source: &str, external_id: &str) {
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO task_external_ids (task_id, source, external_id, imported_at)
             VALUES (?1, ?2, ?3, 1000)",
            rusqlite::params![task_id, source, external_id],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    }

    /// Resolving an external_id returns the aliased task with match_source=Alias.
    #[test]
    fn test_resolve_alias_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();

        let tid = "ALP-01JTEST00000000000000AA";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Aliased task', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'abc')",
            [tid],
        ).unwrap();

        // Write an alias for the task
        insert_external_id(&conn, tid, "previous", "DLS/my-old-task");

        // Resolving the alias string returns the task with Alias match source
        let result = resolve_task_id(&conn, "DLS/my-old-task").unwrap();
        assert_eq!(result.task_id, tid);
        assert_eq!(result.match_source, MatchSource::Alias);
        assert_eq!(result.redirected_from, Some("DLS/my-old-task".to_string()));
    }

    /// When no live task matches, alias lookup finds the task.
    #[test]
    fn test_resolve_alias_fallback_when_no_live_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();

        let tid = "ALP-01JTASKALIAS0000000AA";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Aliased task', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'abc')",
            [&tid],
        ).unwrap();
        insert_external_id(&conn, tid, "previous", "DLS/old-task-id");

        // No live task matches "DLS/old-task-id" — alias fallback finds it
        let result = resolve_task_id(&conn, "DLS/old-task-id").unwrap();
        assert_eq!(result.task_id, tid);
        assert_eq!(result.match_source, MatchSource::Alias);
    }

    /// A string that matches BOTH a live task and an alias returns the live task (live is preferred).
    #[test]
    fn test_resolve_live_preferred_over_alias_for_same_string() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();

        let live_tid = "ALP-01JLIVE0000000000000AA";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Live task', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'dead')",
            [&live_tid],
        ).unwrap();

        // Alias also exists with "dead" as its external_id — but the live path
        // matches by display_id first, so alias is never consulted
        insert_external_id(&conn, live_tid, "previous", "dead");

        // The string "dead" is all-lowercase hex so the hash-based display_id
        // lookup fires and matches the live task before alias fallback is reached.
        let result = resolve_task_id(&conn, "dead").unwrap();
        assert_eq!(result.task_id, live_tid);
        assert_eq!(result.match_source, MatchSource::Live);
        assert_eq!(result.redirected_from, None);
    }

    // ─── resolve_brain_from_prefix defense-in-depth tests ─────────────────
    //
    // These tests construct duplicate-prefix state by dropping the v54→v55
    // UNIQUE index before inserting. This simulates pre-migration databases
    // and the "stale fixture / hand-edited DB" scenarios the resolver defense
    // is documented to protect against.

    fn setup_with_duplicate_prefixes(conn: &Connection, prefix_a: &str, prefix_b: &str) {
        init_schema(conn).unwrap();
        conn.execute_batch("DROP INDEX IF EXISTS idx_brains_prefix")
            .unwrap();
        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('b1', 'alpha', ?1, 1000)",
            [prefix_a],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('b2', 'beta', ?1, 1001)",
            [prefix_b],
        )
        .unwrap();
    }

    #[test]
    fn resolve_brain_from_prefix_returns_error_on_ambiguity() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_duplicate_prefixes(&conn, "DUP", "DUP");

        let err = resolve_brain_from_prefix(&conn, "dup-xyz")
            .expect_err("multi-match must return an explicit error");
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"), "got: {msg}");
        assert!(
            msg.contains("DUP"),
            "error must name the prefix; got: {msg}"
        );
        assert!(
            msg.contains("b1"),
            "error must enumerate brain_ids; got: {msg}"
        );
        assert!(
            msg.contains("b2"),
            "error must enumerate brain_ids; got: {msg}"
        );
    }

    #[test]
    fn resolve_brain_from_prefix_case_insensitive_ambiguity() {
        // 'Cpm' and 'CPM' must both surface as ambiguous because the resolver
        // does `WHERE UPPER(prefix) = ?1`. The defense fires regardless of
        // stored casing.
        let conn = Connection::open_in_memory().unwrap();
        setup_with_duplicate_prefixes(&conn, "Cpm", "CPM");

        let err = resolve_brain_from_prefix(&conn, "cpm-xyz")
            .expect_err("case-different duplicates must still trigger ambiguity error");
        assert!(err.to_string().contains("ambiguous"), "got: {err}");
    }

    #[test]
    fn resolve_brain_from_prefix_single_match_returns_brain_id() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();
        let prefix = crate::db::schema::read_brain_prefix_by_name(&conn, "alpha")
            .unwrap()
            .unwrap();

        let resolved =
            resolve_brain_from_prefix(&conn, &format!("{}-anything", prefix.to_lowercase()))
                .expect("unique prefix should resolve")
                .expect("resolver should return Some");
        assert_eq!(resolved, "brain-aaa");
    }

    #[test]
    fn resolve_brain_from_prefix_no_match_returns_none() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let result =
            resolve_brain_from_prefix(&conn, "xyz-anything").expect("no matches is not an error");
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_brain_from_prefix_no_dash_returns_none() {
        // Input without a `-` is not a prefix-bearing ID — the function
        // returns None without touching the brains table.
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let result =
            resolve_brain_from_prefix(&conn, "01JTEST0000000000000000AA").expect("no error path");
        assert_eq!(result, None);
    }

    // ─── compact_id cross-brain child tests ─────────────────────────────────

    /// A child task in a different brain from its parent must use its own
    /// brain prefix in dot notation, not the parent's prefix (brn-6710).
    #[test]
    fn test_compact_id_child_uses_own_brain_prefix() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();
        ensure_brain_registered(&conn, "brain-bbb", "bravo").unwrap();

        // Parent in brain-aaa with display_id "abc"
        let parent_id = "ALP-01JTEST000000000000000001";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Parent', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'abc')",
            [parent_id],
        ).unwrap();

        // Child in brain-bbb (different brain) with display_id "xyz"
        let child_id = "BRV-01JTEST000000000000000002";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id, parent_task_id, child_seq)
             VALUES (?1, 'brain-bbb', 'Child', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'xyz', ?2, 1)",
            rusqlite::params![child_id, parent_id],
        ).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
             VALUES (lower(hex(randomblob(16))), 'TASK', ?1, 'TASK', ?2, 'parent_of', strftime('%Y-%m-%dT%H:%M:%SZ','now'), NULL)",
            rusqlite::params![parent_id, child_id],
        ).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        // Child's compact_id must use brain-bbb's prefix (bravo → "brv"), not brain-aaa's (alpha → "alp")
        let compact = compact_id(&conn, child_id).unwrap();
        assert!(
            compact.starts_with("brv-"),
            "child in brain-bbb must use brv- prefix, got: {compact}"
        );
        assert!(
            compact.ends_with(".1"),
            "child dot suffix must be .1, got: {compact}"
        );
        assert!(
            compact.contains("xyz"),
            "child display_id 'xyz' must appear, got: {compact}"
        );
    }

    /// compact_ids batch: children use their own pre-computed display ID.
    #[test]
    fn test_compact_ids_child_uses_own_brain_prefix() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        ensure_brain_registered(&conn, "brain-aaa", "alpha").unwrap();
        ensure_brain_registered(&conn, "brain-bbb", "bravo").unwrap();

        let parent_id = "ALP-01JTEST000000000000000001";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-aaa', 'Parent', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'abc')",
            [parent_id],
        ).unwrap();

        let child_id = "BRV-01JTEST000000000000000002";
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id, parent_task_id, child_seq)
             VALUES (?1, 'brain-bbb', 'Child', 'open', 2, strftime('%s','now'), strftime('%s','now'), 'xyz', ?2, 1)",
            rusqlite::params![child_id, parent_id],
        ).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
             VALUES (lower(hex(randomblob(16))), 'TASK', ?1, 'TASK', ?2, 'parent_of', strftime('%Y-%m-%dT%H:%M:%SZ','now'), NULL)",
            rusqlite::params![parent_id, child_id],
        ).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        let compacts = compact_ids(&conn).unwrap();
        let child_compact = compacts.get(child_id).expect("child must be in compacts");
        assert!(
            child_compact.starts_with("brv-"),
            "batch compact for child in brain-bbb must use brv- prefix, got: {child_compact}"
        );
        assert!(
            child_compact.ends_with(".1"),
            "child dot suffix must be .1, got: {child_compact}"
        );
    }
}
