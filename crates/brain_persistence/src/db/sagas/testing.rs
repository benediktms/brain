//! Test-only fixture helpers for the sagas subsystem.
//!
//! Centralizes the SQL fixtures used by `brain_persistence` internal tests
//! and the `brain_sagas` domain crate's test suite, so the no-raw-SQL-outside-
//! persistence rule holds end-to-end. Gated on the `test-utils` feature so
//! production builds never link these symbols.

use rusqlite::{Connection, params};

use crate::sql::SqlResult;

/// Seed a brain row. Idempotent (`INSERT OR IGNORE`). `prefix` is the optional
/// per-brain ID prefix (e.g. `BRN`); pass `None` for the unscoped sentinel.
pub fn seed_brain(
    conn: &Connection,
    brain_id: &str,
    name: &str,
    prefix: Option<&str>,
) -> SqlResult<()> {
    conn.execute(
        "INSERT OR IGNORE INTO brains (brain_id, name, prefix, created_at) VALUES (?1, ?2, ?3, 0)",
        params![brain_id, name, prefix],
    )?;
    Ok(())
}

/// Seed a minimal `tasks` row with default `open`/`task` shape and zero
/// timestamps. The caller is responsible for inserting the brain first
/// (see [`seed_brain`]).
pub fn seed_task(conn: &Connection, task_id: &str, brain_id: &str, title: &str) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'open', 4, 'task', 0, 0)",
        params![task_id, brain_id, title],
    )?;
    Ok(())
}

/// Seed a `saga_tasks` row directly. Allows cross-brain task_ids and orphan
/// rows — `saga_tasks` has no FK to `tasks` by design.
pub fn seed_saga_task_link(
    conn: &Connection,
    saga_id: &str,
    task_id: &str,
    added_at: i64,
) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, ?3)",
        params![saga_id, task_id, added_at],
    )?;
    Ok(())
}

/// Seed a `parent_of` edge between two existing tasks. Mirrors the projection
/// write done by `ParentSet` but skips the full event-sourcing path —
/// cascade tests only care about the graph topology, not the event log.
pub fn seed_parent_of_edge(
    conn: &Connection,
    parent_task_id: &str,
    child_task_id: &str,
) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
         VALUES (lower(hex(randomblob(16))), 'TASK', ?1, 'TASK', ?2, 'parent_of',
                 strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL)",
        params![parent_task_id, child_task_id],
    )?;
    Ok(())
}

/// Force a saga's `status` column, bumping `updated_at` to the supplied
/// timestamp. Bypasses the lifecycle state machine — intended only for
/// tests that need to position a saga in a terminal state without going
/// through the legal transition path.
pub fn force_saga_status(conn: &Connection, saga_id: &str, status: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE sagas SET status = ?1, updated_at = ?2 WHERE saga_id = ?3",
        params![status, ts, saga_id],
    )?;
    Ok(())
}

/// Backdate (or otherwise fix) both `created_at` and `updated_at` to a
/// supplied timestamp. Used by timestamp-monotonicity tests that need a
/// deterministic "before" value without sleeping.
pub fn force_saga_timestamps(conn: &Connection, saga_id: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE sagas SET updated_at = ?1, created_at = ?1 WHERE saga_id = ?2",
        params![ts, saga_id],
    )?;
    Ok(())
}

/// Count of `saga_tasks` rows for a given saga.
pub fn count_saga_tasks(conn: &Connection, saga_id: &str) -> SqlResult<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1",
        [saga_id],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// Count of `saga_tasks` rows for a `(saga, task)` pair (0 or 1). Used to
/// assert presence/absence of a specific membership.
pub fn count_saga_task_pair(conn: &Connection, saga_id: &str, task_id: &str) -> SqlResult<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1 AND task_id = ?2",
        [saga_id, task_id],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// Count of `saga_events` rows of a particular `event_type` for a given saga.
pub fn count_saga_events_of_type(
    conn: &Connection,
    saga_id: &str,
    event_type: &str,
) -> SqlResult<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM saga_events WHERE saga_id = ?1 AND event_type = ?2",
        [saga_id, event_type],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// `(count, latest_actor_or_empty)` of `saga_events` rows of a particular
/// type. Single round trip — used by tests that need both the cardinality
/// and the most recent actor.
pub fn count_and_last_actor_for_event_type(
    conn: &Connection,
    saga_id: &str,
    event_type: &str,
) -> SqlResult<(i64, String)> {
    let row = conn.query_row(
        "SELECT COUNT(*), COALESCE(MAX(actor), '') FROM saga_events WHERE saga_id = ?1 AND event_type = ?2",
        [saga_id, event_type],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
    )?;
    Ok(row)
}

/// Sorted list of `saga_tasks.task_id` values (ascending). Used by tests
/// that need a deterministic membership snapshot.
pub fn list_saga_task_ids_sorted(conn: &Connection, saga_id: &str) -> SqlResult<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY task_id")?;
    let rows = stmt
        .query_map([saga_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// `(event_type, actor)` of the first (oldest) `saga_events` row for a saga.
pub fn first_saga_event_meta(conn: &Connection, saga_id: &str) -> SqlResult<(String, String)> {
    let row = conn.query_row(
        "SELECT event_type, actor FROM saga_events WHERE saga_id = ?1",
        [saga_id],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )?;
    Ok(row)
}

/// `(event_type, payload)` of the first `saga_events` row whose `event_type`
/// matches a SQL LIKE pattern (e.g. `"%updated%"`).
pub fn saga_event_by_type_like(
    conn: &Connection,
    saga_id: &str,
    type_pattern: &str,
) -> SqlResult<(String, String)> {
    let row = conn.query_row(
        "SELECT event_type, payload FROM saga_events WHERE saga_id = ?1 AND event_type LIKE ?2",
        [saga_id, type_pattern],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )?;
    Ok(row)
}
