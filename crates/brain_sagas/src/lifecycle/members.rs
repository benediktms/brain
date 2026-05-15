//! Saga membership operations: add_tasks, remove_tasks.
//!
//! Each function is a typed orchestration helper that runs inside a caller-
//! supplied `&Connection`. Handles cascade-expansion via `task_subtree`, batch
//! deduplication, and `saga_task_added`/`saga_task_removed` event emission.
//! The caller (`SagaStore`) owns transaction lifecycle via
//! `Db::with_write_conn`.
//!
//! Mirrors the `brain_tasks::projections` shape: domain crate owns
//! orchestration; `brain_persistence` exposes typed primitives.

use std::collections::HashSet;

use rusqlite::Connection;

use brain_core::error::BrainCoreError;
use brain_persistence::db::sagas::events::{SagaEvent, SagaEventType, SagaTaskPayload};
use brain_persistence::db::sagas::queries::{self, SagaEventInsert, saga_members_in};
use brain_persistence::db::sagas::resolve_saga_id;
use brain_persistence::db::tasks::queries::{resolve_task_id_scoped, task_subtree};
use brain_persistence::sql::{SqlError, SqlResult};

use crate::status::SagaStatus;

/// Hard upper bound on the number of tasks a single cascade-add or
/// cascade-remove operation may touch.
///
/// The MCP `task_ids` array is capped at 500 input entries, but cascade
/// expansion via `task_subtree` is unbounded by the input length — a single
/// epic with 10 000 descendants would otherwise hold the SQLite writer mutex
/// for the duration of the insert/delete + per-row event emission. This cap
/// restores the same protection intent that the MCP input cap provides for
/// non-cascade calls.
pub const MAX_EXPANDED_BATCH: usize = 2000;

/// Atomically add one or more tasks to a saga (atomic batch + idempotent —
/// already-member tasks are skipped).
///
/// All task IDs are resolved via `resolve_task_id_scoped` (cross-brain
/// aware). If any task ID fails to resolve or the saga is
/// closed/cancelled, the entire transaction rolls back and an error is
/// returned. Tasks that are already members of the saga, and duplicates
/// within the input batch, are silently skipped — they do not insert and
/// do not emit events.
///
/// Returns the canonical task IDs that were *actually inserted* (i.e.
/// the candidate set minus already-members and within-batch duplicates).
/// Callers use `.len()` for the count. Surfacing the set lets transports
/// (MCP, CLI) tell the user which tasks were pulled in — particularly
/// important when `cascade=true` and the input expanded silently.
pub fn add_tasks(
    conn: &Connection,
    saga_id: &str,
    task_ids: &[String],
    cascade: bool,
    actor: &str,
) -> SqlResult<Vec<String>> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    // unchecked_ ok: caller holds the writer mutex via with_write_conn,
    // single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    // Verify the saga exists and is not in a terminal state.
    let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::SagaNotFound(format!(
            "saga not found: {saga_id}"
        )))
    })?;
    let status: SagaStatus = row.status.parse().map_err(|_| {
        SqlError::Domain(BrainCoreError::TaskEvent(format!(
            "saga '{saga_id}' has unrecognised status"
        )))
    })?;
    match status {
        SagaStatus::Closed | SagaStatus::Cancelled => {
            return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "saga '{saga_id}' is {status}; reopen it before adding tasks"
            ))));
        }
        _ => {}
    }

    // Resolve all input IDs first — bad IDs fail-fast before any writes.
    let mut seeds: Vec<String> = Vec::with_capacity(task_ids.len());
    for raw_id in task_ids {
        let full_id = resolve_task_id_scoped(&tx, raw_id, None).map_err(|e| {
            SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "task '{raw_id}' could not be resolved: {e}"
            )))
        })?;
        seeds.push(full_id);
    }

    // When `cascade` is true, expand each input to itself plus every
    // transitive descendant in the parent_of graph. The expansion is
    // a single SQL pass; deduplication is naturally handled by the
    // recursive CTE's UNION (no UNION ALL). Reject runaway expansions
    // before any other work — see MAX_EXPANDED_BATCH for rationale.
    let candidates = if cascade {
        let expanded = task_subtree(&tx, &seeds)?;
        if expanded.len() > MAX_EXPANDED_BATCH {
            return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "cascade expansion of {} tasks exceeds MAX_EXPANDED_BATCH ({}); narrow the seed set",
                expanded.len(),
                MAX_EXPANDED_BATCH
            ))));
        }
        expanded
    } else {
        seeds
    };

    // Pull existing memberships for the candidate set in a single SQL
    // query (uses `json_each` — no per-row round-trips, no SQLite
    // parameter-count limit). Combined with a HashSet for batch
    // dedup, the add path is O(N) in candidates regardless of cascade
    // depth or pre-existing membership count.
    let existing: HashSet<String> = saga_members_in(&tx, &canonical, &candidates)?
        .into_iter()
        .collect();
    let mut seen: HashSet<String> = HashSet::with_capacity(candidates.len());
    let mut to_insert: Vec<String> = Vec::with_capacity(candidates.len());
    for full_id in candidates {
        if existing.contains(&full_id) {
            continue;
        }
        if !seen.insert(full_id.clone()) {
            continue;
        }
        to_insert.push(full_id);
    }

    if to_insert.is_empty() {
        tx.commit()?;
        return Ok(Vec::new());
    }

    queries::insert_saga_tasks(&tx, &canonical, &to_insert)?;

    // Emit one SagaTaskAdded event per newly inserted task.
    for task_id in &to_insert {
        let event = SagaEvent::new(
            &canonical,
            actor,
            SagaEventType::SagaTaskAdded,
            &SagaTaskPayload {
                task_id: task_id.clone(),
            },
        );
        queries::insert_saga_event(
            &tx,
            &SagaEventInsert {
                event_id: &event.event_id,
                saga_id: &event.saga_id,
                event_type: event.event_type.as_column_str(),
                timestamp: event.timestamp,
                actor: &event.actor,
                payload: &serde_json::to_string(&event.payload)?,
            },
        )?;
    }

    tx.commit()?;
    Ok(to_insert)
}

/// Remove tasks from a saga. Idempotent: missing memberships are no-ops.
/// Returns the canonical task IDs that were *actually removed* (i.e.
/// the intersection of the resolved candidate set with current
/// membership). Callers use `.len()` for the count. Surfacing the
/// set lets transports (MCP, CLI) tell the user which tasks were
/// stripped — particularly important when `cascade=true` and the
/// removal expanded silently. Emits one `SagaTaskRemoved` event per
/// actual removal. Single transaction.
pub fn remove_tasks(
    conn: &Connection,
    saga_id: &str,
    task_ids: Vec<String>,
    cascade: bool,
    actor: &str,
) -> SqlResult<Vec<String>> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }

    // H1: SELECT-DELETE-INSERT must be atomic so a concurrent insert
    // between the SELECT and DELETE cannot create a member that is
    // then deleted without a SagaTaskRemoved event being emitted.
    //
    // unchecked_ ok: caller holds the writer mutex via with_write_conn,
    // single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    // Reject closed/cancelled sagas — same guard as `add_tasks`.
    let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::SagaNotFound(format!(
            "saga not found: {saga_id}"
        )))
    })?;
    let saga_status: SagaStatus = row.status.parse().map_err(|_| {
        SqlError::Domain(BrainCoreError::Parse(format!(
            "unknown saga status: {}",
            row.status
        )))
    })?;
    if matches!(saga_status, SagaStatus::Closed | SagaStatus::Cancelled) {
        return Err(SqlError::Domain(BrainCoreError::Parse(format!(
            "cannot remove tasks from saga in '{saga_status}' status; reopen it before modifying"
        ))));
    }

    // Resolve each input ID. The lenient (typo-tolerant) path is the
    // contract for cascade=false — unresolvable inputs become no-ops
    // so that a stale task_id doesn't break a routine cleanup. With
    // cascade=true, the user has explicitly asked for subtree
    // semantics; a typo would silently degrade to a single-task
    // no-op rather than the intended subtree strip, so we fail loud.
    let seeds: Vec<String> = if cascade {
        let mut out = Vec::with_capacity(task_ids.len());
        for raw in &task_ids {
            let full = resolve_task_id_scoped(&tx, raw, None).map_err(|e| {
                SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "task '{raw}' could not be resolved (cascade=true requires resolvable seeds): {e}"
                )))
            })?;
            out.push(full);
        }
        out
    } else {
        task_ids
            .iter()
            .map(|raw| resolve_task_id_scoped(&tx, raw, None).unwrap_or_else(|_| raw.clone()))
            .collect()
    };

    // When `cascade` is true, expand each input to itself plus every
    // transitive descendant in the parent_of graph. The intersection
    // with `saga_tasks` is computed by `saga_members_in` below —
    // descendants that aren't currently members drop out idempotently.
    // Reject runaway expansions before any other work.
    let resolved: Vec<String> = if cascade {
        let expanded = task_subtree(&tx, &seeds)?;
        if expanded.len() > MAX_EXPANDED_BATCH {
            return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "cascade expansion of {} tasks exceeds MAX_EXPANDED_BATCH ({}); narrow the seed set",
                expanded.len(),
                MAX_EXPANDED_BATCH
            ))));
        }
        expanded
    } else {
        seeds
    };

    // Identify which task_ids are currently members before deleting,
    // so we know exactly which ones to emit events for. Single SQL
    // pass via `json_each` — no per-row round-trips, no SQLite
    // parameter-count limit on large cascade-expanded sets.
    let present: Vec<String> = saga_members_in(&tx, &canonical, &resolved)?;

    if present.is_empty() {
        tx.commit()?;
        return Ok(Vec::new());
    }

    // Only delete the rows that were actually members; this also
    // makes the `present.len() == removed_count` invariant explicit.
    queries::remove_saga_tasks(&tx, &canonical, &present)?;

    for task_id in &present {
        let payload = SagaTaskPayload {
            task_id: task_id.clone(),
        };
        let event = SagaEvent::new(&canonical, actor, SagaEventType::SagaTaskRemoved, &payload);
        queries::insert_saga_event(
            &tx,
            &SagaEventInsert {
                event_id: &event.event_id,
                saga_id: &event.saga_id,
                event_type: event.event_type.as_column_str(),
                timestamp: event.timestamp,
                actor: &event.actor,
                payload: &serde_json::to_string(&event.payload)?,
            },
        )?;
    }

    tx.commit()?;
    Ok(present)
}
