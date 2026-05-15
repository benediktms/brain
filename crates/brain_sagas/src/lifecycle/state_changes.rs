//! Saga state-machine transitions: start, close, cancel, reopen.
//!
//! Each function is a typed orchestration helper that runs inside a caller-
//! supplied `&Connection`. The caller (`SagaStore`) owns transaction lifecycle
//! via `Db::with_write_conn`; these helpers compose the persistence primitives
//! (`queries::get_saga`, `queries::close_saga`, `queries::insert_saga_event`,
//! `queries::cascade_member_tasks`, etc.) plus the [`super::validate_transition`]
//! state-machine guard.
//!
//! Mirrors the `brain_tasks::projections::validate_and_apply` shape: domain
//! crate owns orchestration; `brain_persistence` exposes typed primitives.

use rusqlite::Connection;

use brain_core::error::BrainCoreError;
use brain_core::utils::now_ts;
use brain_persistence::db::sagas::events::{
    SagaCancelledPayload, SagaClosedPayload, SagaEvent, SagaEventType,
};
use brain_persistence::db::sagas::queries::{self, CascadeResult, SagaEventInsert, SagaRow};
use brain_persistence::db::sagas::resolve_saga_id;
use brain_persistence::sql::{SqlError, SqlResult};

use crate::lifecycle::validate_transition;
use crate::status::SagaStatus;

/// Transition a saga from `planning` to `open`. Emits `SagaStarted`.
pub fn start(conn: &Connection, saga_id: &str, actor: &str) -> SqlResult<SagaRow> {
    // unchecked_ ok: caller holds the writer mutex via with_write_conn, single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::SagaNotFound(format!(
            "saga not found: {saga_id}"
        )))
    })?;

    let from: SagaStatus = row.status.parse().map_err(|_| {
        SqlError::Domain(BrainCoreError::Parse(format!(
            "unknown saga status: {}",
            row.status
        )))
    })?;

    validate_transition(from, SagaStatus::Open).map_err(SqlError::Domain)?;

    // L4: use the canonical now_ts() helper instead of an inline
    // SystemTime::now().unwrap_or(0) which would silently write
    // epoch-zero on a clock anomaly.
    let now = now_ts();
    queries::start_saga(&tx, &canonical, now)?;

    let event = SagaEvent::new(
        &canonical,
        actor,
        SagaEventType::SagaStarted,
        &serde_json::json!({}),
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

    let result = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::Parse("saga disappeared after start".into()))
    })?;
    tx.commit()?;
    Ok(result)
}

/// Close a saga. Only `open` sagas can be closed.
///
/// Returns `(row, cascade_results)`. With `cascade = true`, every member
/// task is examined and best-effort transitioned to `Done`. The saga's
/// own state change and the entire cascade run inside one SQLite
/// transaction, so a crash mid-cascade cannot leave the saga `closed`
/// with only some member tasks transitioned (H2). The cascade itself is
/// best-effort within that transaction: per-task append failures are
/// recorded as `CascadeOutcome::Failed` and do not roll back the saga's
/// status change.
pub fn close(
    conn: &Connection,
    saga_id: &str,
    cascade: bool,
    actor: &str,
) -> SqlResult<(SagaRow, Vec<CascadeResult>)> {
    // unchecked_ ok: caller holds the writer mutex via with_write_conn, single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    let current = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::SagaNotFound(format!(
            "saga not found: {saga_id}"
        )))
    })?;

    let from: SagaStatus = current.status.parse().map_err(|_| {
        SqlError::Domain(BrainCoreError::Parse(format!(
            "unknown saga status: {}",
            current.status
        )))
    })?;

    validate_transition(from, SagaStatus::Closed).map_err(SqlError::Domain)?;

    let row = queries::close_saga(&tx, &canonical)?;

    let event = SagaEvent::new(
        &canonical,
        actor,
        SagaEventType::SagaClosed,
        &SagaClosedPayload { cascade },
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

    let cascade_results = if cascade {
        queries::cascade_member_tasks(&tx, &canonical, actor, "done")?
    } else {
        vec![]
    };

    tx.commit()?;
    Ok((row, cascade_results))
}

/// Cancel a saga, optionally cascade-cancelling non-terminal member tasks.
///
/// Returns `(row, cascade_results)`. The saga's state change and the
/// entire cascade run inside one SQLite transaction. The cascade itself
/// is best-effort: per-task append failures are recorded as
/// `CascadeOutcome::Failed` and do not roll back the saga's status
/// change. Already-done and already-cancelled tasks are recorded as
/// `Skipped`.
pub fn cancel(
    conn: &Connection,
    saga_id: &str,
    cascade: bool,
    actor: &str,
) -> SqlResult<(SagaRow, Vec<CascadeResult>)> {
    // unchecked_ ok: caller holds the writer mutex via with_write_conn, single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::SagaNotFound(format!(
            "saga not found: {saga_id}"
        )))
    })?;

    let from: SagaStatus = row.status.parse().map_err(|_| {
        SqlError::Domain(BrainCoreError::Parse(format!(
            "unknown saga status: {}",
            row.status
        )))
    })?;
    // Pre-checks for friendlier error messages — `validate_transition`
    // would still reject these, but with a generic "invalid transition"
    // string. Spec: cancel applies only to active states.
    if matches!(from, SagaStatus::Cancelled) {
        return Err(SqlError::Domain(BrainCoreError::Parse(format!(
            "saga '{saga_id}' is already cancelled"
        ))));
    }
    if matches!(from, SagaStatus::Closed) {
        return Err(SqlError::Domain(BrainCoreError::Parse(format!(
            "saga '{saga_id}' is closed; reopen it before cancelling"
        ))));
    }
    validate_transition(from, SagaStatus::Cancelled).map_err(SqlError::Domain)?;

    queries::cancel_saga(&tx, &canonical)?;

    let event = SagaEvent::new(
        &canonical,
        actor,
        SagaEventType::SagaCancelled,
        &SagaCancelledPayload { cascade },
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

    let cascade_results = if cascade {
        queries::cascade_member_tasks(&tx, &canonical, actor, "cancelled")?
    } else {
        vec![]
    };

    let updated = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::Database(
            "saga disappeared after cancel".into(),
        ))
    })?;
    tx.commit()?;
    Ok((updated, cascade_results))
}

/// Reopen a closed or cancelled saga, setting status back to `open`.
/// Clears `closed_at`. Emits `SagaReopened`. Rejected from `planning` or `open`.
pub fn reopen(conn: &Connection, saga_id: &str, actor: &str) -> SqlResult<SagaRow> {
    // unchecked_ ok: caller holds the writer mutex via with_write_conn, single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
        SqlError::Domain(BrainCoreError::SagaNotFound(format!(
            "saga not found: {saga_id}"
        )))
    })?;

    let from: SagaStatus = row.status.parse().map_err(|_| {
        SqlError::Domain(BrainCoreError::Parse(format!(
            "unknown saga status: {}",
            row.status
        )))
    })?;

    // Reopen is only valid from terminal states; planning→open is `start`, not `reopen`.
    match from {
        SagaStatus::Closed | SagaStatus::Cancelled => {}
        other => {
            return Err(SqlError::Domain(BrainCoreError::Parse(format!(
                "cannot reopen saga in '{other}' status; allowed: closed, cancelled"
            ))));
        }
    }

    let updated = queries::reopen_saga(&tx, &canonical)?;

    let event = SagaEvent::new(
        &canonical,
        actor,
        SagaEventType::SagaReopened,
        &serde_json::json!({}),
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

    tx.commit()?;
    Ok(updated)
}
