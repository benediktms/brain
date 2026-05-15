//! Saga metadata operations: create, update.
//!
//! Each function is a typed orchestration helper that runs inside a caller-
//! supplied `&Connection`. These verbs do not touch the lifecycle state
//! machine — `create` inserts a `planning` row and `update` mutates
//! `title`/`description` independent of status. The caller (`SagaStore`) owns
//! transaction lifecycle via `Db::with_write_conn`.
//!
//! Mirrors the `brain_tasks::projections` shape: domain crate owns
//! orchestration; `brain_persistence` exposes typed primitives.

use rusqlite::Connection;

use brain_core::error::BrainCoreError;
use brain_persistence::db::sagas::events::{
    SagaEvent, SagaEventType, SagaUpdatedPayload, new_saga_id,
};
use brain_persistence::db::sagas::queries::{self, SagaEventInsert, SagaRow};
use brain_persistence::db::sagas::resolve_saga_id;
use brain_persistence::sql::{SqlError, SqlResult};

/// Create a new saga in `planning` status. Returns the resulting row.
pub fn create(
    conn: &Connection,
    title: &str,
    description: Option<&str>,
    actor: &str,
) -> SqlResult<SagaRow> {
    if title.trim().is_empty() {
        return Err(SqlError::Domain(BrainCoreError::Parse(
            "saga title must not be empty".into(),
        )));
    }

    let saga_id = new_saga_id();

    // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
    // H1: wrap projection write + event insert in one SQLite tx so a failure
    // between the two cannot leave the projection mutated without a corresponding
    // saga_events row. Mirrors every other verb in this file.
    let tx = conn.unchecked_transaction()?;

    let row = queries::insert_saga(&tx, &saga_id, title, description)?;

    let event = SagaEvent::new(
        &saga_id,
        actor,
        SagaEventType::SagaCreated,
        &serde_json::json!({ "title": title, "description": description }),
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
    Ok(row)
}

/// Update title and/or description. At least one field required. Allowed in any status.
///
/// `description` uses `Option<Option<&str>>`:
/// - `None` = don't touch description
/// - `Some(None)` = set description to NULL
/// - `Some(Some("text"))` = set description to "text"
/// - `Some(Some(""))` is canonicalized to `Some(None)` so empty strings
///   are stored as NULL, keeping the column shape consistent.
pub fn update(
    conn: &Connection,
    saga_id: &str,
    title: Option<&str>,
    description: Option<Option<&str>>,
    actor: &str,
) -> SqlResult<SagaRow> {
    if title.is_none() && description.is_none() {
        return Err(SqlError::Domain(BrainCoreError::Parse(
            "update: at least one of title or description must be provided".into(),
        )));
    }
    let title = match title {
        Some(t) => {
            let trimmed = t.trim();
            if trimmed.is_empty() {
                return Err(SqlError::Domain(BrainCoreError::Parse(
                    "update: title must not be empty".into(),
                )));
            }
            Some(trimmed)
        }
        None => None,
    };
    // Canonicalize empty description to NULL so the store is consistent.
    let description = description.map(|d| match d {
        Some("") => None,
        other => other,
    });

    // H1: wrap projection write + event insert in one SQLite tx so a
    // failure between the two cannot leave the projection mutated
    // without a corresponding saga_events row.
    //
    // unchecked_ ok: caller MUST hold the writer mutex (typically via
    // Db::with_write_conn), single writer guaranteed.
    let tx = conn.unchecked_transaction()?;
    let canonical = resolve_saga_id(&tx, saga_id)?;

    let row = queries::update_saga(&tx, &canonical, title, description)?;

    let payload = SagaUpdatedPayload {
        title: title.map(|t| t.to_string()),
        description: description.map(|d| d.map(|s| s.to_string())),
    };
    let event = SagaEvent::new(&canonical, actor, SagaEventType::SagaUpdated, &payload);
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
    Ok(row)
}
