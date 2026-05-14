use rusqlite::Connection;

use brain_core::error::BrainCoreError;
use brain_persistence::db::tasks::queries::next_child_seq;
use brain_persistence::db::tasks::writers;
use brain_persistence::sql::{SqlError, SqlResult};

use crate::events::{
    CommentPayload, CommentUpdatedPayload, DependencyPayload, EventType,
    ExternalBlockerAddedPayload, ExternalBlockerResolvedPayload, ExternalIdPayload, LabelPayload,
    NoteLinkPayload, ParentSetPayload, StatusChangedPayload, TaskCreatedPayload, TaskEvent,
    TaskTransferredPayload, TaskUpdatedPayload,
};

/// Apply a single event to SQLite projections (no transaction wrapper).
///
/// Called by `apply_event` (which wraps in its own tx), `rebuild` (inside an
/// existing tx), and saga cascade flows (which pass through their outer tx).
/// Callers operating inside an open transaction MUST use this rather than
/// `apply_event` to avoid SQLite's nested-`BEGIN` rejection.
pub(crate) fn apply_event_inner(
    conn: &Connection,
    event: &TaskEvent,
    brain_id: &str,
) -> SqlResult<()> {
    match event.event_type {
        EventType::TaskCreated => {
            let p: TaskCreatedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad TaskCreated payload: {e}"
                    )))
                })?;

            let child_seq = p
                .parent_task_id
                .as_deref()
                .map(|pid| next_child_seq(conn, pid))
                .transpose()?;

            writers::insert_task_row(
                conn,
                &event.task_id,
                brain_id,
                &p.title,
                p.description.as_deref(),
                p.status.as_ref(),
                p.priority,
                p.due_ts,
                p.task_type.unwrap_or_default().as_str(),
                p.assignee.as_deref(),
                p.defer_until,
                p.parent_task_id.as_deref(),
                child_seq,
                event.timestamp,
                p.display_id.as_deref(),
            )?;
        }

        EventType::TaskUpdated => {
            let p: TaskUpdatedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad TaskUpdated payload: {e}"
                    )))
                })?;

            // For now, call the deprecated catch-all writer. Intent-per-field
            // dispatch is a deliberate follow-up.
            #[allow(deprecated)]
            writers::update_task(
                conn,
                &event.task_id,
                &writers::TaskUpdateFields {
                    title: p.title.as_deref(),
                    description: p.description.as_deref(),
                    priority: p.priority,
                    due_ts: p.due_ts,
                    blocked_reason: p.blocked_reason.as_deref(),
                    task_type: p.task_type.map(|t| t.as_str()),
                    assignee: p.assignee.as_deref(),
                    defer_until: p.defer_until,
                },
                event.timestamp,
            )?;
        }

        EventType::StatusChanged => {
            let p: StatusChangedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad StatusChanged payload: {e}"
                    )))
                })?;

            writers::set_task_status(conn, &event.task_id, p.new_status.as_ref(), event.timestamp)?;
        }

        EventType::DependencyAdded => {
            let p: DependencyPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad DependencyAdded payload: {e}"
                    )))
                })?;

            writers::add_dependency(conn, &event.task_id, &p.depends_on_task_id)?;
        }

        EventType::DependencyRemoved => {
            let p: DependencyPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad DependencyRemoved payload: {e}"
                    )))
                })?;

            writers::remove_dependency(conn, &event.task_id, &p.depends_on_task_id)?;
        }

        EventType::NoteLinked => {
            let p: NoteLinkPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad NoteLinked payload: {e}"
                    )))
                })?;

            writers::link_note(conn, &event.task_id, &p.chunk_id)?;
        }

        EventType::NoteUnlinked => {
            let p: NoteLinkPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad NoteUnlinked payload: {e}"
                    )))
                })?;

            writers::unlink_note(conn, &event.task_id, &p.chunk_id)?;
        }

        EventType::LabelAdded => {
            let p: LabelPayload = serde_json::from_value(event.payload.clone()).map_err(|e| {
                SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "bad LabelAdded payload: {e}"
                )))
            })?;

            writers::add_label(conn, &event.task_id, &p.label)?;
        }

        EventType::LabelRemoved => {
            let p: LabelPayload = serde_json::from_value(event.payload.clone()).map_err(|e| {
                SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "bad LabelRemoved payload: {e}"
                )))
            })?;

            writers::remove_label(conn, &event.task_id, &p.label)?;
        }

        EventType::CommentAdded => {
            let p: CommentPayload = serde_json::from_value(event.payload.clone()).map_err(|e| {
                SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "bad CommentAdded payload: {e}"
                )))
            })?;

            writers::add_comment(
                conn,
                &event.event_id,
                &event.task_id,
                &event.actor,
                &p.body,
                event.timestamp,
            )?;
        }

        EventType::CommentUpdated => {
            let p: CommentUpdatedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad CommentUpdated payload: {e}"
                    )))
                })?;

            writers::update_comment(
                conn,
                &p.comment_id,
                &event.task_id,
                &p.body,
                event.timestamp,
            )?;
        }

        EventType::ParentSet => {
            let p: ParentSetPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad ParentSet payload: {e}"
                    )))
                })?;

            match &p.parent_task_id {
                Some(parent_id) => {
                    let child_seq = next_child_seq(conn, parent_id)?;
                    writers::set_task_parent(
                        conn,
                        &event.task_id,
                        parent_id,
                        Some(child_seq),
                        event.timestamp,
                    )?;
                }
                None => {
                    writers::clear_task_parent(conn, &event.task_id, event.timestamp)?;
                }
            }
        }

        EventType::ExternalIdAdded => {
            let p: ExternalIdPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad ExternalIdAdded payload: {e}"
                    )))
                })?;

            writers::add_external_id(
                conn,
                &event.task_id,
                &p.source,
                &p.external_id,
                p.external_url.as_deref(),
                event.timestamp,
            )?;
        }

        EventType::ExternalIdRemoved => {
            let p: ExternalIdPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad ExternalIdRemoved payload: {e}"
                    )))
                })?;

            writers::remove_external_id(conn, &event.task_id, &p.source, &p.external_id)?;
        }

        EventType::ExternalBlockerAdded => {
            // Insert or promote a row in `task_external_ids` to act as a real
            // blocker (always `blocking = 1`). Idempotent: re-applying the
            // event for an existing row clears any prior `resolved_at` so
            // event-log replay reaches the right state.
            let p: ExternalBlockerAddedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad ExternalBlockerAdded payload: {e}"
                    )))
                })?;

            writers::add_external_blocker(
                conn,
                &event.task_id,
                &p.source,
                &p.external_id,
                p.external_url.as_deref(),
                event.timestamp,
            )?;
        }

        EventType::TaskTransferred => {
            // Replay path: apply the brain_id and display_id from the payload to the
            // tasks row. The transfer transaction applies this directly; the projection
            // here is for event-log replay consistency.
            let p: TaskTransferredPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad TaskTransferred payload: {e}"
                    )))
                })?;

            writers::transfer_task(
                conn,
                &event.task_id,
                &p.to_brain_id,
                &p.to_display_id,
                event.timestamp,
            )?;
        }

        EventType::ExternalBlockerResolved => {
            // Stamp `resolved_at` on the matching blocker row. Read the row
            // state first so we can differentiate the four outcomes for an
            // operator: row absent (likely caller bug), row is metadata-only
            // (caller used the wrong event), already resolved (idempotent
            // replay), or fresh resolution (the happy path).
            let p: ExternalBlockerResolvedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| {
                SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "bad ExternalBlockerResolved payload: {e}"
                )))
            })?;
            let resolved_at = p.resolved_at.unwrap_or(event.timestamp);

            match writers::resolve_external_blocker(
                conn,
                &event.task_id,
                &p.source,
                &p.external_id,
                resolved_at,
            )? {
                writers::ExternalBlockerResolveOutcome::NoMatchingRow => {
                    tracing::warn!(
                        task_id = %event.task_id,
                        source = %p.source,
                        external_id = %p.external_id,
                        "external_blocker_resolved: no matching row — caller likely resolved a blocker that was never recorded"
                    );
                }
                writers::ExternalBlockerResolveOutcome::MetadataOnly => {
                    tracing::warn!(
                        task_id = %event.task_id,
                        source = %p.source,
                        external_id = %p.external_id,
                        "external_blocker_resolved: row exists but blocking=0 (metadata only) — use external_blocker_added first to promote, or external_id_removed to retire"
                    );
                }
                writers::ExternalBlockerResolveOutcome::AlreadyResolved { prior } => {
                    tracing::debug!(
                        task_id = %event.task_id,
                        source = %p.source,
                        external_id = %p.external_id,
                        prior_resolved_at = prior,
                        new_resolved_at = resolved_at,
                        "external_blocker_resolved: blocker already resolved — re-stamping (idempotent replay)"
                    );
                }
                writers::ExternalBlockerResolveOutcome::FreshResolve => {}
            }
        }
    }

    // Record the event itself
    let payload_json = serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".into());
    writers::append_task_event_log(
        conn,
        &event.event_id,
        &event.task_id,
        &format!("{:?}", event.event_type),
        event.timestamp,
        &event.actor,
        &payload_json,
    )?;

    Ok(())
}

/// Apply a single event to the SQLite projection tables.
///
/// `brain_id` is stamped on the `tasks` row for all `TaskCreated` events.
/// For all other event types the brain_id is not re-written (the row already
/// carries the brain_id set at creation time).
///
/// The projection mutation and event INSERT are wrapped in an explicit
/// transaction for atomicity.
pub fn apply_event(conn: &Connection, event: &TaskEvent, brain_id: &str) -> SqlResult<()> {
    let tx = conn.unchecked_transaction()?;
    apply_event_inner(&tx, event, brain_id)?;
    tx.commit()?;
    Ok(())
}

/// Validate an event and apply it in a single transaction.
///
/// Validation rules:
/// - `TaskCreated`: task_id must NOT already exist; parent must exist if set
/// - `TaskUpdated`/`StatusChanged`: task must exist
/// - `DependencyAdded`: both tasks must exist, no cycle
/// - `ParentSet`: task must exist; parent must exist and not be self
/// - Others: task must exist
pub fn validate_and_apply(conn: &Connection, event: &TaskEvent, brain_id: &str) -> SqlResult<()> {
    use brain_persistence::db::tasks::cycle;
    use brain_persistence::db::tasks::queries::task_exists;

    let tx = conn.unchecked_transaction()?;

    // ── Validation ────────────────────────────────────────────────
    match event.event_type {
        EventType::TaskCreated => {
            if task_exists(&tx, &event.task_id)? {
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "task already exists: {}",
                    event.task_id
                ))));
            }
            let payload: TaskCreatedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad TaskCreated payload: {e}"
                    )))
                })?;
            if let Some(ref parent_id) = payload.parent_task_id {
                if parent_id == &event.task_id {
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(
                        "task cannot be its own parent".to_string(),
                    )));
                }
                if !task_exists(&tx, parent_id)? {
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "parent task not found: {parent_id}"
                    ))));
                }
            }
        }

        EventType::TaskUpdated | EventType::StatusChanged => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                ))));
            }
        }

        EventType::DependencyAdded => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                ))));
            }
            let payload: DependencyPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad DependencyAdded payload: {e}"
                    )))
                })?;
            if !task_exists(&tx, &payload.depends_on_task_id)? {
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "dependency target not found: {}",
                    payload.depends_on_task_id
                ))));
            }
            cycle::check_cycle(&tx, &event.task_id, &payload.depends_on_task_id)?;
        }

        EventType::ParentSet => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                ))));
            }
            let payload: ParentSetPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "bad ParentSet payload: {e}"
                    )))
                })?;
            if let Some(ref parent_id) = payload.parent_task_id {
                if parent_id == &event.task_id {
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(
                        "task cannot be its own parent".to_string(),
                    )));
                }
                if !task_exists(&tx, parent_id)? {
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                        "parent task not found: {parent_id}"
                    ))));
                }
            }
        }

        EventType::DependencyRemoved
        | EventType::NoteLinked
        | EventType::NoteUnlinked
        | EventType::LabelAdded
        | EventType::LabelRemoved
        | EventType::CommentAdded
        | EventType::CommentUpdated
        | EventType::ExternalIdAdded
        | EventType::ExternalIdRemoved
        | EventType::ExternalBlockerAdded
        | EventType::ExternalBlockerResolved
        | EventType::TaskTransferred => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(SqlError::Domain(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                ))));
            }
        }
    }

    // ── Apply ─────────────────────────────────────────────────────
    apply_event_inner(&tx, event, brain_id)?;
    tx.commit()?;
    Ok(())
}

/// Rebuild all projection tables from a full event sequence.
///
/// Drops FTS task triggers before the bulk delete to avoid content-sync
/// deletes on a potentially corrupt index, then rebuilds the FTS index
/// and re-creates triggers after commit.
pub fn rebuild(conn: &Connection, events: &[TaskEvent]) -> SqlResult<()> {
    writers::drop_fts_triggers(conn)?;

    let tx = conn.unchecked_transaction()?;
    writers::rebuild_clear_all(&tx)?;

    for event in events {
        // During rebuild, preserve the existing brain_id value from the event log
        // by reading it from the DB if available, or default to empty string.
        apply_event_inner(&tx, event, "")?;
    }

    tx.commit()?;

    writers::rebuild_fts_index(conn)?;

    // Re-create triggers
    brain_persistence::db::schema::ensure_fts5(conn)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_persistence::db::schema::init_schema;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn make_created_event(task_id: &str, title: &str, priority: i32) -> TaskEvent {
        use crate::events::{TaskCreatedPayload, TaskStatus};
        TaskEvent::from_payload(
            task_id,
            "user",
            TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
                display_id: None,
            },
        )
    }

    #[test]
    fn test_apply_task_created() {
        let conn = setup();
        let ev = make_created_event("t1", "My Task", 2);
        apply_event(&conn, &ev, "").unwrap();

        let title: String = conn
            .query_row("SELECT title FROM tasks WHERE task_id = 't1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "My Task");

        let priority: i32 = conn
            .query_row(
                "SELECT priority FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(priority, 2);
    }

    #[test]
    fn test_apply_task_updated() {
        use crate::events::TaskUpdatedPayload;
        let conn = setup();
        let ev = make_created_event("t1", "Original", 4);
        apply_event(&conn, &ev, "").unwrap();

        let update = TaskEvent::from_payload(
            "t1",
            "user",
            TaskUpdatedPayload {
                title: Some("Updated".to_string()),
                description: Some("A description".to_string()),
                priority: Some(1),
                due_ts: None,
                blocked_reason: None,
                task_type: None,
                assignee: None,
                defer_until: None,
            },
        );
        apply_event(&conn, &update, "").unwrap();

        let (title, desc, priority): (String, Option<String>, i32) = conn
            .query_row(
                "SELECT title, description, priority FROM tasks WHERE task_id = 't1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(title, "Updated");
        assert_eq!(desc.as_deref(), Some("A description"));
        assert_eq!(priority, 1);
    }

    #[test]
    fn test_apply_status_changed() {
        use crate::events::{StatusChangedPayload, TaskStatus};
        let conn = setup();
        let ev = make_created_event("t1", "Task", 2);
        apply_event(&conn, &ev, "").unwrap();

        let status_ev = TaskEvent::from_payload(
            "t1",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        apply_event(&conn, &status_ev, "").unwrap();

        let status: String = conn
            .query_row("SELECT status FROM tasks WHERE task_id = 't1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "done");
    }

    #[test]
    fn test_apply_dependency_added_removed() {
        use crate::events::DependencyPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("t1", "Task 1", 2), "").unwrap();
        apply_event(&conn, &make_created_event("t2", "Task 2", 2), "").unwrap();

        let dep_add = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        apply_event(&conn, &dep_add, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_deps WHERE task_id = 't1' AND depends_on = 't2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Remove
        let dep_rm = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyRemoved,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        apply_event(&conn, &dep_rm, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_deps WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_apply_note_linked_unlinked() {
        use crate::events::NoteLinkPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("t1", "Task", 2), "").unwrap();

        // Create a file and chunk to satisfy FK constraint on task_note_links
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/test.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content) VALUES ('c1', 'f1', 0, 'h0', 'test')",
            [],
        )
        .unwrap();

        let link = TaskEvent::new(
            "t1",
            "user",
            EventType::NoteLinked,
            &NoteLinkPayload {
                chunk_id: "c1".to_string(),
            },
        );
        apply_event(&conn, &link, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_note_links WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let unlink = TaskEvent::new(
            "t1",
            "user",
            EventType::NoteUnlinked,
            &NoteLinkPayload {
                chunk_id: "c1".to_string(),
            },
        );
        apply_event(&conn, &unlink, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_note_links WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_event_recorded_in_task_events() {
        let conn = setup();
        let ev = make_created_event("t1", "Task", 2);
        apply_event(&conn, &ev, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_events WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_rebuild_idempotent() {
        let conn = setup();

        let events = vec![
            make_created_event("t1", "Task 1", 1),
            make_created_event("t2", "Task 2", 3),
        ];

        rebuild(&conn, &events).unwrap();
        let count1: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count1, 2);

        // Rebuild again — should produce same state
        rebuild(&conn, &events).unwrap();
        let count2: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count2, 2);
    }

    #[test]
    fn test_rebuild_survives_corrupt_fts_index() {
        let conn = setup();

        // Insert tasks so FTS has content
        let events = vec![
            make_created_event("t1", "Alpha searchable task", 1),
            make_created_event("t2", "Beta findable task", 2),
        ];
        rebuild(&conn, &events).unwrap();

        // Corrupt the FTS index by deleting shadow table data
        conn.execute_batch("DELETE FROM fts_tasks_data;").unwrap();

        // Rebuild must succeed despite corrupt FTS index
        rebuild(&conn, &events).unwrap();

        // Verify FTS search works after rebuild
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'searchable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'findable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    // ── Dual-write: entity_links mirrors legacy task relationship events ───────

    fn count_entity_links(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM entity_links", [], |r| r.get(0))
            .unwrap()
    }

    fn entity_link_exists(conn: &Connection, from: &str, to: &str, kind: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM entity_links
             WHERE from_type = 'TASK' AND from_id = ?1
               AND to_type   = 'TASK' AND to_id   = ?2
               AND edge_kind = ?3",
            rusqlite::params![from, to, kind],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    #[test]
    fn dual_write_dependency_added_creates_entity_link() {
        use crate::events::DependencyPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("t1", "Task 1", 2), "").unwrap();
        apply_event(&conn, &make_created_event("t2", "Task 2", 2), "").unwrap();

        let dep_add = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        apply_event(&conn, &dep_add, "").unwrap();

        assert!(
            entity_link_exists(&conn, "t1", "t2", "blocks"),
            "entity_links must contain blocks edge t1→t2"
        );
        assert_eq!(count_entity_links(&conn), 1);
    }

    #[test]
    fn dual_write_dependency_removed_deletes_entity_link() {
        use crate::events::DependencyPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("t1", "Task 1", 2), "").unwrap();
        apply_event(&conn, &make_created_event("t2", "Task 2", 2), "").unwrap();

        apply_event(
            &conn,
            &TaskEvent::new(
                "t1",
                "user",
                EventType::DependencyAdded,
                &DependencyPayload {
                    depends_on_task_id: "t2".to_string(),
                },
            ),
            "",
        )
        .unwrap();
        assert_eq!(count_entity_links(&conn), 1);

        apply_event(
            &conn,
            &TaskEvent::new(
                "t1",
                "user",
                EventType::DependencyRemoved,
                &DependencyPayload {
                    depends_on_task_id: "t2".to_string(),
                },
            ),
            "",
        )
        .unwrap();

        assert_eq!(count_entity_links(&conn), 0, "blocks edge must be removed");
    }

    #[test]
    fn dual_write_parent_set_creates_entity_link() {
        use crate::events::ParentSetPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("parent", "Parent", 2), "").unwrap();
        apply_event(&conn, &make_created_event("child", "Child", 2), "").unwrap();

        let parent_set = TaskEvent::new(
            "child",
            "user",
            EventType::ParentSet,
            &ParentSetPayload {
                parent_task_id: Some("parent".to_string()),
            },
        );
        apply_event(&conn, &parent_set, "").unwrap();

        assert!(
            entity_link_exists(&conn, "parent", "child", "parent_of"),
            "entity_links must contain parent_of edge parent→child"
        );
        assert_eq!(count_entity_links(&conn), 1);
    }

    #[test]
    fn dual_write_parent_cleared_removes_entity_link() {
        use crate::events::ParentSetPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("parent", "Parent", 2), "").unwrap();
        apply_event(&conn, &make_created_event("child", "Child", 2), "").unwrap();

        apply_event(
            &conn,
            &TaskEvent::new(
                "child",
                "user",
                EventType::ParentSet,
                &ParentSetPayload {
                    parent_task_id: Some("parent".to_string()),
                },
            ),
            "",
        )
        .unwrap();
        assert_eq!(count_entity_links(&conn), 1);

        apply_event(
            &conn,
            &TaskEvent::new(
                "child",
                "user",
                EventType::ParentSet,
                &ParentSetPayload {
                    parent_task_id: None,
                },
            ),
            "",
        )
        .unwrap();

        assert_eq!(
            count_entity_links(&conn),
            0,
            "parent_of edge must be removed on parent clear"
        );
    }

    #[test]
    fn dual_write_cardinality_matches_legacy_counts() {
        use crate::events::{DependencyPayload, ParentSetPayload};
        let conn = setup();
        // Create 4 tasks
        for id in ["t1", "t2", "t3", "t4"] {
            apply_event(&conn, &make_created_event(id, id, 2), "").unwrap();
        }

        // 2 dep edges: t1→t2, t1→t3
        apply_event(
            &conn,
            &TaskEvent::new(
                "t1",
                "user",
                EventType::DependencyAdded,
                &DependencyPayload {
                    depends_on_task_id: "t2".to_string(),
                },
            ),
            "",
        )
        .unwrap();
        apply_event(
            &conn,
            &TaskEvent::new(
                "t1",
                "user",
                EventType::DependencyAdded,
                &DependencyPayload {
                    depends_on_task_id: "t3".to_string(),
                },
            ),
            "",
        )
        .unwrap();

        // 1 parent edge: t4 child of t1
        apply_event(
            &conn,
            &TaskEvent::new(
                "t4",
                "user",
                EventType::ParentSet,
                &ParentSetPayload {
                    parent_task_id: Some("t1".to_string()),
                },
            ),
            "",
        )
        .unwrap();

        let dep_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM task_deps", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dep_count, 2, "legacy task_deps must have 2 rows");

        assert_eq!(
            count_entity_links(&conn),
            3,
            "entity_links must equal dep_count + parent_count = 2 + 1"
        );
    }

    #[test]
    fn dual_write_parent_clear_noop_when_no_prior_parent() {
        use crate::events::ParentSetPayload;
        let conn = setup();
        apply_event(&conn, &make_created_event("orphan", "Orphan", 2), "").unwrap();

        // Clear parent on a task that never had one — must not error, must not insert link
        apply_event(
            &conn,
            &TaskEvent::new(
                "orphan",
                "user",
                EventType::ParentSet,
                &ParentSetPayload {
                    parent_task_id: None,
                },
            ),
            "",
        )
        .unwrap();

        assert_eq!(count_entity_links(&conn), 0);
    }
}
