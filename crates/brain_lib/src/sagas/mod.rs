use std::collections::HashSet;

use brain_persistence::db::sagas::events::SagaCancelledPayload;
use brain_persistence::db::tasks::events::TaskStatus;
use brain_persistence::sql::{SqlError, SqlResultExt};

use brain_persistence::db::Db;
use brain_persistence::db::sagas::SagaListFilter;
use brain_persistence::db::sagas::events::{
    SagaClosedPayload, SagaEvent, SagaEventType, SagaTaskPayload, SagaUpdatedPayload, new_saga_id,
};
use brain_persistence::db::sagas::queries::{
    self, LabelCount, SagaEventInsert, SagaMemberStub, SagaRow, SagaStatsRow, close_saga,
    list_saga_member_stubs, list_saga_task_ids, saga_members_in, start_saga,
};
use brain_persistence::db::sagas::reopen_saga;
use brain_persistence::db::sagas::{compact_saga_id, resolve_saga_id};
use brain_persistence::db::tasks::queries::{
    TaskRow, list_ready_actionable_for_tasks, resolve_task_id_scoped, task_subtree,
};

pub use brain_persistence::db::sagas::queries::BrainSummary;

use crate::error::{BrainCoreError, Result};

pub mod lifecycle;
pub mod status;
pub use lifecycle::validate_transition;
pub use status::SagaStatus;

/// The ready tasks in a saga plus the set of brains those tasks belong to.
///
/// `status` is always populated from the saga's row. For non-`Open` sagas the
/// `tasks` and `brains` vecs are empty by contract — callers can distinguish
/// "no ready tasks" (Open with empty `tasks`) from "saga is in a non-Open
/// state" (`status != Open`) by inspecting `status`.
pub struct SagaFrontier {
    pub tasks: Vec<TaskRow>,
    pub brains: Vec<BrainSummary>,
    pub status: SagaStatus,
}

/// Aggregated statistics for a saga's member tasks.
pub struct SagaStats {
    pub counts: SagaStatsRow,
    pub label_histogram: Vec<LabelCount>,
    pub brains: Vec<BrainSummary>,
}

// Cascade types live in brain-persistence (they need rusqlite-backed helpers
// to walk member tasks). Re-exported here so the public API is still
// `brain_lib::sagas::CascadeResult` / `CascadeOutcome`.
pub use brain_persistence::db::sagas::queries::{CascadeOutcome, CascadeResult};

/// Store for saga lifecycle operations. Registry-level: not scoped to any brain.
/// Hard upper bound on the number of tasks a single cascade-add or
/// cascade-remove operation may touch.
///
/// The MCP `task_ids` array is capped at 500 input entries, but cascade
/// expansion via `task_subtree` is unbounded by the input length — a single
/// epic with 10 000 descendants would otherwise hold the SQLite writer mutex
/// for the duration of the insert/delete + per-row event emission. This cap
/// restores the same protection intent that the MCP input cap provides for
/// non-cascade calls.
const MAX_EXPANDED_BATCH: usize = 2000;

pub struct SagaStore {
    pub(crate) db: Db,
}

impl SagaStore {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Resolve a saga reference (bare ULID or `saga-<hex>`) and return both
    /// the canonical 26-char ULID and the user-facing short form.
    ///
    /// Used by MCP tools that need the canonical for subsequent store calls
    /// AND the short form for the response payload — one round-trip instead
    /// of two. The display_id mapping never changes after insert so a single
    /// read connection is sufficient.
    pub fn resolve_short(&self, input: &str) -> Result<(String, String)> {
        let input = input.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &input)?;
                let row = queries::get_saga(conn, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(crate::error::BrainCoreError::Internal(format!(
                        "saga {canonical} disappeared between resolve and fetch"
                    )))
                })?;
                Ok((canonical, compact_saga_id(&row.display_id)))
            })
            .into_brain_core()
    }

    /// Create a new saga in `planning` status. Returns the resulting row.
    pub fn create(&self, title: &str, description: Option<&str>, actor: &str) -> Result<SagaRow> {
        if title.trim().is_empty() {
            return Err(brain_core::error::BrainCoreError::Parse(
                "saga title must not be empty".into(),
            ));
        }
        let saga_id = new_saga_id();
        let row = self
            .db
            .with_write_conn(|conn| {
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
            })
            .into_brain_core()?;
        Ok(row)
    }

    /// Update title and/or description. At least one field required. Allowed in any status.
    ///
    /// `description` uses `Option<Option<&str>>`:
    /// - `None` = don't touch description
    /// - `Some(None)` = set description to NULL
    /// - `Some(Some("text"))` = set description to "text"
    pub fn update(
        &self,
        saga_id: &str,
        title: Option<&str>,
        description: Option<Option<&str>>,
        actor: &str,
    ) -> Result<SagaRow> {
        if title.is_none() && description.is_none() {
            return Err(crate::error::BrainCoreError::Parse(
                "update: at least one of title or description must be provided".into(),
            ));
        }
        let title = match title {
            Some(t) => {
                let trimmed = t.trim();
                if trimmed.is_empty() {
                    return Err(crate::error::BrainCoreError::Parse(
                        "update: title must not be empty".into(),
                    ));
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

        let row = self
            .db
            .with_write_conn(|conn| {
                // H1: wrap projection write + event insert in one SQLite tx so a
                // failure between the two cannot leave the projection mutated
                // without a corresponding saga_events row. `with_write_conn` only
                // locks the writer mutex; it does NOT open a transaction.
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
            })
            .into_brain_core()?;
        Ok(row)
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
        &self,
        saga_id: &str,
        cascade: bool,
        actor: &str,
    ) -> Result<(SagaRow, Vec<CascadeResult>)> {
        let actor_owned = actor.to_string();
        self.db
            .with_write_conn(|conn| {
                // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
                let tx = conn.unchecked_transaction()?;
                let canonical = resolve_saga_id(&tx, saga_id)?;

                let current = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
                })?;

                let from: SagaStatus = current.status.parse().map_err(|_| {
                    SqlError::Domain(BrainCoreError::Parse(format!("unknown saga status: {}", current.status)))
                })?;

                validate_transition(from, SagaStatus::Closed).map_err(SqlError::Domain)?;

                let row = close_saga(&tx, &canonical)?;

                let event = SagaEvent::new(
                    &canonical,
                    &actor_owned,
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
                    queries::cascade_member_tasks(&tx, &canonical, &actor_owned, TaskStatus::Done)?
                } else {
                    vec![]
                };

                tx.commit()?;
                Ok((row, cascade_results))
            })
            .into_brain_core()
    }

    /// Fetch a saga by ID. Returns None if not found.
    ///
    /// Accepts either the canonical ULID or the `saga-<hex>` short form. Per
    /// the read-side convention, an unresolvable input returns `Ok(None)`
    /// rather than propagating `SagaNotFound` — callers see "no such saga"
    /// as a null result, not as an error.
    pub fn get(&self, saga_id: &str) -> Result<Option<SagaRow>> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = match resolve_saga_id(conn, &saga_id) {
                    Ok(c) => c,
                    Err(SqlError::Domain(BrainCoreError::SagaNotFound(_))) => return Ok(None),
                    Err(e) => return Err(e),
                };
                queries::get_saga(conn, &canonical)
            })
            .into_brain_core()
    }

    /// List sagas with optional filters.
    pub fn list(&self, filter: SagaListFilter) -> Result<Vec<SagaRow>> {
        self.db
            .with_read_conn(move |conn| queries::list_sagas(conn, &filter))
            .into_brain_core()
    }

    /// Transition a saga from `planning` to `open`. Emits `SagaStarted`.
    pub fn start(&self, saga_id: &str, actor: &str) -> Result<SagaRow> {
        self.db
            .with_write_conn(|conn| {
                // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
                let tx = conn.unchecked_transaction()?;
                let canonical = resolve_saga_id(&tx, saga_id)?;

                let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
                })?;

                let from: SagaStatus = row.status.parse().map_err(|_| {
                    SqlError::Domain(BrainCoreError::Parse(format!("unknown saga status: {}", row.status)))
                })?;

                validate_transition(from, SagaStatus::Open).map_err(SqlError::Domain)?;

                // L4: use the canonical now_ts() helper instead of an inline
                // SystemTime::now().unwrap_or(0) which would silently write
                // epoch-zero on a clock anomaly.
                let now = crate::utils::now_ts();
                start_saga(&tx, &canonical, now)?;

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

                let result = queries::get_saga(&tx, &canonical)?
                    .ok_or_else(|| SqlError::Domain(BrainCoreError::Parse("saga disappeared after start".into())))?;
                tx.commit()?;
                Ok(result)
            })
            .into_brain_core()
    }

    /// Return the distinct set of brains that have member tasks in this saga.
    ///
    /// Derived at read time — no denormalized table. Empty vec when saga has no members.
    pub fn brains_for_saga(&self, saga_id: &str) -> Result<Vec<BrainSummary>> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                queries::brains_for_saga(conn, &canonical)
            })
            .into_brain_core()
    }

    /// Return ready-actionable member tasks (same rules as `tasks next`) plus
    /// the brains those tasks belong to. Empty for planning/closed/cancelled sagas.
    pub fn frontier(&self, saga_id: &str) -> Result<SagaFrontier> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                // Spec invariant: only `open` sagas can have a non-empty frontier.
                // Without this guard the result is empty only by accident (no ready
                // member tasks); the contract should be explicit.
                let row = queries::get_saga(conn, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
                })?;
                let status: SagaStatus = row.status.parse().map_err(|_| {
                    SqlError::Domain(BrainCoreError::Parse(format!("unknown saga status: {}", row.status)))
                })?;
                if !matches!(status, SagaStatus::Open) {
                    return Ok(SagaFrontier {
                        tasks: vec![],
                        brains: vec![],
                        status,
                    });
                }

                let task_ids: Vec<String> = list_saga_task_ids(conn, &canonical)?;

                let tasks = list_ready_actionable_for_tasks(conn, &task_ids)?;
                let brains = queries::brains_for_saga(conn, &canonical)?;
                Ok(SagaFrontier {
                    tasks,
                    brains,
                    status,
                })
            })
            .into_brain_core()
    }

    /// Return saga member task stubs (task_id, brain_id, title, status, task_type)
    /// in `added_at` order. Single JOIN — no N+1.
    ///
    /// Orphaned memberships (task deleted from another brain) are silently
    /// dropped: `saga_tasks` has no FK to `tasks` by design, so the INNER JOIN
    /// is the only place these get filtered.
    pub fn list_member_stubs(&self, saga_id: &str) -> Result<Vec<SagaMemberStub>> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                list_saga_member_stubs(conn, &canonical)
            })
            .into_brain_core()
    }

    /// Return raw task IDs for saga membership without joining `tasks`.
    /// Includes orphans. Use `list_member_stubs` instead unless you specifically
    /// need orphan IDs for cleanup.
    pub fn list_members(&self, saga_id: &str) -> Result<Vec<String>> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                list_saga_task_ids(conn, &canonical)
            })
            .into_brain_core()
    }

    /// Aggregate counts, completion %, label histogram, and brains for a saga.
    pub fn stats(&self, saga_id: &str) -> Result<SagaStats> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                let counts = queries::saga_stats(conn, &canonical)?;
                let label_histogram = queries::saga_label_histogram(conn, &canonical)?;
                let brains = queries::brains_for_saga(conn, &canonical)?;
                Ok(SagaStats {
                    counts,
                    label_histogram,
                    brains,
                })
            })
            .into_brain_core()
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
        &self,
        saga_id: &str,
        cascade: bool,
        actor: &str,
    ) -> Result<(SagaRow, Vec<CascadeResult>)> {
        let actor_owned = actor.to_string();
        self.db
            .with_write_conn(|conn| {
                // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
                let tx = conn.unchecked_transaction()?;
                let canonical = resolve_saga_id(&tx, saga_id)?;

                let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
                })?;

                let from: SagaStatus = row.status.parse().map_err(|_| {
                    SqlError::Domain(BrainCoreError::Parse(format!("unknown saga status: {}", row.status)))
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
                    &actor_owned,
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
                    queries::cascade_member_tasks(
                        &tx,
                        &canonical,
                        &actor_owned,
                        TaskStatus::Cancelled,
                    )?
                } else {
                    vec![]
                };

                let updated = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(BrainCoreError::Database("saga disappeared after cancel".into()))
                })?;
                tx.commit()?;
                Ok((updated, cascade_results))
            })
            .into_brain_core()
    }

    #[cfg(test)]
    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

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
        &self,
        saga_id: &str,
        task_ids: &[String],
        cascade: bool,
        actor: &str,
    ) -> Result<Vec<String>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
        self.db.with_write_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let canonical = resolve_saga_id(&tx, saga_id)?;

            // Verify the saga exists and is not in a terminal state.
            let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
            })?;
            let status: SagaStatus = row.status.parse().map_err(|_| {
                SqlError::Domain(BrainCoreError::TaskEvent(format!("saga '{saga_id}' has unrecognised status")))
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
                    SqlError::Domain(BrainCoreError::TaskEvent(format!("task '{raw_id}' could not be resolved: {e}")))
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
        })
            .into_brain_core()
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
        &self,
        saga_id: &str,
        task_ids: Vec<String>,
        cascade: bool,
        actor: &str,
    ) -> Result<Vec<String>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let actor = actor.to_string();
        let saga_id = saga_id.to_string();
        // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
        self.db.with_write_conn(move |conn| {
            // H1: SELECT-DELETE-INSERT must be atomic so a concurrent insert
            // between the SELECT and DELETE cannot create a member that is
            // then deleted without a SagaTaskRemoved event being emitted.
            let tx = conn.unchecked_transaction()?;
            let canonical = resolve_saga_id(&tx, &saga_id)?;

            // Reject closed/cancelled sagas — same guard as `add_tasks`.
            let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
            })?;
            let saga_status: SagaStatus = row.status.parse().map_err(|_| {
                SqlError::Domain(BrainCoreError::Parse(format!("unknown saga status: {}", row.status)))
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
                    .map(|raw| {
                        resolve_task_id_scoped(&tx, raw, None).unwrap_or_else(|_| raw.clone())
                    })
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
                let event =
                    SagaEvent::new(&canonical, &actor, SagaEventType::SagaTaskRemoved, &payload);
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
        })
            .into_brain_core()
    }

    /// Reopen a closed or cancelled saga, setting status back to `open`.
    /// Clears `closed_at`. Emits `SagaReopened`. Rejected from `planning` or `open`.
    pub fn reopen(&self, saga_id: &str, actor: &str) -> Result<SagaRow> {
        let actor = actor.to_string();
        let saga_id = saga_id.to_string();
        self.db
            .with_write_conn(move |conn| {
                // unchecked_ ok: with_write_conn holds the writer mutex, single writer guaranteed.
                let tx = conn.unchecked_transaction()?;
                let canonical = resolve_saga_id(&tx, &saga_id)?;

                let row = queries::get_saga(&tx, &canonical)?.ok_or_else(|| {
                    SqlError::Domain(BrainCoreError::SagaNotFound(format!("saga not found: {saga_id}")))
                })?;

                let from: SagaStatus = row.status.parse().map_err(|_| {
                    SqlError::Domain(BrainCoreError::Parse(format!("unknown saga status: {}", row.status)))
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

                let updated = reopen_saga(&tx, &canonical)?;

                let event = SagaEvent::new(
                    &canonical,
                    &actor,
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
            })
            .into_brain_core()
    }

    /// Force a saga's status directly (test-only).
    ///
    /// L5: uses `now_ts()` (seconds) like every other timestamp in the table —
    /// previously wrote `as_millis()` which was ~1000× larger and would look
    /// like a future timestamp to anything that compared across paths.
    #[cfg(test)]
    pub fn force_status_for_test(&self, saga_id: &str, status: SagaStatus) -> Result<()> {
        let saga_id = saga_id.to_string();
        let status_str = status.as_str();
        let ts = crate::utils::now_ts();
        self.db
            .with_write_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                #[allow(clippy::disallowed_macros)]
                conn.execute(
                    "UPDATE sagas SET status = ?1, updated_at = ?2 WHERE saga_id = ?3",
                    rusqlite::params![status_str, ts, canonical],
                )?;
                Ok(())
            })
            .into_brain_core()
    }

    /// Backdate a saga's `updated_at` directly (test-only).
    ///
    /// Used by L6: timestamp-monotonicity tests no longer need a 1.1s sleep —
    /// they backdate the row, then call `update()` and assert the new
    /// `updated_at` is strictly greater than the backdated value.
    #[cfg(test)]
    pub fn force_updated_at_for_test(&self, saga_id: &str, ts: i64) -> Result<()> {
        let saga_id = saga_id.to_string();
        self.db
            .with_write_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                #[allow(clippy::disallowed_macros)]
                conn.execute(
                    "UPDATE sagas SET updated_at = ?1, created_at = ?1 WHERE saga_id = ?2",
                    rusqlite::params![ts, canonical],
                )?;
                Ok(())
            })
            .into_brain_core()
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_macros)]
mod tests {
    use super::*;
    use brain_persistence::db::Db;

    fn in_memory_store() -> SagaStore {
        let db = Db::open_in_memory().unwrap();
        SagaStore::new(db)
    }

    #[test]
    fn create_returns_planning_status() {
        let store = in_memory_store();
        let row = store.create("My Saga", None, "test").unwrap();
        assert_eq!(row.status, "planning");
        assert_eq!(row.title, "My Saga");
        assert!(row.description.is_none());
        assert!(row.closed_at.is_none());
        assert_eq!(row.saga_id.len(), 26, "saga_id must be bare 26-char ULID");
        assert!(!row.saga_id.contains('-'), "saga_id must have no prefix");
    }

    #[test]
    fn create_with_description() {
        let store = in_memory_store();
        let row = store.create("Saga", Some("desc"), "test").unwrap();
        assert_eq!(row.description.as_deref(), Some("desc"));
    }

    #[test]
    fn get_returns_created_saga() {
        let store = in_memory_store();
        let created = store.create("Get Test", None, "test").unwrap();
        let fetched = store.get(&created.saga_id).unwrap().unwrap();
        assert_eq!(fetched.saga_id, created.saga_id);
        assert_eq!(fetched.title, "Get Test");
        assert_eq!(fetched.status, "planning");
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = in_memory_store();
        assert!(store.get("01NONEXISTENT000000000000").unwrap().is_none());
    }

    #[test]
    fn create_timestamps_populated() {
        let store = in_memory_store();
        let row = store.create("Timestamps", None, "test").unwrap();
        assert!(row.created_at > 0);
        assert!(row.updated_at > 0);
        assert_eq!(row.created_at, row.updated_at);
    }

    // T1: SagaCreated event row is written on create
    #[test]
    fn create_writes_saga_created_event() {
        let store = in_memory_store();
        let row = store.create("X", None, "actor").unwrap();
        let (event_type, actor): (String, String) = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT event_type, actor FROM saga_events WHERE saga_id = ?1",
                    [&row.saga_id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert!(event_type.contains("saga_created"), "got: {event_type}");
        assert_eq!(actor, "actor");
    }

    // T2: empty title is rejected
    #[test]
    fn create_rejects_empty_title() {
        let store = in_memory_store();
        assert!(store.create("", None, "actor").is_err());
        assert!(store.create("   ", None, "actor").is_err());
    }

    // T3: saga_tasks allows cross-brain task_id (no FK on task_id)
    #[test]
    fn saga_tasks_allows_cross_brain_task_id() {
        let store = in_memory_store();
        let row = store.create("Cross-brain saga", None, "test").unwrap();
        // Insert a saga_tasks row with a task_id from a different brain —
        // saga_tasks has no FK on task_id by design so cross-brain links are allowed.
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, ?3)",
                    [row.saga_id.as_str(), "OTHER-BRAIN-TASK-01JXYZ", "1000000"],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();
        let count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1",
                    [&row.saga_id],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(
            count, 1,
            "cross-brain task_id should be stored without error"
        );
    }

    #[test]
    fn list_default_excludes_closed_and_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        let _b = store.create("Beta", None, "test").unwrap();

        // Manually force-close saga a by direct DB write.
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let rows = store.list(SagaListFilter::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Beta");
    }

    #[test]
    fn list_include_closed() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let rows = store
            .list(SagaListFilter {
                include_closed: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn update_title_only() {
        let store = in_memory_store();
        let created = store.create("Original", None, "test").unwrap();
        let updated = store
            .update(&created.saga_id, Some("Renamed"), None, "test")
            .unwrap();
        assert_eq!(updated.title, "Renamed");
        assert!(updated.updated_at >= created.updated_at);
    }

    #[test]
    fn update_description_only() {
        let store = in_memory_store();
        let created = store.create("Title", None, "test").unwrap();
        let updated = store
            .update(&created.saga_id, None, Some(Some("new desc")), "test")
            .unwrap();
        assert_eq!(updated.description.as_deref(), Some("new desc"));
        assert_eq!(updated.title, "Title");
    }

    #[test]
    fn update_both_fields() {
        let store = in_memory_store();
        let created = store.create("Old", Some("old desc"), "test").unwrap();
        let updated = store
            .update(
                &created.saga_id,
                Some("New"),
                Some(Some("new desc")),
                "test",
            )
            .unwrap();
        assert_eq!(updated.title, "New");
        assert_eq!(updated.description.as_deref(), Some("new desc"));
    }

    #[test]
    fn update_no_fields_errors() {
        let store = in_memory_store();
        let created = store.create("Saga", None, "test").unwrap();
        let result = store.update(&created.saga_id, None, None, "test");
        assert!(result.is_err());
    }

    #[test]
    fn update_empty_title_errors() {
        let store = in_memory_store();
        let created = store.create("Saga", None, "test").unwrap();
        let result = store.update(&created.saga_id, Some("  "), None, "test");
        assert!(result.is_err());
    }

    #[test]
    fn list_all_includes_closed_and_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        let b = store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&b.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let rows = store
            .list(SagaListFilter {
                include_closed: true,
                include_cancelled: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // N4: default filter also excludes cancelled (not just closed)
    #[test]
    fn list_default_excludes_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let rows = store.list(SagaListFilter::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Beta");
    }

    // T4: include_cancelled alone (without --all)
    #[test]
    fn list_include_cancelled_only() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let rows = store
            .list(SagaListFilter {
                include_cancelled: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // Helper: insert a minimal task row with a given task_id and brain_id.
    // Ensures the brain row exists first (tasks.brain_id has a FK to brains).
    fn insert_task(store: &SagaStore, task_id: &str, brain_id: &str) {
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES (?1, ?1, 1000)",
                    [brain_id],
                )?;
                conn.execute(
                    "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
                     VALUES (?1, ?2, 'task', 'open', 4, 'task', 1000, 1000)",
                    [task_id, brain_id],
                )?;
                Ok(())
            })
                .into_brain_core()
            .unwrap();
    }

    // Helper: link a task to a saga.
    fn link_task(store: &SagaStore, saga_id: &str, task_id: &str) {
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, 1000)",
                    [saga_id, task_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();
    }

    // T1: containing_brain happy path — only returns sagas with a member-task in that brain.
    #[test]
    fn containing_brain_returns_only_matching_saga() {
        let store = in_memory_store();
        let a = store.create("Saga A", None, "test").unwrap();
        let b = store.create("Saga B", None, "test").unwrap();

        insert_task(&store, "task-x-brain", "brain-x");
        insert_task(&store, "task-y-brain", "brain-y");
        link_task(&store, &a.saga_id, "task-x-brain");
        link_task(&store, &b.saga_id, "task-y-brain");

        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].saga_id, a.saga_id);
    }

    // T2: cross-brain saga appears in both containing_brain queries.
    #[test]
    fn containing_brain_cross_brain_saga_appears_for_both() {
        let store = in_memory_store();
        let saga = store.create("Cross-Brain Saga", None, "test").unwrap();

        insert_task(&store, "task-in-x", "brain-x");
        insert_task(&store, "task-in-y", "brain-y");
        link_task(&store, &saga.saga_id, "task-in-x");
        link_task(&store, &saga.saga_id, "task-in-y");

        let rows_x = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        let rows_y = store
            .list(SagaListFilter {
                containing_brain: Some("brain-y".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows_x.len(), 1, "should find saga via brain-x");
        assert_eq!(rows_y.len(), 1, "should find saga via brain-y");
        assert_eq!(rows_x[0].saga_id, saga.saga_id);
        assert_eq!(rows_y[0].saga_id, saga.saga_id);
    }

    // T3: containing_brain for non-existent brain returns empty list.
    #[test]
    fn containing_brain_nonexistent_brain_returns_empty() {
        let store = in_memory_store();
        store.create("Saga A", None, "test").unwrap();

        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("no-such-brain".into()),
                ..Default::default()
            })
            .unwrap();
        assert!(rows.is_empty());
    }

    // T5: combined filters — include_closed=true + containing_brain.
    #[test]
    fn containing_brain_combined_with_include_closed() {
        let store = in_memory_store();
        let a = store.create("Open Saga", None, "test").unwrap();
        let b = store.create("Closed Saga", None, "test").unwrap();

        insert_task(&store, "task-open", "brain-x");
        insert_task(&store, "task-closed", "brain-x");
        link_task(&store, &a.saga_id, "task-open");
        link_task(&store, &b.saga_id, "task-closed");

        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&b.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        // Without include_closed: only open saga returned.
        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].saga_id, a.saga_id);

        // With include_closed: both returned.
        let rows_all = store
            .list(SagaListFilter {
                include_closed: true,
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows_all.len(), 2);
    }

    // T1: update on non-existent saga returns error containing "not found"
    #[test]
    fn update_nonexistent_saga_returns_not_found() {
        let store = in_memory_store();
        let err = store
            .update("01NONEXISTENTSAGA0000000", None, Some(Some("x")), "actor")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not found"), "expected 'not found' in: {msg}");
    }

    // T2: update on closed/cancelled saga succeeds (metadata edit is lifecycle-independent)
    #[test]
    fn update_on_closed_saga_succeeds() {
        let store = in_memory_store();
        let row = store.create("Active", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&row.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();
        let updated = store
            .update(&row.saga_id, Some("Closed but renamed"), None, "actor")
            .unwrap();
        assert_eq!(updated.title, "Closed but renamed");
        assert_eq!(updated.status, "closed");
    }

    // T3: clear-description test — create with desc, update with Some(None), assert NULL
    #[test]
    fn update_clears_description_when_some_none() {
        let store = in_memory_store();
        let row = store.create("Has Desc", Some("original"), "test").unwrap();
        assert!(row.description.is_some());
        let updated = store
            .update(&row.saga_id, None, Some(None), "actor")
            .unwrap();
        assert!(
            updated.description.is_none(),
            "description should be NULL after clear"
        );
    }

    // T4: after update, saga_events has one row with event_type saga_updated and new title
    #[test]
    fn update_writes_saga_updated_event() {
        let store = in_memory_store();
        let row = store.create("Before", None, "test").unwrap();
        store
            .update(&row.saga_id, Some("After"), None, "actor")
            .unwrap();
        let (event_type, payload): (String, String) = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT event_type, payload FROM saga_events WHERE saga_id = ?1 AND event_type LIKE '%updated%'",
                    [&row.saga_id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .map_err(Into::into)
            })
                .into_brain_core()
            .unwrap();
        assert!(event_type.contains("saga_updated"), "got: {event_type}");
        assert!(
            payload.contains("After"),
            "payload should contain new title, got: {payload}"
        );
    }

    // T5: whitespace-only title is rejected
    #[test]
    fn update_rejects_whitespace_only_title() {
        let store = in_memory_store();
        let row = store.create("Valid", None, "test").unwrap();
        assert!(
            store
                .update(&row.saga_id, Some("   "), None, "actor")
                .is_err()
        );
        assert!(
            store
                .update(&row.saga_id, Some("\t\n"), None, "actor")
                .is_err()
        );
    }

    // T6: updated_at is strictly greater than created_at after update.
    //
    // L6: previously slept 1.1s to cross now_ts()'s second boundary. That was
    // both slow and racy on under-resourced CI. Instead, backdate the row by
    // 2 seconds via the test-only helper, then update normally — the assertion
    // is deterministic regardless of clock granularity.
    #[test]
    fn update_bumps_updated_at_strictly() {
        let store = in_memory_store();
        let row = store.create("Timing", None, "test").unwrap();
        let backdated = crate::utils::now_ts() - 2;
        store
            .force_updated_at_for_test(&row.saga_id, backdated)
            .unwrap();
        let updated = store
            .update(&row.saga_id, Some("Timing Updated"), None, "actor")
            .unwrap();
        assert!(
            updated.updated_at > backdated,
            "updated_at ({}) must be strictly greater than backdated value ({})",
            updated.updated_at,
            backdated
        );
    }

    // ── add_tasks tests ────────────────────────────────────────────────────

    // T1: mid-batch atomicity — [good_id, bad_id] must leave good_id NOT a member.
    #[test]
    fn add_tasks_atomicity_bad_id_rolls_back_good_id() {
        let store = in_memory_store();
        let saga = store.create("Atomic Saga", None, "test").unwrap();
        insert_task(&store, "good-brain-task01", "brain-x");

        let result = store.add_tasks(
            &saga.saga_id,
            &[
                "good-brain-task01".to_string(),
                "NONEXISTENT-TASK-ID".to_string(),
            ],
            false,
            "test",
        );
        assert!(result.is_err(), "batch with bad ID should fail");

        // good-brain-task01 must NOT be a member because the transaction rolled back.
        let count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1 AND task_id = ?2",
                    [saga.saga_id.as_str(), "good-brain-task01"],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(count, 0, "rolled-back task must not appear in saga_tasks");
    }

    // T2: closed saga is rejected with a message mentioning "reopen".
    #[test]
    fn add_tasks_closed_saga_rejected() {
        let store = in_memory_store();
        let saga = store.create("Closed Saga", None, "test").unwrap();
        insert_task(&store, "brain-z-task01", "brain-z");

        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&saga.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let err = store
            .add_tasks(
                &saga.saga_id,
                &["brain-z-task01".to_string()],
                false,
                "test",
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("reopen"),
            "error should mention 'reopen', got: {msg}"
        );
    }

    // T3: cancelled saga is rejected.
    #[test]
    fn add_tasks_cancelled_saga_rejected() {
        let store = in_memory_store();
        let saga = store.create("Cancelled Saga", None, "test").unwrap();
        insert_task(&store, "brain-w-task01", "brain-w");

        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&saga.saga_id],
                )?;
                Ok(())
            })
            .into_brain_core()
            .unwrap();

        let err = store
            .add_tasks(
                &saga.saga_id,
                &["brain-w-task01".to_string()],
                false,
                "test",
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("reopen"),
            "error should mention 'reopen', got: {msg}"
        );
    }

    // T4: cross-brain mixed batch — tasks from two different brains added to one saga.
    #[test]
    fn add_tasks_cross_brain_mixed_batch() {
        let store = in_memory_store();
        let saga = store.create("Cross-Brain Saga", None, "test").unwrap();
        insert_task(&store, "brain-a-task01", "brain-a");
        insert_task(&store, "brain-b-task01", "brain-b");

        let count = store
            .add_tasks(
                &saga.saga_id,
                &["brain-a-task01".to_string(), "brain-b-task01".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(count.len(), 2);

        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| {
                let mut stmt = c
                    .prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY task_id")
                    .unwrap();
                let ids = stmt
                    .query_map([saga.saga_id.as_str()], |r| r.get(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .unwrap();
                Ok(ids)
            })
            .into_brain_core()
            .unwrap();
        assert!(ids.contains(&"brain-a-task01".to_string()));
        assert!(ids.contains(&"brain-b-task01".to_string()));
    }

    // T5: after multi-add, saga_events has SagaTaskAdded count == tasks added.
    #[test]
    fn add_tasks_emits_one_event_per_task() {
        let store = in_memory_store();
        let saga = store.create("Event Saga", None, "test").unwrap();
        insert_task(&store, "ev-brain-task01", "brain-ev");
        insert_task(&store, "ev-brain-task02", "brain-ev");
        insert_task(&store, "ev-brain-task03", "brain-ev");

        let count = store
            .add_tasks(
                &saga.saga_id,
                &[
                    "ev-brain-task01".to_string(),
                    "ev-brain-task02".to_string(),
                    "ev-brain-task03".to_string(),
                ],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(count.len(), 3);

        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = 'saga_task_added'",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(
            event_count, 3,
            "expected 3 SagaTaskAdded events, got {event_count}"
        );
    }

    // M7: empty batch returns Ok(0) immediately.
    #[test]
    fn add_tasks_empty_batch_is_noop() {
        let store = in_memory_store();
        let saga = store.create("Noop Saga", None, "test").unwrap();
        let count = store.add_tasks(&saga.saga_id, &[], false, "test").unwrap();
        assert_eq!(count.len(), 0);
    }

    // ── start tests ────────────────────────────────────────────────────────

    #[test]
    fn start_planning_saga_succeeds() {
        let store = in_memory_store();
        let created = store.create("To Start", None, "test").unwrap();
        assert_eq!(created.status, "planning");

        let started = store.start(&created.saga_id, "test").unwrap();
        assert_eq!(started.status, "open");
        assert_eq!(started.saga_id, created.saga_id);
    }

    #[test]
    fn start_already_open_fails() {
        let store = in_memory_store();
        let created = store.create("Double Start", None, "test").unwrap();
        store.start(&created.saga_id, "test").unwrap();
        let err = store.start(&created.saga_id, "test").unwrap_err();
        assert!(
            err.to_string()
                .contains("invalid saga lifecycle transition")
        );
    }

    #[test]
    fn start_nonexistent_saga_fails() {
        let store = in_memory_store();
        let err = store
            .start("01NONEXISTENT000000000000", "test")
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn brains_for_saga_empty_when_no_members() {
        let store = in_memory_store();
        let saga = store.create("Empty", None, "test").unwrap();
        let brains = store.brains_for_saga(&saga.saga_id).unwrap();
        assert!(brains.is_empty(), "expected no brains for memberless saga");
    }

    #[test]
    fn brains_for_saga_returns_distinct_brains() {
        let store = in_memory_store();
        let saga = store.create("Multi-brain", None, "test").unwrap();

        // Insert two brains and tasks directly into the DB.
        store.db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('brain-a', 'Brain A', 'BRA', 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('brain-b', 'Brain B', 'BRB', 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, task_type, priority, created_at, updated_at)
                 VALUES ('BRA-01TASK0000000000000000001', 'brain-a', 'Task A', 'open', 'task', 2, 0, 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, task_type, priority, created_at, updated_at)
                 VALUES ('BRB-01TASK0000000000000000002', 'brain-b', 'Task B', 'open', 'task', 2, 0, 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 'BRA-01TASK0000000000000000001', 0)",
                rusqlite::params![saga.saga_id],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 'BRB-01TASK0000000000000000002', 0)",
                rusqlite::params![saga.saga_id],
            )?;
            Ok(())
        }).into_brain_core().unwrap();

        let brains = store.brains_for_saga(&saga.saga_id).unwrap();
        assert_eq!(brains.len(), 2);
        let ids: Vec<&str> = brains.iter().map(|b| b.brain_id.as_str()).collect();
        assert!(ids.contains(&"brain-a"));
        assert!(ids.contains(&"brain-b"));
    }

    #[test]
    fn brains_for_saga_deduplicates_same_brain() {
        let store = in_memory_store();
        let saga = store
            .create("Single Brain Two Tasks", None, "test")
            .unwrap();

        store.db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES ('brain-c', 'Brain C', 'BRC', 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, task_type, priority, created_at, updated_at)
                 VALUES ('BRC-01TASK0000000000000000003', 'brain-c', 'Task C1', 'open', 'task', 2, 0, 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, task_type, priority, created_at, updated_at)
                 VALUES ('BRC-01TASK0000000000000000004', 'brain-c', 'Task C2', 'open', 'task', 2, 0, 0)",
                [],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 'BRC-01TASK0000000000000000003', 0)",
                rusqlite::params![saga.saga_id],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, 'BRC-01TASK0000000000000000004', 0)",
                rusqlite::params![saga.saga_id],
            )?;
            Ok(())
        }).into_brain_core().unwrap();

        let brains = store.brains_for_saga(&saga.saga_id).unwrap();
        assert_eq!(
            brains.len(),
            1,
            "two tasks in same brain should yield one BrainSummary"
        );
        assert_eq!(brains[0].brain_id, "brain-c");
        assert_eq!(brains[0].name, "Brain C");
        assert_eq!(brains[0].prefix.as_deref(), Some("BRC"));
    }

    // ── M4: add_tasks idempotency ──────────────────────────────────────────

    // M4a: duplicates within a single input batch are deduped — exactly one
    // insert and one event are emitted.
    #[test]
    fn add_tasks_dedup_within_input_batch() {
        let store = in_memory_store();
        let saga = store.create("Dedup Saga", None, "test").unwrap();
        insert_task(&store, "dup-brain-task01", "brain-d");

        let count = store
            .add_tasks(
                &saga.saga_id,
                &[
                    "dup-brain-task01".to_string(),
                    "dup-brain-task01".to_string(),
                ],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(count.len(), 1, "duplicate within batch should count once");

        let member_count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(member_count, 1, "should be exactly 1 saga_tasks row");

        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = 'saga_task_added'",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(event_count, 1, "should emit exactly 1 SagaTaskAdded event");
    }

    // M4b: tasks already members of the saga are skipped; only new ones are
    // inserted/event-emitted, but the call still succeeds.
    #[test]
    fn add_tasks_skips_already_member_no_error() {
        let store = in_memory_store();
        let saga = store.create("Idempotent Saga", None, "test").unwrap();
        insert_task(&store, "idem-task01", "brain-i");
        insert_task(&store, "idem-task02", "brain-i");

        // Add t1 first.
        let first = store
            .add_tasks(&saga.saga_id, &["idem-task01".to_string()], false, "test")
            .unwrap();
        assert_eq!(first.len(), 1);

        // Now add [t1, t2] — t1 is already a member, t2 is new.
        let second = store
            .add_tasks(
                &saga.saga_id,
                &["idem-task01".to_string(), "idem-task02".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(second.len(), 1, "only t2 should be newly inserted");

        // Both tasks must end up as members.
        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| {
                let mut stmt = c
                    .prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY task_id")
                    .unwrap();
                let ids = stmt
                    .query_map([saga.saga_id.as_str()], |r| r.get(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .unwrap();
                Ok(ids)
            })
            .into_brain_core()
            .unwrap();
        assert!(ids.contains(&"idem-task01".to_string()));
        assert!(ids.contains(&"idem-task02".to_string()));

        // Exactly 2 SagaTaskAdded events total (1 from first call, 1 from
        // second call — the dup must not emit a second event for t1).
        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = 'saga_task_added'",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(event_count, 2);
    }

    // Helper: insert a parent_of edge between two existing tasks directly via
    // entity_links. Mirrors the projection write done by ParentSet but skips
    // the full event-sourcing path because the cascade tests only care about
    // the graph topology, not the event log.
    fn link_parent_of(store: &SagaStore, parent_id: &str, child_id: &str) {
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
                     VALUES (lower(hex(randomblob(16))), 'TASK', ?1, 'TASK', ?2, 'parent_of',
                             strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL)",
                    [parent_id, child_id],
                )?;
                Ok(())
            })
                .into_brain_core()
            .unwrap();
    }

    // Helper: count member tasks in a saga.
    fn member_count(store: &SagaStore, saga_id: &str) -> usize {
        store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1",
                    [saga_id],
                    |r| r.get::<_, i64>(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap() as usize
    }

    // ── cascade=true expansion ─────────────────────────────────────────────

    /// cascade-add of a parent pulls the parent plus every transitive
    /// descendant into the saga in one atomic call, emitting one
    /// SagaTaskAdded event per actual insertion.
    #[test]
    fn add_tasks_cascade_pulls_full_subtree() {
        let store = in_memory_store();
        let saga = store.create("Cascade Saga", None, "test").unwrap();
        // Tree: epic
        //       ├── child1
        //       │   └── grandchild
        //       └── child2
        insert_task(&store, "epic", "brain-c");
        insert_task(&store, "child1", "brain-c");
        insert_task(&store, "grandchild", "brain-c");
        insert_task(&store, "child2", "brain-c");
        link_parent_of(&store, "epic", "child1");
        link_parent_of(&store, "epic", "child2");
        link_parent_of(&store, "child1", "grandchild");

        let count = store
            .add_tasks(&saga.saga_id, &["epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(count.len(), 4, "cascade should add epic + 3 descendants");
        assert_eq!(member_count(&store, &saga.saga_id), 4);

        // One SagaTaskAdded event per insertion.
        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = 'saga_task_added'",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(event_count, 4);
    }

    /// cascade-add silently skips tasks already members of the saga. Adding an
    /// epic whose descendants are partly already members succeeds and reports
    /// only the new insertions.
    #[test]
    fn add_tasks_cascade_skips_already_member_in_subtree() {
        let store = in_memory_store();
        let saga = store.create("Partial Cascade", None, "test").unwrap();
        insert_task(&store, "epic2", "brain-p");
        insert_task(&store, "kid1", "brain-p");
        insert_task(&store, "kid2", "brain-p");
        link_parent_of(&store, "epic2", "kid1");
        link_parent_of(&store, "epic2", "kid2");

        // Pre-add kid1 so cascade has to dedupe against it.
        store
            .add_tasks(&saga.saga_id, &["kid1".to_string()], false, "test")
            .unwrap();

        let count = store
            .add_tasks(&saga.saga_id, &["epic2".to_string()], true, "test")
            .unwrap();
        assert_eq!(
            count.len(),
            2,
            "kid1 already a member; cascade adds epic2 + kid2"
        );
        assert_eq!(member_count(&store, &saga.saga_id), 3);
    }

    /// cascade expansion follows parent_of edges across brain boundaries —
    /// entity_links rows for `parent_of` are not brain-scoped.
    #[test]
    fn add_tasks_cascade_cross_brain_subtree() {
        let store = in_memory_store();
        let saga = store.create("Cross-brain Cascade", None, "test").unwrap();
        // Parent in brain-x, child in brain-y.
        insert_task(&store, "xb-parent", "brain-x");
        insert_task(&store, "yb-child", "brain-y");
        link_parent_of(&store, "xb-parent", "yb-child");

        let count = store
            .add_tasks(&saga.saga_id, &["xb-parent".to_string()], true, "test")
            .unwrap();
        assert_eq!(count.len(), 2);

        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| {
                let mut stmt = c
                    .prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY task_id")
                    .unwrap();
                let ids = stmt
                    .query_map([saga.saga_id.as_str()], |r| r.get(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .unwrap();
                Ok(ids)
            })
            .into_brain_core()
            .unwrap();
        assert!(ids.contains(&"xb-parent".to_string()));
        assert!(ids.contains(&"yb-child".to_string()));
    }

    /// cascade on a leaf task (no parent_of edges going down) adds just the
    /// leaf — behaves identically to cascade=false for this case.
    #[test]
    fn add_tasks_cascade_on_leaf_is_just_the_leaf() {
        let store = in_memory_store();
        let saga = store.create("Leaf Cascade", None, "test").unwrap();
        insert_task(&store, "lonely", "brain-l");

        let count = store
            .add_tasks(&saga.saga_id, &["lonely".to_string()], true, "test")
            .unwrap();
        assert_eq!(count.len(), 1);
        assert_eq!(member_count(&store, &saga.saga_id), 1);
    }

    /// cascade-remove strips the parent plus every descendant currently in
    /// the saga. Each removal emits its own SagaTaskRemoved event.
    #[test]
    fn remove_tasks_cascade_strips_subtree_intersection() {
        let store = in_memory_store();
        let saga = store.create("Remove Cascade", None, "test").unwrap();
        insert_task(&store, "rc-epic", "brain-rc");
        insert_task(&store, "rc-a", "brain-rc");
        insert_task(&store, "rc-b", "brain-rc");
        link_parent_of(&store, "rc-epic", "rc-a");
        link_parent_of(&store, "rc-epic", "rc-b");

        // Add all three first.
        store
            .add_tasks(&saga.saga_id, &["rc-epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(member_count(&store, &saga.saga_id), 3);

        let removed = store
            .remove_tasks(&saga.saga_id, vec!["rc-epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(
            removed.len(),
            3,
            "cascade-remove should strip the full subtree"
        );
        assert_eq!(member_count(&store, &saga.saga_id), 0);

        let removed_events: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = 'saga_task_removed'",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(removed_events, 3);
    }

    /// cascade-add rejects an expansion that would exceed MAX_EXPANDED_BATCH.
    /// Restores the writer-mutex protection that the MCP `task_ids: 500`
    /// cap provides for non-cascade calls — without this, a single seed with
    /// a runaway descendant set could hold the SQLite writer through a
    /// multi-thousand-row insert + event loop.
    #[test]
    fn add_tasks_cascade_rejects_over_max_expanded_batch() {
        let store = in_memory_store();
        let saga = store.create("Oversize Cascade", None, "test").unwrap();
        // Wire a chain of MAX_EXPANDED_BATCH+1 tasks (root + N descendants).
        insert_task(&store, "root", "brain-big");
        let mut prev = "root".to_string();
        for i in 0..MAX_EXPANDED_BATCH {
            let id = format!("child-{i:05}");
            insert_task(&store, &id, "brain-big");
            link_parent_of(&store, &prev, &id);
            prev = id;
        }

        let result = store.add_tasks(&saga.saga_id, &["root".to_string()], true, "test");
        let err = result.expect_err("cascade above MAX_EXPANDED_BATCH must error");
        assert!(
            format!("{err}").contains("MAX_EXPANDED_BATCH"),
            "error message should name the cap: {err}"
        );
        // Nothing was inserted because the cap fires before any writes.
        assert_eq!(member_count(&store, &saga.saga_id), 0);
    }

    /// cascade-remove with an unresolvable input errors loudly (different
    /// from non-cascade remove, which treats typos as idempotent no-ops).
    /// The user explicitly asked for subtree semantics — silently degrading
    /// to a single-task no-op would mask the intent.
    #[test]
    fn remove_tasks_cascade_hard_errors_on_unresolved_seed() {
        let store = in_memory_store();
        let saga = store.create("Cascade Resolve Saga", None, "test").unwrap();
        insert_task(&store, "real-task", "brain-rs");
        store
            .add_tasks(&saga.saga_id, &["real-task".to_string()], false, "test")
            .unwrap();

        // cascade=false: unresolved typo is a silent no-op (existing contract).
        let lenient = store
            .remove_tasks(
                &saga.saga_id,
                vec!["nonexistent-typo".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(lenient.len(), 0);

        // cascade=true: unresolved typo should fail loud.
        let strict = store.remove_tasks(
            &saga.saga_id,
            vec!["nonexistent-typo".to_string()],
            true,
            "test",
        );
        let err = strict.expect_err("cascade=true with unresolved seed must error");
        assert!(
            format!("{err}").contains("could not be resolved"),
            "error should explain resolution failure: {err}"
        );
        // The real task is still a member — cascade-remove failed atomically.
        assert_eq!(member_count(&store, &saga.saga_id), 1);
    }

    /// cascade-remove of an epic whose descendants are NOT all currently
    /// members only removes the intersection — non-member descendants are
    /// silently ignored, preserving the existing idempotency contract.
    #[test]
    fn remove_tasks_cascade_on_partial_membership() {
        let store = in_memory_store();
        let saga = store
            .create("Partial Remove Cascade", None, "test")
            .unwrap();
        insert_task(&store, "pe-epic", "brain-pe");
        insert_task(&store, "pe-orphan-child", "brain-pe");
        link_parent_of(&store, "pe-epic", "pe-orphan-child");

        // Only the epic is a saga member; the child is not.
        store
            .add_tasks(&saga.saga_id, &["pe-epic".to_string()], false, "test")
            .unwrap();
        assert_eq!(member_count(&store, &saga.saga_id), 1);

        let removed = store
            .remove_tasks(&saga.saga_id, vec!["pe-epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(
            removed.len(),
            1,
            "only the epic was a member; cascade is a no-op for the non-member child"
        );
        assert_eq!(member_count(&store, &saga.saga_id), 0);
    }

    // ── M5: remove_tasks status guard ──────────────────────────────────────

    #[test]
    fn remove_tasks_closed_saga_rejected() {
        let store = in_memory_store();
        let saga = store.create("Closed Remove Saga", None, "test").unwrap();
        insert_task(&store, "rem-brain-task01", "brain-r");

        // Add the task while still in a non-terminal state.
        store
            .add_tasks(
                &saga.saga_id,
                &["rem-brain-task01".to_string()],
                false,
                "test",
            )
            .unwrap();

        // Force the saga to closed.
        store
            .force_status_for_test(&saga.saga_id, SagaStatus::Closed)
            .unwrap();

        let err = store
            .remove_tasks(
                &saga.saga_id,
                vec!["rem-brain-task01".to_string()],
                false,
                "test",
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("closed") && msg.contains("reopen"),
            "error should mention 'closed' and 'reopen', got: {msg}"
        );

        // Membership must be unchanged.
        let count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1 AND task_id = ?2",
                    [saga.saga_id.as_str(), "rem-brain-task01"],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(
            count, 1,
            "membership must be unchanged when remove rejected"
        );
    }

    // ── reopen integration tests ───────────────────────────────────────────

    #[test]
    fn reopen_closed_succeeds_and_clears_closed_at() {
        let store = in_memory_store();
        let saga = store.create("Reopen Closed", None, "test").unwrap();
        store.start(&saga.saga_id, "test").unwrap();
        let (closed, _) = store.close(&saga.saga_id, false, "test").unwrap();
        assert_eq!(closed.status, "closed");
        assert!(closed.closed_at.is_some(), "close should set closed_at");

        let reopened = store.reopen(&saga.saga_id, "test").unwrap();
        assert_eq!(reopened.status, "open");
        assert!(
            reopened.closed_at.is_none(),
            "reopen must clear closed_at, got: {:?}",
            reopened.closed_at
        );
    }

    #[test]
    fn reopen_cancelled_succeeds() {
        let store = in_memory_store();
        let saga = store.create("Reopen Cancelled", None, "test").unwrap();
        store.start(&saga.saga_id, "test").unwrap();
        let (cancelled, _) = store.cancel(&saga.saga_id, false, "test").unwrap();
        assert_eq!(cancelled.status, "cancelled");

        let reopened = store.reopen(&saga.saga_id, "test").unwrap();
        assert_eq!(reopened.status, "open");
    }

    #[test]
    fn reopen_planning_rejected() {
        let store = in_memory_store();
        let saga = store.create("Planning Saga", None, "test").unwrap();
        assert_eq!(saga.status, "planning");

        let err = store.reopen(&saga.saga_id, "test").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("planning"),
            "error should mention 'planning' status, got: {msg}"
        );
    }

    #[test]
    fn reopen_open_rejected() {
        let store = in_memory_store();
        let saga = store.create("Open Saga", None, "test").unwrap();
        store.start(&saga.saga_id, "test").unwrap();

        let err = store.reopen(&saga.saga_id, "test").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("open"),
            "error should mention 'open' status, got: {msg}"
        );
    }

    #[test]
    fn reopen_emits_saga_reopened_event() {
        let store = in_memory_store();
        let saga = store.create("Reopen Event Saga", None, "test").unwrap();
        store.start(&saga.saga_id, "test").unwrap();
        store.close(&saga.saga_id, false, "test").unwrap();

        store.reopen(&saga.saga_id, "actor-x").unwrap();

        let (count, actor): (i64, String) = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*), COALESCE(MAX(actor), '') FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = 'saga_reopened'",
                    [saga.saga_id.as_str()],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                )
                .map_err(Into::into)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(count, 1, "expected exactly 1 saga_reopened event");
        assert_eq!(actor, "actor-x");
    }
}
