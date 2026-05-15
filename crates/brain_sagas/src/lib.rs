use brain_persistence::sql::{SqlError, SqlResultExt};

use brain_persistence::db::Db;
use brain_persistence::db::sagas::queries::{self, list_saga_member_stubs, list_saga_task_ids};
use brain_persistence::db::sagas::{compact_saga_id, resolve_saga_id};
use brain_persistence::db::tasks::queries::list_ready_actionable_for_tasks;

use brain_core::error::{BrainCoreError, Result};

pub(crate) mod domain;
pub(crate) mod lifecycle;
pub mod status;
pub use lifecycle::validate_transition;
pub use status::SagaStatus;

pub use domain::{
    BrainSummary, CascadeOutcome, CascadeResult, LabelCount, Saga, SagaId, SagaListFilter,
    SagaMember, SagaStatsCounts,
};

/// The ready tasks in a saga plus the set of brains those tasks belong to.
///
/// `status` is always populated from the saga's row. For non-`Open` sagas the
/// `tasks` and `brains` vecs are empty by contract — callers can distinguish
/// "no ready tasks" (Open with empty `tasks`) from "saga is in a non-Open
/// state" (`status != Open`) by inspecting `status`.
pub struct SagaFrontier {
    pub tasks: Vec<brain_tasks::Task>,
    pub brains: Vec<BrainSummary>,
    pub status: SagaStatus,
}

/// Aggregated statistics for a saga's member tasks.
pub struct SagaStats {
    pub counts: SagaStatsCounts,
    pub label_histogram: Vec<LabelCount>,
    pub brains: Vec<BrainSummary>,
}

/// Store for saga lifecycle operations. Registry-level: not scoped to any brain.
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
                    SqlError::Domain(brain_core::error::BrainCoreError::Internal(format!(
                        "saga {canonical} disappeared between resolve and fetch"
                    )))
                })?;
                Ok((canonical, compact_saga_id(&row.display_id)))
            })
            .into_brain_core()
    }

    /// Create a new saga in `planning` status. Returns the resulting saga.
    pub fn create(&self, title: &str, description: Option<&str>, actor: &str) -> Result<Saga> {
        let title = title.to_string();
        let description = description.map(str::to_string);
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| {
                crate::lifecycle::create(conn, &title, description.as_deref(), &actor)
            })
            .into_brain_core()
            .and_then(Saga::try_from)
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
    ) -> Result<Saga> {
        let saga_id = saga_id.to_string();
        let title = title.map(str::to_string);
        let description = description.map(|d| d.map(str::to_string));
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| {
                crate::lifecycle::update(
                    conn,
                    &saga_id,
                    title.as_deref(),
                    description.as_ref().map(|d| d.as_deref()),
                    &actor,
                )
            })
            .into_brain_core()
            .and_then(Saga::try_from)
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
    ) -> Result<(Saga, Vec<CascadeResult>)> {
        let saga_id = saga_id.to_string();
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| crate::lifecycle::close(conn, &saga_id, cascade, &actor))
            .into_brain_core()
            .and_then(|(row, cascade)| {
                Ok((
                    Saga::try_from(row)?,
                    cascade.into_iter().map(CascadeResult::from).collect(),
                ))
            })
    }

    /// Fetch a saga by ID. Returns None if not found.
    ///
    /// Accepts either the canonical ULID or the `saga-<hex>` short form. Per
    /// the read-side convention, an unresolvable input returns `Ok(None)`
    /// rather than propagating `SagaNotFound` — callers see "no such saga"
    /// as a null result, not as an error.
    pub fn get(&self, saga_id: &str) -> Result<Option<Saga>> {
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
            .and_then(|opt| opt.map(Saga::try_from).transpose())
    }

    /// List sagas with optional filters.
    pub fn list(&self, filter: SagaListFilter) -> Result<Vec<Saga>> {
        let filter: brain_persistence::db::sagas::queries::SagaListFilterRow = filter.into();
        self.db
            .with_read_conn(move |conn| queries::list_sagas(conn, &filter))
            .into_brain_core()
            .and_then(|rows| rows.into_iter().map(Saga::try_from).collect())
    }

    /// Transition a saga from `planning` to `open`. Emits `SagaStarted`.
    pub fn start(&self, saga_id: &str, actor: &str) -> Result<Saga> {
        let saga_id = saga_id.to_string();
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| crate::lifecycle::start(conn, &saga_id, &actor))
            .into_brain_core()
            .and_then(Saga::try_from)
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
            .map(|rows| rows.into_iter().map(BrainSummary::from).collect())
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
                    SqlError::Domain(BrainCoreError::SagaNotFound(format!(
                        "saga not found: {saga_id}"
                    )))
                })?;
                let status: SagaStatus = row.status.parse().map_err(|_| {
                    SqlError::Domain(BrainCoreError::Parse(format!(
                        "unknown saga status: {}",
                        row.status
                    )))
                })?;
                if !matches!(status, SagaStatus::Open) {
                    return Ok(SagaFrontier {
                        tasks: vec![],
                        brains: vec![],
                        status,
                    });
                }

                let task_ids: Vec<String> = list_saga_task_ids(conn, &canonical)?;

                let task_rows = list_ready_actionable_for_tasks(conn, &task_ids)?;
                let brain_rows = queries::brains_for_saga(conn, &canonical)?;
                Ok(SagaFrontier {
                    tasks: task_rows.into_iter().map(brain_tasks::Task::from).collect(),
                    brains: brain_rows.into_iter().map(BrainSummary::from).collect(),
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
    pub fn list_member_stubs(&self, saga_id: &str) -> Result<Vec<SagaMember>> {
        let saga_id = saga_id.to_string();
        self.db
            .with_read_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                list_saga_member_stubs(conn, &canonical)
            })
            .into_brain_core()
            .and_then(|stubs| stubs.into_iter().map(SagaMember::try_from).collect())
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
                let counts_row = queries::saga_stats(conn, &canonical)?;
                let label_rows = queries::saga_label_histogram(conn, &canonical)?;
                let brain_rows = queries::brains_for_saga(conn, &canonical)?;
                Ok(SagaStats {
                    counts: counts_row.into(),
                    label_histogram: label_rows.into_iter().map(LabelCount::from).collect(),
                    brains: brain_rows.into_iter().map(BrainSummary::from).collect(),
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
    ) -> Result<(Saga, Vec<CascadeResult>)> {
        let saga_id = saga_id.to_string();
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| crate::lifecycle::cancel(conn, &saga_id, cascade, &actor))
            .into_brain_core()
            .and_then(|(row, cascade)| {
                Ok((
                    Saga::try_from(row)?,
                    cascade.into_iter().map(CascadeResult::from).collect(),
                ))
            })
    }

    /// Test-only accessor for the underlying `Db` handle. Mirrors the
    /// precedent set by `brain_tasks::TaskStore::db_for_tests` — production
    /// code must call through the inherent verbs on `SagaStore` (or port
    /// traits once they exist).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn db_for_tests(&self) -> &Db {
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
        // Short-circuit empty batches before acquiring the writer mutex.
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let saga_id = saga_id.to_string();
        let task_ids = task_ids.to_vec();
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| {
                crate::lifecycle::add_tasks(conn, &saga_id, &task_ids, cascade, &actor)
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
        // Short-circuit empty batches before acquiring the writer mutex.
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let saga_id = saga_id.to_string();
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| {
                crate::lifecycle::remove_tasks(conn, &saga_id, task_ids, cascade, &actor)
            })
            .into_brain_core()
    }

    /// Reopen a closed or cancelled saga, setting status back to `open`.
    /// Clears `closed_at`. Emits `SagaReopened`. Rejected from `planning` or `open`.
    pub fn reopen(&self, saga_id: &str, actor: &str) -> Result<Saga> {
        let saga_id = saga_id.to_string();
        let actor = actor.to_string();
        self.db
            .with_write_conn(move |conn| crate::lifecycle::reopen(conn, &saga_id, &actor))
            .into_brain_core()
            .and_then(Saga::try_from)
    }

    /// Force a saga's status directly (test-only).
    ///
    /// L5: uses `now_ts()` (seconds) like every other timestamp in the table —
    /// previously wrote `as_millis()` which was ~1000× larger and would look
    /// like a future timestamp to anything that compared across paths.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn force_status_for_test(&self, saga_id: &str, status: SagaStatus) -> Result<()> {
        let saga_id = saga_id.to_string();
        let status_str = status.as_str();
        let ts = brain_core::utils::now_ts();
        self.db
            .with_write_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                brain_persistence::db::sagas::testing::force_saga_status(
                    conn, &canonical, status_str, ts,
                )
            })
            .into_brain_core()
    }

    /// Backdate a saga's `updated_at` directly (test-only).
    ///
    /// Used by L6: timestamp-monotonicity tests no longer need a 1.1s sleep —
    /// they backdate the row, then call `update()` and assert the new
    /// `updated_at` is strictly greater than the backdated value.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn force_updated_at_for_test(&self, saga_id: &str, ts: i64) -> Result<()> {
        let saga_id = saga_id.to_string();
        self.db
            .with_write_conn(move |conn| {
                let canonical = resolve_saga_id(conn, &saga_id)?;
                brain_persistence::db::sagas::testing::force_saga_timestamps(conn, &canonical, ts)
            })
            .into_brain_core()
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_macros)]
mod tests {
    use super::*;
    use crate::lifecycle::members::MAX_EXPANDED_BATCH;
    use brain_persistence::db::Db;
    use brain_persistence::db::sagas::testing;

    fn in_memory_store() -> SagaStore {
        let db = Db::open_in_memory().unwrap();
        SagaStore::new(db)
    }

    #[test]
    fn create_returns_planning_status() {
        let store = in_memory_store();
        let row = store.create("My Saga", None, "test").unwrap();
        assert_eq!(row.status, SagaStatus::Planning);
        assert_eq!(row.title, "My Saga");
        assert!(row.description.is_none());
        assert!(row.closed_at.is_none());
        assert_eq!(
            row.id.as_str().len(),
            26,
            "saga_id must be bare 26-char ULID"
        );
        assert!(
            !row.id.as_str().contains('-'),
            "saga_id must have no prefix"
        );
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
        let fetched = store.get(created.id.as_str()).unwrap().unwrap();
        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.title, "Get Test");
        assert_eq!(fetched.status, SagaStatus::Planning);
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
        assert!(row.created_at.timestamp() > 0);
        assert!(row.updated_at.timestamp() > 0);
        assert_eq!(row.created_at, row.updated_at);
    }

    // T1: SagaCreated event row is written on create
    #[test]
    fn create_writes_saga_created_event() {
        let store = in_memory_store();
        let row = store.create("X", None, "actor").unwrap();
        let (event_type, actor): (String, String) = store
            .db
            .with_read_conn(|c| testing::first_saga_event_meta(c, row.id.as_str()))
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
                testing::seed_saga_task_link(
                    conn,
                    row.id.as_str(),
                    "OTHER-BRAIN-TASK-01JXYZ",
                    1_000_000,
                )
            })
            .into_brain_core()
            .unwrap();
        let count: i64 = store
            .db
            .with_read_conn(|c| testing::count_saga_tasks(c, row.id.as_str()))
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
            .with_write_conn(|conn| testing::force_saga_status(conn, a.id.as_str(), "closed", 0))
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
            .with_write_conn(|conn| testing::force_saga_status(conn, a.id.as_str(), "closed", 0))
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
            .update(created.id.as_str(), Some("Renamed"), None, "test")
            .unwrap();
        assert_eq!(updated.title, "Renamed");
        assert!(updated.updated_at >= created.updated_at);
    }

    #[test]
    fn update_description_only() {
        let store = in_memory_store();
        let created = store.create("Title", None, "test").unwrap();
        let updated = store
            .update(created.id.as_str(), None, Some(Some("new desc")), "test")
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
                created.id.as_str(),
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
        let result = store.update(created.id.as_str(), None, None, "test");
        assert!(result.is_err());
    }

    #[test]
    fn update_empty_title_errors() {
        let store = in_memory_store();
        let created = store.create("Saga", None, "test").unwrap();
        let result = store.update(created.id.as_str(), Some("  "), None, "test");
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
                testing::force_saga_status(conn, a.id.as_str(), "closed", 0)?;
                testing::force_saga_status(conn, b.id.as_str(), "cancelled", 0)
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
            .with_write_conn(|conn| testing::force_saga_status(conn, a.id.as_str(), "cancelled", 0))
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
            .with_write_conn(|conn| testing::force_saga_status(conn, a.id.as_str(), "cancelled", 0))
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
                testing::seed_brain(conn, brain_id, brain_id, None)?;
                testing::seed_task(conn, task_id, brain_id, "task")
            })
            .into_brain_core()
            .unwrap();
    }

    // Helper: link a task to a saga.
    fn link_task(store: &SagaStore, saga_id: &str, task_id: &str) {
        store
            .db
            .with_write_conn(|conn| testing::seed_saga_task_link(conn, saga_id, task_id, 1000))
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
        link_task(&store, a.id.as_str(), "task-x-brain");
        link_task(&store, b.id.as_str(), "task-y-brain");

        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, a.id);
    }

    // T2: cross-brain saga appears in both containing_brain queries.
    #[test]
    fn containing_brain_cross_brain_saga_appears_for_both() {
        let store = in_memory_store();
        let saga = store.create("Cross-Brain Saga", None, "test").unwrap();

        insert_task(&store, "task-in-x", "brain-x");
        insert_task(&store, "task-in-y", "brain-y");
        link_task(&store, saga.id.as_str(), "task-in-x");
        link_task(&store, saga.id.as_str(), "task-in-y");

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
        assert_eq!(rows_x[0].id, saga.id);
        assert_eq!(rows_y[0].id, saga.id);
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
        link_task(&store, a.id.as_str(), "task-open");
        link_task(&store, b.id.as_str(), "task-closed");

        store
            .db
            .with_write_conn(|conn| testing::force_saga_status(conn, b.id.as_str(), "closed", 0))
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
        assert_eq!(rows[0].id, a.id);

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
            .with_write_conn(|conn| testing::force_saga_status(conn, row.id.as_str(), "closed", 0))
            .into_brain_core()
            .unwrap();
        let updated = store
            .update(row.id.as_str(), Some("Closed but renamed"), None, "actor")
            .unwrap();
        assert_eq!(updated.title, "Closed but renamed");
        assert_eq!(updated.status, SagaStatus::Closed);
    }

    // T3: clear-description test — create with desc, update with Some(None), assert NULL
    #[test]
    fn update_clears_description_when_some_none() {
        let store = in_memory_store();
        let row = store.create("Has Desc", Some("original"), "test").unwrap();
        assert!(row.description.is_some());
        let updated = store
            .update(row.id.as_str(), None, Some(None), "actor")
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
            .update(row.id.as_str(), Some("After"), None, "actor")
            .unwrap();
        let (event_type, payload): (String, String) = store
            .db
            .with_read_conn(|c| testing::saga_event_by_type_like(c, row.id.as_str(), "%updated%"))
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
                .update(row.id.as_str(), Some("   "), None, "actor")
                .is_err()
        );
        assert!(
            store
                .update(row.id.as_str(), Some("\t\n"), None, "actor")
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
        let backdated = brain_core::utils::now_ts() - 2;
        store
            .force_updated_at_for_test(row.id.as_str(), backdated)
            .unwrap();
        let updated = store
            .update(row.id.as_str(), Some("Timing Updated"), None, "actor")
            .unwrap();
        assert!(
            updated.updated_at.timestamp() > backdated,
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
            saga.id.as_str(),
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
                testing::count_saga_task_pair(c, saga.id.as_str(), "good-brain-task01")
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
            .with_write_conn(|conn| testing::force_saga_status(conn, saga.id.as_str(), "closed", 0))
            .into_brain_core()
            .unwrap();

        let err = store
            .add_tasks(
                saga.id.as_str(),
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
                testing::force_saga_status(conn, saga.id.as_str(), "cancelled", 0)
            })
            .into_brain_core()
            .unwrap();

        let err = store
            .add_tasks(
                saga.id.as_str(),
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
                saga.id.as_str(),
                &["brain-a-task01".to_string(), "brain-b-task01".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(count.len(), 2);

        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| testing::list_saga_task_ids_sorted(c, saga.id.as_str()))
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
                saga.id.as_str(),
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
                testing::count_saga_events_of_type(c, saga.id.as_str(), "saga_task_added")
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
        let count = store
            .add_tasks(saga.id.as_str(), &[], false, "test")
            .unwrap();
        assert_eq!(count.len(), 0);
    }

    // ── start tests ────────────────────────────────────────────────────────

    #[test]
    fn start_planning_saga_succeeds() {
        let store = in_memory_store();
        let created = store.create("To Start", None, "test").unwrap();
        assert_eq!(created.status, SagaStatus::Planning);

        let started = store.start(created.id.as_str(), "test").unwrap();
        assert_eq!(started.status, SagaStatus::Open);
        assert_eq!(started.id, created.id);
    }

    #[test]
    fn start_already_open_fails() {
        let store = in_memory_store();
        let created = store.create("Double Start", None, "test").unwrap();
        store.start(created.id.as_str(), "test").unwrap();
        let err = store.start(created.id.as_str(), "test").unwrap_err();
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
        let brains = store.brains_for_saga(saga.id.as_str()).unwrap();
        assert!(brains.is_empty(), "expected no brains for memberless saga");
    }

    #[test]
    fn brains_for_saga_returns_distinct_brains() {
        let store = in_memory_store();
        let saga = store.create("Multi-brain", None, "test").unwrap();

        // Insert two brains and tasks directly into the DB.
        store
            .db
            .with_write_conn(|conn| {
                testing::seed_brain(conn, "brain-a", "Brain A", Some("BRA"))?;
                testing::seed_brain(conn, "brain-b", "Brain B", Some("BRB"))?;
                testing::seed_task(conn, "BRA-01TASK0000000000000000001", "brain-a", "Task A")?;
                testing::seed_task(conn, "BRB-01TASK0000000000000000002", "brain-b", "Task B")?;
                testing::seed_saga_task_link(
                    conn,
                    saga.id.as_str(),
                    "BRA-01TASK0000000000000000001",
                    0,
                )?;
                testing::seed_saga_task_link(
                    conn,
                    saga.id.as_str(),
                    "BRB-01TASK0000000000000000002",
                    0,
                )
            })
            .into_brain_core()
            .unwrap();

        let brains = store.brains_for_saga(saga.id.as_str()).unwrap();
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

        store
            .db
            .with_write_conn(|conn| {
                testing::seed_brain(conn, "brain-c", "Brain C", Some("BRC"))?;
                testing::seed_task(conn, "BRC-01TASK0000000000000000003", "brain-c", "Task C1")?;
                testing::seed_task(conn, "BRC-01TASK0000000000000000004", "brain-c", "Task C2")?;
                testing::seed_saga_task_link(
                    conn,
                    saga.id.as_str(),
                    "BRC-01TASK0000000000000000003",
                    0,
                )?;
                testing::seed_saga_task_link(
                    conn,
                    saga.id.as_str(),
                    "BRC-01TASK0000000000000000004",
                    0,
                )
            })
            .into_brain_core()
            .unwrap();

        let brains = store.brains_for_saga(saga.id.as_str()).unwrap();
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
                saga.id.as_str(),
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
            .with_read_conn(|c| testing::count_saga_tasks(c, saga.id.as_str()))
            .into_brain_core()
            .unwrap();
        assert_eq!(member_count, 1, "should be exactly 1 saga_tasks row");

        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                testing::count_saga_events_of_type(c, saga.id.as_str(), "saga_task_added")
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
            .add_tasks(
                saga.id.as_str(),
                &["idem-task01".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(first.len(), 1);

        // Now add [t1, t2] — t1 is already a member, t2 is new.
        let second = store
            .add_tasks(
                saga.id.as_str(),
                &["idem-task01".to_string(), "idem-task02".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(second.len(), 1, "only t2 should be newly inserted");

        // Both tasks must end up as members.
        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| testing::list_saga_task_ids_sorted(c, saga.id.as_str()))
            .into_brain_core()
            .unwrap();
        assert!(ids.contains(&"idem-task01".to_string()));
        assert!(ids.contains(&"idem-task02".to_string()));

        // Exactly 2 SagaTaskAdded events total (1 from first call, 1 from
        // second call — the dup must not emit a second event for t1).
        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                testing::count_saga_events_of_type(c, saga.id.as_str(), "saga_task_added")
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
            .with_write_conn(|conn| testing::seed_parent_of_edge(conn, parent_id, child_id))
            .into_brain_core()
            .unwrap();
    }

    // Helper: count member tasks in a saga.
    fn member_count(store: &SagaStore, saga_id: &str) -> usize {
        store
            .db
            .with_read_conn(|c| testing::count_saga_tasks(c, saga_id))
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
            .add_tasks(saga.id.as_str(), &["epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(count.len(), 4, "cascade should add epic + 3 descendants");
        assert_eq!(member_count(&store, saga.id.as_str()), 4);

        // One SagaTaskAdded event per insertion.
        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                testing::count_saga_events_of_type(c, saga.id.as_str(), "saga_task_added")
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
            .add_tasks(saga.id.as_str(), &["kid1".to_string()], false, "test")
            .unwrap();

        let count = store
            .add_tasks(saga.id.as_str(), &["epic2".to_string()], true, "test")
            .unwrap();
        assert_eq!(
            count.len(),
            2,
            "kid1 already a member; cascade adds epic2 + kid2"
        );
        assert_eq!(member_count(&store, saga.id.as_str()), 3);
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
            .add_tasks(saga.id.as_str(), &["xb-parent".to_string()], true, "test")
            .unwrap();
        assert_eq!(count.len(), 2);

        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| testing::list_saga_task_ids_sorted(c, saga.id.as_str()))
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
            .add_tasks(saga.id.as_str(), &["lonely".to_string()], true, "test")
            .unwrap();
        assert_eq!(count.len(), 1);
        assert_eq!(member_count(&store, saga.id.as_str()), 1);
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
            .add_tasks(saga.id.as_str(), &["rc-epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(member_count(&store, saga.id.as_str()), 3);

        let removed = store
            .remove_tasks(saga.id.as_str(), vec!["rc-epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(
            removed.len(),
            3,
            "cascade-remove should strip the full subtree"
        );
        assert_eq!(member_count(&store, saga.id.as_str()), 0);

        let removed_events: i64 = store
            .db
            .with_read_conn(|c| {
                testing::count_saga_events_of_type(c, saga.id.as_str(), "saga_task_removed")
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

        let result = store.add_tasks(saga.id.as_str(), &["root".to_string()], true, "test");
        let err = result.expect_err("cascade above MAX_EXPANDED_BATCH must error");
        assert!(
            format!("{err}").contains("MAX_EXPANDED_BATCH"),
            "error message should name the cap: {err}"
        );
        // Nothing was inserted because the cap fires before any writes.
        assert_eq!(member_count(&store, saga.id.as_str()), 0);
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
            .add_tasks(saga.id.as_str(), &["real-task".to_string()], false, "test")
            .unwrap();

        // cascade=false: unresolved typo is a silent no-op (existing contract).
        let lenient = store
            .remove_tasks(
                saga.id.as_str(),
                vec!["nonexistent-typo".to_string()],
                false,
                "test",
            )
            .unwrap();
        assert_eq!(lenient.len(), 0);

        // cascade=true: unresolved typo should fail loud.
        let strict = store.remove_tasks(
            saga.id.as_str(),
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
        assert_eq!(member_count(&store, saga.id.as_str()), 1);
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
            .add_tasks(saga.id.as_str(), &["pe-epic".to_string()], false, "test")
            .unwrap();
        assert_eq!(member_count(&store, saga.id.as_str()), 1);

        let removed = store
            .remove_tasks(saga.id.as_str(), vec!["pe-epic".to_string()], true, "test")
            .unwrap();
        assert_eq!(
            removed.len(),
            1,
            "only the epic was a member; cascade is a no-op for the non-member child"
        );
        assert_eq!(member_count(&store, saga.id.as_str()), 0);
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
                saga.id.as_str(),
                &["rem-brain-task01".to_string()],
                false,
                "test",
            )
            .unwrap();

        // Force the saga to closed.
        store
            .force_status_for_test(saga.id.as_str(), SagaStatus::Closed)
            .unwrap();

        let err = store
            .remove_tasks(
                saga.id.as_str(),
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
                testing::count_saga_task_pair(c, saga.id.as_str(), "rem-brain-task01")
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
        store.start(saga.id.as_str(), "test").unwrap();
        let (closed, _) = store.close(saga.id.as_str(), false, "test").unwrap();
        assert_eq!(closed.status, SagaStatus::Closed);
        assert!(closed.closed_at.is_some(), "close should set closed_at");

        let reopened = store.reopen(saga.id.as_str(), "test").unwrap();
        assert_eq!(reopened.status, SagaStatus::Open);
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
        store.start(saga.id.as_str(), "test").unwrap();
        let (cancelled, _) = store.cancel(saga.id.as_str(), false, "test").unwrap();
        assert_eq!(cancelled.status, SagaStatus::Cancelled);

        let reopened = store.reopen(saga.id.as_str(), "test").unwrap();
        assert_eq!(reopened.status, SagaStatus::Open);
    }

    #[test]
    fn reopen_planning_rejected() {
        let store = in_memory_store();
        let saga = store.create("Planning Saga", None, "test").unwrap();
        assert_eq!(saga.status, SagaStatus::Planning);

        let err = store.reopen(saga.id.as_str(), "test").unwrap_err();
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
        store.start(saga.id.as_str(), "test").unwrap();

        let err = store.reopen(saga.id.as_str(), "test").unwrap_err();
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
        store.start(saga.id.as_str(), "test").unwrap();
        store.close(saga.id.as_str(), false, "test").unwrap();

        store.reopen(saga.id.as_str(), "actor-x").unwrap();

        let (count, actor): (i64, String) = store
            .db
            .with_read_conn(|c| {
                testing::count_and_last_actor_for_event_type(c, saga.id.as_str(), "saga_reopened")
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(count, 1, "expected exactly 1 saga_reopened event");
        assert_eq!(actor, "actor-x");
    }
}
