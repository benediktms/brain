//! Wire-protocol domain types. Pure data, framework-free.
//!
//! All types here are serde-roundtrippable and contain no I/O, DB, or
//! domain-crate references. This is the "inside" of the hexagon — the
//! abstract message vocabulary that adapters translate to and from bytes.
//!
//! # Anti-corruption-layer note
//!
//! Wire types (e.g. [`TaskSummary`]) deliberately do NOT re-use the
//! corresponding internal types from `brain_tasks` / `brain_sagas` /
//! etc. The duplication is a cost we accept on purpose: brain_rpc is
//! the wire contract and must stay decoupled from internal storage
//! shapes. If `brain_tasks::Task` adds a field tomorrow, the wire
//! format doesn't move with it — the daemon's dispatcher explicitly
//! maps the new field into the wire type (or drops it) at the
//! boundary. The flip side: wire-format changes are deliberate and
//! visible (and force a [`PROTOCOL_VERSION`] bump for breaking
//! changes), not silent.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The wire-protocol version negotiated on every connection.
///
/// Bumped on any breaking change to [`Request`] / [`Response`] / [`RpcError`]
/// shape. Client and daemon exchange this on connect; a mismatch returns
/// [`RpcError::VersionMismatch`] with both versions so the operator can be
/// told which side to restart.
pub const PROTOCOL_VERSION: u32 = 4;

/// A client-originated message sent over the wire.
///
/// New variants are added as CLI/MCP operations migrate to the daemon.
/// First-real-data variant: [`Request::TasksList`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Version-negotiation handshake. Sent first on every connection.
    Handshake { version: u32 },
    /// No-op liveness check. Server echoes [`Response::Pong`].
    Ping,
    /// List tasks with optional filters. Server returns
    /// [`Response::TasksList`].
    TasksList { params: TasksListParams },
    /// Fetch a single task by ID. Server returns
    /// [`Response::TasksShow`] with `None` when the task is not found.
    TasksShow { id: String },
    /// Return the next highest-priority actionable task. Server returns
    /// [`Response::TasksNext`] with `None` when there are no ready tasks.
    TasksNext,
    /// Create a new task. Server returns [`Response::TasksCreate`] with
    /// the newly-created `TaskSummary` and the originating `event_id`.
    TasksCreate { params: TasksCreateParams },
    /// Update non-status fields of an existing task. Server returns
    /// [`Response::TasksUpdate`].
    TasksUpdate { params: TasksUpdateParams },
    /// Apply a status-mutating action to a task (close / open / block /
    /// in_progress / cancel). Server returns [`Response::TasksMutate`].
    /// Modeled separately from `TasksUpdate` because status changes are
    /// a distinct event type in the underlying log.
    TasksMutate { params: TasksMutateParams },
    /// Add a dependency edge: `task_id` depends on `depends_on_task_id`.
    /// Server returns [`Response::TasksDepAdded`].
    TasksAddDep {
        task_id: String,
        depends_on_task_id: String,
    },
    /// Remove a dependency edge previously added via
    /// [`Request::TasksAddDep`]. Server returns [`Response::TasksDepRemoved`].
    TasksRemoveDep {
        task_id: String,
        depends_on_task_id: String,
    },
    /// Add a label to a task. Server returns [`Response::TasksLabelAdded`].
    TasksAddLabel { task_id: String, label: String },
    /// Remove a label from a task. Server returns
    /// [`Response::TasksLabelRemoved`].
    TasksRemoveLabel { task_id: String, label: String },
    /// Transfer a task to a different brain (preserve-ID move). Server
    /// returns [`Response::TasksTransfer`] with the updated summary.
    TasksTransfer { params: TasksTransferParams },
    /// Run an integrity verification pass over the records object
    /// store. Server returns [`Response::RecordsVerify`] with a
    /// [`RecordsVerifyReport`] mirroring the JSON output that
    /// `brain records verify --json` produces locally.
    RecordsVerify,
    /// List analysis records. Server returns
    /// [`Response::AnalysesList`].
    AnalysesList { params: RecordsListParams },
    /// Fetch a single analysis record by ID. Server returns
    /// [`Response::AnalysesShow`] with `None` when not found.
    AnalysesShow { id: String },
    /// Create a new analysis record. Server returns
    /// [`Response::AnalysesCreate`].
    AnalysesCreate { params: RecordsCreateParams },
    /// List artifact records (cross-kind read view). Server returns
    /// [`Response::ArtifactsList`].
    ArtifactsList { params: ArtifactsListParams },
    /// Fetch a single artifact record by ID. Server returns
    /// [`Response::ArtifactsShow`] with `None` when not found.
    ArtifactsShow { id: String },
    /// List document records. Server returns
    /// [`Response::DocumentsList`].
    DocumentsList { params: RecordsListParams },
    /// Fetch a single document record by ID. Server returns
    /// [`Response::DocumentsShow`] with `None` when not found.
    DocumentsShow { id: String },
    /// Create a new document record. Server returns
    /// [`Response::DocumentsCreate`].
    DocumentsCreate { params: RecordsCreateParams },
    /// List plan records. Server returns [`Response::PlansList`].
    PlansList { params: RecordsListParams },
    /// Fetch a single plan record by ID. Server returns
    /// [`Response::PlansShow`] with `None` when not found.
    PlansShow { id: String },
    /// Create a new plan record. Server returns
    /// [`Response::PlansCreate`].
    PlansCreate { params: RecordsCreateParams },
    /// List snapshot records. Server returns
    /// [`Response::SnapshotsList`].
    SnapshotsList { params: RecordsListParams },
    /// Fetch a single snapshot record by ID. Server returns
    /// [`Response::SnapshotsShow`] with `None` when not found.
    SnapshotsShow { id: String },
    /// Create (save) a new snapshot record. Server returns
    /// [`Response::SnapshotsCreate`]. Mirrors `brain snapshots save`.
    SnapshotsCreate { params: RecordsCreateParams },
    /// List sagas with optional filters. Server returns
    /// [`Response::SagasList`].
    SagasList { params: SagasListParams },
    /// Fetch a single saga by ID. Server returns
    /// [`Response::SagasGet`] with `None` when not found.
    SagasGet { saga_id: String },
    /// Create a new saga in `planning` status. Server returns
    /// [`Response::SagasCreate`].
    SagasCreate { params: SagasCreateParams },
    /// Update saga title and/or description. Server returns
    /// [`Response::SagasUpdate`].
    SagasUpdate { params: SagasUpdateParams },
    /// Add one or more tasks to a saga. Server returns
    /// [`Response::SagasAddTasks`].
    SagasAddTasks {
        saga_id: String,
        task_ids: Vec<String>,
        cascade: bool,
    },
    /// Remove one or more tasks from a saga. Server returns
    /// [`Response::SagasRemoveTasks`].
    SagasRemoveTasks {
        saga_id: String,
        task_ids: Vec<String>,
        cascade: bool,
    },
    /// Return the ready-actionable member tasks for a saga plus the
    /// brains those tasks belong to. Server returns
    /// [`Response::SagasFrontier`].
    SagasFrontier { saga_id: String },
    /// Transition a saga from `planning` to `open`. Server returns
    /// [`Response::SagasStart`].
    SagasStart { saga_id: String },
    /// Close an `open` saga, optionally cascading member tasks to
    /// `done`. Server returns [`Response::SagasClose`].
    SagasClose { saga_id: String, cascade: bool },
    /// Cancel a saga, optionally cascading non-terminal member tasks
    /// to `cancelled`. Server returns [`Response::SagasCancel`].
    SagasCancel { saga_id: String, cascade: bool },
    /// Reopen a closed or cancelled saga. Server returns
    /// [`Response::SagasReopen`].
    SagasReopen { saga_id: String },
    /// Return aggregated statistics for a saga's member tasks. Server
    /// returns [`Response::SagasStats`].
    SagasStats { saga_id: String },
    /// Record a goal/actions/outcome episode. Server returns
    /// [`Response::MemoryWriteEpisode`].
    MemoryWriteEpisode { params: MemoryWriteEpisodeParams },
    /// Store a step-by-step procedure. Server returns
    /// [`Response::MemoryWriteProcedure`].
    MemoryWriteProcedure { params: MemoryWriteProcedureParams },
    /// Retrieve memory chunks at a requested level of detail. Server returns
    /// [`Response::MemoryRetrieve`].
    MemoryRetrieve { params: MemoryRetrieveParams },
    /// Group recent episodes into consolidation clusters. Server returns
    /// [`Response::MemoryConsolidate`].
    MemoryConsolidate { params: MemoryConsolidateParams },
    /// Generate or retrieve a scope summary. Server returns
    /// [`Response::MemorySummarizeScope`].
    MemorySummarizeScope { params: MemorySummarizeScopeParams },
    /// Retrieve source material for reflection (prepare) or store a
    /// reflection (commit). Server returns [`Response::MemoryReflect`].
    MemoryReflect { params: MemoryReflectParams },

    // ── tags ────────────────────────────────────────────────────────────
    /// List tag_aliases rows with optional filtering. Server returns
    /// [`Response::TagsAliasesList`].
    TagsAliasesList { params: TagsAliasesListParams },
    /// Show tag-clustering health summary. Server returns
    /// [`Response::TagsAliasesStatus`].
    TagsAliasesStatus,

    // ── jobs ────────────────────────────────────────────────────────────
    /// Show job queue health summary. Server returns
    /// [`Response::JobsStatus`]. `params` carries server-side filters
    /// (kind / status / limit) so the daemon — not the MCP tool — owns
    /// the filtering loop.
    JobsStatus { params: JobsStatusParams },

    // ── status ──────────────────────────────────────────────────────────
    /// Show brain health status. Server returns [`Response::BrainStatus`].
    BrainStatus,

    // ── provider ────────────────────────────────────────────────────────
    /// List configured providers. Server returns
    /// [`Response::ProviderList`].
    ProviderList,

    // ── watch ────────────────────────────────────────────────────────────
    /// Register a filesystem path for watching. Server returns
    /// [`Response::WatchAdded`].
    WatchAdd { path: String },
    /// Deregister a previously-added watch path. Server returns
    /// [`Response::WatchRemoved`].
    WatchRemove { path: String },
    /// List all currently registered watch paths. Server returns
    /// [`Response::WatchList`].
    WatchList,

    // ── links ────────────────────────────────────────────────────────────
    /// Add a polymorphic link edge between two entities. Server returns
    /// [`Response::LinksAdd`].
    LinksAdd { params: LinksAddParams },
    /// Remove a polymorphic link edge. Server returns
    /// [`Response::LinksRemove`].
    LinksRemove { params: LinksRemoveParams },
    /// List incident link edges for an entity. Server returns
    /// [`Response::LinksForEntity`].
    LinksForEntity { params: LinksForEntityParams },

    // ── records (mutations) ─────────────────────────────────────────────
    /// Archive an existing record. Server returns
    /// [`Response::RecordsArchive`].
    RecordsArchive { params: RecordsArchiveParams },
    /// Add a link from a record to another entity. Server returns
    /// [`Response::RecordsLinkAdd`].
    RecordsLinkAdd { params: RecordsLinkParams },
    /// Remove a link from a record to another entity. Server returns
    /// [`Response::RecordsLinkRemove`].
    RecordsLinkRemove { params: RecordsLinkParams },
    /// Add a tag to a record. Server returns
    /// [`Response::RecordsTagAdd`].
    RecordsTagAdd { record_id: String, tag: String },
    /// Remove a tag from a record. Server returns
    /// [`Response::RecordsTagRemove`].
    RecordsTagRemove { record_id: String, tag: String },

    // ── records (read: search + content) ────────────────────────────────
    /// Run a hybrid semantic + FTS search filtered to record-kind
    /// results. Server returns [`Response::RecordsSearch`].
    RecordsSearch { params: RecordsSearchParams },
    /// Fetch the raw content (text or base64-encoded bytes) for a
    /// record. Server returns [`Response::RecordsFetchContent`].
    RecordsFetchContent { params: RecordsFetchContentParams },

    // ── tasks (batch + opaque event) ────────────────────────────────────
    /// Apply a raw task event (the MCP `tasks.apply_event` surface).
    /// Server returns [`Response::TasksApplyEvent`].
    TasksApplyEvent { params: TasksApplyEventParams },
    /// Batch dependency operations (add/remove/chain/fan/clear). Server
    /// returns [`Response::TasksDepsBatch`].
    TasksDepsBatch { params: TasksDepsBatchParams },
    /// Batch label operations across tasks. Server returns
    /// [`Response::TasksLabelsBatch`].
    TasksLabelsBatch { params: TasksLabelsBatchParams },
    /// Return all unique labels with counts and associated task IDs.
    /// Server returns [`Response::TasksLabelsSummary`].
    TasksLabelsSummary,

    // ── memory (DAG walk) ───────────────────────────────────────────────
    /// Walk the `continues` thread starting from an episode or
    /// procedure. Server returns [`Response::MemoryWalkThread`].
    MemoryWalkThread { params: MemoryWalkThreadParams },

    // ── tags (recluster) ────────────────────────────────────────────────
    /// Recluster tag synonyms via the embedder. Server returns
    /// [`Response::TagsRecluster`].
    TagsRecluster { params: TagsReclusterParams },

    // ── brains (enumeration) ────────────────────────────────────────────
    /// List all registered brain projects. Server returns
    /// [`Response::BrainsList`].
    BrainsList { params: BrainsListParams },
}

/// Optional filter and pagination params for [`Request::TasksList`].
///
/// Mirrors the most common flags of `brain tasks list`. Full param
/// parity with the existing CLI surface (assignee, label, ready,
/// blocked, group_by, brain) lands in a follow-up — MVP keeps this
/// minimal to nail down the wire shape first.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct TasksListParams {
    /// Filter by status ("open", "in_progress", "blocked", "done", "cancelled").
    pub status: Option<String>,
    /// Filter by priority (0-4).
    pub priority: Option<u8>,
    /// Maximum number of tasks to return. None = server default.
    pub limit: Option<u32>,
    /// FTS5 query on title + description.
    pub search: Option<String>,
}

/// Wire-format params for [`Request::TasksCreate`].
///
/// Mirrors the user-facing field set of `brain tasks create`. `priority`
/// is `u8` on the wire (0=critical .. 4=backlog) — the daemon maps it
/// onto the internal `i32` field at the boundary. `task_type` is a
/// stringly-typed enum on the wire ("task" / "bug" / "feature" / "epic"
/// / "spike") for the same forward-compatibility reason
/// [`TaskSummary::status`] is a string.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TasksCreateParams {
    pub title: String,
    pub description: Option<String>,
    pub priority: u8,
    pub task_type: String,
    pub assignee: Option<String>,
    pub parent: Option<String>,
}

/// Wire-format params for [`Request::TasksUpdate`].
///
/// Each field is optional; only set fields are applied. Status changes
/// go through [`Request::TasksMutate`] instead (they're a different
/// event type in the underlying log).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TasksUpdateParams {
    pub id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub priority: Option<u8>,
    pub assignee: Option<String>,
}

/// Wire-format params for [`Request::TasksMutate`].
///
/// `action` is one of `"close"`, `"open"`, `"block"`, `"in_progress"`,
/// or `"cancel"`. The dispatcher maps each value onto the corresponding
/// internal `TaskStatus` and emits a `StatusChanged` event.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TasksMutateParams {
    pub id: String,
    pub action: String,
}

/// Wire-format params for [`Request::TasksTransfer`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TasksTransferParams {
    pub task_id: String,
    pub target_brain: String,
}

/// Optional filter and pagination params shared across
/// [`Request::AnalysesList`], [`Request::DocumentsList`],
/// [`Request::PlansList`], and [`Request::SnapshotsList`].
///
/// Mirrors the user-facing flags of the equivalent `brain <kind> list`
/// commands. Fields not relevant to a given kind (`task_id` on snapshots
/// for instance) are accepted on the wire but the dispatcher will
/// surface a Protocol error when they cannot be honoured.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordsListParams {
    /// Filter by single tag (exact match).
    pub tag: Option<String>,
    /// Filter by linked task ID.
    pub task_id: Option<String>,
    /// Filter by status string ("active", "archived"). `None` accepts
    /// the default ("active") chosen by the dispatcher.
    pub status: Option<String>,
    /// Maximum result count. `None` = server default.
    pub limit: Option<u32>,
}

/// Optional filter and pagination params for [`Request::ArtifactsList`].
///
/// Artifacts are a cross-kind read view, so this struct adds a `kind`
/// filter over [`RecordsListParams`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct ArtifactsListParams {
    /// Filter by record kind (e.g. `"document"`, `"plan"`,
    /// `"snapshot"`, `"analysis"`, or any custom kind string).
    pub kind: Option<String>,
    /// Filter by single tag (exact match).
    pub tag: Option<String>,
    /// Filter by status string ("active", "archived").
    pub status: Option<String>,
    /// Maximum result count.
    pub limit: Option<u32>,
}

/// Wire-format params for record-creation operations
/// ([`Request::AnalysesCreate`], [`Request::DocumentsCreate`],
/// [`Request::PlansCreate`], [`Request::SnapshotsCreate`]).
///
/// `body` carries the raw payload bytes — the daemon writes them to the
/// object store (compressing past threshold) at the boundary. The wire
/// shape keeps payload-source negotiation (`--file` vs `--stdin` vs
/// `--text`) on the CLI side; what crosses the wire is always a
/// resolved byte buffer.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct RecordsCreateParams {
    pub title: String,
    pub description: Option<String>,
    /// Raw payload bytes. Serializes as a JSON array of integers; in
    /// practice the wire is local Unix sockets so the encoding cost
    /// is acceptable. A future ticket may add a base64 encoding for
    /// remote transports.
    pub body: Vec<u8>,
    pub media_type: Option<String>,
    pub task_id: Option<String>,
    pub tags: Vec<String>,
    /// Optional target brain name or ID. `None` writes to the
    /// daemon's local scope.
    pub brain: Option<String>,
}

/// Optional filter params for [`Request::SagasList`].
///
/// Mirrors the user-facing flags of `brain sagas list`. By default the
/// daemon excludes `closed` and `cancelled` sagas; setting the
/// corresponding flag (or both, equivalent to the CLI's `--all`)
/// widens the result set.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct SagasListParams {
    /// Include `closed` sagas in the result.
    pub include_closed: bool,
    /// Include `cancelled` sagas in the result.
    pub include_cancelled: bool,
    /// Only return sagas that have at least one member-task in this
    /// brain (resolved by name or ID daemon-side).
    pub containing_brain: Option<String>,
}

/// Wire-format params for [`Request::SagasCreate`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SagasCreateParams {
    pub title: String,
    pub description: Option<String>,
}

/// Wire-format params for [`Request::SagasUpdate`].
///
/// `description` uses [`SagaDescriptionUpdate`] so callers can
/// disambiguate "don't touch" from "set to NULL" — the underlying
/// `SagaStore::update` API takes `Option<Option<&str>>` for the same
/// reason.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SagasUpdateParams {
    pub saga_id: String,
    pub title: Option<String>,
    /// `None` = don't touch description.
    /// `Some(SagaDescriptionUpdate::Clear)` = set description to NULL.
    /// `Some(SagaDescriptionUpdate::Set(text))` = set description to
    /// `text`.
    pub description: Option<SagaDescriptionUpdate>,
}

/// Wire-format variant for updating a saga's description.
///
/// Tagged on the wire so the JSON shape distinguishes "set to a new
/// string" from "set to NULL" — a single nested `Option<Option<String>>`
/// would serialize ambiguously through serde's default representation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SagaDescriptionUpdate {
    /// Set description to NULL.
    Clear,
    /// Set description to `value`.
    Set { value: String },
}

/// A server-originated reply to a [`Request`].
// `Eq` is intentionally NOT derived: [`Response::RecordsSearch`] carries
// per-hit `f64` scores via [`WireRecordHit`], and `f64` is not `Eq`
// (NaN ≠ NaN). `PartialEq` suffices for round-trip tests and is what
// every consumer in this workspace actually uses — no `HashMap<Response>`
// or `HashSet<Response>` exists anywhere.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// Reply to [`Request::Handshake`] carrying the server's protocol version.
    HandshakeOk { server_version: u32 },
    /// Reply to [`Request::Ping`].
    Pong,
    /// Reply to [`Request::TasksList`].
    TasksList { tasks: Vec<TaskSummary> },
    /// Reply to [`Request::TasksShow`]. `task` is `None` when the
    /// requested task does not exist.
    TasksShow { task: Option<TaskSummary> },
    /// Reply to [`Request::TasksNext`]. `task` is `None` when there
    /// are no ready actionable tasks.
    TasksNext { task: Option<TaskSummary> },
    /// Reply to [`Request::TasksCreate`].
    TasksCreate { task: TaskSummary, event_id: String },
    /// Reply to [`Request::TasksUpdate`].
    TasksUpdate { task: TaskSummary, event_id: String },
    /// Reply to [`Request::TasksMutate`].
    TasksMutate { task: TaskSummary, event_id: String },
    /// Reply to [`Request::TasksAddDep`].
    TasksDepAdded { event_id: String },
    /// Reply to [`Request::TasksRemoveDep`].
    TasksDepRemoved { event_id: String },
    /// Reply to [`Request::TasksAddLabel`].
    TasksLabelAdded { event_id: String },
    /// Reply to [`Request::TasksRemoveLabel`].
    TasksLabelRemoved { event_id: String },
    /// Reply to [`Request::TasksTransfer`].
    TasksTransfer { task: TaskSummary, event_id: String },
    /// Reply to [`Request::RecordsVerify`].
    RecordsVerify { report: RecordsVerifyReport },
    /// Reply to [`Request::AnalysesList`].
    AnalysesList { records: Vec<AnalysisSummary> },
    /// Reply to [`Request::AnalysesShow`]. `record` is `None` when not found.
    AnalysesShow { record: Option<AnalysisSummary> },
    /// Reply to [`Request::AnalysesCreate`].
    AnalysesCreate {
        record: AnalysisSummary,
        content_hash: String,
        size: u64,
    },
    /// Reply to [`Request::ArtifactsList`].
    ArtifactsList { records: Vec<ArtifactSummary> },
    /// Reply to [`Request::ArtifactsShow`]. `record` is `None` when not found.
    ArtifactsShow { record: Option<ArtifactSummary> },
    /// Reply to [`Request::DocumentsList`].
    DocumentsList { records: Vec<DocumentSummary> },
    /// Reply to [`Request::DocumentsShow`]. `record` is `None` when not found.
    DocumentsShow { record: Option<DocumentSummary> },
    /// Reply to [`Request::DocumentsCreate`].
    DocumentsCreate {
        record: DocumentSummary,
        content_hash: String,
        size: u64,
    },
    /// Reply to [`Request::PlansList`].
    PlansList { records: Vec<PlanSummary> },
    /// Reply to [`Request::PlansShow`]. `record` is `None` when not found.
    PlansShow { record: Option<PlanSummary> },
    /// Reply to [`Request::PlansCreate`].
    PlansCreate {
        record: PlanSummary,
        content_hash: String,
        size: u64,
    },
    /// Reply to [`Request::SnapshotsList`].
    SnapshotsList { records: Vec<SnapshotSummary> },
    /// Reply to [`Request::SnapshotsShow`]. `record` is `None` when not found.
    SnapshotsShow { record: Option<SnapshotSummary> },
    /// Reply to [`Request::SnapshotsCreate`].
    SnapshotsCreate {
        record: SnapshotSummary,
        content_hash: String,
        size: u64,
    },
    /// Reply to [`Request::SagasList`].
    SagasList { sagas: Vec<SagaSummary> },
    /// Reply to [`Request::SagasGet`]. `saga` is `None` when the
    /// requested saga does not exist.
    SagasGet { saga: Option<SagaSummary> },
    /// Reply to [`Request::SagasCreate`].
    SagasCreate { saga: SagaSummary },
    /// Reply to [`Request::SagasUpdate`].
    SagasUpdate { saga: SagaSummary },
    /// Reply to [`Request::SagasAddTasks`]. `added_task_ids` carries
    /// the canonical task IDs that were actually inserted (excludes
    /// already-member tasks and within-batch duplicates). `added` is
    /// `added_task_ids.len()` for caller convenience and to match the
    /// shape of the CLI's `--json` output.
    SagasAddTasks {
        saga_id: String,
        added: u32,
        added_task_ids: Vec<String>,
    },
    /// Reply to [`Request::SagasRemoveTasks`]. `removed_task_ids`
    /// carries the canonical task IDs that were actually removed
    /// (intersection of the resolved input with current membership).
    SagasRemoveTasks {
        saga_id: String,
        removed: u32,
        removed_task_ids: Vec<String>,
    },
    /// Reply to [`Request::SagasFrontier`]. `saga_status` is the
    /// saga's lifecycle state at the time of the call; `tasks` and
    /// `brains` are empty by contract for any non-`open` saga.
    SagasFrontier {
        saga_id: String,
        saga_status: String,
        tasks: Vec<SagaFrontierTask>,
        brains: Vec<SagaBrainSummary>,
    },
    /// Reply to [`Request::SagasStart`].
    SagasStart { saga: SagaSummary },
    /// Reply to [`Request::SagasClose`]. `cascade` echoes the request
    /// flag so the caller can render output identical to the local
    /// CLI's JSON path; `cascade_results` lists the per-task outcome
    /// for every member touched.
    SagasClose {
        saga: SagaSummary,
        cascade: bool,
        cascade_results: Vec<SagaCascadeResult>,
    },
    /// Reply to [`Request::SagasCancel`]. Same shape as
    /// [`Response::SagasClose`].
    SagasCancel {
        saga: SagaSummary,
        cascade: bool,
        cascade_results: Vec<SagaCascadeResult>,
    },
    /// Reply to [`Request::SagasReopen`].
    SagasReopen { saga: SagaSummary },
    /// Reply to [`Request::SagasStats`].
    SagasStats {
        saga_id: String,
        stats: SagaStatsReport,
        label_histogram: Vec<SagaLabelCount>,
        brains: Vec<SagaBrainSummary>,
    },
    /// Reply to [`Request::MemoryWriteEpisode`].
    MemoryWriteEpisode { summary_id: String, uri: String },
    /// Reply to [`Request::MemoryWriteProcedure`].
    MemoryWriteProcedure { summary_id: String, uri: String },
    /// Reply to [`Request::MemoryRetrieve`]. `result_json` carries the
    /// serialized retrieve response from the MCP tool dispatcher — the
    /// CLI renders it using the same logic as the local path.
    MemoryRetrieve { result_json: String },
    /// Reply to [`Request::MemoryConsolidate`]. `result_json` carries the
    /// serialized consolidation report.
    MemoryConsolidate { result_json: String },
    /// Reply to [`Request::MemorySummarizeScope`]. `result_json` carries
    /// the serialized scope summary.
    MemorySummarizeScope { result_json: String },
    /// Reply to [`Request::MemoryReflect`]. `result_json` carries the
    /// serialized reflect report.
    MemoryReflect { result_json: String },

    // ── tags ────────────────────────────────────────────────────────────
    /// Reply to [`Request::TagsAliasesList`].
    TagsAliasesList { rows: Vec<TagAliasSummary> },
    /// Reply to [`Request::TagsAliasesStatus`].
    TagsAliasesStatus { report: TagAliasesStatusReport },

    // ── jobs ────────────────────────────────────────────────────────────
    /// Reply to [`Request::JobsStatus`].
    JobsStatus { report: JobsStatusReport },

    // ── status ──────────────────────────────────────────────────────────
    /// Reply to [`Request::BrainStatus`].
    BrainStatus { report: BrainStatusReport },

    // ── provider ────────────────────────────────────────────────────────
    /// Reply to [`Request::ProviderList`].
    ProviderList { providers: Vec<ProviderSummary> },

    // ── watch ────────────────────────────────────────────────────────────
    /// Reply to [`Request::WatchAdd`].
    WatchAdded { path: String, brain_name: String },
    /// Reply to [`Request::WatchRemove`].
    WatchRemoved { path: String },
    /// Reply to [`Request::WatchList`].
    WatchList { watches: Vec<WatchSummary> },

    // ── links ────────────────────────────────────────────────────────────
    /// Reply to [`Request::LinksAdd`].
    LinksAdd { created: bool },
    /// Reply to [`Request::LinksRemove`].
    LinksRemove { removed: bool },
    /// Reply to [`Request::LinksForEntity`].
    LinksForEntity { links: Vec<WireLinkSummary> },

    // ── records (mutations) ─────────────────────────────────────────────
    /// Reply to [`Request::RecordsArchive`].
    RecordsArchive {
        record_id: String,
        uri: String,
        status: String,
    },
    /// Reply to [`Request::RecordsLinkAdd`].
    RecordsLinkAdd { created: bool },
    /// Reply to [`Request::RecordsLinkRemove`].
    RecordsLinkRemove { removed: bool },
    /// Reply to [`Request::RecordsSearch`]. The report mirrors the legacy
    /// `records.search` JSON envelope byte-for-byte (modulo serde
    /// rename: `summary_2sent` → `summary`).
    RecordsSearch { report: RecordsSearchReport },
    /// Reply to [`Request::RecordsFetchContent`]. Exactly one of
    /// [`RecordContent::text`] / [`RecordContent::data_base64`] is
    /// populated, per the `encoding` discriminator.
    RecordsFetchContent { content: RecordContent },
    /// Reply to [`Request::RecordsTagAdd`].
    RecordsTagAdd { tag: String },
    /// Reply to [`Request::RecordsTagRemove`].
    RecordsTagRemove { removed: bool },

    // ── tasks (batch + opaque event) ────────────────────────────────────
    /// Reply to [`Request::TasksApplyEvent`]. The `result_json` mirrors
    /// the MCP tool's JSON output.
    TasksApplyEvent { result_json: String },
    /// Reply to [`Request::TasksDepsBatch`]. Opaque JSON output (variable
    /// per action).
    TasksDepsBatch { result_json: String },
    /// Reply to [`Request::TasksLabelsBatch`]. Opaque JSON output.
    TasksLabelsBatch { result_json: String },
    /// Reply to [`Request::TasksLabelsSummary`].
    TasksLabelsSummary { labels: Vec<WireTaskLabelSummary> },

    // ── memory (DAG walk) ───────────────────────────────────────────────
    /// Reply to [`Request::MemoryWalkThread`]. Opaque JSON mirrors the
    /// MCP tool surface.
    MemoryWalkThread { result_json: String },

    // ── tags (recluster) ────────────────────────────────────────────────
    /// Reply to [`Request::TagsRecluster`]. Opaque JSON output.
    TagsRecluster { result_json: String },

    // ── brains (enumeration) ────────────────────────────────────────────
    /// Reply to [`Request::BrainsList`].
    BrainsList {
        brains: Vec<WireBrainSummary>,
        count: u32,
    },
}

/// Wire-format summary of a single task.
///
/// Minimal field set — just what `brain tasks list` renders by default.
/// Future wire types (TaskDetail, TaskWithComments, …) live alongside
/// rather than extending this one; small types compose better than
/// god-objects on the wire.
///
/// Mirrors but does not re-use `brain_tasks::Task` — see module rustdoc
/// for the anti-corruption-layer rationale.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TaskSummary {
    /// Display ID (e.g. "brn-2fe.27"). Stable user-visible identifier.
    pub task_id: String,
    /// Task title.
    pub title: String,
    /// Status as a string ("open", "in_progress", "blocked", "done",
    /// "cancelled"). Stringly-typed on the wire so adding a new status
    /// variant on the server doesn't break older clients catastrophically
    /// — they just see an unrecognized value.
    pub status: String,
    /// Priority: 0=critical, 1=high, 2=medium, 3=low, 4=backlog.
    pub priority: u8,
    /// Brain identifier the task belongs to ("" for unscoped).
    pub brain_id: String,
}

/// Wire-format integrity report returned by [`Response::RecordsVerify`].
///
/// Mirrors the JSON output produced by the local `brain records verify
/// --json` code path: counts of each finding category plus the totals.
/// Detailed per-record findings are not surfaced on the wire today —
/// the verbose CLI rendering is a local-only feature.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RecordsVerifyReport {
    /// `true` iff every count below is zero.
    pub clean: bool,
    /// Total records inspected during the verification pass.
    pub records_checked: u64,
    /// Total blobs inspected during the verification pass.
    pub blobs_checked: u64,
    /// Number of records whose referenced blob is missing from the
    /// object store.
    pub missing: u64,
    /// Number of blobs whose stored bytes do not match the expected
    /// BLAKE3 hash.
    pub corrupt: u64,
    /// Number of blobs in the object store not referenced by any
    /// record.
    pub orphans: u64,
    /// Number of records flagged `payload_available=false` whose blob
    /// nonetheless still exists on disk.
    pub stale_flags: u64,
}

/// Wire-format summary of an analysis record.
///
/// Mirrors but does not re-use `brain_records::Record` — see module
/// rustdoc for the anti-corruption-layer rationale.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AnalysisSummary {
    /// Stable record ID (e.g. "BRN-01J…"). User-visible identifier.
    pub record_id: String,
    /// Record title.
    pub title: String,
    /// ISO 8601 / RFC 3339 timestamp when the record was created.
    pub created_at: String,
    /// Brain identifier the record belongs to ("" for unscoped).
    pub brain_id: String,
}

/// Wire-format summary of an artifact record.
///
/// Artifacts are the cross-kind read view over all record kinds, so
/// the summary surfaces `kind` and `status` alongside the common
/// identity fields.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactSummary {
    pub record_id: String,
    pub title: String,
    /// Record kind string ("document", "analysis", "plan", "snapshot",
    /// or any custom kind). Stringly-typed on the wire so adding a new
    /// kind server-side does not break older clients.
    pub kind: String,
    /// Lifecycle status string ("active", "archived", or any forward-
    /// compatible value).
    pub status: String,
    pub created_at: String,
    pub brain_id: String,
}

/// Wire-format summary of a document record.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DocumentSummary {
    pub record_id: String,
    pub title: String,
    pub created_at: String,
    pub brain_id: String,
}

/// Wire-format summary of a plan record.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PlanSummary {
    pub record_id: String,
    pub title: String,
    pub created_at: String,
    pub brain_id: String,
}

/// Wire-format summary of a snapshot record.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSummary {
    pub record_id: String,
    pub title: String,
    pub created_at: String,
    pub brain_id: String,
}

/// Wire-format summary of a brain referenced by a saga.
///
/// Mirrors `brain_sagas::BrainSummary` field-for-field.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SagaBrainSummary {
    pub brain_id: String,
    pub name: String,
    pub prefix: Option<String>,
}

/// Wire-format member task stub for [`SagaSummary::members`].
/// Mirrors `brain_sagas::SagaMember` field-for-field.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SagaMember {
    pub task_id: String,
    pub brain_id: String,
    pub title: String,
    pub status: String,
    pub task_type: String,
}

/// Wire-format summary of a saga.
///
/// Mirrors but does not re-use `brain_sagas::Saga` — see module
/// rustdoc for the anti-corruption-layer rationale. `saga_id` is the
/// user-facing short form (`saga-<hex>`) the CLI displays; the
/// canonical 26-char ULID stays inside the daemon.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SagaSummary {
    /// Short user-facing form (`saga-<hex>`).
    pub saga_id: String,
    pub title: String,
    pub description: Option<String>,
    /// Lifecycle status string ("planning", "open", "closed",
    /// "cancelled"). Stringly-typed on the wire so adding a new status
    /// server-side does not break older clients catastrophically.
    pub status: String,
    /// RFC 3339 / ISO 8601 timestamp.
    pub created_at: String,
    /// RFC 3339 / ISO 8601 timestamp.
    pub updated_at: String,
    /// RFC 3339 / ISO 8601 timestamp, or `None` when the saga has
    /// never been closed/cancelled (or was subsequently reopened).
    pub closed_at: Option<String>,
    /// Current member task stubs (task_id, brain_id, title, status, task_type)
    /// in `added_at` order. Empty for planning/closed/cancelled sagas.
    pub members: Vec<SagaMember>,
    /// Brains that have at least one live member task in this saga.
    /// Empty for planning/closed/cancelled sagas.
    pub brains: Vec<SagaBrainSummary>,
}

/// Wire-format member of [`Response::SagasFrontier`] — one ready
/// actionable task in a saga.
///
/// Mirrors the CLI's `frontier --json` shape: short task ID, title,
/// status string, integer priority, task-type string. The `task_type`
/// is stringly-typed for the same forward-compatibility reason as
/// [`TaskSummary::status`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SagaFrontierTask {
    /// Short compact task ID (e.g. "brn-2fe.27" or the raw canonical
    /// when no display alias exists).
    pub task_id: String,
    pub title: String,
    pub status: String,
    pub priority: i32,
    pub task_type: String,
}

/// Wire-format per-task outcome of `close --cascade` / `cancel --cascade`.
///
/// Mirrors the CLI's `cascade_results` JSON shape: exactly one of the
/// boolean flags is `true`, with the optional `reason` / `error`
/// populated for `skipped` / `failed`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SagaCascadeOutcome {
    /// Task transitioned to `done` (close-cascade success).
    Closed,
    /// Task transitioned to `cancelled` (cancel-cascade success).
    Cancelled,
    /// Task was already terminal — left untouched.
    Skipped { reason: String },
    /// Task event append failed; saga's own state still committed.
    Failed { error: String },
}

/// Wire-format wrapper pairing a member task ID with its cascade
/// outcome.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SagaCascadeResult {
    /// Compact task ID (e.g. "brn-2fe.27").
    pub task_id: String,
    pub outcome: SagaCascadeOutcome,
}

/// Wire-format aggregate counts for [`Response::SagasStats`].
///
/// Mirrors `brain_sagas::SagaStatsCounts`. `completion_pct` is a
/// 0-100 percentage; `None` when the live-task count is zero.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SagaStatsReport {
    pub total: i64,
    pub open: i64,
    pub in_progress: i64,
    pub blocked: i64,
    pub done: i64,
    pub cancelled: i64,
    pub orphan: i64,
    pub completion_pct: Option<f64>,
}

// `f64` does not implement `Eq` in general (NaN != NaN), but wire-format
// values deserialized from JSON are never NaN, so asserting `Eq` here is
// safe. The manual impl lets `Response`, which derives `Eq`, include
// `SagasStats` without a compilation error.
impl Eq for SagaStatsReport {}

/// Wire-format `(label, count)` pair for the saga label-histogram
/// surface.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SagaLabelCount {
    pub label: String,
    pub count: i64,
}

// ── Memory param types ───────────────────────────────────────────────────────

/// Wire-format params for [`Request::MemoryWriteEpisode`].
///
/// `importance_millis` encodes the 0.0–1.0 importance as an integer in the
/// range 0–1000 so the struct satisfies [`Eq`] (required by the [`Request`]
/// enum derive). The CLI converts via `(importance * 1000.0) as u32`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MemoryWriteEpisodeParams {
    pub goal: String,
    pub actions: String,
    pub outcome: String,
    pub tags: Vec<String>,
    /// Importance scaled to 0–1000 (millis). Divide by 1000.0 to recover
    /// the original float.
    pub importance_millis: u32,
    /// Optional `summary_id` of a prior episode this one continues.
    /// Daemon validates predecessor existence pre-write and rejects
    /// the write if the predecessor cannot be resolved — preserves
    /// the legacy MCP semantics that a missing predecessor aborts
    /// the episode write rather than persisting the episode then
    /// reporting a broken link.
    #[serde(default)]
    pub continues: Option<String>,
}

/// Wire-format params for [`Request::MemoryWriteProcedure`].
///
/// See [`MemoryWriteEpisodeParams`] for the `importance_millis` convention.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MemoryWriteProcedureParams {
    pub title: String,
    pub steps: String,
    pub tags: Vec<String>,
    /// Importance scaled to 0–1000 (millis). Divide by 1000.0 to recover
    /// the original float.
    pub importance_millis: u32,
}

/// Wire-format params for [`Request::MemoryRetrieve`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct MemoryRetrieveParams {
    pub query: Option<String>,
    pub uri: Option<String>,
    pub lod: String,
    pub count: u64,
    pub strategy: String,
    pub brains: Vec<String>,
    pub time_scope: Option<String>,
    pub time_after: Option<i64>,
    pub time_before: Option<i64>,
    pub tags: Vec<String>,
    pub tags_require: Vec<String>,
    pub tags_exclude: Vec<String>,
    pub kinds: Vec<String>,
    pub explain: bool,
}

/// Wire-format params for [`Request::MemoryConsolidate`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MemoryConsolidateParams {
    pub limit: usize,
    pub gap_seconds: i64,
    pub auto_summarize: bool,
    /// Optionally scope consolidation to a specific brain. Defaults to the
    /// daemon's own brain when `None`.
    #[serde(default)]
    pub brain_id: Option<String>,
}

/// Wire-format params for [`Request::MemorySummarizeScope`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MemorySummarizeScopeParams {
    pub scope_type: String,
    pub scope_value: String,
    pub regenerate: bool,
    pub async_llm: bool,
}

/// Wire-format params for [`Request::MemoryReflect`].
///
/// See [`MemoryWriteEpisodeParams`] for the `importance_millis` convention.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct MemoryReflectParams {
    pub commit: bool,
    // prepare fields
    pub topic: Option<String>,
    pub budget: usize,
    pub brains: Vec<String>,
    // commit fields
    pub title: Option<String>,
    pub content: Option<String>,
    pub source_ids: Vec<String>,
    pub tags: Vec<String>,
    /// Importance scaled to 0–1000 (millis), or `None` to use the default.
    pub importance_millis: Option<u32>,
}

// ── tags wire types ──────────────────────────────────────────────────────────

/// Filter / pagination params for [`Request::TagsAliasesList`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct TagsAliasesListParams {
    /// Filter to rows whose `canonical_tag` equals this value.
    pub canonical: Option<String>,
    /// Filter to rows in the given `cluster_id`.
    pub cluster_id: Option<String>,
    /// Maximum rows to return.
    pub limit: i64,
    /// Row offset for pagination.
    pub offset: i64,
}

/// One row from the `tag_aliases` table, returned by
/// [`Response::TagsAliasesList`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TagAliasSummary {
    pub raw_tag: String,
    pub canonical_tag: String,
    pub cluster_id: String,
    pub updated_at: String,
}

/// Clustering health summary returned by [`Response::TagsAliasesStatus`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TagAliasesStatusReport {
    /// Total alias rows (raw_count from the store).
    pub total_aliases: u64,
    /// Number of distinct clusters.
    pub total_clusters: u64,
    /// Number of distinct canonical tags.
    pub canonical_count: u64,
    /// `run_id` of the most recent clustering run, or `None` if never run.
    pub last_run_id: Option<String>,
    /// ISO 8601 timestamp of the last run's `started_at`, or `None`.
    pub last_run_started_at: Option<String>,
    /// Embedder version stamp from the last run, or `None`.
    pub last_run_embedder_version: Option<String>,
}

// ── jobs wire types ───────────────────────────────────────────────────────────

/// Job queue health summary returned by [`Response::JobsStatus`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobsStatusReport {
    pub pending: u64,
    pub running: u64,
    pub ready: u64,
    pub done: u64,
    pub failed: u64,
    /// Resolved listing-status used when populating `recent_failures`.
    /// Always the canonical lowercase form (`pending` / `ready` /
    /// `in_progress` / `done` / `failed`) — the daemon parses the
    /// caller's filter through [`brain_persistence::db::job::JobStatus`]
    /// and echoes the canonical name so MCP/CLI clients can render
    /// `filters.status` without reproducing the parser. Defaults to
    /// `"failed"` when the caller omits the filter.
    pub listing_status: String,
    /// Up to 10 most recently failed jobs.
    pub recent_failures: Vec<JobSummary>,
    /// Jobs that appear stuck (InProgress beyond timeout).
    pub stuck_jobs: Vec<JobSummary>,
}

/// Wire summary of a single job row.
///
/// `status` is the lowercase string form of [`brain_persistence::db::job::JobStatus`]
/// (`pending` / `ready` / `in_progress` / `done` / `failed`); the daemon
/// converts via `as_ref()` at the wire boundary.
///
/// `started_at` is an RFC 3339 / ISO 8601 string (never a raw epoch
/// integer) and is `None` when the job has not yet been picked up by a
/// worker.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobSummary {
    pub job_id: String,
    pub kind: String,
    pub ref_id: String,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub status: String,
    pub started_at: Option<String>,
    pub updated_at: String,
}

/// Filter parameters for [`Request::JobsStatus`].
///
/// All three fields are server-side filters: the daemon owns the
/// `list_jobs_by_status` + post-fetch `kind` retain loop so MCP and
/// CLI clients become thin wire-echo bodies. `limit` defaults to 10
/// when constructed via [`JobsStatusParams::default`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobsStatusParams {
    /// Filter recent-failures + stuck-jobs lists to a single job kind
    /// (e.g. `"summarize_scope"`). `None` keeps both lists unfiltered.
    #[serde(default)]
    pub kind: Option<String>,
    /// Job status whose list of recent rows is returned (lowercase form
    /// of [`brain_persistence::db::job::JobStatus`]). When `None` the
    /// daemon defaults to `failed` — preserving the legacy MCP default.
    #[serde(default)]
    pub status: Option<String>,
    /// Cap on the number of recent rows returned. Defaults to 10 when
    /// the wire omits the field.
    #[serde(default = "jobs_status_default_limit")]
    pub limit: u64,
}

fn jobs_status_default_limit() -> u64 {
    10
}

impl Default for JobsStatusParams {
    fn default() -> Self {
        Self {
            kind: None,
            status: None,
            limit: 10,
        }
    }
}

// ── status wire types ─────────────────────────────────────────────────────────

/// Latency histogram snapshot returned inside [`MetricsSnapshot`].
///
/// Field names are byte-stable with the legacy `status` MCP envelope
/// (`p50_us` / `p95_us` / `total_samples`) — clients depend on this
/// shape.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct LatencyHistogram {
    pub p50_us: u64,
    pub p95_us: u64,
    pub total_samples: u64,
}

/// Runtime metrics snapshot carried inside [`BrainStatusReport`].
///
/// Mirrors but does NOT re-use `brain_core::metrics::MetricsSnapshot`
/// — see module rustdoc for the anti-corruption-layer rationale. The
/// daemon maps internal → wire field-by-field at the dispatcher
/// boundary so a future field on the internal snapshot doesn't
/// silently appear on the wire.
///
/// `dual_store_stuck_files` and `stale_hashes_prevented` are NOT
/// carried here; they already live on [`BrainStatusReport`] (since
/// they predate the metrics extension) and the MCP `status` tool
/// reads them off the report directly to assemble the legacy envelope.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct MetricsSnapshot {
    pub uptime_seconds: u64,
    pub indexing_latency: LatencyHistogram,
    pub query_latency: LatencyHistogram,
    pub queue_depth: u64,
    pub lancedb_unoptimized_rows: u64,
    pub lancedb_optimize_failures: u64,
    pub indexing_errors: u64,
    pub query_errors: u64,
}

/// Brain health status returned by [`Response::BrainStatus`].
///
/// `metrics` was added when the legacy `status` MCP tool moved into
/// `brain_mcp`; the field is populated unconditionally because the
/// snapshot is cheap (8 atomic loads + 2 percentile sweeps). Older
/// CLI consumers that destructure named fields ignore it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BrainStatusReport {
    pub brain_name: String,
    pub brain_id: String,
    pub tasks_open: u64,
    pub tasks_in_progress: u64,
    pub tasks_blocked: u64,
    pub tasks_done: u64,
    pub stuck_files: u64,
    pub stale_hashes_prevented: u64,
    pub metrics: MetricsSnapshot,
}

// ── provider wire types ───────────────────────────────────────────────────────

/// Wire summary of a configured provider, returned by
/// [`Response::ProviderList`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProviderSummary {
    pub id: String,
    pub name: String,
    /// First 8 characters of the key hash (masked display).
    pub key_hash_prefix: String,
}

/// Wire-format summary of a single filesystem watch registration.
///
/// Mirrors but does not re-use any internal watcher state — see module
/// rustdoc for the anti-corruption-layer rationale.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WatchSummary {
    /// Human-readable brain name.
    pub brain_name: String,
    /// Stable brain identifier.
    pub brain_id: String,
    /// Filesystem path being watched.
    pub note_dir: String,
    /// Whether the watch is currently active.
    pub watching: bool,
}

/// Structured wire-format error.
///
/// Every variant carries plain primitives — strings and numbers only. No
/// `Box<dyn Error>` source chains, no `io::Error`, no `anyhow::Error`. This
/// is load-bearing: a non-serializable field would silently break
/// round-tripping and force every caller to handle opaque internals. The
/// trade-off is that the original error source is dropped on the wire; the
/// daemon is expected to log full source chains locally before stringifying.
///
/// All variants are struct-shaped (not newtype) so they round-trip cleanly
/// under serde's internally-tagged representation — newtype variants wrapping
/// a primitive cannot be flattened into a `{"kind": "...", "...": ...}`
/// object.
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RpcError {
    /// Underlying transport (socket / I/O) failure. The message is a
    /// human-readable description.
    #[error("transport: {message}")]
    Transport { message: String },

    /// Protocol-level failure: framing error, serde decode failure, or an
    /// unexpected response shape (e.g. Pong arriving where HandshakeOk was
    /// expected).
    #[error("protocol: {message}")]
    Protocol { message: String },

    /// Handshake version mismatch — client and daemon disagree on
    /// [`PROTOCOL_VERSION`]. Restart the older side.
    #[error("version mismatch: client={client}, server={server}")]
    VersionMismatch { client: u32, server: u32 },

    /// The requested entity (task, record, brain, etc.) was not found
    /// server-side. `id` is a human-readable identifier hint.
    #[error("not found: {id}")]
    NotFound { id: String },

    /// Server-side failure not covered by the more specific variants.
    #[error("{message}")]
    Unknown { message: String },
}

// ── Wire types and params for link / record-mutation / task-batch /
// walk / recluster / brains variants ───────────────────────────────────

/// Polymorphic entity reference shared by `Links*` and `Records*` link
/// variants. `kind` is the snake-case discriminant string (`"task"`,
/// `"record"`, `"episode"`, `"procedure"`, `"chunk"`, `"note"`)
/// matching the brain-side `EntityKind` enum.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WireEntityRef {
    pub kind: String,
    pub id: String,
}

/// Wire-format link summary returned from [`Response::LinksForEntity`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WireLinkSummary {
    pub from: WireEntityRef,
    pub to: WireEntityRef,
    pub edge_kind: String,
    /// RFC 3339 UTC timestamp. `None` when the underlying read API
    /// doesn't surface a timestamp for this edge — preferred over
    /// emitting an empty-string `created_at` that would violate the
    /// RFC 3339 contract per `feedback_iso_timestamps_on_wire`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

/// Wire-format brain summary returned from [`Response::BrainsList`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WireBrainSummary {
    pub name: String,
    pub id: Option<String>,
    pub root: String,
    pub aliases: Vec<String>,
    pub extra_roots: Vec<String>,
    pub prefix: Option<String>,
    pub archived: bool,
}

/// Wire-format per-label histogram entry returned from
/// [`Response::TasksLabelsSummary`]. `task_ids` carries the short
/// prefixes used by the MCP surface; the daemon resolves canonical
/// IDs at the boundary.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WireTaskLabelSummary {
    pub label: String,
    pub count: u32,
    pub task_ids: Vec<String>,
}

/// Wire-format params for [`Request::LinksAdd`]. `edge_kind` is one of
/// `parent_of`, `blocks`, `covers`, `relates_to`, `see_also`,
/// `supersedes`, `contradicts`, `continues` — DAG-validated kinds
/// reject cycles dispatcher-side.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinksAddParams {
    pub from: WireEntityRef,
    pub to: WireEntityRef,
    pub edge_kind: String,
}

/// Wire-format params for [`Request::LinksRemove`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinksRemoveParams {
    pub from: WireEntityRef,
    pub to: WireEntityRef,
    pub edge_kind: String,
}

/// Wire-format params for [`Request::LinksForEntity`]. `direction` is
/// one of `incoming`, `outgoing`, or `both`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinksForEntityParams {
    pub entity: WireEntityRef,
    pub direction: String,
    pub limit: Option<u32>,
}

/// Wire-format params for [`Request::RecordsArchive`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RecordsArchiveParams {
    pub record_id: String,
    pub reason: Option<String>,
}

/// Wire-format params shared by [`Request::RecordsLinkAdd`] and
/// [`Request::RecordsLinkRemove`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RecordsLinkParams {
    pub record_id: String,
    pub target: WireEntityRef,
    pub link_kind: String,
}

/// Wire-format params for [`Request::RecordsSearch`]. Defaults match
/// the legacy MCP tool (`k = 10`, `budget_tokens = 800`, no tag /
/// brain filter).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RecordsSearchParams {
    pub query: String,
    #[serde(default = "records_search_default_k")]
    pub k: u64,
    #[serde(default = "records_search_default_budget_tokens")]
    pub budget_tokens: u64,
    /// Tag filter applied to the hybrid retrieval pipeline. AND across
    /// all entries (case-insensitive). Empty = no filter.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Brain scope. Empty = current brain only. `["all"]` expands to
    /// every registered brain in the federation; named entries are
    /// matched by brain name or id.
    #[serde(default)]
    pub brains: Vec<String>,
}

fn records_search_default_k() -> u64 {
    10
}

fn records_search_default_budget_tokens() -> u64 {
    800
}

impl Default for RecordsSearchParams {
    fn default() -> Self {
        Self {
            query: String::new(),
            k: 10,
            budget_tokens: 800,
            tags: Vec::new(),
            brains: Vec::new(),
        }
    }
}

/// A single ranked record hit on the wire.
///
/// `score` is the hybrid retrieval score from `brain_retrieval`. It is
/// `f64` (not `u64`) for byte-shape parity with the legacy MCP envelope,
/// which is the only reason [`Response`] now derives `PartialEq` and not
/// `Eq` — `f64` cannot be `Eq` (NaN).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct WireRecordHit {
    pub record_id: String,
    pub memory_id: String,
    pub title: String,
    pub summary: String,
    pub score: f64,
    pub kind: String,
    pub uri: String,
    /// Federated-search attribution. `None` for single-brain queries —
    /// callers can fall back to the current brain name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brain_name: Option<String>,
}

/// Records-search report returned by [`Response::RecordsSearch`].
///
/// Mirrors the legacy `records.search` JSON envelope:
/// `{budget_tokens, used_tokens_est, result_count, total_available, results}`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RecordsSearchReport {
    pub budget_tokens: u64,
    pub used_tokens_est: u64,
    pub result_count: u64,
    pub total_available: u64,
    pub results: Vec<WireRecordHit>,
}

/// Wire-format params for [`Request::RecordsFetchContent`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RecordsFetchContentParams {
    pub record_id: String,
    /// Optional target brain (name or id). When `None`, fetches from
    /// the current brain.
    #[serde(default)]
    pub brain: Option<String>,
}

/// Wire-format record content returned by
/// [`Response::RecordsFetchContent`].
///
/// `encoding` is one of `"utf-8"` or `"base64"`. Exactly one of
/// [`RecordContent::text`] (utf-8 case) / [`RecordContent::data_base64`]
/// (binary case) is populated — the daemon decodes text-like
/// `media_type` values up-front so clients do not duplicate the
/// detection rule.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct RecordContent {
    pub record_id: String,
    pub title: String,
    pub kind: String,
    pub content_hash: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub encoding: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Base64-encoded raw bytes (set when `encoding == "base64"`).
    /// Renamed to `"data"` on the wire to preserve byte-shape parity
    /// with the legacy `records.fetch_content` MCP envelope.
    #[serde(default, rename = "data", skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,
    pub uri: String,
    /// Echoes the resolved remote-brain name when the request carried a
    /// non-`None` `brain`; `None` for local fetches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brain: Option<String>,
}

impl<'de> serde::Deserialize<'de> for RecordContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            RecordId,
            Title,
            Kind,
            ContentHash,
            Size,
            MediaType,
            Encoding,
            Text,
            #[serde(rename = "data")]
            Data,
            Uri,
            Brain,
        }

        struct RecordContentVisitor;

        impl<'de> Visitor<'de> for RecordContentVisitor {
            type Value = RecordContent;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct RecordContent")
            }

            fn visit_map<V>(self, mut map: V) -> Result<RecordContent, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut record_id = None;
                let mut title = None;
                let mut kind = None;
                let mut content_hash = None;
                let mut size = None;
                let mut media_type = None;
                let mut encoding = None;
                let mut text = None;
                let mut data_base64 = None;
                let mut uri = None;
                let mut brain = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::RecordId => {
                            if record_id.is_some() {
                                return Err(de::Error::duplicate_field("record_id"));
                            }
                            record_id = Some(map.next_value()?);
                        }
                        Field::Title => {
                            if title.is_some() {
                                return Err(de::Error::duplicate_field("title"));
                            }
                            title = Some(map.next_value()?);
                        }
                        Field::Kind => {
                            if kind.is_some() {
                                return Err(de::Error::duplicate_field("kind"));
                            }
                            kind = Some(map.next_value()?);
                        }
                        Field::ContentHash => {
                            if content_hash.is_some() {
                                return Err(de::Error::duplicate_field("content_hash"));
                            }
                            content_hash = Some(map.next_value()?);
                        }
                        Field::Size => {
                            if size.is_some() {
                                return Err(de::Error::duplicate_field("size"));
                            }
                            size = Some(map.next_value()?);
                        }
                        Field::MediaType => {
                            if media_type.is_some() {
                                return Err(de::Error::duplicate_field("media_type"));
                            }
                            media_type = map.next_value::<Option<String>>()?;
                        }
                        Field::Encoding => {
                            if encoding.is_some() {
                                return Err(de::Error::duplicate_field("encoding"));
                            }
                            encoding = Some(map.next_value::<String>()?);
                        }
                        Field::Text => {
                            if text.is_some() {
                                return Err(de::Error::duplicate_field("text"));
                            }
                            text = map.next_value::<Option<String>>()?;
                        }
                        Field::Data => {
                            if data_base64.is_some() {
                                return Err(de::Error::duplicate_field("data"));
                            }
                            data_base64 = map.next_value::<Option<String>>()?;
                        }
                        Field::Uri => {
                            if uri.is_some() {
                                return Err(de::Error::duplicate_field("uri"));
                            }
                            uri = map.next_value::<Option<String>>()?;
                        }
                        Field::Brain => {
                            if brain.is_some() {
                                return Err(de::Error::duplicate_field("brain"));
                            }
                            brain = map.next_value::<Option<String>>()?;
                        }
                    }
                }

                let record_id = record_id.ok_or_else(|| de::Error::missing_field("record_id"))?;
                let title = title.ok_or_else(|| de::Error::missing_field("title"))?;
                let kind = kind.ok_or_else(|| de::Error::missing_field("kind"))?;
                let content_hash =
                    content_hash.ok_or_else(|| de::Error::missing_field("content_hash"))?;
                let size = size.ok_or_else(|| de::Error::missing_field("size"))?;
                let encoding = encoding.ok_or_else(|| de::Error::missing_field("encoding"))?;
                let uri = uri.ok_or_else(|| de::Error::missing_field("uri"))?;

                // Enforce XOR invariant: exactly one of text or data_base64 must be Some
                // and must match the declared encoding
                match (encoding.as_str(), &text, &data_base64) {
                    ("utf-8", None, _) => {
                        return Err(de::Error::custom(
                            "encoding 'utf-8' requires 'text' payload",
                        ));
                    }
                    ("utf-8", Some(_), Some(_)) => {
                        return Err(de::Error::custom(
                            "RecordContent must have exactly one of 'text' or 'data' (both provided)",
                        ));
                    }
                    ("base64", _, None) => {
                        return Err(de::Error::custom(
                            "encoding 'base64' requires 'data' payload",
                        ));
                    }
                    ("base64", Some(_), Some(_)) => {
                        return Err(de::Error::custom(
                            "RecordContent must have exactly one of 'text' or 'data' (both provided)",
                        ));
                    }
                    ("utf-8", Some(_), None) | ("base64", None, Some(_)) => {
                        // Valid combinations
                    }
                    (enc, None, None) => {
                        return Err(de::Error::custom(format!(
                            "RecordContent with encoding '{}' must have exactly one of 'text' or 'data' (neither provided)",
                            enc
                        )));
                    }
                    (enc, _, _) => {
                        return Err(de::Error::custom(format!(
                            "unknown encoding '{}' (expected 'utf-8' or 'base64')",
                            enc
                        )));
                    }
                }

                Ok(RecordContent {
                    record_id,
                    title,
                    kind,
                    content_hash,
                    size,
                    media_type,
                    encoding,
                    text,
                    data_base64,
                    uri,
                    brain,
                })
            }
        }

        const FIELDS: &[&str] = &[
            "record_id",
            "title",
            "kind",
            "content_hash",
            "size",
            "media_type",
            "encoding",
            "text",
            "data",
            "uri",
            "brain",
        ];
        deserializer.deserialize_struct("RecordContent", FIELDS, RecordContentVisitor)
    }
}

/// Wire-format params for [`Request::TasksApplyEvent`]. The body is
/// kept opaque (`serde_json::Value`) so the multi-variant event-type
/// union on the MCP surface does not need to be mirrored in the wire
/// layer. The dispatcher parses and dispatches; brain_rpc just
/// transports the raw event JSON.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TasksApplyEventParams {
    pub event_json: serde_json::Value,
}

/// Wire-format params for [`Request::TasksDepsBatch`]. Opaque JSON to
/// avoid mirroring the MCP tool's 5-action union (add / remove /
/// chain / fan / clear) in the protocol layer.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TasksDepsBatchParams {
    pub params_json: serde_json::Value,
}

/// Wire-format params for [`Request::TasksLabelsBatch`]. Opaque JSON
/// — same rationale as [`TasksDepsBatchParams`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TasksLabelsBatchParams {
    pub params_json: serde_json::Value,
}

/// Wire-format params for [`Request::MemoryWalkThread`]. Opaque JSON
/// mirrors the existing `MemoryRetrieve` pattern: the daemon owns the
/// shape, brain_rpc carries it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MemoryWalkThreadParams {
    pub params_json: serde_json::Value,
}

/// Wire-format params for [`Request::TagsRecluster`]. Opaque JSON.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TagsReclusterParams {
    pub params_json: serde_json::Value,
}

/// Wire-format params for [`Request::BrainsList`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct BrainsListParams {
    pub include_archived: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let bytes = serde_json::to_vec(value).expect("serialize");
        serde_json::from_slice(&bytes).expect("deserialize")
    }

    #[test]
    fn protocol_version_is_four() {
        // Bumped 3 → 4 when Request::RecordsSearch +
        // Request::RecordsFetchContent (and their Response twins) were
        // added so the daemon answers records.search +
        // records.fetch_content MCP tools directly. The handshake
        // check rejects rolling restarts that pair a pre-bump client
        // with a post-bump daemon.
        assert_eq!(PROTOCOL_VERSION, 4);
    }

    #[test]
    fn request_ping_roundtrips() {
        let req = Request::Ping;
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_handshake_roundtrips() {
        let req = Request::Handshake {
            version: PROTOCOL_VERSION,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_pong_roundtrips() {
        let res = Response::Pong;
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_handshake_ok_roundtrips() {
        let res = Response::HandshakeOk {
            server_version: PROTOCOL_VERSION,
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn rpc_error_version_mismatch_roundtrips() {
        let err = RpcError::VersionMismatch {
            client: 1,
            server: 2,
        };
        assert_eq!(roundtrip(&err), err);
    }

    #[test]
    fn rpc_error_transport_roundtrips() {
        let err = RpcError::Transport {
            message: "connection refused".into(),
        };
        assert_eq!(roundtrip(&err), err);
    }

    #[test]
    fn rpc_error_protocol_roundtrips() {
        let err = RpcError::Protocol {
            message: "unexpected response type".into(),
        };
        assert_eq!(roundtrip(&err), err);
    }

    #[test]
    fn rpc_error_not_found_roundtrips() {
        let err = RpcError::NotFound {
            id: "brn-2fe.99".into(),
        };
        assert_eq!(roundtrip(&err), err);
    }

    #[test]
    fn rpc_error_unknown_roundtrips() {
        let err = RpcError::Unknown {
            message: "daemon panicked".into(),
        };
        assert_eq!(roundtrip(&err), err);
    }

    #[test]
    fn rpc_error_implements_std_error() {
        // Compile-time assertion: RpcError satisfies the std::error::Error
        // trait. If thiserror ever stops generating this impl, the test fails
        // to compile rather than silently degrading the public API.
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&RpcError::Protocol {
            message: "test".into(),
        });
    }

    #[test]
    fn rpc_error_display_includes_payload() {
        let err = RpcError::VersionMismatch {
            client: 1,
            server: 7,
        };
        let display = format!("{err}");
        assert!(display.contains("client=1"));
        assert!(display.contains("server=7"));
    }

    #[test]
    fn request_wire_format_is_internally_tagged() {
        // Pin the JSON shape so downstream consumers (and clients in other
        // languages) can rely on it. A breaking shape change should fail this
        // test and force a PROTOCOL_VERSION bump.
        let req = Request::Handshake { version: 1 };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"handshake","version":1}"#);
    }

    #[test]
    fn response_wire_format_is_internally_tagged() {
        let res = Response::HandshakeOk { server_version: 1 };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(json, r#"{"type":"handshake_ok","server_version":1}"#);
    }

    #[test]
    fn request_tasks_list_roundtrips() {
        let req = Request::TasksList {
            params: TasksListParams {
                status: Some("open".into()),
                priority: Some(2),
                limit: Some(50),
                search: Some("daemon".into()),
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_list_with_default_params_roundtrips() {
        let req = Request::TasksList {
            params: TasksListParams::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_tasks_list_empty_roundtrips() {
        let res = Response::TasksList { tasks: Vec::new() };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_list_with_tasks_roundtrips() {
        let res = Response::TasksList {
            tasks: vec![
                TaskSummary {
                    task_id: "brn-2fe.27".into(),
                    title: "vertical slice".into(),
                    status: "in_progress".into(),
                    priority: 0,
                    brain_id: "eAx_dEFA".into(),
                },
                TaskSummary {
                    task_id: "brn-2fe.28".into(),
                    title: "final cleanup".into(),
                    status: "open".into(),
                    priority: 0,
                    brain_id: "eAx_dEFA".into(),
                },
            ],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_tasks_list_wire_format_is_stable() {
        // Pin the JSON shape — a future field reorder or rename forces
        // a PROTOCOL_VERSION bump.
        let req = Request::TasksList {
            params: TasksListParams {
                status: Some("open".into()),
                priority: None,
                limit: Some(10),
                search: None,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_list","params":{"status":"open","priority":null,"limit":10,"search":null}}"#
        );
    }

    #[test]
    fn response_tasks_list_wire_format_is_stable() {
        let res = Response::TasksList {
            tasks: vec![TaskSummary {
                task_id: "brn-2fe.27".into(),
                title: "vertical slice".into(),
                status: "in_progress".into(),
                priority: 0,
                brain_id: "eAx_dEFA".into(),
            }],
        };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_list","tasks":[{"task_id":"brn-2fe.27","title":"vertical slice","status":"in_progress","priority":0,"brain_id":"eAx_dEFA"}]}"#
        );
    }

    #[test]
    fn task_summary_roundtrips() {
        let task = TaskSummary {
            task_id: "brn-2fe.27".into(),
            title: "test".into(),
            status: "open".into(),
            priority: 0,
            brain_id: "eAx_dEFA".into(),
        };
        assert_eq!(roundtrip(&task), task);
    }

    #[test]
    fn task_summary_wire_format_is_stable() {
        // Pin the JSON shape so a future field reorder / rename forces a
        // PROTOCOL_VERSION bump (the wire contract is now load-bearing
        // for production clients).
        let task = TaskSummary {
            task_id: "brn-2fe.27".into(),
            title: "vertical slice".into(),
            status: "in_progress".into(),
            priority: 0,
            brain_id: "eAx_dEFA".into(),
        };
        let json = serde_json::to_string(&task).unwrap();
        assert_eq!(
            json,
            r#"{"task_id":"brn-2fe.27","title":"vertical slice","status":"in_progress","priority":0,"brain_id":"eAx_dEFA"}"#
        );
    }

    #[test]
    fn rpc_error_wire_format_is_internally_tagged() {
        let err = RpcError::VersionMismatch {
            client: 1,
            server: 2,
        };
        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(json, r#"{"kind":"version_mismatch","client":1,"server":2}"#);
    }

    // ── tasks_show ─────────────────────────────────────────────

    fn sample_summary() -> TaskSummary {
        TaskSummary {
            task_id: "brn-2fe.27".into(),
            title: "vertical slice".into(),
            status: "in_progress".into(),
            priority: 0,
            brain_id: "eAx_dEFA".into(),
        }
    }

    #[test]
    fn request_tasks_show_roundtrips() {
        let req = Request::TasksShow {
            id: "brn-2fe.27".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_show_wire_format_is_stable() {
        let req = Request::TasksShow {
            id: "brn-2fe.27".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"tasks_show","id":"brn-2fe.27"}"#);
    }

    #[test]
    fn response_tasks_show_some_roundtrips() {
        let res = Response::TasksShow {
            task: Some(sample_summary()),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_show_none_roundtrips() {
        let res = Response::TasksShow { task: None };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_show_wire_format_is_stable() {
        let res = Response::TasksShow { task: None };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(json, r#"{"type":"tasks_show","task":null}"#);
    }

    // ── tasks_next ─────────────────────────────────────────────

    #[test]
    fn request_tasks_next_roundtrips() {
        let req = Request::TasksNext;
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_next_wire_format_is_stable() {
        let req = Request::TasksNext;
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"tasks_next"}"#);
    }

    #[test]
    fn response_tasks_next_roundtrips() {
        let res = Response::TasksNext {
            task: Some(sample_summary()),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_next_none_wire_format_is_stable() {
        let res = Response::TasksNext { task: None };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(json, r#"{"type":"tasks_next","task":null}"#);
    }

    // ── tasks_create ───────────────────────────────────────────

    fn sample_create_params() -> TasksCreateParams {
        TasksCreateParams {
            title: "new task".into(),
            description: Some("body".into()),
            priority: 2,
            task_type: "task".into(),
            assignee: Some("alice".into()),
            parent: None,
        }
    }

    #[test]
    fn request_tasks_create_roundtrips() {
        let req = Request::TasksCreate {
            params: sample_create_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_create_wire_format_is_stable() {
        let req = Request::TasksCreate {
            params: TasksCreateParams {
                title: "t".into(),
                description: None,
                priority: 2,
                task_type: "task".into(),
                assignee: None,
                parent: None,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_create","params":{"title":"t","description":null,"priority":2,"task_type":"task","assignee":null,"parent":null}}"#
        );
    }

    #[test]
    fn response_tasks_create_roundtrips() {
        let res = Response::TasksCreate {
            task: sample_summary(),
            event_id: "01JABCDE".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_create_wire_format_is_stable() {
        let res = Response::TasksCreate {
            task: sample_summary(),
            event_id: "01JABCDE".into(),
        };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_create","task":{"task_id":"brn-2fe.27","title":"vertical slice","status":"in_progress","priority":0,"brain_id":"eAx_dEFA"},"event_id":"01JABCDE"}"#
        );
    }

    // ── tasks_update ───────────────────────────────────────────

    #[test]
    fn request_tasks_update_roundtrips() {
        let req = Request::TasksUpdate {
            params: TasksUpdateParams {
                id: "brn-2fe.27".into(),
                title: Some("renamed".into()),
                description: None,
                priority: Some(1),
                assignee: None,
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_update_wire_format_is_stable() {
        let req = Request::TasksUpdate {
            params: TasksUpdateParams {
                id: "brn-2fe.27".into(),
                title: None,
                description: None,
                priority: Some(1),
                assignee: None,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_update","params":{"id":"brn-2fe.27","title":null,"description":null,"priority":1,"assignee":null}}"#
        );
    }

    #[test]
    fn response_tasks_update_roundtrips() {
        let res = Response::TasksUpdate {
            task: sample_summary(),
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── tasks_mutate ───────────────────────────────────────────

    #[test]
    fn request_tasks_mutate_roundtrips() {
        let req = Request::TasksMutate {
            params: TasksMutateParams {
                id: "brn-2fe.27".into(),
                action: "close".into(),
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_mutate_wire_format_is_stable() {
        let req = Request::TasksMutate {
            params: TasksMutateParams {
                id: "brn-2fe.27".into(),
                action: "in_progress".into(),
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_mutate","params":{"id":"brn-2fe.27","action":"in_progress"}}"#
        );
    }

    #[test]
    fn response_tasks_mutate_roundtrips() {
        let res = Response::TasksMutate {
            task: sample_summary(),
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── tasks_add_dep / tasks_remove_dep ───────────────────────

    #[test]
    fn request_tasks_add_dep_roundtrips() {
        let req = Request::TasksAddDep {
            task_id: "brn-2fe.27".into(),
            depends_on_task_id: "brn-2fe.28".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_add_dep_wire_format_is_stable() {
        let req = Request::TasksAddDep {
            task_id: "brn-2fe.27".into(),
            depends_on_task_id: "brn-2fe.28".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_add_dep","task_id":"brn-2fe.27","depends_on_task_id":"brn-2fe.28"}"#
        );
    }

    #[test]
    fn request_tasks_remove_dep_roundtrips() {
        let req = Request::TasksRemoveDep {
            task_id: "brn-2fe.27".into(),
            depends_on_task_id: "brn-2fe.28".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_tasks_dep_added_roundtrips() {
        let res = Response::TasksDepAdded {
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_dep_added_wire_format_is_stable() {
        let res = Response::TasksDepAdded {
            event_id: "evt".into(),
        };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(json, r#"{"type":"tasks_dep_added","event_id":"evt"}"#);
    }

    #[test]
    fn response_tasks_dep_removed_roundtrips() {
        let res = Response::TasksDepRemoved {
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── tasks_add_label / tasks_remove_label ───────────────────

    #[test]
    fn request_tasks_add_label_roundtrips() {
        let req = Request::TasksAddLabel {
            task_id: "brn-2fe.27".into(),
            label: "blocked".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_add_label_wire_format_is_stable() {
        let req = Request::TasksAddLabel {
            task_id: "brn-2fe.27".into(),
            label: "blocked".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_add_label","task_id":"brn-2fe.27","label":"blocked"}"#
        );
    }

    #[test]
    fn request_tasks_remove_label_roundtrips() {
        let req = Request::TasksRemoveLabel {
            task_id: "brn-2fe.27".into(),
            label: "blocked".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_tasks_label_added_roundtrips() {
        let res = Response::TasksLabelAdded {
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_tasks_label_added_wire_format_is_stable() {
        let res = Response::TasksLabelAdded {
            event_id: "evt".into(),
        };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(json, r#"{"type":"tasks_label_added","event_id":"evt"}"#);
    }

    #[test]
    fn response_tasks_label_removed_roundtrips() {
        let res = Response::TasksLabelRemoved {
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── tasks_transfer ─────────────────────────────────────────

    #[test]
    fn request_tasks_transfer_roundtrips() {
        let req = Request::TasksTransfer {
            params: TasksTransferParams {
                task_id: "brn-2fe.27".into(),
                target_brain: "other".into(),
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_tasks_transfer_wire_format_is_stable() {
        let req = Request::TasksTransfer {
            params: TasksTransferParams {
                task_id: "brn-2fe.27".into(),
                target_brain: "other".into(),
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"tasks_transfer","params":{"task_id":"brn-2fe.27","target_brain":"other"}}"#
        );
    }

    #[test]
    fn response_tasks_transfer_roundtrips() {
        let res = Response::TasksTransfer {
            task: sample_summary(),
            event_id: "evt".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── records_verify ─────────────────────────────────────────

    fn sample_verify_report() -> RecordsVerifyReport {
        RecordsVerifyReport {
            clean: true,
            records_checked: 42,
            blobs_checked: 50,
            missing: 0,
            corrupt: 0,
            orphans: 0,
            stale_flags: 0,
        }
    }

    #[test]
    fn request_records_verify_roundtrips() {
        let req = Request::RecordsVerify;
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_records_verify_wire_format_is_stable() {
        let req = Request::RecordsVerify;
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"records_verify"}"#);
    }

    #[test]
    fn response_records_verify_roundtrips() {
        let res = Response::RecordsVerify {
            report: sample_verify_report(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn records_verify_report_roundtrips() {
        let rep = sample_verify_report();
        assert_eq!(roundtrip(&rep), rep);
    }

    #[test]
    fn records_verify_report_wire_format_is_stable() {
        let rep = sample_verify_report();
        let json = serde_json::to_string(&rep).unwrap();
        assert_eq!(
            json,
            r#"{"clean":true,"records_checked":42,"blobs_checked":50,"missing":0,"corrupt":0,"orphans":0,"stale_flags":0}"#
        );
    }

    // ── shared params (RecordsListParams / RecordsCreateParams) ──

    fn sample_records_list_params() -> RecordsListParams {
        RecordsListParams {
            tag: Some("ops".into()),
            task_id: Some("brn-2fe.27".into()),
            status: Some("active".into()),
            limit: Some(25),
        }
    }

    fn sample_records_create_params() -> RecordsCreateParams {
        RecordsCreateParams {
            title: "title".into(),
            description: Some("desc".into()),
            body: b"hello".to_vec(),
            media_type: Some("text/plain".into()),
            task_id: Some("brn-2fe.27".into()),
            tags: vec!["ops".into()],
            brain: None,
        }
    }

    #[test]
    fn records_list_params_roundtrips() {
        let p = sample_records_list_params();
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn records_list_params_wire_format_is_stable() {
        let p = RecordsListParams {
            tag: Some("ops".into()),
            task_id: None,
            status: Some("active".into()),
            limit: Some(25),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(
            json,
            r#"{"tag":"ops","task_id":null,"status":"active","limit":25}"#
        );
    }

    #[test]
    fn records_create_params_roundtrips() {
        let p = sample_records_create_params();
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn records_create_params_wire_format_is_stable() {
        let p = RecordsCreateParams {
            title: "t".into(),
            description: None,
            body: vec![0x68, 0x69],
            media_type: Some("text/plain".into()),
            task_id: None,
            tags: vec![],
            brain: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(
            json,
            r#"{"title":"t","description":null,"body":[104,105],"media_type":"text/plain","task_id":null,"tags":[],"brain":null}"#
        );
    }

    #[test]
    fn artifacts_list_params_roundtrips() {
        let p = ArtifactsListParams {
            kind: Some("document".into()),
            tag: None,
            status: Some("active".into()),
            limit: Some(50),
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn artifacts_list_params_wire_format_is_stable() {
        let p = ArtifactsListParams {
            kind: Some("document".into()),
            tag: None,
            status: Some("active".into()),
            limit: Some(50),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(
            json,
            r#"{"kind":"document","tag":null,"status":"active","limit":50}"#
        );
    }

    // ── per-family summaries ───────────────────────────────────

    fn sample_analysis_summary() -> AnalysisSummary {
        AnalysisSummary {
            record_id: "BRN-01J".into(),
            title: "perf review".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    fn sample_artifact_summary() -> ArtifactSummary {
        ArtifactSummary {
            record_id: "BRN-01J".into(),
            title: "perf review".into(),
            kind: "document".into(),
            status: "active".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    fn sample_document_summary() -> DocumentSummary {
        DocumentSummary {
            record_id: "BRN-01J".into(),
            title: "doc".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    fn sample_plan_summary() -> PlanSummary {
        PlanSummary {
            record_id: "BRN-01J".into(),
            title: "plan".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    fn sample_snapshot_summary() -> SnapshotSummary {
        SnapshotSummary {
            record_id: "BRN-01J".into(),
            title: "snap".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    #[test]
    fn analysis_summary_roundtrips() {
        let s = sample_analysis_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn analysis_summary_wire_format_is_stable() {
        let s = sample_analysis_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"record_id":"BRN-01J","title":"perf review","created_at":"2026-05-17T00:00:00Z","brain_id":"eAx_dEFA"}"#
        );
    }

    #[test]
    fn artifact_summary_roundtrips() {
        let s = sample_artifact_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn artifact_summary_wire_format_is_stable() {
        let s = sample_artifact_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"record_id":"BRN-01J","title":"perf review","kind":"document","status":"active","created_at":"2026-05-17T00:00:00Z","brain_id":"eAx_dEFA"}"#
        );
    }

    #[test]
    fn document_summary_roundtrips() {
        let s = sample_document_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn document_summary_wire_format_is_stable() {
        let s = sample_document_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"record_id":"BRN-01J","title":"doc","created_at":"2026-05-17T00:00:00Z","brain_id":"eAx_dEFA"}"#
        );
    }

    #[test]
    fn plan_summary_roundtrips() {
        let s = sample_plan_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn plan_summary_wire_format_is_stable() {
        let s = sample_plan_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"record_id":"BRN-01J","title":"plan","created_at":"2026-05-17T00:00:00Z","brain_id":"eAx_dEFA"}"#
        );
    }

    #[test]
    fn snapshot_summary_roundtrips() {
        let s = sample_snapshot_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn snapshot_summary_wire_format_is_stable() {
        let s = sample_snapshot_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"record_id":"BRN-01J","title":"snap","created_at":"2026-05-17T00:00:00Z","brain_id":"eAx_dEFA"}"#
        );
    }

    // ── analyses Request/Response ───────────────────────────────

    #[test]
    fn request_analyses_list_roundtrips() {
        let req = Request::AnalysesList {
            params: sample_records_list_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_analyses_show_roundtrips() {
        let req = Request::AnalysesShow {
            id: "BRN-01J".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_analyses_create_roundtrips() {
        let req = Request::AnalysesCreate {
            params: sample_records_create_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_analyses_list_roundtrips() {
        let res = Response::AnalysesList {
            records: vec![sample_analysis_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_analyses_show_some_roundtrips() {
        let res = Response::AnalysesShow {
            record: Some(sample_analysis_summary()),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_analyses_show_none_roundtrips() {
        let res = Response::AnalysesShow { record: None };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_analyses_create_roundtrips() {
        let res = Response::AnalysesCreate {
            record: sample_analysis_summary(),
            content_hash: "ab12".into(),
            size: 5,
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── artifacts Request/Response ──────────────────────────────

    #[test]
    fn request_artifacts_list_roundtrips() {
        let req = Request::ArtifactsList {
            params: ArtifactsListParams {
                kind: Some("document".into()),
                tag: None,
                status: Some("active".into()),
                limit: Some(50),
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_artifacts_show_roundtrips() {
        let req = Request::ArtifactsShow {
            id: "BRN-01J".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_artifacts_list_roundtrips() {
        let res = Response::ArtifactsList {
            records: vec![sample_artifact_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_artifacts_show_some_roundtrips() {
        let res = Response::ArtifactsShow {
            record: Some(sample_artifact_summary()),
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── documents Request/Response ──────────────────────────────

    #[test]
    fn request_documents_list_roundtrips() {
        let req = Request::DocumentsList {
            params: sample_records_list_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_documents_show_roundtrips() {
        let req = Request::DocumentsShow {
            id: "BRN-01J".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_documents_create_roundtrips() {
        let req = Request::DocumentsCreate {
            params: sample_records_create_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_documents_list_roundtrips() {
        let res = Response::DocumentsList {
            records: vec![sample_document_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_documents_create_roundtrips() {
        let res = Response::DocumentsCreate {
            record: sample_document_summary(),
            content_hash: "ab12".into(),
            size: 5,
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── plans Request/Response ──────────────────────────────────

    #[test]
    fn request_plans_list_roundtrips() {
        let req = Request::PlansList {
            params: sample_records_list_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_plans_show_roundtrips() {
        let req = Request::PlansShow {
            id: "BRN-01J".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_plans_create_roundtrips() {
        let req = Request::PlansCreate {
            params: sample_records_create_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_plans_create_roundtrips() {
        let res = Response::PlansCreate {
            record: sample_plan_summary(),
            content_hash: "ab12".into(),
            size: 5,
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── snapshots Request/Response ──────────────────────────────

    #[test]
    fn request_snapshots_list_roundtrips() {
        let req = Request::SnapshotsList {
            params: sample_records_list_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_snapshots_show_roundtrips() {
        let req = Request::SnapshotsShow {
            id: "BRN-01J".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_snapshots_create_roundtrips() {
        let req = Request::SnapshotsCreate {
            params: sample_records_create_params(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_snapshots_list_roundtrips() {
        let res = Response::SnapshotsList {
            records: vec![sample_snapshot_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_snapshots_create_roundtrips() {
        let res = Response::SnapshotsCreate {
            record: sample_snapshot_summary(),
            content_hash: "ab12".into(),
            size: 5,
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── sagas summary / params / report ────────────────────────

    fn sample_saga_summary() -> SagaSummary {
        SagaSummary {
            saga_id: "saga-deadbeef".into(),
            title: "Q4 migration".into(),
            description: Some("desc".into()),
            status: "open".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            updated_at: "2026-05-17T00:00:00Z".into(),
            closed_at: None,
            members: vec![],
            brains: vec![],
        }
    }

    fn sample_saga_brain_summary() -> SagaBrainSummary {
        SagaBrainSummary {
            brain_id: "brain-x".into(),
            name: "Brain X".into(),
            prefix: Some("BRX".into()),
        }
    }

    fn sample_saga_frontier_task() -> SagaFrontierTask {
        SagaFrontierTask {
            task_id: "brn-2fe.27".into(),
            title: "vertical slice".into(),
            status: "open".into(),
            priority: 2,
            task_type: "task".into(),
        }
    }

    fn sample_saga_cascade_result() -> SagaCascadeResult {
        SagaCascadeResult {
            task_id: "brn-2fe.27".into(),
            outcome: SagaCascadeOutcome::Closed,
        }
    }

    fn sample_saga_stats_report() -> SagaStatsReport {
        SagaStatsReport {
            total: 5,
            open: 1,
            in_progress: 1,
            blocked: 0,
            done: 2,
            cancelled: 1,
            orphan: 0,
            completion_pct: Some(50.0),
        }
    }

    #[test]
    fn saga_summary_roundtrips() {
        let s = sample_saga_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn saga_summary_wire_format_is_stable() {
        let s = sample_saga_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"saga_id":"saga-deadbeef","title":"Q4 migration","description":"desc","status":"open","created_at":"2026-05-17T00:00:00Z","updated_at":"2026-05-17T00:00:00Z","closed_at":null,"members":[],"brains":[]}"#
        );
    }

    #[test]
    fn saga_brain_summary_roundtrips() {
        let s = sample_saga_brain_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn saga_brain_summary_wire_format_is_stable() {
        let s = sample_saga_brain_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"brain_id":"brain-x","name":"Brain X","prefix":"BRX"}"#
        );
    }

    #[test]
    fn saga_frontier_task_roundtrips() {
        let s = sample_saga_frontier_task();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn saga_frontier_task_wire_format_is_stable() {
        let s = sample_saga_frontier_task();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"task_id":"brn-2fe.27","title":"vertical slice","status":"open","priority":2,"task_type":"task"}"#
        );
    }

    #[test]
    fn saga_cascade_outcome_closed_wire_format_is_stable() {
        let o = SagaCascadeOutcome::Closed;
        let json = serde_json::to_string(&o).unwrap();
        assert_eq!(json, r#"{"outcome":"closed"}"#);
    }

    #[test]
    fn saga_cascade_outcome_skipped_wire_format_is_stable() {
        let o = SagaCascadeOutcome::Skipped {
            reason: "terminal".into(),
        };
        let json = serde_json::to_string(&o).unwrap();
        assert_eq!(json, r#"{"outcome":"skipped","reason":"terminal"}"#);
    }

    #[test]
    fn saga_cascade_outcome_failed_wire_format_is_stable() {
        let o = SagaCascadeOutcome::Failed {
            error: "boom".into(),
        };
        let json = serde_json::to_string(&o).unwrap();
        assert_eq!(json, r#"{"outcome":"failed","error":"boom"}"#);
    }

    #[test]
    fn saga_cascade_outcome_all_variants_roundtrip() {
        for o in [
            SagaCascadeOutcome::Closed,
            SagaCascadeOutcome::Cancelled,
            SagaCascadeOutcome::Skipped { reason: "x".into() },
            SagaCascadeOutcome::Failed { error: "e".into() },
        ] {
            assert_eq!(roundtrip(&o), o);
        }
    }

    #[test]
    fn saga_cascade_result_roundtrips() {
        let r = sample_saga_cascade_result();
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn saga_stats_report_roundtrips() {
        let r = sample_saga_stats_report();
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: SagaStatsReport = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn saga_stats_report_wire_format_is_stable() {
        let r = sample_saga_stats_report();
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(
            json,
            r#"{"total":5,"open":1,"in_progress":1,"blocked":0,"done":2,"cancelled":1,"orphan":0,"completion_pct":50.0}"#
        );
    }

    #[test]
    fn saga_label_count_roundtrips() {
        let l = SagaLabelCount {
            label: "p0".into(),
            count: 3,
        };
        assert_eq!(roundtrip(&l), l);
    }

    // ── sagas params ───────────────────────────────────────────

    #[test]
    fn sagas_list_params_roundtrips() {
        let p = SagasListParams {
            include_closed: true,
            include_cancelled: false,
            containing_brain: Some("brain-x".into()),
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn sagas_list_params_default_wire_format_is_stable() {
        let p = SagasListParams::default();
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(
            json,
            r#"{"include_closed":false,"include_cancelled":false,"containing_brain":null}"#
        );
    }

    #[test]
    fn sagas_create_params_roundtrips() {
        let p = SagasCreateParams {
            title: "Q4 migration".into(),
            description: Some("desc".into()),
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn sagas_update_params_roundtrips_with_clear() {
        let p = SagasUpdateParams {
            saga_id: "saga-abc".into(),
            title: Some("new".into()),
            description: Some(SagaDescriptionUpdate::Clear),
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn sagas_update_params_roundtrips_with_set() {
        let p = SagasUpdateParams {
            saga_id: "saga-abc".into(),
            title: None,
            description: Some(SagaDescriptionUpdate::Set {
                value: "new desc".into(),
            }),
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn saga_description_update_clear_wire_format_is_stable() {
        let u = SagaDescriptionUpdate::Clear;
        let json = serde_json::to_string(&u).unwrap();
        assert_eq!(json, r#"{"op":"clear"}"#);
    }

    #[test]
    fn saga_description_update_set_wire_format_is_stable() {
        let u = SagaDescriptionUpdate::Set {
            value: "abc".into(),
        };
        let json = serde_json::to_string(&u).unwrap();
        assert_eq!(json, r#"{"op":"set","value":"abc"}"#);
    }

    // ── sagas Request/Response ─────────────────────────────────

    #[test]
    fn request_sagas_list_roundtrips() {
        let req = Request::SagasList {
            params: SagasListParams::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_sagas_list_wire_format_is_stable() {
        let req = Request::SagasList {
            params: SagasListParams::default(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"sagas_list","params":{"include_closed":false,"include_cancelled":false,"containing_brain":null}}"#
        );
    }

    #[test]
    fn response_sagas_list_roundtrips() {
        let res = Response::SagasList {
            sagas: vec![sample_saga_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_get_roundtrips() {
        let req = Request::SagasGet {
            saga_id: "saga-abc".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_get_some_roundtrips() {
        let res = Response::SagasGet {
            saga: Some(sample_saga_summary()),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_sagas_get_none_roundtrips() {
        let res = Response::SagasGet { saga: None };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_create_roundtrips() {
        let req = Request::SagasCreate {
            params: SagasCreateParams {
                title: "t".into(),
                description: None,
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_create_roundtrips() {
        let res = Response::SagasCreate {
            saga: sample_saga_summary(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_update_roundtrips() {
        let req = Request::SagasUpdate {
            params: SagasUpdateParams {
                saga_id: "saga-abc".into(),
                title: Some("new".into()),
                description: None,
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_update_roundtrips() {
        let res = Response::SagasUpdate {
            saga: sample_saga_summary(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_add_tasks_roundtrips() {
        let req = Request::SagasAddTasks {
            saga_id: "saga-abc".into(),
            task_ids: vec!["brn-2fe.27".into(), "brn-2fe.28".into()],
            cascade: true,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_sagas_add_tasks_wire_format_is_stable() {
        let req = Request::SagasAddTasks {
            saga_id: "saga-abc".into(),
            task_ids: vec!["brn-2fe.27".into()],
            cascade: false,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"sagas_add_tasks","saga_id":"saga-abc","task_ids":["brn-2fe.27"],"cascade":false}"#
        );
    }

    #[test]
    fn response_sagas_add_tasks_roundtrips() {
        let res = Response::SagasAddTasks {
            saga_id: "saga-abc".into(),
            added: 2,
            added_task_ids: vec!["brn-2fe.27".into(), "brn-2fe.28".into()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_remove_tasks_roundtrips() {
        let req = Request::SagasRemoveTasks {
            saga_id: "saga-abc".into(),
            task_ids: vec!["brn-2fe.27".into()],
            cascade: true,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_remove_tasks_roundtrips() {
        let res = Response::SagasRemoveTasks {
            saga_id: "saga-abc".into(),
            removed: 1,
            removed_task_ids: vec!["brn-2fe.27".into()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_frontier_roundtrips() {
        let req = Request::SagasFrontier {
            saga_id: "saga-abc".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_frontier_roundtrips() {
        let res = Response::SagasFrontier {
            saga_id: "saga-abc".into(),
            saga_status: "open".into(),
            tasks: vec![sample_saga_frontier_task()],
            brains: vec![sample_saga_brain_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_start_roundtrips() {
        let req = Request::SagasStart {
            saga_id: "saga-abc".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_start_roundtrips() {
        let res = Response::SagasStart {
            saga: sample_saga_summary(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_close_roundtrips() {
        let req = Request::SagasClose {
            saga_id: "saga-abc".into(),
            cascade: true,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_close_roundtrips() {
        let res = Response::SagasClose {
            saga: sample_saga_summary(),
            cascade: true,
            cascade_results: vec![sample_saga_cascade_result()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_cancel_roundtrips() {
        let req = Request::SagasCancel {
            saga_id: "saga-abc".into(),
            cascade: false,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_cancel_roundtrips() {
        let res = Response::SagasCancel {
            saga: sample_saga_summary(),
            cascade: false,
            cascade_results: vec![],
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_reopen_roundtrips() {
        let req = Request::SagasReopen {
            saga_id: "saga-abc".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_reopen_roundtrips() {
        let res = Response::SagasReopen {
            saga: sample_saga_summary(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn request_sagas_stats_roundtrips() {
        let req = Request::SagasStats {
            saga_id: "saga-abc".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_sagas_stats_roundtrips() {
        let res = Response::SagasStats {
            saga_id: "saga-abc".into(),
            stats: sample_saga_stats_report(),
            label_histogram: vec![SagaLabelCount {
                label: "p0".into(),
                count: 3,
            }],
            brains: vec![sample_saga_brain_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── watch ─────────────────────────────────────────────────────────

    fn sample_watch_summary() -> WatchSummary {
        WatchSummary {
            brain_name: "default".into(),
            brain_id: "abc123".into(),
            note_dir: "/notes".into(),
            watching: true,
        }
    }

    #[test]
    fn watch_summary_roundtrips() {
        let s = sample_watch_summary();
        assert_eq!(roundtrip(&s), s);
    }

    #[test]
    fn watch_summary_wire_format_is_stable() {
        let s = sample_watch_summary();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"brain_name":"default","brain_id":"abc123","note_dir":"/notes","watching":true}"#
        );
    }

    #[test]
    fn request_watch_add_roundtrips() {
        let req = Request::WatchAdd {
            path: "/notes".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_watch_add_wire_format_is_stable() {
        let req = Request::WatchAdd {
            path: "/notes".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"watch_add","path":"/notes"}"#);
    }

    #[test]
    fn request_watch_remove_roundtrips() {
        let req = Request::WatchRemove {
            path: "/notes".into(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_watch_remove_wire_format_is_stable() {
        let req = Request::WatchRemove {
            path: "/notes".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"watch_remove","path":"/notes"}"#);
    }

    #[test]
    fn request_watch_list_roundtrips() {
        let req = Request::WatchList;
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_watch_list_wire_format_is_stable() {
        let req = Request::WatchList;
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"watch_list"}"#);
    }

    #[test]
    fn response_watch_added_roundtrips() {
        let res = Response::WatchAdded {
            path: "/notes".into(),
            brain_name: "default".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_watch_added_wire_format_is_stable() {
        let res = Response::WatchAdded {
            path: "/notes".into(),
            brain_name: "default".into(),
        };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(
            json,
            r#"{"type":"watch_added","path":"/notes","brain_name":"default"}"#
        );
    }

    #[test]
    fn response_watch_removed_roundtrips() {
        let res = Response::WatchRemoved {
            path: "/notes".into(),
        };
        assert_eq!(roundtrip(&res), res);
    }

    #[test]
    fn response_watch_list_roundtrips() {
        let res = Response::WatchList {
            watches: vec![sample_watch_summary()],
        };
        assert_eq!(roundtrip(&res), res);
    }

    // ── status / jobs extension round-trip tests ───────────────────

    fn sample_latency_histogram() -> LatencyHistogram {
        LatencyHistogram {
            p50_us: 1_500,
            p95_us: 9_800,
            total_samples: 42,
        }
    }

    fn sample_metrics_snapshot() -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_seconds: 3_600,
            indexing_latency: sample_latency_histogram(),
            query_latency: LatencyHistogram {
                p50_us: 200,
                p95_us: 1_100,
                total_samples: 17,
            },
            queue_depth: 3,
            lancedb_unoptimized_rows: 12_000,
            lancedb_optimize_failures: 1,
            indexing_errors: 0,
            query_errors: 2,
        }
    }

    #[test]
    fn latency_histogram_roundtrips() {
        let h = sample_latency_histogram();
        assert_eq!(roundtrip(&h), h);
    }

    #[test]
    fn metrics_snapshot_roundtrips() {
        let m = sample_metrics_snapshot();
        assert_eq!(roundtrip(&m), m);
    }

    #[test]
    fn metrics_snapshot_field_names_are_stable() {
        // Byte-shape contract with legacy `status` MCP envelope.
        let m = sample_metrics_snapshot();
        let json = serde_json::to_value(&m).unwrap();
        assert!(json.get("uptime_seconds").is_some());
        assert!(json.get("indexing_latency").is_some());
        assert!(json.get("query_latency").is_some());
        assert!(json.get("queue_depth").is_some());
        assert!(json.get("lancedb_unoptimized_rows").is_some());
        assert!(json.get("lancedb_optimize_failures").is_some());
        assert!(json.get("indexing_errors").is_some());
        assert!(json.get("query_errors").is_some());
        assert!(json["indexing_latency"]["p50_us"].is_u64());
        assert!(json["indexing_latency"]["p95_us"].is_u64());
        assert!(json["indexing_latency"]["total_samples"].is_u64());
    }

    #[test]
    fn brain_status_report_roundtrips_with_metrics() {
        let report = BrainStatusReport {
            brain_name: "brain".into(),
            brain_id: "eAx_dEFA".into(),
            tasks_open: 5,
            tasks_in_progress: 1,
            tasks_blocked: 0,
            tasks_done: 12,
            stuck_files: 2,
            stale_hashes_prevented: 7,
            metrics: sample_metrics_snapshot(),
        };
        assert_eq!(roundtrip(&report), report);
    }

    #[test]
    fn job_summary_roundtrips_with_status_and_started_at() {
        let job = JobSummary {
            job_id: "job-abc".into(),
            kind: "summarize_scope".into(),
            ref_id: "sum-123".into(),
            attempts: 2,
            last_error: Some("connection refused".into()),
            status: "failed".into(),
            started_at: Some("2026-05-18T12:00:00Z".into()),
            updated_at: "2026-05-18T12:01:00Z".into(),
        };
        assert_eq!(roundtrip(&job), job);
    }

    #[test]
    fn job_summary_roundtrips_with_no_started_at() {
        let job = JobSummary {
            job_id: "job-def".into(),
            kind: "consolidate_cluster".into(),
            ref_id: "cluster-9".into(),
            attempts: 0,
            last_error: None,
            status: "pending".into(),
            started_at: None,
            updated_at: "2026-05-18T12:01:00Z".into(),
        };
        assert_eq!(roundtrip(&job), job);
    }

    #[test]
    fn jobs_status_params_default_matches_legacy_mcp_defaults() {
        let p = JobsStatusParams::default();
        assert_eq!(p.kind, None);
        assert_eq!(p.status, None);
        assert_eq!(p.limit, 10);
    }

    #[test]
    fn jobs_status_params_roundtrips() {
        let p = JobsStatusParams {
            kind: Some("summarize_scope".into()),
            status: Some("failed".into()),
            limit: 25,
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn request_jobs_status_roundtrips_with_params() {
        let req = Request::JobsStatus {
            params: JobsStatusParams {
                kind: Some("consolidate_cluster".into()),
                status: Some("ready".into()),
                limit: 5,
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn request_jobs_status_wire_format_is_stable() {
        let req = Request::JobsStatus {
            params: JobsStatusParams::default(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"jobs_status","params":{"kind":null,"status":null,"limit":10}}"#
        );
    }

    #[test]
    fn jobs_status_report_roundtrips_with_listing_status() {
        let report = JobsStatusReport {
            pending: 1,
            running: 2,
            ready: 3,
            done: 4,
            failed: 5,
            listing_status: "failed".into(),
            recent_failures: vec![],
            stuck_jobs: vec![],
        };
        assert_eq!(roundtrip(&report), report);
    }

    // ── records.search + records.fetch_content wire-shape tests ─────────

    fn sample_record_hit() -> WireRecordHit {
        WireRecordHit {
            record_id: "rec_abc".into(),
            memory_id: "record:rec_abc:0".into(),
            title: "Rust notes".into(),
            summary: "Two-sentence summary about Rust.".into(),
            score: 0.8423,
            kind: "record".into(),
            uri: "synapse://brain/record/rec_abc".into(),
            brain_name: None,
        }
    }

    #[test]
    fn records_search_params_default_matches_legacy_mcp() {
        let p = RecordsSearchParams::default();
        assert_eq!(p.k, 10);
        assert_eq!(p.budget_tokens, 800);
        assert!(p.tags.is_empty());
        assert!(p.brains.is_empty());
    }

    #[test]
    fn records_search_params_default_via_serde() {
        // Wire JSON that omits optional fields should yield the legacy
        // MCP defaults (k=10, budget_tokens=800, empty tags/brains).
        let p: RecordsSearchParams =
            serde_json::from_str(r#"{"query":"hello"}"#).expect("deserialize defaults");
        assert_eq!(p.query, "hello");
        assert_eq!(p.k, 10);
        assert_eq!(p.budget_tokens, 800);
        assert!(p.tags.is_empty());
        assert!(p.brains.is_empty());
    }

    #[test]
    fn records_search_params_roundtrips() {
        let p = RecordsSearchParams {
            query: "rust".into(),
            k: 5,
            budget_tokens: 400,
            tags: vec!["wave:1".into()],
            brains: vec!["brain".into(), "neural_link".into()],
        };
        let json = serde_json::to_vec(&p).unwrap();
        let back: RecordsSearchParams = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn wire_record_hit_roundtrips_with_brain_name() {
        let mut hit = sample_record_hit();
        hit.brain_name = Some("neural_link".into());
        let json = serde_json::to_vec(&hit).unwrap();
        let back: WireRecordHit = serde_json::from_slice(&json).unwrap();
        // `WireRecordHit` derives `PartialEq` (not `Eq`) because of
        // `score: f64`. Equality still holds bit-for-bit after JSON
        // round-trip when neither side is NaN.
        assert_eq!(back, hit);
    }

    #[test]
    fn wire_record_hit_skips_brain_name_when_none() {
        let hit = sample_record_hit();
        let json = serde_json::to_value(&hit).unwrap();
        assert!(
            json.get("brain_name").is_none(),
            "brain_name=None must be skipped on the wire; got {json}"
        );
    }

    #[test]
    fn records_search_report_roundtrips() {
        let report = RecordsSearchReport {
            budget_tokens: 800,
            used_tokens_est: 42,
            result_count: 1,
            total_available: 1,
            results: vec![sample_record_hit()],
        };
        let json = serde_json::to_vec(&report).unwrap();
        let back: RecordsSearchReport = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, report);
    }

    #[test]
    fn request_records_search_roundtrips_with_params() {
        let req = Request::RecordsSearch {
            params: RecordsSearchParams {
                query: "rust".into(),
                k: 5,
                budget_tokens: 400,
                tags: vec!["wave:1".into()],
                brains: vec![],
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn response_records_search_roundtrips() {
        let res = Response::RecordsSearch {
            report: RecordsSearchReport {
                budget_tokens: 800,
                used_tokens_est: 0,
                result_count: 0,
                total_available: 0,
                results: vec![],
            },
        };
        let bytes = serde_json::to_vec(&res).unwrap();
        let back: Response = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, res);
    }

    #[test]
    fn records_fetch_content_params_roundtrips() {
        let p = RecordsFetchContentParams {
            record_id: "rec_abc".into(),
            brain: Some("neural_link".into()),
        };
        assert_eq!(roundtrip(&p), p);
    }

    #[test]
    fn record_content_text_roundtrips() {
        let c = RecordContent {
            record_id: "rec_abc".into(),
            title: "Notes".into(),
            kind: "document".into(),
            content_hash: "sha256:cafe".into(),
            size: 5,
            media_type: Some("text/plain".into()),
            encoding: "utf-8".into(),
            text: Some("hello".into()),
            data_base64: None,
            uri: "synapse://brain/record/rec_abc".into(),
            brain: None,
        };
        assert_eq!(roundtrip(&c), c);
    }

    #[test]
    fn record_content_binary_roundtrips() {
        let c = RecordContent {
            record_id: "rec_bin".into(),
            title: "Binary".into(),
            kind: "snapshot".into(),
            content_hash: "sha256:beef".into(),
            size: 4,
            media_type: Some("application/octet-stream".into()),
            encoding: "base64".into(),
            text: None,
            data_base64: Some("3q2+7w==".into()),
            uri: "synapse://brain/record/rec_bin".into(),
            brain: Some("neural_link".into()),
        };
        assert_eq!(roundtrip(&c), c);
    }

    #[test]
    fn record_content_skips_none_optionals_on_wire() {
        let c = RecordContent {
            record_id: "rec_abc".into(),
            title: "Notes".into(),
            kind: "document".into(),
            content_hash: "sha256:cafe".into(),
            size: 5,
            media_type: None,
            encoding: "utf-8".into(),
            text: Some("hi".into()),
            data_base64: None,
            uri: "synapse://brain/record/rec_abc".into(),
            brain: None,
        };
        let json = serde_json::to_value(&c).unwrap();
        assert!(json.get("media_type").is_none());
        // `data_base64` is renamed to `"data"` on the wire — assert the
        // wire key is absent when the Rust field is `None`.
        assert!(json.get("data").is_none());
        assert!(json.get("data_base64").is_none());
        assert!(json.get("brain").is_none());
        // `text` is populated so it must be on the wire.
        assert_eq!(json["text"], "hi");
    }

    #[test]
    fn record_content_binary_uses_data_wire_key() {
        // Byte-shape parity with legacy `records.fetch_content` MCP
        // envelope: the base64 payload travels under the `data` key,
        // not `data_base64`.
        let c = RecordContent {
            record_id: "rec_bin".into(),
            title: "Binary".into(),
            kind: "snapshot".into(),
            content_hash: "sha256:beef".into(),
            size: 4,
            media_type: Some("application/octet-stream".into()),
            encoding: "base64".into(),
            text: None,
            data_base64: Some("3q2+7w==".into()),
            uri: "synapse://brain/record/rec_bin".into(),
            brain: None,
        };
        let json = serde_json::to_value(&c).unwrap();
        assert!(
            json.get("data_base64").is_none(),
            "Rust-side name must not leak to wire"
        );
        assert_eq!(json["data"], "3q2+7w==");
    }

    #[test]
    fn request_records_fetch_content_roundtrips() {
        let req = Request::RecordsFetchContent {
            params: RecordsFetchContentParams {
                record_id: "rec_abc".into(),
                brain: None,
            },
        };
        assert_eq!(roundtrip(&req), req);
    }
}
