//! `DaemonClient` — typed RPC entry point generic over [`Transport`].
//!
//! # Hexagonal payoff
//!
//! `DaemonClient<T: Transport>` is the part of the crate that downstream
//! code (`brain` CLI, `brain-mcp`) uses to talk to the daemon. By taking a
//! generic transport, the same client implementation is exercised in tests
//! with [`crate::testing::InMemoryTransport`] (no sockets) and in
//! production with [`crate::unix::UnixSocketTransport`] (US-006). No
//! conditional compilation, no two code paths.
//!
//! # Construction policy
//!
//! There are two ways to build a `DaemonClient`:
//!
//! - [`DaemonClient::connect`] — public; performs the
//!   [`crate::PROTOCOL_VERSION`] handshake before returning. A stale daemon
//!   is caught up-front with [`RpcError::VersionMismatch`].
//! - `DaemonClient::from_transport` — `pub(crate)`; bypasses the handshake.
//!   Used only by in-crate unit tests that want to exercise `call()`
//!   with a hand-crafted transport. Integration tests in `tests/*.rs`
//!   cannot reach it (visibility), so the public path is forced through
//!   `connect()` — meaning the handshake is exercised by every external
//!   test that constructs a client.

use crate::domain::{
    AnalysisSummary, ArtifactSummary, ArtifactsListParams, BrainStatusReport, BrainsListParams,
    DocumentSummary, JobsStatusReport, LinksAddParams, LinksForEntityParams, LinksRemoveParams,
    MemoryConsolidateParams, MemoryReflectParams, MemoryRetrieveParams,
    MemorySummarizeScopeParams, MemoryWalkThreadParams, MemoryWriteEpisodeParams,
    MemoryWriteProcedureParams, PROTOCOL_VERSION, PlanSummary, ProviderSummary,
    RecordsArchiveParams, RecordsCreateParams, RecordsLinkParams, RecordsListParams,
    RecordsVerifyReport, Request, Response, RpcError, SagaBrainSummary, SagaCascadeResult,
    SagaFrontierTask, SagaLabelCount, SagaStatsReport, SagaSummary, SagasCreateParams,
    SagasListParams, SagasUpdateParams, SnapshotSummary, TagAliasSummary, TagAliasesStatusReport,
    TagsAliasesListParams, TagsReclusterParams, TaskSummary, TasksApplyEventParams,
    TasksCreateParams, TasksDepsBatchParams, TasksLabelsBatchParams, TasksListParams,
    TasksMutateParams, TasksTransferParams, TasksUpdateParams, WatchSummary, WireBrainSummary,
    WireLinkSummary, WireTaskLabelSummary,
};
use crate::transport::Transport;

/// Typed RPC client. Wraps a [`Transport`] and exposes typed
/// `Request` -> `Response` calls.
pub struct DaemonClient<T: Transport> {
    transport: T,
}

impl<T: Transport> DaemonClient<T> {
    /// Establish a session with the daemon. Sends [`Request::Handshake`]
    /// and rejects the connection on version mismatch or unexpected reply.
    ///
    /// Returns a ready-to-use client on success.
    ///
    /// # Errors
    ///
    /// - [`RpcError::VersionMismatch`] — the daemon's `PROTOCOL_VERSION`
    ///   differs from the client's. Restart the older side.
    /// - [`RpcError::Protocol`] — the daemon replied to the handshake with
    ///   something other than [`Response::HandshakeOk`] (e.g. `Pong`).
    /// - Any error the underlying transport raises (e.g.
    ///   [`RpcError::Transport`] for socket failures).
    pub fn connect(mut transport: T) -> Result<Self, RpcError> {
        let res = transport.call(Request::Handshake {
            version: PROTOCOL_VERSION,
        })?;
        match res {
            Response::HandshakeOk { server_version } if server_version == PROTOCOL_VERSION => {
                Ok(Self { transport })
            }
            Response::HandshakeOk { server_version } => Err(RpcError::VersionMismatch {
                client: PROTOCOL_VERSION,
                server: server_version,
            }),
            other => Err(RpcError::Protocol {
                message: format!("expected HandshakeOk in reply to Handshake, got {other:?}"),
            }),
        }
    }

    /// Wrap a transport **without** performing the handshake.
    ///
    /// `cfg(test)`-only and `pub(crate)`: external callers must use
    /// [`Self::connect`], and the symbol simply does not exist in
    /// non-test builds — that's how we guarantee version negotiation is
    /// never accidentally skipped in production code. In-crate unit tests
    /// use this constructor when they need to drive `call()` with a
    /// hand-crafted transport (e.g. an always-failing handler) without
    /// the handshake getting in the way.
    #[cfg(test)]
    pub(crate) fn from_transport(transport: T) -> Self {
        Self { transport }
    }

    /// Send `req` and return the matching response, surfacing any
    /// [`RpcError`] the transport produces.
    ///
    /// This is the **untyped** escape hatch — useful for tests and for
    /// experimental wire variants that don't yet have a typed wrapper.
    /// Prefer the typed methods (e.g. [`Self::tasks_list`]) at call sites
    /// where one exists: they narrow the `Response` enum to the specific
    /// variant the request expects and return [`RpcError::Protocol`] on
    /// shape mismatch, so consumers never have to paste the same
    /// `match resp { Response::X { .. } => …, other => bail!() }` block.
    pub fn call(&mut self, req: Request) -> Result<Response, RpcError> {
        self.transport.call(req)
    }

    /// Probe daemon liveness. Sends [`Request::Ping`] and returns `Ok(())`
    /// iff the daemon replies with [`Response::Pong`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than `Pong` (e.g. an out-of-order response after a previous
    ///   request failed mid-flight).
    /// - Any error the underlying transport raises.
    pub fn ping(&mut self) -> Result<(), RpcError> {
        match self.call(Request::Ping)? {
            Response::Pong => Ok(()),
            other => Err(RpcError::Protocol {
                message: format!("expected Pong in reply to Ping, got {other:?}"),
            }),
        }
    }

    /// List tasks via [`Request::TasksList`] and return the unwrapped
    /// [`TaskSummary`] vector.
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// match client.call(Request::TasksList { params })? {
    ///     Response::TasksList { tasks } => Ok(tasks),
    ///     other => Err(RpcError::Protocol { … }),
    /// }
    /// ```
    ///
    /// hoisted into a single typed call. Use this at every consumer site
    /// instead of pasting the `match` block — the audit flagged the
    /// duplication as a multiplier risk once more wire variants land.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than `Response::TasksList`.
    /// - Any error the dispatcher surfaces (e.g. an unknown `status`
    ///   filter today drops the connection, which surfaces as
    ///   [`RpcError::Transport`] — that will tighten to a wire-level
    ///   error envelope in a follow-up ticket).
    pub fn tasks_list(&mut self, params: TasksListParams) -> Result<Vec<TaskSummary>, RpcError> {
        match self.call(Request::TasksList { params })? {
            Response::TasksList { tasks } => Ok(tasks),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksList in reply to TasksList, got {other:?}"),
            }),
        }
    }

    /// Fetch a single task by ID. Returns `None` when the task does not
    /// exist on the daemon side.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksShow`].
    /// - Any error the underlying transport raises.
    pub fn tasks_show(&mut self, id: String) -> Result<Option<TaskSummary>, RpcError> {
        match self.call(Request::TasksShow { id })? {
            Response::TasksShow { task } => Ok(task),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksShow in reply to TasksShow, got {other:?}"),
            }),
        }
    }

    /// Return the next highest-priority actionable task, or `None` if
    /// there are no ready tasks.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksNext`].
    pub fn tasks_next(&mut self) -> Result<Option<TaskSummary>, RpcError> {
        match self.call(Request::TasksNext)? {
            Response::TasksNext { task } => Ok(task),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksNext in reply to TasksNext, got {other:?}"),
            }),
        }
    }

    /// Create a new task. Returns the freshly created [`TaskSummary`]
    /// plus the originating `event_id` (callers needing audit-log
    /// correlation should retain it).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksCreate`].
    pub fn tasks_create(
        &mut self,
        params: TasksCreateParams,
    ) -> Result<(TaskSummary, String), RpcError> {
        match self.call(Request::TasksCreate { params })? {
            Response::TasksCreate { task, event_id } => Ok((task, event_id)),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksCreate in reply to TasksCreate, got {other:?}"),
            }),
        }
    }

    /// Update non-status fields of an existing task.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksUpdate`].
    pub fn tasks_update(
        &mut self,
        params: TasksUpdateParams,
    ) -> Result<(TaskSummary, String), RpcError> {
        match self.call(Request::TasksUpdate { params })? {
            Response::TasksUpdate { task, event_id } => Ok((task, event_id)),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksUpdate in reply to TasksUpdate, got {other:?}"),
            }),
        }
    }

    /// Apply a status-mutating action ("close" / "open" / "block" /
    /// "in_progress" / "cancel") to a task.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksMutate`].
    pub fn tasks_mutate(
        &mut self,
        params: TasksMutateParams,
    ) -> Result<(TaskSummary, String), RpcError> {
        match self.call(Request::TasksMutate { params })? {
            Response::TasksMutate { task, event_id } => Ok((task, event_id)),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksMutate in reply to TasksMutate, got {other:?}"),
            }),
        }
    }

    /// Add a dependency edge: `task_id` depends on
    /// `depends_on_task_id`. Returns the originating `event_id`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksDepAdded`].
    pub fn tasks_add_dep(
        &mut self,
        task_id: String,
        depends_on_task_id: String,
    ) -> Result<String, RpcError> {
        match self.call(Request::TasksAddDep {
            task_id,
            depends_on_task_id,
        })? {
            Response::TasksDepAdded { event_id } => Ok(event_id),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksDepAdded in reply to TasksAddDep, got {other:?}"),
            }),
        }
    }

    /// Remove a dependency edge.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksDepRemoved`].
    pub fn tasks_remove_dep(
        &mut self,
        task_id: String,
        depends_on_task_id: String,
    ) -> Result<String, RpcError> {
        match self.call(Request::TasksRemoveDep {
            task_id,
            depends_on_task_id,
        })? {
            Response::TasksDepRemoved { event_id } => Ok(event_id),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksDepRemoved in reply to TasksRemoveDep, got {other:?}"
                ),
            }),
        }
    }

    /// Add a label to a task.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksLabelAdded`].
    pub fn tasks_add_label(&mut self, task_id: String, label: String) -> Result<String, RpcError> {
        match self.call(Request::TasksAddLabel { task_id, label })? {
            Response::TasksLabelAdded { event_id } => Ok(event_id),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksLabelAdded in reply to TasksAddLabel, got {other:?}"
                ),
            }),
        }
    }

    /// Remove a label from a task.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksLabelRemoved`].
    pub fn tasks_remove_label(
        &mut self,
        task_id: String,
        label: String,
    ) -> Result<String, RpcError> {
        match self.call(Request::TasksRemoveLabel { task_id, label })? {
            Response::TasksLabelRemoved { event_id } => Ok(event_id),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksLabelRemoved in reply to TasksRemoveLabel, got {other:?}"
                ),
            }),
        }
    }

    /// Transfer a task to a different brain (preserve-ID move).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksTransfer`].
    pub fn tasks_transfer(
        &mut self,
        params: TasksTransferParams,
    ) -> Result<(TaskSummary, String), RpcError> {
        match self.call(Request::TasksTransfer { params })? {
            Response::TasksTransfer { task, event_id } => Ok((task, event_id)),
            other => Err(RpcError::Protocol {
                message: format!("expected TasksTransfer in reply to TasksTransfer, got {other:?}"),
            }),
        }
    }

    /// Run an integrity verification pass over the records object
    /// store. Returns the wire-format [`RecordsVerifyReport`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::RecordsVerify`].
    pub fn records_verify(&mut self) -> Result<RecordsVerifyReport, RpcError> {
        match self.call(Request::RecordsVerify)? {
            Response::RecordsVerify { report } => Ok(report),
            other => Err(RpcError::Protocol {
                message: format!("expected RecordsVerify in reply to RecordsVerify, got {other:?}"),
            }),
        }
    }

    /// List analysis records via [`Request::AnalysesList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::AnalysesList`].
    pub fn analyses_list(
        &mut self,
        params: RecordsListParams,
    ) -> Result<Vec<AnalysisSummary>, RpcError> {
        match self.call(Request::AnalysesList { params })? {
            Response::AnalysesList { records } => Ok(records),
            other => Err(RpcError::Protocol {
                message: format!("expected AnalysesList in reply to AnalysesList, got {other:?}"),
            }),
        }
    }

    /// Fetch a single analysis record by ID. Returns `None` when not found.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::AnalysesShow`].
    pub fn analyses_show(&mut self, id: String) -> Result<Option<AnalysisSummary>, RpcError> {
        match self.call(Request::AnalysesShow { id })? {
            Response::AnalysesShow { record } => Ok(record),
            other => Err(RpcError::Protocol {
                message: format!("expected AnalysesShow in reply to AnalysesShow, got {other:?}"),
            }),
        }
    }

    /// Create a new analysis record. Returns the summary plus the
    /// stored blob's content hash and size.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::AnalysesCreate`].
    pub fn analyses_create(
        &mut self,
        params: RecordsCreateParams,
    ) -> Result<(AnalysisSummary, String, u64), RpcError> {
        match self.call(Request::AnalysesCreate { params })? {
            Response::AnalysesCreate {
                record,
                content_hash,
                size,
            } => Ok((record, content_hash, size)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected AnalysesCreate in reply to AnalysesCreate, got {other:?}"
                ),
            }),
        }
    }

    /// List artifact records (cross-kind read view) via
    /// [`Request::ArtifactsList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::ArtifactsList`].
    pub fn artifacts_list(
        &mut self,
        params: ArtifactsListParams,
    ) -> Result<Vec<ArtifactSummary>, RpcError> {
        match self.call(Request::ArtifactsList { params })? {
            Response::ArtifactsList { records } => Ok(records),
            other => Err(RpcError::Protocol {
                message: format!("expected ArtifactsList in reply to ArtifactsList, got {other:?}"),
            }),
        }
    }

    /// Fetch a single artifact record by ID. Returns `None` when not found.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::ArtifactsShow`].
    pub fn artifacts_show(&mut self, id: String) -> Result<Option<ArtifactSummary>, RpcError> {
        match self.call(Request::ArtifactsShow { id })? {
            Response::ArtifactsShow { record } => Ok(record),
            other => Err(RpcError::Protocol {
                message: format!("expected ArtifactsShow in reply to ArtifactsShow, got {other:?}"),
            }),
        }
    }

    /// List document records via [`Request::DocumentsList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::DocumentsList`].
    pub fn documents_list(
        &mut self,
        params: RecordsListParams,
    ) -> Result<Vec<DocumentSummary>, RpcError> {
        match self.call(Request::DocumentsList { params })? {
            Response::DocumentsList { records } => Ok(records),
            other => Err(RpcError::Protocol {
                message: format!("expected DocumentsList in reply to DocumentsList, got {other:?}"),
            }),
        }
    }

    /// Fetch a single document record by ID. Returns `None` when not found.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::DocumentsShow`].
    pub fn documents_show(&mut self, id: String) -> Result<Option<DocumentSummary>, RpcError> {
        match self.call(Request::DocumentsShow { id })? {
            Response::DocumentsShow { record } => Ok(record),
            other => Err(RpcError::Protocol {
                message: format!("expected DocumentsShow in reply to DocumentsShow, got {other:?}"),
            }),
        }
    }

    /// Create a new document record.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::DocumentsCreate`].
    pub fn documents_create(
        &mut self,
        params: RecordsCreateParams,
    ) -> Result<(DocumentSummary, String, u64), RpcError> {
        match self.call(Request::DocumentsCreate { params })? {
            Response::DocumentsCreate {
                record,
                content_hash,
                size,
            } => Ok((record, content_hash, size)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected DocumentsCreate in reply to DocumentsCreate, got {other:?}"
                ),
            }),
        }
    }

    /// List plan records via [`Request::PlansList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::PlansList`].
    pub fn plans_list(&mut self, params: RecordsListParams) -> Result<Vec<PlanSummary>, RpcError> {
        match self.call(Request::PlansList { params })? {
            Response::PlansList { records } => Ok(records),
            other => Err(RpcError::Protocol {
                message: format!("expected PlansList in reply to PlansList, got {other:?}"),
            }),
        }
    }

    /// Fetch a single plan record by ID. Returns `None` when not found.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::PlansShow`].
    pub fn plans_show(&mut self, id: String) -> Result<Option<PlanSummary>, RpcError> {
        match self.call(Request::PlansShow { id })? {
            Response::PlansShow { record } => Ok(record),
            other => Err(RpcError::Protocol {
                message: format!("expected PlansShow in reply to PlansShow, got {other:?}"),
            }),
        }
    }

    /// Create a new plan record.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::PlansCreate`].
    pub fn plans_create(
        &mut self,
        params: RecordsCreateParams,
    ) -> Result<(PlanSummary, String, u64), RpcError> {
        match self.call(Request::PlansCreate { params })? {
            Response::PlansCreate {
                record,
                content_hash,
                size,
            } => Ok((record, content_hash, size)),
            other => Err(RpcError::Protocol {
                message: format!("expected PlansCreate in reply to PlansCreate, got {other:?}"),
            }),
        }
    }

    /// List snapshot records via [`Request::SnapshotsList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SnapshotsList`].
    pub fn snapshots_list(
        &mut self,
        params: RecordsListParams,
    ) -> Result<Vec<SnapshotSummary>, RpcError> {
        match self.call(Request::SnapshotsList { params })? {
            Response::SnapshotsList { records } => Ok(records),
            other => Err(RpcError::Protocol {
                message: format!("expected SnapshotsList in reply to SnapshotsList, got {other:?}"),
            }),
        }
    }

    /// Fetch a single snapshot record by ID. Returns `None` when not found.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SnapshotsShow`].
    pub fn snapshots_show(&mut self, id: String) -> Result<Option<SnapshotSummary>, RpcError> {
        match self.call(Request::SnapshotsShow { id })? {
            Response::SnapshotsShow { record } => Ok(record),
            other => Err(RpcError::Protocol {
                message: format!("expected SnapshotsShow in reply to SnapshotsShow, got {other:?}"),
            }),
        }
    }

    /// Create (save) a new snapshot record. Mirrors
    /// `brain snapshots save`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SnapshotsCreate`].
    pub fn snapshots_create(
        &mut self,
        params: RecordsCreateParams,
    ) -> Result<(SnapshotSummary, String, u64), RpcError> {
        match self.call(Request::SnapshotsCreate { params })? {
            Response::SnapshotsCreate {
                record,
                content_hash,
                size,
            } => Ok((record, content_hash, size)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected SnapshotsCreate in reply to SnapshotsCreate, got {other:?}"
                ),
            }),
        }
    }

    /// List sagas with optional filters via [`Request::SagasList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasList`].
    pub fn sagas_list(&mut self, params: SagasListParams) -> Result<Vec<SagaSummary>, RpcError> {
        match self.call(Request::SagasList { params })? {
            Response::SagasList { sagas } => Ok(sagas),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasList in reply to SagasList, got {other:?}"),
            }),
        }
    }

    /// Fetch a saga by ID. Returns `None` when the saga does not
    /// exist on the daemon side.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasGet`].
    pub fn sagas_get(&mut self, saga_id: String) -> Result<Option<SagaSummary>, RpcError> {
        match self.call(Request::SagasGet { saga_id })? {
            Response::SagasGet { saga } => Ok(saga),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasGet in reply to SagasGet, got {other:?}"),
            }),
        }
    }

    /// Create a new saga.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasCreate`].
    pub fn sagas_create(&mut self, params: SagasCreateParams) -> Result<SagaSummary, RpcError> {
        match self.call(Request::SagasCreate { params })? {
            Response::SagasCreate { saga } => Ok(saga),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasCreate in reply to SagasCreate, got {other:?}"),
            }),
        }
    }

    /// Update a saga's title and/or description.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasUpdate`].
    pub fn sagas_update(&mut self, params: SagasUpdateParams) -> Result<SagaSummary, RpcError> {
        match self.call(Request::SagasUpdate { params })? {
            Response::SagasUpdate { saga } => Ok(saga),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasUpdate in reply to SagasUpdate, got {other:?}"),
            }),
        }
    }

    /// Add tasks to a saga. Returns `(saga_id_short, added_task_ids)`.
    /// `added_task_ids.len()` matches the `added` count in the wire
    /// response.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasAddTasks`].
    pub fn sagas_add_tasks(
        &mut self,
        saga_id: String,
        task_ids: Vec<String>,
        cascade: bool,
    ) -> Result<(String, Vec<String>), RpcError> {
        match self.call(Request::SagasAddTasks {
            saga_id,
            task_ids,
            cascade,
        })? {
            Response::SagasAddTasks {
                saga_id,
                added_task_ids,
                ..
            } => Ok((saga_id, added_task_ids)),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasAddTasks in reply to SagasAddTasks, got {other:?}"),
            }),
        }
    }

    /// Remove tasks from a saga. Returns `(saga_id_short, removed_task_ids)`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasRemoveTasks`].
    pub fn sagas_remove_tasks(
        &mut self,
        saga_id: String,
        task_ids: Vec<String>,
        cascade: bool,
    ) -> Result<(String, Vec<String>), RpcError> {
        match self.call(Request::SagasRemoveTasks {
            saga_id,
            task_ids,
            cascade,
        })? {
            Response::SagasRemoveTasks {
                saga_id,
                removed_task_ids,
                ..
            } => Ok((saga_id, removed_task_ids)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected SagasRemoveTasks in reply to SagasRemoveTasks, got {other:?}"
                ),
            }),
        }
    }

    /// Return the ready actionable member tasks for a saga.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasFrontier`].
    pub fn sagas_frontier(
        &mut self,
        saga_id: String,
    ) -> Result<(String, String, Vec<SagaFrontierTask>, Vec<SagaBrainSummary>), RpcError> {
        match self.call(Request::SagasFrontier { saga_id })? {
            Response::SagasFrontier {
                saga_id,
                saga_status,
                tasks,
                brains,
            } => Ok((saga_id, saga_status, tasks, brains)),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasFrontier in reply to SagasFrontier, got {other:?}"),
            }),
        }
    }

    /// Transition a saga from `planning` to `open`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasStart`].
    pub fn sagas_start(&mut self, saga_id: String) -> Result<SagaSummary, RpcError> {
        match self.call(Request::SagasStart { saga_id })? {
            Response::SagasStart { saga } => Ok(saga),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasStart in reply to SagasStart, got {other:?}"),
            }),
        }
    }

    /// Close a saga, optionally cascading member tasks to `done`.
    /// Returns `(saga, cascade_results)` — `cascade_results` is empty
    /// when `cascade` was `false`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasClose`].
    pub fn sagas_close(
        &mut self,
        saga_id: String,
        cascade: bool,
    ) -> Result<(SagaSummary, Vec<SagaCascadeResult>), RpcError> {
        match self.call(Request::SagasClose { saga_id, cascade })? {
            Response::SagasClose {
                saga,
                cascade_results,
                ..
            } => Ok((saga, cascade_results)),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasClose in reply to SagasClose, got {other:?}"),
            }),
        }
    }

    /// Cancel a saga, optionally cascading non-terminal member tasks
    /// to `cancelled`. Returns `(saga, cascade_results)`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasCancel`].
    pub fn sagas_cancel(
        &mut self,
        saga_id: String,
        cascade: bool,
    ) -> Result<(SagaSummary, Vec<SagaCascadeResult>), RpcError> {
        match self.call(Request::SagasCancel { saga_id, cascade })? {
            Response::SagasCancel {
                saga,
                cascade_results,
                ..
            } => Ok((saga, cascade_results)),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasCancel in reply to SagasCancel, got {other:?}"),
            }),
        }
    }

    /// Reopen a closed or cancelled saga.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasReopen`].
    pub fn sagas_reopen(&mut self, saga_id: String) -> Result<SagaSummary, RpcError> {
        match self.call(Request::SagasReopen { saga_id })? {
            Response::SagasReopen { saga } => Ok(saga),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasReopen in reply to SagasReopen, got {other:?}"),
            }),
        }
    }

    /// Return aggregated statistics for a saga.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::SagasStats`].
    pub fn sagas_stats(
        &mut self,
        saga_id: String,
    ) -> Result<
        (
            String,
            SagaStatsReport,
            Vec<SagaLabelCount>,
            Vec<SagaBrainSummary>,
        ),
        RpcError,
    > {
        match self.call(Request::SagasStats { saga_id })? {
            Response::SagasStats {
                saga_id,
                stats,
                label_histogram,
                brains,
            } => Ok((saga_id, stats, label_histogram, brains)),
            other => Err(RpcError::Protocol {
                message: format!("expected SagasStats in reply to SagasStats, got {other:?}"),
            }),
        }
    }

    /// Record an episode in the memory log. Returns `(summary_id, uri)`
    /// — the URI follows the `synapse://<brain>/memory/<id>` convention.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemoryWriteEpisode`].
    pub fn memory_write_episode(
        &mut self,
        params: MemoryWriteEpisodeParams,
    ) -> Result<(String, String), RpcError> {
        match self.call(Request::MemoryWriteEpisode { params })? {
            Response::MemoryWriteEpisode { summary_id, uri } => Ok((summary_id, uri)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected MemoryWriteEpisode in reply to MemoryWriteEpisode, got {other:?}"
                ),
            }),
        }
    }

    /// Store a procedure. Returns `(summary_id, uri)`.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemoryWriteProcedure`].
    pub fn memory_write_procedure(
        &mut self,
        params: MemoryWriteProcedureParams,
    ) -> Result<(String, String), RpcError> {
        match self.call(Request::MemoryWriteProcedure { params })? {
            Response::MemoryWriteProcedure { summary_id, uri } => Ok((summary_id, uri)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected MemoryWriteProcedure in reply to MemoryWriteProcedure, got {other:?}"
                ),
            }),
        }
    }

    /// Retrieve memory chunks via the daemon. Returns the daemon's
    /// serialized JSON result envelope — the caller deserializes it the
    /// same way it would the local-path MCP tool output.
    ///
    /// The wire type is `String` (rather than a typed struct) by design
    /// for now: the memory retrieve JSON shape is still evolving, so
    /// freezing it into a wire-format struct would force a protocol
    /// version bump on every minor change. Migrating to a typed
    /// envelope is tracked separately.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemoryRetrieve`].
    pub fn memory_retrieve(&mut self, params: MemoryRetrieveParams) -> Result<String, RpcError> {
        match self.call(Request::MemoryRetrieve { params })? {
            Response::MemoryRetrieve { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected MemoryRetrieve in reply to MemoryRetrieve, got {other:?}"
                ),
            }),
        }
    }

    /// Group recent episodes into consolidation clusters. Returns the
    /// daemon's serialized JSON report — see [`Self::memory_retrieve`]
    /// for the design rationale on the `String` wire type.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemoryConsolidate`].
    pub fn memory_consolidate(
        &mut self,
        params: MemoryConsolidateParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::MemoryConsolidate { params })? {
            Response::MemoryConsolidate { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected MemoryConsolidate in reply to MemoryConsolidate, got {other:?}"
                ),
            }),
        }
    }

    /// Generate or retrieve a scope summary. Returns the daemon's
    /// serialized JSON report.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemorySummarizeScope`].
    pub fn memory_summarize_scope(
        &mut self,
        params: MemorySummarizeScopeParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::MemorySummarizeScope { params })? {
            Response::MemorySummarizeScope { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected MemorySummarizeScope in reply to \
                     MemorySummarizeScope, got {other:?}"
                ),
            }),
        }
    }

    /// Prepare a reflection (source-material fetch) or commit a
    /// reflection — driven by [`MemoryReflectParams::commit`]. Returns
    /// the daemon's serialized JSON envelope.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemoryReflect`].
    pub fn memory_reflect(&mut self, params: MemoryReflectParams) -> Result<String, RpcError> {
        match self.call(Request::MemoryReflect { params })? {
            Response::MemoryReflect { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!("expected MemoryReflect in reply to MemoryReflect, got {other:?}"),
            }),
        }
    }

    /// List tag_aliases rows via [`Request::TagsAliasesList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TagsAliasesList`].
    pub fn tags_aliases_list(
        &mut self,
        params: TagsAliasesListParams,
    ) -> Result<Vec<TagAliasSummary>, RpcError> {
        match self.call(Request::TagsAliasesList { params })? {
            Response::TagsAliasesList { rows } => Ok(rows),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TagsAliasesList in reply to TagsAliasesList, got {other:?}"
                ),
            }),
        }
    }

    /// Get tag-clustering health summary via [`Request::TagsAliasesStatus`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TagsAliasesStatus`].
    pub fn tags_aliases_status(&mut self) -> Result<TagAliasesStatusReport, RpcError> {
        match self.call(Request::TagsAliasesStatus)? {
            Response::TagsAliasesStatus { report } => Ok(report),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TagsAliasesStatus in reply to TagsAliasesStatus, got {other:?}"
                ),
            }),
        }
    }

    /// Get job queue health summary via [`Request::JobsStatus`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::JobsStatus`].
    pub fn jobs_status(&mut self) -> Result<JobsStatusReport, RpcError> {
        match self.call(Request::JobsStatus)? {
            Response::JobsStatus { report } => Ok(report),
            other => Err(RpcError::Protocol {
                message: format!("expected JobsStatus in reply to JobsStatus, got {other:?}"),
            }),
        }
    }

    /// Get brain health status via [`Request::BrainStatus`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::BrainStatus`].
    pub fn brain_status(&mut self) -> Result<BrainStatusReport, RpcError> {
        match self.call(Request::BrainStatus)? {
            Response::BrainStatus { report } => Ok(report),
            other => Err(RpcError::Protocol {
                message: format!("expected BrainStatus in reply to BrainStatus, got {other:?}"),
            }),
        }
    }

    /// List configured providers via [`Request::ProviderList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::ProviderList`].
    pub fn provider_list(&mut self) -> Result<Vec<ProviderSummary>, RpcError> {
        match self.call(Request::ProviderList)? {
            Response::ProviderList { providers } => Ok(providers),
            other => Err(RpcError::Protocol {
                message: format!("expected ProviderList in reply to ProviderList, got {other:?}"),
            }),
        }
    }

    /// Register a filesystem path for watching via [`Request::WatchAdd`].
    /// Returns `(path, brain_name)` from [`Response::WatchAdded`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::WatchAdded`].
    pub fn watch_add(&mut self, path: String) -> Result<(String, String), RpcError> {
        match self.call(Request::WatchAdd { path })? {
            Response::WatchAdded { path, brain_name } => Ok((path, brain_name)),
            other => Err(RpcError::Protocol {
                message: format!("expected WatchAdded in reply to WatchAdd, got {other:?}"),
            }),
        }
    }

    /// Deregister a watch path via [`Request::WatchRemove`].
    /// Returns the removed path from [`Response::WatchRemoved`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::WatchRemoved`].
    pub fn watch_remove(&mut self, path: String) -> Result<String, RpcError> {
        match self.call(Request::WatchRemove { path })? {
            Response::WatchRemoved { path } => Ok(path),
            other => Err(RpcError::Protocol {
                message: format!("expected WatchRemoved in reply to WatchRemove, got {other:?}"),
            }),
        }
    }

    /// List all registered watch paths via [`Request::WatchList`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::WatchList`].
    pub fn watch_list(&mut self) -> Result<Vec<WatchSummary>, RpcError> {
        match self.call(Request::WatchList)? {
            Response::WatchList { watches } => Ok(watches),
            other => Err(RpcError::Protocol {
                message: format!("expected WatchList in reply to WatchList, got {other:?}"),
            }),
        }
    }

    // ── links / records / tasks / memory / tags / brains typed methods ──

    /// Add a polymorphic link edge via [`Request::LinksAdd`]. Returns
    /// `true` if a new edge was created, `false` if the edge already
    /// existed (idempotent insert).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::LinksAdd`].
    pub fn links_add(&mut self, params: LinksAddParams) -> Result<bool, RpcError> {
        match self.call(Request::LinksAdd { params })? {
            Response::LinksAdd { created } => Ok(created),
            other => Err(RpcError::Protocol {
                message: format!("expected LinksAdd in reply to LinksAdd, got {other:?}"),
            }),
        }
    }

    /// Remove a polymorphic link edge via [`Request::LinksRemove`].
    /// Returns `true` if an edge was removed, `false` if no matching
    /// edge existed.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::LinksRemove`].
    pub fn links_remove(&mut self, params: LinksRemoveParams) -> Result<bool, RpcError> {
        match self.call(Request::LinksRemove { params })? {
            Response::LinksRemove { removed } => Ok(removed),
            other => Err(RpcError::Protocol {
                message: format!("expected LinksRemove in reply to LinksRemove, got {other:?}"),
            }),
        }
    }

    /// List link edges incident on an entity via
    /// [`Request::LinksForEntity`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::LinksForEntity`].
    pub fn links_for_entity(
        &mut self,
        params: LinksForEntityParams,
    ) -> Result<Vec<WireLinkSummary>, RpcError> {
        match self.call(Request::LinksForEntity { params })? {
            Response::LinksForEntity { links } => Ok(links),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected LinksForEntity in reply to LinksForEntity, got {other:?}"
                ),
            }),
        }
    }

    /// Archive a record via [`Request::RecordsArchive`]. Returns the
    /// canonical record id, its synapse URI, and the post-archive
    /// status string from [`Response::RecordsArchive`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::RecordsArchive`].
    pub fn records_archive(
        &mut self,
        params: RecordsArchiveParams,
    ) -> Result<(String, String, String), RpcError> {
        match self.call(Request::RecordsArchive { params })? {
            Response::RecordsArchive {
                record_id,
                uri,
                status,
            } => Ok((record_id, uri, status)),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected RecordsArchive in reply to RecordsArchive, got {other:?}"
                ),
            }),
        }
    }

    /// Add a link from a record to another entity via
    /// [`Request::RecordsLinkAdd`]. Returns `true` if a new edge was
    /// created.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::RecordsLinkAdd`].
    pub fn records_link_add(&mut self, params: RecordsLinkParams) -> Result<bool, RpcError> {
        match self.call(Request::RecordsLinkAdd { params })? {
            Response::RecordsLinkAdd { created } => Ok(created),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected RecordsLinkAdd in reply to RecordsLinkAdd, got {other:?}"
                ),
            }),
        }
    }

    /// Remove a link from a record via [`Request::RecordsLinkRemove`].
    /// Returns `true` if an edge was removed.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::RecordsLinkRemove`].
    pub fn records_link_remove(&mut self, params: RecordsLinkParams) -> Result<bool, RpcError> {
        match self.call(Request::RecordsLinkRemove { params })? {
            Response::RecordsLinkRemove { removed } => Ok(removed),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected RecordsLinkRemove in reply to RecordsLinkRemove, got {other:?}"
                ),
            }),
        }
    }

    /// Add a tag to a record via [`Request::RecordsTagAdd`]. Returns
    /// the canonical tag string (after server-side normalisation /
    /// alias resolution).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::RecordsTagAdd`].
    pub fn records_tag_add(
        &mut self,
        record_id: String,
        tag: String,
    ) -> Result<String, RpcError> {
        match self.call(Request::RecordsTagAdd { record_id, tag })? {
            Response::RecordsTagAdd { tag } => Ok(tag),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected RecordsTagAdd in reply to RecordsTagAdd, got {other:?}"
                ),
            }),
        }
    }

    /// Remove a tag from a record via [`Request::RecordsTagRemove`].
    /// Returns `true` if the tag was present and removed.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::RecordsTagRemove`].
    pub fn records_tag_remove(
        &mut self,
        record_id: String,
        tag: String,
    ) -> Result<bool, RpcError> {
        match self.call(Request::RecordsTagRemove { record_id, tag })? {
            Response::RecordsTagRemove { removed } => Ok(removed),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected RecordsTagRemove in reply to RecordsTagRemove, got {other:?}"
                ),
            }),
        }
    }

    /// Apply a raw task event via [`Request::TasksApplyEvent`]. Returns
    /// the JSON result blob; callers parse it according to the event
    /// type (the MCP surface intentionally keeps a single tool for all
    /// task event types).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksApplyEvent`].
    pub fn tasks_apply_event(
        &mut self,
        params: TasksApplyEventParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::TasksApplyEvent { params })? {
            Response::TasksApplyEvent { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksApplyEvent in reply to TasksApplyEvent, got {other:?}"
                ),
            }),
        }
    }

    /// Batch dependency operations via [`Request::TasksDepsBatch`].
    /// Returns the opaque JSON result blob (shape varies per action).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksDepsBatch`].
    pub fn tasks_deps_batch(
        &mut self,
        params: TasksDepsBatchParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::TasksDepsBatch { params })? {
            Response::TasksDepsBatch { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksDepsBatch in reply to TasksDepsBatch, got {other:?}"
                ),
            }),
        }
    }

    /// Batch label operations via [`Request::TasksLabelsBatch`].
    /// Returns the opaque JSON result blob.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksLabelsBatch`].
    pub fn tasks_labels_batch(
        &mut self,
        params: TasksLabelsBatchParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::TasksLabelsBatch { params })? {
            Response::TasksLabelsBatch { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksLabelsBatch in reply to TasksLabelsBatch, got {other:?}"
                ),
            }),
        }
    }

    /// Return all unique labels with counts and associated task IDs via
    /// [`Request::TasksLabelsSummary`].
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TasksLabelsSummary`].
    pub fn tasks_labels_summary(
        &mut self,
    ) -> Result<Vec<WireTaskLabelSummary>, RpcError> {
        match self.call(Request::TasksLabelsSummary)? {
            Response::TasksLabelsSummary { labels } => Ok(labels),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TasksLabelsSummary in reply to TasksLabelsSummary, got {other:?}"
                ),
            }),
        }
    }

    /// Walk a `continues` thread starting from an episode or procedure
    /// via [`Request::MemoryWalkThread`]. Returns the opaque JSON result
    /// blob (the MCP surface keeps the shape).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::MemoryWalkThread`].
    pub fn memory_walk_thread(
        &mut self,
        params: MemoryWalkThreadParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::MemoryWalkThread { params })? {
            Response::MemoryWalkThread { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected MemoryWalkThread in reply to MemoryWalkThread, got {other:?}"
                ),
            }),
        }
    }

    /// Recluster tag synonyms via [`Request::TagsRecluster`]. Returns
    /// the opaque JSON result blob (move counts, cluster summaries,
    /// or a dry-run preview depending on the params).
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::TagsRecluster`].
    pub fn tags_recluster(
        &mut self,
        params: TagsReclusterParams,
    ) -> Result<String, RpcError> {
        match self.call(Request::TagsRecluster { params })? {
            Response::TagsRecluster { result_json } => Ok(result_json),
            other => Err(RpcError::Protocol {
                message: format!(
                    "expected TagsRecluster in reply to TagsRecluster, got {other:?}"
                ),
            }),
        }
    }

    /// List all registered brain projects via [`Request::BrainsList`].
    /// Returns `(brains, count)` from [`Response::BrainsList`] —
    /// `count` matches `brains.len()` and is mirrored on the wire for
    /// CLI / MCP output parity.
    ///
    /// # Errors
    ///
    /// - [`RpcError::Protocol`] — the daemon replied with anything other
    ///   than [`Response::BrainsList`].
    pub fn brains_list(
        &mut self,
        params: BrainsListParams,
    ) -> Result<(Vec<WireBrainSummary>, u32), RpcError> {
        match self.call(Request::BrainsList { params })? {
            Response::BrainsList { brains, count } => Ok((brains, count)),
            other => Err(RpcError::Protocol {
                message: format!("expected BrainsList in reply to BrainsList, got {other:?}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::InMemoryTransport;

    // ── connect() path ────────────────────────────────────────────────
    //
    // Note: `DaemonClient` deliberately does not implement `Debug` (its
    // generic `T: Transport` parameter would force every implementor to
    // be `Debug` too, and InMemoryTransport's Box<dyn FnMut> is not).
    // Test assertions pattern-match on the error variant directly rather
    // than catching the whole Result, so the formatting never needs to
    // render the Ok side.

    #[test]
    fn connect_succeeds_with_matching_version() {
        let _client = DaemonClient::connect(InMemoryTransport::echo()).expect("connect");
    }

    #[test]
    fn connect_returns_version_mismatch_on_disagreeing_server() {
        let result = DaemonClient::connect(InMemoryTransport::new(|req| match req {
            Request::Handshake { .. } => Ok(Response::HandshakeOk { server_version: 99 }),
            _ => Ok(Response::Pong),
        }));
        match result {
            Ok(_) => panic!("expected VersionMismatch, got Ok"),
            Err(RpcError::VersionMismatch { client, server }) => {
                assert_eq!(client, PROTOCOL_VERSION);
                assert_eq!(server, 99);
            }
            Err(other) => panic!("expected VersionMismatch, got Err({other:?})"),
        }
    }

    #[test]
    fn connect_returns_protocol_error_on_wrong_reply_shape() {
        // Daemon replies Pong to a Handshake — protocol violation.
        let result = DaemonClient::connect(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match result {
            Ok(_) => panic!("expected Protocol error, got Ok"),
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("HandshakeOk"),
                    "message should mention HandshakeOk, got: {message}"
                );
            }
            Err(other) => panic!("expected Protocol error, got Err({other:?})"),
        }
    }

    #[test]
    fn connect_propagates_underlying_transport_error() {
        let result = DaemonClient::connect(InMemoryTransport::new(|_| {
            Err(RpcError::Transport {
                message: "synthetic".into(),
            })
        }));
        match result {
            Ok(_) => panic!("expected Transport error, got Ok"),
            Err(RpcError::Transport { message }) => assert_eq!(message, "synthetic"),
            Err(other) => panic!("expected Transport error, got Err({other:?})"),
        }
    }

    #[test]
    fn connect_then_call_returns_response() {
        let mut client = DaemonClient::connect(InMemoryTransport::echo()).expect("connect");
        assert_eq!(client.call(Request::Ping).unwrap(), Response::Pong);
    }

    // ── from_transport() (pub(crate)) bypass path ─────────────────────

    #[test]
    fn from_transport_skips_handshake() {
        // Echo handles both Handshake and Ping. With from_transport,
        // no handshake is sent, so the FIRST call goes straight through.
        let mut client = DaemonClient::from_transport(InMemoryTransport::echo());
        assert_eq!(client.call(Request::Ping).unwrap(), Response::Pong);
    }

    #[test]
    fn from_transport_propagates_errors_unchanged() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|_| {
            Err(RpcError::NotFound { id: "x".into() })
        }));
        match client.call(Request::Ping) {
            Err(RpcError::NotFound { id }) => assert_eq!(id, "x"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn from_transport_threads_handler_state_via_fnmut() {
        let mut count = 0;
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(move |_| {
            count += 1;
            Err(RpcError::NotFound {
                id: format!("call-{count}"),
            })
        }));
        for expected in 1..=3 {
            match client.call(Request::Ping) {
                Err(RpcError::NotFound { id }) => assert_eq!(id, format!("call-{expected}")),
                other => panic!("expected NotFound, got {other:?}"),
            }
        }
    }

    // ── typed-method coverage ─────────────────────────────────────────
    //
    // These tests use `from_transport` to skip the handshake — we're
    // exercising the variant-narrowing logic in the typed wrappers, not
    // re-testing connect(). The public-facing integration test in
    // tests/client_in_memory.rs covers the happy path through `connect`.

    #[test]
    fn ping_returns_ok_on_pong() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::Ping => Ok(Response::Pong),
            _ => panic!("unexpected request: {req:?}"),
        }));
        client.ping().expect("ping should succeed");
    }

    #[test]
    fn ping_returns_protocol_error_on_wrong_response_shape() {
        // Daemon replies HandshakeOk to a Ping — protocol violation.
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|_| {
            Ok(Response::HandshakeOk {
                server_version: PROTOCOL_VERSION,
            })
        }));
        match client.ping() {
            Ok(()) => panic!("expected Protocol error, got Ok"),
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("Pong"),
                    "message should mention Pong, got: {message}"
                );
            }
            Err(other) => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn ping_propagates_transport_error() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|_| {
            Err(RpcError::Transport {
                message: "synthetic".into(),
            })
        }));
        match client.ping() {
            Err(RpcError::Transport { message }) => assert_eq!(message, "synthetic"),
            other => panic!("expected Transport, got {other:?}"),
        }
    }

    #[test]
    fn tasks_list_returns_unwrapped_payload() {
        let expected = vec![
            TaskSummary {
                task_id: "brn-001".into(),
                title: "first".into(),
                status: "open".into(),
                priority: 0,
                brain_id: "eAx".into(),
            },
            TaskSummary {
                task_id: "brn-002".into(),
                title: "second".into(),
                status: "in_progress".into(),
                priority: 1,
                brain_id: "eAx".into(),
            },
        ];
        let expected_clone = expected.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksList { .. } => Ok(Response::TasksList {
                    tasks: expected_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .tasks_list(TasksListParams::default())
            .expect("tasks_list");
        assert_eq!(got, expected);
    }

    #[test]
    fn tasks_list_forwards_params_unchanged() {
        // Verify the params struct round-trips through the wire wrapper
        // without the typed method silently dropping fields.
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksList { params } => {
                assert_eq!(params.status.as_deref(), Some("open"));
                assert_eq!(params.priority, Some(2));
                assert_eq!(params.limit, Some(50));
                assert_eq!(params.search.as_deref(), Some("daemon"));
                Ok(Response::TasksList { tasks: vec![] })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let params = TasksListParams {
            status: Some("open".into()),
            priority: Some(2),
            limit: Some(50),
            search: Some("daemon".into()),
        };
        let got = client.tasks_list(params).expect("tasks_list");
        assert!(got.is_empty());
    }

    #[test]
    fn tasks_list_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_list(TasksListParams::default()) {
            Ok(_) => panic!("expected Protocol error, got Ok"),
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("TasksList"),
                    "message should mention TasksList, got: {message}"
                );
            }
            Err(other) => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn tasks_list_propagates_dispatcher_error() {
        // A dispatcher rejecting a bad filter today returns RpcError::Unknown
        // (the wire-level error envelope ticket will swap this for a more
        // specific variant). The typed wrapper must surface it unchanged.
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|_| {
            Err(RpcError::Unknown {
                message: "unknown status".into(),
            })
        }));
        match client.tasks_list(TasksListParams::default()) {
            Err(RpcError::Unknown { message }) => assert_eq!(message, "unknown status"),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    fn sample_summary() -> TaskSummary {
        TaskSummary {
            task_id: "brn-2fe.27".into(),
            title: "vertical slice".into(),
            status: "in_progress".into(),
            priority: 0,
            brain_id: "eAx_dEFA".into(),
        }
    }

    // ── tasks_show ─────────────────────────────────────────────

    #[test]
    fn tasks_show_returns_some_payload() {
        let summary = sample_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksShow { .. } => Ok(Response::TasksShow {
                    task: Some(summary_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.tasks_show("brn-2fe.27".into()).expect("tasks_show");
        assert_eq!(got, Some(summary));
    }

    #[test]
    fn tasks_show_returns_none_when_not_found() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|_| {
            Ok(Response::TasksShow { task: None })
        }));
        let got = client.tasks_show("missing".into()).expect("tasks_show");
        assert!(got.is_none());
    }

    #[test]
    fn tasks_show_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_show("x".into()) {
            Ok(_) => panic!("expected Protocol error, got Ok"),
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("TasksShow"),
                    "message should mention TasksShow, got: {message}"
                );
            }
            Err(other) => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_next ─────────────────────────────────────────────

    #[test]
    fn tasks_next_returns_some_payload() {
        let summary = sample_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksNext => Ok(Response::TasksNext {
                    task: Some(summary_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.tasks_next().expect("tasks_next");
        assert_eq!(got, Some(summary));
    }

    #[test]
    fn tasks_next_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_next() {
            Ok(_) => panic!("expected Protocol error, got Ok"),
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("TasksNext"),
                    "message should mention TasksNext, got: {message}"
                );
            }
            Err(other) => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_create ───────────────────────────────────────────

    fn sample_create_params() -> TasksCreateParams {
        TasksCreateParams {
            title: "new task".into(),
            description: None,
            priority: 2,
            task_type: "task".into(),
            assignee: None,
            parent: None,
        }
    }

    #[test]
    fn tasks_create_returns_summary_and_event_id() {
        let summary = sample_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksCreate { params } => {
                    assert_eq!(params.title, "new task");
                    Ok(Response::TasksCreate {
                        task: summary_clone.clone(),
                        event_id: "evt-123".into(),
                    })
                }
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (task, event_id) = client
            .tasks_create(sample_create_params())
            .expect("tasks_create");
        assert_eq!(task, summary);
        assert_eq!(event_id, "evt-123");
    }

    #[test]
    fn tasks_create_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_create(sample_create_params()) {
            Ok(_) => panic!("expected Protocol error, got Ok"),
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("TasksCreate"),
                    "message should mention TasksCreate, got: {message}"
                );
            }
            Err(other) => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_update ───────────────────────────────────────────

    fn sample_update_params() -> TasksUpdateParams {
        TasksUpdateParams {
            id: "brn-2fe.27".into(),
            title: Some("renamed".into()),
            description: None,
            priority: None,
            assignee: None,
        }
    }

    #[test]
    fn tasks_update_returns_summary_and_event_id() {
        let summary = sample_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksUpdate { .. } => Ok(Response::TasksUpdate {
                    task: summary_clone.clone(),
                    event_id: "evt".into(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (task, event_id) = client
            .tasks_update(sample_update_params())
            .expect("tasks_update");
        assert_eq!(task, summary);
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_update_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_update(sample_update_params()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksUpdate"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_mutate ───────────────────────────────────────────

    #[test]
    fn tasks_mutate_returns_summary_and_event_id() {
        let summary = sample_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksMutate { params } => {
                    assert_eq!(params.action, "close");
                    Ok(Response::TasksMutate {
                        task: summary_clone.clone(),
                        event_id: "evt".into(),
                    })
                }
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (task, event_id) = client
            .tasks_mutate(TasksMutateParams {
                id: "brn-2fe.27".into(),
                action: "close".into(),
            })
            .expect("tasks_mutate");
        assert_eq!(task, summary);
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_mutate_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_mutate(TasksMutateParams {
            id: "x".into(),
            action: "close".into(),
        }) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksMutate"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_add_dep / tasks_remove_dep ───────────────────────

    #[test]
    fn tasks_add_dep_returns_event_id() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksAddDep { .. } => Ok(Response::TasksDepAdded {
                event_id: "evt".into(),
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let event_id = client
            .tasks_add_dep("a".into(), "b".into())
            .expect("tasks_add_dep");
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_add_dep_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_add_dep("a".into(), "b".into()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksDepAdded"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn tasks_remove_dep_returns_event_id() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksRemoveDep { .. } => Ok(Response::TasksDepRemoved {
                event_id: "evt".into(),
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let event_id = client
            .tasks_remove_dep("a".into(), "b".into())
            .expect("tasks_remove_dep");
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_remove_dep_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_remove_dep("a".into(), "b".into()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksDepRemoved"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_add_label / tasks_remove_label ───────────────────

    #[test]
    fn tasks_add_label_returns_event_id() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksAddLabel { .. } => Ok(Response::TasksLabelAdded {
                event_id: "evt".into(),
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let event_id = client
            .tasks_add_label("a".into(), "blocked".into())
            .expect("tasks_add_label");
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_add_label_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_add_label("a".into(), "blocked".into()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksLabelAdded"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn tasks_remove_label_returns_event_id() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksRemoveLabel { .. } => Ok(Response::TasksLabelRemoved {
                event_id: "evt".into(),
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let event_id = client
            .tasks_remove_label("a".into(), "blocked".into())
            .expect("tasks_remove_label");
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_remove_label_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_remove_label("a".into(), "blocked".into()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksLabelRemoved"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── tasks_transfer ─────────────────────────────────────────

    #[test]
    fn tasks_transfer_returns_summary_and_event_id() {
        let summary = sample_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::TasksTransfer { params } => {
                    assert_eq!(params.task_id, "brn-2fe.27");
                    assert_eq!(params.target_brain, "other");
                    Ok(Response::TasksTransfer {
                        task: summary_clone.clone(),
                        event_id: "evt".into(),
                    })
                }
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (task, event_id) = client
            .tasks_transfer(TasksTransferParams {
                task_id: "brn-2fe.27".into(),
                target_brain: "other".into(),
            })
            .expect("tasks_transfer");
        assert_eq!(task, summary);
        assert_eq!(event_id, "evt");
    }

    #[test]
    fn tasks_transfer_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.tasks_transfer(TasksTransferParams {
            task_id: "x".into(),
            target_brain: "other".into(),
        }) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("TasksTransfer"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── records_verify ─────────────────────────────────────────

    #[test]
    fn records_verify_returns_report() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::RecordsVerify => Ok(Response::RecordsVerify {
                report: RecordsVerifyReport {
                    clean: true,
                    records_checked: 1,
                    blobs_checked: 1,
                    missing: 0,
                    corrupt: 0,
                    orphans: 0,
                    stale_flags: 0,
                },
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let report = client.records_verify().expect("records_verify");
        assert!(report.clean);
        assert_eq!(report.records_checked, 1);
    }

    #[test]
    fn records_verify_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.records_verify() {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("RecordsVerify"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── analyses ───────────────────────────────────────────────

    fn sample_analysis_summary() -> AnalysisSummary {
        AnalysisSummary {
            record_id: "BRN-01J".into(),
            title: "title".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    fn sample_records_create_params() -> RecordsCreateParams {
        RecordsCreateParams {
            title: "t".into(),
            description: None,
            body: b"x".to_vec(),
            media_type: Some("text/plain".into()),
            task_id: None,
            tags: vec![],
            brain: None,
        }
    }

    #[test]
    fn analyses_list_returns_records() {
        let want = vec![sample_analysis_summary()];
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::AnalysesList { .. } => Ok(Response::AnalysesList {
                    records: want_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .analyses_list(RecordsListParams::default())
            .expect("analyses_list");
        assert_eq!(got, want);
    }

    #[test]
    fn analyses_list_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.analyses_list(RecordsListParams::default()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("AnalysesList")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn analyses_show_returns_some_payload() {
        let want = sample_analysis_summary();
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::AnalysesShow { .. } => Ok(Response::AnalysesShow {
                    record: Some(want_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.analyses_show("BRN-01J".into()).expect("show");
        assert_eq!(got, Some(want));
    }

    #[test]
    fn analyses_show_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.analyses_show("x".into()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("AnalysesShow")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn analyses_create_returns_summary_and_hash() {
        let summary = sample_analysis_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::AnalysesCreate { .. } => Ok(Response::AnalysesCreate {
                    record: summary_clone.clone(),
                    content_hash: "h".into(),
                    size: 1,
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (got, hash, size) = client
            .analyses_create(sample_records_create_params())
            .expect("create");
        assert_eq!(got, summary);
        assert_eq!(hash, "h");
        assert_eq!(size, 1);
    }

    #[test]
    fn analyses_create_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.analyses_create(sample_records_create_params()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("AnalysesCreate")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── artifacts ──────────────────────────────────────────────

    fn sample_artifact_summary() -> ArtifactSummary {
        ArtifactSummary {
            record_id: "BRN-01J".into(),
            title: "t".into(),
            kind: "document".into(),
            status: "active".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    #[test]
    fn artifacts_list_returns_records() {
        let want = vec![sample_artifact_summary()];
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::ArtifactsList { .. } => Ok(Response::ArtifactsList {
                    records: want_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .artifacts_list(ArtifactsListParams::default())
            .expect("artifacts_list");
        assert_eq!(got, want);
    }

    #[test]
    fn artifacts_list_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.artifacts_list(ArtifactsListParams::default()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("ArtifactsList")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn artifacts_show_returns_some_payload() {
        let want = sample_artifact_summary();
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::ArtifactsShow { .. } => Ok(Response::ArtifactsShow {
                    record: Some(want_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.artifacts_show("BRN-01J".into()).expect("show");
        assert_eq!(got, Some(want));
    }

    #[test]
    fn artifacts_show_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.artifacts_show("x".into()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("ArtifactsShow")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── documents ──────────────────────────────────────────────

    fn sample_document_summary() -> DocumentSummary {
        DocumentSummary {
            record_id: "BRN-01J".into(),
            title: "doc".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    #[test]
    fn documents_list_returns_records() {
        let want = vec![sample_document_summary()];
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::DocumentsList { .. } => Ok(Response::DocumentsList {
                    records: want_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .documents_list(RecordsListParams::default())
            .expect("documents_list");
        assert_eq!(got, want);
    }

    #[test]
    fn documents_show_returns_some_payload() {
        let want = sample_document_summary();
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::DocumentsShow { .. } => Ok(Response::DocumentsShow {
                    record: Some(want_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.documents_show("BRN-01J".into()).expect("show");
        assert_eq!(got, Some(want));
    }

    #[test]
    fn documents_create_returns_summary_and_hash() {
        let summary = sample_document_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::DocumentsCreate { .. } => Ok(Response::DocumentsCreate {
                    record: summary_clone.clone(),
                    content_hash: "h".into(),
                    size: 1,
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (got, hash, size) = client
            .documents_create(sample_records_create_params())
            .expect("create");
        assert_eq!(got, summary);
        assert_eq!(hash, "h");
        assert_eq!(size, 1);
    }

    #[test]
    fn documents_create_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.documents_create(sample_records_create_params()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("DocumentsCreate")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── plans ──────────────────────────────────────────────────

    fn sample_plan_summary() -> PlanSummary {
        PlanSummary {
            record_id: "BRN-01J".into(),
            title: "plan".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    #[test]
    fn plans_list_returns_records() {
        let want = vec![sample_plan_summary()];
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::PlansList { .. } => Ok(Response::PlansList {
                    records: want_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .plans_list(RecordsListParams::default())
            .expect("plans_list");
        assert_eq!(got, want);
    }

    #[test]
    fn plans_show_returns_some_payload() {
        let want = sample_plan_summary();
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::PlansShow { .. } => Ok(Response::PlansShow {
                    record: Some(want_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.plans_show("BRN-01J".into()).expect("show");
        assert_eq!(got, Some(want));
    }

    #[test]
    fn plans_create_returns_summary_and_hash() {
        let summary = sample_plan_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::PlansCreate { .. } => Ok(Response::PlansCreate {
                    record: summary_clone.clone(),
                    content_hash: "h".into(),
                    size: 1,
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (got, hash, size) = client
            .plans_create(sample_records_create_params())
            .expect("create");
        assert_eq!(got, summary);
        assert_eq!(hash, "h");
        assert_eq!(size, 1);
    }

    // ── snapshots ──────────────────────────────────────────────

    fn sample_snapshot_summary() -> SnapshotSummary {
        SnapshotSummary {
            record_id: "BRN-01J".into(),
            title: "snap".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            brain_id: "eAx_dEFA".into(),
        }
    }

    #[test]
    fn snapshots_list_returns_records() {
        let want = vec![sample_snapshot_summary()];
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SnapshotsList { .. } => Ok(Response::SnapshotsList {
                    records: want_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .snapshots_list(RecordsListParams::default())
            .expect("snapshots_list");
        assert_eq!(got, want);
    }

    #[test]
    fn snapshots_show_returns_some_payload() {
        let want = sample_snapshot_summary();
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SnapshotsShow { .. } => Ok(Response::SnapshotsShow {
                    record: Some(want_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.snapshots_show("BRN-01J".into()).expect("show");
        assert_eq!(got, Some(want));
    }

    #[test]
    fn snapshots_create_returns_summary_and_hash() {
        let summary = sample_snapshot_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SnapshotsCreate { .. } => Ok(Response::SnapshotsCreate {
                    record: summary_clone.clone(),
                    content_hash: "h".into(),
                    size: 1,
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (got, hash, size) = client
            .snapshots_create(sample_records_create_params())
            .expect("create");
        assert_eq!(got, summary);
        assert_eq!(hash, "h");
        assert_eq!(size, 1);
    }

    #[test]
    fn snapshots_create_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.snapshots_create(sample_records_create_params()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SnapshotsCreate")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── sagas ──────────────────────────────────────────────────

    fn sample_saga_summary() -> SagaSummary {
        SagaSummary {
            saga_id: "saga-abc".into(),
            title: "t".into(),
            description: None,
            status: "planning".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
            updated_at: "2026-05-17T00:00:00Z".into(),
            closed_at: None,
        }
    }

    #[test]
    fn sagas_list_returns_unwrapped_payload() {
        let want = vec![sample_saga_summary()];
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasList { .. } => Ok(Response::SagasList {
                    sagas: want_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .sagas_list(SagasListParams::default())
            .expect("sagas_list");
        assert_eq!(got, want);
    }

    #[test]
    fn sagas_list_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_list(SagasListParams::default()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasList")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_get_returns_some_payload() {
        let want = sample_saga_summary();
        let want_clone = want.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasGet { .. } => Ok(Response::SagasGet {
                    saga: Some(want_clone.clone()),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.sagas_get("saga-abc".into()).expect("sagas_get");
        assert_eq!(got, Some(want));
    }

    #[test]
    fn sagas_get_returns_none_when_not_found() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|_| {
            Ok(Response::SagasGet { saga: None })
        }));
        assert!(client.sagas_get("missing".into()).unwrap().is_none());
    }

    #[test]
    fn sagas_get_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_get("x".into()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasGet")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_create_returns_summary() {
        let summary = sample_saga_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasCreate { params } => {
                    assert_eq!(params.title, "Q4");
                    Ok(Response::SagasCreate {
                        saga: summary_clone.clone(),
                    })
                }
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .sagas_create(SagasCreateParams {
                title: "Q4".into(),
                description: None,
            })
            .expect("sagas_create");
        assert_eq!(got, summary);
    }

    #[test]
    fn sagas_create_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_create(SagasCreateParams {
            title: "t".into(),
            description: None,
        }) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasCreate")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_update_returns_summary() {
        let summary = sample_saga_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasUpdate { .. } => Ok(Response::SagasUpdate {
                    saga: summary_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .sagas_update(SagasUpdateParams {
                saga_id: "saga-abc".into(),
                title: Some("new".into()),
                description: None,
            })
            .expect("sagas_update");
        assert_eq!(got, summary);
    }

    #[test]
    fn sagas_update_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_update(SagasUpdateParams {
            saga_id: "saga-abc".into(),
            title: Some("t".into()),
            description: None,
        }) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasUpdate")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_add_tasks_forwards_inputs_and_returns_ids() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::SagasAddTasks {
                saga_id,
                task_ids,
                cascade,
            } => {
                assert_eq!(saga_id, "saga-abc");
                assert_eq!(task_ids, vec!["brn-2fe.27".to_string()]);
                assert!(cascade);
                Ok(Response::SagasAddTasks {
                    saga_id: "saga-abc".into(),
                    added: 1,
                    added_task_ids: vec!["brn-2fe.27".into()],
                })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let (saga_id, added) = client
            .sagas_add_tasks("saga-abc".into(), vec!["brn-2fe.27".into()], true)
            .expect("sagas_add_tasks");
        assert_eq!(saga_id, "saga-abc");
        assert_eq!(added, vec!["brn-2fe.27".to_string()]);
    }

    #[test]
    fn sagas_add_tasks_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_add_tasks("saga-abc".into(), vec![], false) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasAddTasks")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_remove_tasks_returns_ids() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::SagasRemoveTasks { .. } => Ok(Response::SagasRemoveTasks {
                saga_id: "saga-abc".into(),
                removed: 1,
                removed_task_ids: vec!["brn-2fe.27".into()],
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let (saga_id, removed) = client
            .sagas_remove_tasks("saga-abc".into(), vec!["brn-2fe.27".into()], false)
            .expect("sagas_remove_tasks");
        assert_eq!(saga_id, "saga-abc");
        assert_eq!(removed, vec!["brn-2fe.27".to_string()]);
    }

    #[test]
    fn sagas_remove_tasks_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_remove_tasks("saga-abc".into(), vec![], false) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasRemoveTasks")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_frontier_returns_tasks_and_brains() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::SagasFrontier { .. } => Ok(Response::SagasFrontier {
                saga_id: "saga-abc".into(),
                saga_status: "open".into(),
                tasks: vec![SagaFrontierTask {
                    task_id: "brn-2fe.27".into(),
                    title: "t".into(),
                    status: "open".into(),
                    priority: 0,
                    task_type: "task".into(),
                }],
                brains: vec![SagaBrainSummary {
                    brain_id: "b".into(),
                    name: "Brain".into(),
                    prefix: None,
                }],
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let (saga_id, status, tasks, brains) = client
            .sagas_frontier("saga-abc".into())
            .expect("sagas_frontier");
        assert_eq!(saga_id, "saga-abc");
        assert_eq!(status, "open");
        assert_eq!(tasks.len(), 1);
        assert_eq!(brains.len(), 1);
    }

    #[test]
    fn sagas_frontier_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.sagas_frontier("saga-abc".into()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("SagasFrontier")),
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn sagas_start_returns_summary() {
        let summary = sample_saga_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasStart { .. } => Ok(Response::SagasStart {
                    saga: summary_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client.sagas_start("saga-abc".into()).expect("sagas_start");
        assert_eq!(got, summary);
    }

    #[test]
    fn sagas_close_returns_summary_and_cascade() {
        let summary = sample_saga_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasClose { cascade, .. } => {
                    assert!(cascade);
                    Ok(Response::SagasClose {
                        saga: summary_clone.clone(),
                        cascade: true,
                        cascade_results: vec![SagaCascadeResult {
                            task_id: "brn-2fe.27".into(),
                            outcome: crate::domain::SagaCascadeOutcome::Closed,
                        }],
                    })
                }
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (saga, results) = client
            .sagas_close("saga-abc".into(), true)
            .expect("sagas_close");
        assert_eq!(saga, summary);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn sagas_cancel_returns_summary_and_cascade() {
        let summary = sample_saga_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasCancel { .. } => Ok(Response::SagasCancel {
                    saga: summary_clone.clone(),
                    cascade: false,
                    cascade_results: vec![],
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let (saga, results) = client
            .sagas_cancel("saga-abc".into(), false)
            .expect("sagas_cancel");
        assert_eq!(saga, summary);
        assert!(results.is_empty());
    }

    #[test]
    fn sagas_reopen_returns_summary() {
        let summary = sample_saga_summary();
        let summary_clone = summary.clone();
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(move |req| match req {
                Request::SagasReopen { .. } => Ok(Response::SagasReopen {
                    saga: summary_clone.clone(),
                }),
                _ => panic!("unexpected request: {req:?}"),
            }));
        let got = client
            .sagas_reopen("saga-abc".into())
            .expect("sagas_reopen");
        assert_eq!(got, summary);
    }

    #[test]
    fn sagas_stats_returns_report_and_aggregates() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::SagasStats { .. } => Ok(Response::SagasStats {
                saga_id: "saga-abc".into(),
                stats: SagaStatsReport {
                    total: 3,
                    open: 1,
                    in_progress: 1,
                    blocked: 0,
                    done: 1,
                    cancelled: 0,
                    orphan: 0,
                    completion_pct: Some(33.3),
                },
                label_histogram: vec![SagaLabelCount {
                    label: "p0".into(),
                    count: 1,
                }],
                brains: vec![SagaBrainSummary {
                    brain_id: "b".into(),
                    name: "Brain".into(),
                    prefix: None,
                }],
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let (saga_id, stats, labels, brains) =
            client.sagas_stats("saga-abc".into()).expect("sagas_stats");
        assert_eq!(saga_id, "saga-abc");
        assert_eq!(stats.total, 3);
        assert_eq!(labels.len(), 1);
        assert_eq!(brains.len(), 1);
    }

    // ── memory typed-method coverage ──────────────────────────

    fn sample_episode_params() -> MemoryWriteEpisodeParams {
        MemoryWriteEpisodeParams {
            goal: "test goal".into(),
            actions: "test actions".into(),
            outcome: "test outcome".into(),
            tags: vec!["unit-test".into()],
            importance_millis: 500,
        }
    }

    #[test]
    fn memory_write_episode_returns_summary_and_uri() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemoryWriteEpisode { params } => {
                assert_eq!(params.goal, "test goal");
                assert_eq!(params.importance_millis, 500);
                Ok(Response::MemoryWriteEpisode {
                    summary_id: "sid-1".into(),
                    uri: "synapse://b/memory/sid-1".into(),
                })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let (summary_id, uri) = client
            .memory_write_episode(sample_episode_params())
            .expect("memory_write_episode");
        assert_eq!(summary_id, "sid-1");
        assert_eq!(uri, "synapse://b/memory/sid-1");
    }

    #[test]
    fn memory_write_episode_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.memory_write_episode(sample_episode_params()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("MemoryWriteEpisode"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    fn sample_procedure_params() -> MemoryWriteProcedureParams {
        MemoryWriteProcedureParams {
            title: "doit".into(),
            steps: "1. step".into(),
            tags: vec![],
            importance_millis: 750,
        }
    }

    #[test]
    fn memory_write_procedure_returns_summary_and_uri() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemoryWriteProcedure { .. } => Ok(Response::MemoryWriteProcedure {
                summary_id: "p-1".into(),
                uri: "synapse://b/procedure/p-1".into(),
            }),
            _ => panic!("unexpected request: {req:?}"),
        }));
        let (summary_id, uri) = client
            .memory_write_procedure(sample_procedure_params())
            .expect("memory_write_procedure");
        assert_eq!(summary_id, "p-1");
        assert_eq!(uri, "synapse://b/procedure/p-1");
    }

    #[test]
    fn memory_write_procedure_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.memory_write_procedure(sample_procedure_params()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("MemoryWriteProcedure"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn memory_retrieve_returns_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemoryRetrieve { params } => {
                assert_eq!(params.lod, "L2");
                Ok(Response::MemoryRetrieve {
                    result_json: r#"{"results":[]}"#.into(),
                })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let params = MemoryRetrieveParams {
            query: Some("hello".into()),
            lod: "L2".into(),
            count: 5,
            strategy: "hybrid".into(),
            ..Default::default()
        };
        let got = client.memory_retrieve(params).expect("memory_retrieve");
        assert_eq!(got, r#"{"results":[]}"#);
    }

    #[test]
    fn memory_retrieve_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.memory_retrieve(MemoryRetrieveParams::default()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("MemoryRetrieve"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn memory_consolidate_returns_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemoryConsolidate { params } => {
                assert_eq!(params.limit, 10);
                Ok(Response::MemoryConsolidate {
                    result_json: r#"{"cluster_count":0}"#.into(),
                })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let got = client
            .memory_consolidate(MemoryConsolidateParams {
                limit: 10,
                gap_seconds: 600,
                auto_summarize: false,
            })
            .expect("memory_consolidate");
        assert_eq!(got, r#"{"cluster_count":0}"#);
    }

    #[test]
    fn memory_consolidate_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.memory_consolidate(MemoryConsolidateParams {
            limit: 1,
            gap_seconds: 60,
            auto_summarize: false,
        }) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("MemoryConsolidate"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn memory_summarize_scope_returns_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemorySummarizeScope { params } => {
                assert_eq!(params.scope_type, "tag");
                assert_eq!(params.scope_value, "rust");
                Ok(Response::MemorySummarizeScope {
                    result_json: r#"{"content":"..."}"#.into(),
                })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let got = client
            .memory_summarize_scope(MemorySummarizeScopeParams {
                scope_type: "tag".into(),
                scope_value: "rust".into(),
                regenerate: false,
                async_llm: false,
            })
            .expect("memory_summarize_scope");
        assert_eq!(got, r#"{"content":"..."}"#);
    }

    #[test]
    fn memory_summarize_scope_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.memory_summarize_scope(MemorySummarizeScopeParams {
            scope_type: "tag".into(),
            scope_value: "x".into(),
            regenerate: false,
            async_llm: false,
        }) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("MemorySummarizeScope"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    #[test]
    fn memory_reflect_returns_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemoryReflect { params } => {
                assert!(params.commit);
                Ok(Response::MemoryReflect {
                    result_json: r#"{"mode":"commit"}"#.into(),
                })
            }
            _ => panic!("unexpected request: {req:?}"),
        }));
        let params = MemoryReflectParams {
            commit: true,
            title: Some("t".into()),
            content: Some("c".into()),
            source_ids: vec!["s1".into()],
            importance_millis: Some(800),
            ..Default::default()
        };
        let got = client.memory_reflect(params).expect("memory_reflect");
        assert_eq!(got, r#"{"mode":"commit"}"#);
    }

    #[test]
    fn memory_reflect_returns_protocol_error_on_wrong_response_shape() {
        let mut client =
            DaemonClient::from_transport(InMemoryTransport::new(|_| Ok(Response::Pong)));
        match client.memory_reflect(MemoryReflectParams::default()) {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("MemoryReflect"));
            }
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    // ── watch ─────────────────────────────────────────────────────────

    #[test]
    fn watch_add_returns_path_and_brain_name() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::WatchAdd { path } => {
                assert_eq!(path, "/notes");
                Ok(Response::WatchAdded {
                    path: "/notes".into(),
                    brain_name: "default".into(),
                })
            }
            other => Err(RpcError::Unknown {
                message: format!("unexpected request: {other:?}"),
            }),
        }));

        let (path, brain_name) = client.watch_add("/notes".into()).expect("watch_add");
        assert_eq!(path, "/notes");
        assert_eq!(brain_name, "default");
    }

    #[test]
    fn watch_add_returns_protocol_error_on_wrong_response_shape() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::WatchAdd { .. } => Ok(Response::Pong),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));

        match client.watch_add("/notes".into()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("WatchAdded")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn watch_remove_returns_path() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::WatchRemove { path } => Ok(Response::WatchRemoved { path }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let path = client.watch_remove("/notes".into()).expect("watch_remove");
        assert_eq!(path, "/notes");
    }

    #[test]
    fn watch_remove_returns_protocol_error_on_wrong_response_shape() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::WatchRemove { .. } => Ok(Response::Pong),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        match client.watch_remove("/notes".into()) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("WatchRemoved")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn watch_list_returns_watches() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::WatchList => Ok(Response::WatchList {
                watches: vec![WatchSummary {
                    brain_name: "default".into(),
                    brain_id: "abc".into(),
                    note_dir: "/notes".into(),
                    watching: true,
                }],
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let watches = client.watch_list().expect("watch_list");
        assert_eq!(watches.len(), 1);
        assert_eq!(watches[0].brain_name, "default");
    }

    #[test]
    fn watch_list_returns_protocol_error_on_wrong_response_shape() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::WatchList => Ok(Response::Pong),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        match client.watch_list() {
            Err(RpcError::Protocol { message }) => assert!(message.contains("WatchList")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    // ── links / records / tasks / memory / tags / brains: round-trip tests ──
    //
    // Each new typed method gets one happy-path test that confirms the
    // method (a) sends the right Request variant, (b) unwraps the matching
    // Response variant, and (c) returns the expected payload. The Protocol-
    // error path is identical across all of them; one representative test
    // (`links_add_returns_protocol_error_on_wrong_response_shape`) locks
    // that contract.

    fn wire_entity(kind: &str, id: &str) -> crate::domain::WireEntityRef {
        crate::domain::WireEntityRef {
            kind: kind.into(),
            id: id.into(),
        }
    }

    #[test]
    fn links_add_unwraps_created_flag() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::LinksAdd { .. } => Ok(Response::LinksAdd { created: true }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let created = client
            .links_add(LinksAddParams {
                from: wire_entity("task", "t1"),
                to: wire_entity("record", "r1"),
                edge_kind: "covers".into(),
            })
            .expect("links_add");
        assert!(created);
    }

    #[test]
    fn links_add_returns_protocol_error_on_wrong_response_shape() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::LinksAdd { .. } => Ok(Response::Pong),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        match client.links_add(LinksAddParams {
            from: wire_entity("task", "t1"),
            to: wire_entity("record", "r1"),
            edge_kind: "covers".into(),
        }) {
            Err(RpcError::Protocol { message }) => assert!(message.contains("LinksAdd")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn links_remove_unwraps_removed_flag() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::LinksRemove { .. } => Ok(Response::LinksRemove { removed: false }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let removed = client
            .links_remove(LinksRemoveParams {
                from: wire_entity("task", "t1"),
                to: wire_entity("record", "r1"),
                edge_kind: "covers".into(),
            })
            .expect("links_remove");
        assert!(!removed);
    }

    #[test]
    fn links_for_entity_unwraps_links_vec() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::LinksForEntity { .. } => Ok(Response::LinksForEntity {
                links: vec![WireLinkSummary {
                    from: wire_entity("task", "t1"),
                    to: wire_entity("record", "r1"),
                    edge_kind: "covers".into(),
                    created_at: "2026-05-17T12:00:00Z".into(),
                }],
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let links = client
            .links_for_entity(LinksForEntityParams {
                entity: wire_entity("task", "t1"),
                direction: "outgoing".into(),
                limit: Some(10),
            })
            .expect("links_for_entity");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].edge_kind, "covers");
    }

    #[test]
    fn records_archive_unwraps_triple() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::RecordsArchive { .. } => Ok(Response::RecordsArchive {
                record_id: "rec_abc".into(),
                uri: "synapse://brain/record/rec_abc".into(),
                status: "archived".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let (id, uri, status) = client
            .records_archive(RecordsArchiveParams {
                record_id: "rec_abc".into(),
                reason: Some("superseded".into()),
            })
            .expect("records_archive");
        assert_eq!(id, "rec_abc");
        assert_eq!(uri, "synapse://brain/record/rec_abc");
        assert_eq!(status, "archived");
    }

    #[test]
    fn records_link_add_unwraps_created_flag() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::RecordsLinkAdd { .. } => Ok(Response::RecordsLinkAdd { created: true }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let created = client
            .records_link_add(RecordsLinkParams {
                record_id: "rec_abc".into(),
                target: wire_entity("task", "t1"),
                link_kind: "covers".into(),
            })
            .expect("records_link_add");
        assert!(created);
    }

    #[test]
    fn records_link_remove_unwraps_removed_flag() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::RecordsLinkRemove { .. } => {
                Ok(Response::RecordsLinkRemove { removed: true })
            }
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let removed = client
            .records_link_remove(RecordsLinkParams {
                record_id: "rec_abc".into(),
                target: wire_entity("task", "t1"),
                link_kind: "covers".into(),
            })
            .expect("records_link_remove");
        assert!(removed);
    }

    #[test]
    fn records_tag_add_unwraps_canonical_tag() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::RecordsTagAdd { .. } => Ok(Response::RecordsTagAdd {
                tag: "rust".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let canonical = client
            .records_tag_add("rec_abc".into(), "Rust".into())
            .expect("records_tag_add");
        assert_eq!(canonical, "rust");
    }

    #[test]
    fn records_tag_remove_unwraps_removed_flag() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::RecordsTagRemove { .. } => {
                Ok(Response::RecordsTagRemove { removed: true })
            }
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let removed = client
            .records_tag_remove("rec_abc".into(), "rust".into())
            .expect("records_tag_remove");
        assert!(removed);
    }

    #[test]
    fn tasks_apply_event_unwraps_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksApplyEvent { .. } => Ok(Response::TasksApplyEvent {
                result_json: "{\"task_id\":\"t1\"}".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let result = client
            .tasks_apply_event(TasksApplyEventParams {
                event_json: serde_json::json!({"event_type": "task_created"}),
            })
            .expect("tasks_apply_event");
        assert!(result.contains("task_id"));
    }

    #[test]
    fn tasks_deps_batch_unwraps_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksDepsBatch { .. } => Ok(Response::TasksDepsBatch {
                result_json: "{\"succeeded\":[]}".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let result = client
            .tasks_deps_batch(TasksDepsBatchParams {
                params_json: serde_json::json!({"action": "add", "pairs": []}),
            })
            .expect("tasks_deps_batch");
        assert!(result.contains("succeeded"));
    }

    #[test]
    fn tasks_labels_batch_unwraps_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksLabelsBatch { .. } => Ok(Response::TasksLabelsBatch {
                result_json: "{\"updated\":0}".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let result = client
            .tasks_labels_batch(TasksLabelsBatchParams {
                params_json: serde_json::json!({"action": "add"}),
            })
            .expect("tasks_labels_batch");
        assert!(result.contains("updated"));
    }

    #[test]
    fn tasks_labels_summary_unwraps_labels_vec() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TasksLabelsSummary => Ok(Response::TasksLabelsSummary {
                labels: vec![WireTaskLabelSummary {
                    label: "urgent".into(),
                    count: 3,
                    task_ids: vec!["t1".into(), "t2".into(), "t3".into()],
                }],
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let labels = client.tasks_labels_summary().expect("tasks_labels_summary");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].count, 3);
    }

    #[test]
    fn memory_walk_thread_unwraps_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::MemoryWalkThread { .. } => Ok(Response::MemoryWalkThread {
                result_json: "{\"episodes\":[]}".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let result = client
            .memory_walk_thread(MemoryWalkThreadParams {
                params_json: serde_json::json!({"source_id": "abc"}),
            })
            .expect("memory_walk_thread");
        assert!(result.contains("episodes"));
    }

    #[test]
    fn tags_recluster_unwraps_result_json() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::TagsRecluster { .. } => Ok(Response::TagsRecluster {
                result_json: "{\"moved\":0}".into(),
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let result = client
            .tags_recluster(TagsReclusterParams {
                params_json: serde_json::json!({"dry_run": true}),
            })
            .expect("tags_recluster");
        assert!(result.contains("moved"));
    }

    #[test]
    fn brains_list_unwraps_brains_and_count() {
        let mut client = DaemonClient::from_transport(InMemoryTransport::new(|req| match req {
            Request::BrainsList { .. } => Ok(Response::BrainsList {
                brains: vec![WireBrainSummary {
                    name: "default".into(),
                    id: Some("abc".into()),
                    root: "/home/user/brain".into(),
                    aliases: vec![],
                    extra_roots: vec![],
                    prefix: Some("BRN".into()),
                    archived: false,
                }],
                count: 1,
            }),
            other => Err(RpcError::Unknown {
                message: format!("unexpected: {other:?}"),
            }),
        }));
        let (brains, count) = client
            .brains_list(BrainsListParams::default())
            .expect("brains_list");
        assert_eq!(brains.len(), 1);
        assert_eq!(count, 1);
        assert_eq!(brains[0].name, "default");
    }
}
