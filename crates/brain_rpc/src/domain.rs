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
pub const PROTOCOL_VERSION: u32 = 1;

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

/// A server-originated reply to a [`Request`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
    fn protocol_version_is_one() {
        assert_eq!(PROTOCOL_VERSION, 1);
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
}
