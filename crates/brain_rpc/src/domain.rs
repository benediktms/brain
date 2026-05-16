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
}
