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

use crate::domain::{PROTOCOL_VERSION, Request, Response, RpcError, TaskSummary, TasksListParams};
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
}
