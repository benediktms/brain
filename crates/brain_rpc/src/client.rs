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

use crate::domain::{PROTOCOL_VERSION, Request, Response, RpcError};
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
    pub fn call(&mut self, req: Request) -> Result<Response, RpcError> {
        self.transport.call(req)
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
}
