//! Integration test: `DaemonClient::connect` performs the handshake and
//! round-trips subsequent `Request`/`Response` traffic through the in-memory
//! transport — no real socket, no framing, no I/O.
//!
//! Integration tests live in a separate crate that depends on `brain_rpc`
//! as a normal dependency. Visibility-wise, they can only call the *public*
//! API — `DaemonClient::connect`, not `from_transport`. That's deliberate:
//! it forces every external test through the same construction path
//! production callers use, so the handshake is exercised on every run.
//!
//! Requires the `test-utils` feature, activated by the dev-dependency
//! self-reference in `crates/brain_rpc/Cargo.toml`.

use brain_rpc::testing::InMemoryTransport;
use brain_rpc::{DaemonClient, PROTOCOL_VERSION, Request, Response, RpcError};

#[test]
fn connect_and_ping_pong() {
    // echo() handshakes correctly and replies to Ping with Pong.
    let mut client = DaemonClient::connect(InMemoryTransport::echo()).expect("connect");
    let res = client.call(Request::Ping).expect("ping should succeed");
    assert_eq!(res, Response::Pong);
}

#[test]
fn connect_rejects_mismatched_server_version() {
    let result = DaemonClient::connect(InMemoryTransport::new(|req| match req {
        Request::Handshake { .. } => Ok(Response::HandshakeOk { server_version: 99 }),
        _ => Ok(Response::Pong),
    }));
    // Pattern-match the error variant directly so the Ok arm never needs
    // to format `DaemonClient` (which intentionally is not `Debug`).
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
fn errors_after_handshake_propagate_unchanged() {
    // A daemon that handshakes correctly but then errors on every other call.
    let mut client = DaemonClient::connect(InMemoryTransport::new(|req| match req {
        Request::Handshake { .. } => Ok(Response::HandshakeOk {
            server_version: PROTOCOL_VERSION,
        }),
        _ => Err(RpcError::Transport {
            message: "synthetic failure".into(),
        }),
    }))
    .expect("connect");

    match client.call(Request::Ping) {
        Err(RpcError::Transport { message }) => assert_eq!(message, "synthetic failure"),
        other => panic!("expected Transport error, got {other:?}"),
    }
}
