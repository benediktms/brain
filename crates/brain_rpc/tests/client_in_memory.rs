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
use brain_rpc::{
    DaemonClient, PROTOCOL_VERSION, Request, Response, RpcError, TaskSummary, TasksListParams,
};

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

#[test]
fn typed_ping_and_tasks_list_round_trip_through_public_api() {
    // Lock the public surface: a consumer constructs a client via the
    // public `connect()` path and then calls the typed methods. No
    // pub(crate) escape hatches, no manual `match resp` blocks.
    let mut client = DaemonClient::connect(InMemoryTransport::new(|req| match req {
        Request::Handshake { .. } => Ok(Response::HandshakeOk {
            server_version: PROTOCOL_VERSION,
        }),
        Request::Ping => Ok(Response::Pong),
        Request::TasksList { params } => {
            // Mirror back the params via the title so the test asserts
            // that params travelled end-to-end through the typed method.
            let echo = TaskSummary {
                task_id: "brn-test".into(),
                title: format!("status={:?}", params.status),
                status: "open".into(),
                priority: 0,
                brain_id: "eAx".into(),
            };
            Ok(Response::TasksList { tasks: vec![echo] })
        }
    }))
    .expect("connect");

    client.ping().expect("typed ping");
    let tasks = client
        .tasks_list(TasksListParams {
            status: Some("open".into()),
            ..TasksListParams::default()
        })
        .expect("typed tasks_list");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].title, "status=Some(\"open\")");
}
