//! End-to-end integration test: spin up `UnixSocketServer` with the
//! production `DefaultDispatcher`, connect a real `brain_rpc`
//! `DaemonClient` over a `UnixSocketTransport`, round-trip
//! `Ping`/`Pong` and `Handshake`/`HandshakeOk`, then signal clean
//! shutdown.
//!
//! This is the hexagonal payoff: the same `DaemonClient` that flows
//! over `InMemoryTransport` in `brain_rpc`'s own tests also drives
//! the production server here, byte-identical. If the wire format
//! ever drifts between the two sides, this test fails.

#![cfg(unix)]

use std::thread;
use std::time::Duration;

use brain_daemon::{DaemonConfig, DefaultDispatcher, UnixSocketServer};
use brain_rpc::{DaemonClient, PROTOCOL_VERSION, Request, Response, UnixSocketTransport};
use tempfile::TempDir;

/// Spawn the server on a fresh temp-dir socket and return the path
/// plus a shutdown signaller and join handle. The caller signals
/// shutdown and joins the thread to clean up.
fn spawn_server() -> (TempDir, std::path::PathBuf, ServerGuard) {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let config = DaemonConfig::new(&sock_path);
    let server = UnixSocketServer::bind(&config, DefaultDispatcher).expect("bind UnixSocketServer");
    let shutdown = server.shutdown_handle();
    let handle = thread::spawn(move || server.run());
    // Tiny pause so the accept loop is definitely running before the
    // client tries to connect. The non-blocking listener accepts
    // immediately, but the thread might not have been scheduled yet.
    thread::sleep(Duration::from_millis(20));
    (
        tmp,
        sock_path,
        ServerGuard {
            shutdown: Some(shutdown),
            handle: Some(handle),
        },
    )
}

struct ServerGuard {
    shutdown: Option<brain_daemon::ShutdownHandle>,
    handle: Option<thread::JoinHandle<Result<(), brain_rpc::RpcError>>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(s) = self.shutdown.take() {
            s.request();
        }
        if let Some(h) = self.handle.take() {
            // 200ms is generous — the accept loop polls every 50ms.
            // We don't fail the test on a slow join because the test
            // body's assertions are what matters; this is just cleanup.
            let _ = h.join();
        }
    }
}

#[test]
fn ping_pong_over_real_server() {
    let (_tmp, sock_path, _guard) = spawn_server();

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let mut client = DaemonClient::connect(transport).expect("connect client (handshake)");

    let resp = client.call(Request::Ping).expect("ping");
    assert_eq!(resp, Response::Pong);
}

#[test]
fn handshake_negotiates_protocol_version() {
    // DaemonClient::connect performs the handshake; if the server's
    // protocol version diverged from the client's, this would return
    // VersionMismatch. Both sides being on PROTOCOL_VERSION makes
    // connect succeed — and "succeeded" is the assertion.
    let (_tmp, sock_path, _guard) = spawn_server();

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let _client = DaemonClient::connect(transport).expect("connect client");

    // Explicit cross-check: PROTOCOL_VERSION matched on both sides.
    // (No public API exposes the negotiated version after connect;
    // the type-level guarantee that connect returned Ok IS the proof.)
    assert_eq!(PROTOCOL_VERSION, 1);
}

#[test]
fn multiple_requests_on_one_connection() {
    let (_tmp, sock_path, _guard) = spawn_server();

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let mut client = DaemonClient::connect(transport).expect("connect client");

    for _ in 0..5 {
        assert_eq!(client.call(Request::Ping).expect("ping"), Response::Pong);
    }
}

#[test]
fn shutdown_handle_stops_run_loop() {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let config = DaemonConfig::new(&sock_path);
    let server = UnixSocketServer::bind(&config, DefaultDispatcher).expect("bind");
    let shutdown = server.shutdown_handle();

    let handle = thread::spawn(move || server.run());

    // Let the accept loop tick once.
    thread::sleep(Duration::from_millis(80));

    shutdown.request();

    // The loop polls every 50 ms; 500 ms is more than enough.
    let join = handle.join();
    assert!(join.is_ok(), "server thread should join cleanly");
    assert!(join.unwrap().is_ok(), "server.run() should return Ok");
}
