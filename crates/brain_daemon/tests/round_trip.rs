//! End-to-end integration tests for `UnixSocketServer` with both the
//! lightweight `DefaultDispatcher` (Ping + Handshake) and the production
//! `BrainStoresDispatcher` backed by real temp-store paths.
//!
//! The wire format (`DaemonClient` ↔ `UnixSocketTransport` ↔
//! `UnixSocketServer`) is identical for both dispatcher types — if it
//! works for one it works for both. The `BrainStoresDispatcher` tests
//! here focus on the initialization path (stores + dispatcher + server)
//! and the `TasksList` response shape, which are the gaps that
//! subprocess-spawn tests used to cover.

#![cfg(unix)]

mod common;

use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use brain_daemon::{BrainStoresDispatcher, DaemonConfig, DefaultDispatcher, UnixSocketServer};
use brain_lib::stores::BrainStores;
use brain_rpc::{
    DaemonClient, PROTOCOL_VERSION, Request, Response, RpcError, TasksListParams,
    UnixSocketTransport,
};
use tempfile::TempDir;

/// Process-wide lock serializing BRAIN_HOME mutations across concurrent tests.
///
/// cargo-nextest isolates test binaries into separate processes, but within a
/// single binary the multi-threaded tokio runtime runs tests concurrently.
static BRAIN_HOME_LOCK: Mutex<()> = Mutex::new(());

/// Spawn the server on a fresh temp-dir socket with `DefaultDispatcher`.
fn spawn_default_server() -> (TempDir, std::path::PathBuf, common::ServerGuard) {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let config = DaemonConfig::new(&sock_path);
    let server = UnixSocketServer::bind(&config, DefaultDispatcher).expect("bind");
    let shutdown = server.shutdown_handle();
    let handle = thread::spawn(move || server.run());
    thread::sleep(Duration::from_millis(20));
    (
        tmp,
        sock_path,
        common::ServerGuard {
            shutdown: Some(shutdown),
            handle: Some(handle),
        },
    )
}

#[test]
fn ping_pong_over_real_server() {
    let (_tmp, sock_path, _guard) = spawn_default_server();

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let mut client = DaemonClient::connect(transport).expect("connect client (handshake)");

    let resp = client.call(Request::Ping).expect("ping");
    assert_eq!(resp, Response::Pong);
}

#[test]
fn handshake_negotiates_protocol_version() {
    let (_tmp, sock_path, _guard) = spawn_default_server();

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let _client = DaemonClient::connect(transport).expect("connect client");

    // Both sides on PROTOCOL_VERSION → connect returns Ok.
    assert_eq!(PROTOCOL_VERSION, 3);
}

#[test]
fn multiple_requests_on_one_connection() {
    let (_tmp, sock_path, _guard) = spawn_default_server();

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
    thread::sleep(Duration::from_millis(80));

    shutdown.request();

    let join = handle.join();
    assert!(join.is_ok(), "server thread should join cleanly");
    assert!(join.unwrap().is_ok(), "server.run() should return Ok");
}

/// Spawn a server backed by `BrainStoresDispatcher` with real temp stores.
/// The `stores` are moved into the dispatcher (owned by the server thread),
/// so we return nothing from this function — the caller just holds the guard.
fn spawn_brain_stores_server(tmp: &TempDir) -> (std::path::PathBuf, common::ServerGuard) {
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("lance");
    let sock_path = tmp.path().join("brain.sock");

    // Isolate BrainStores from the developer's real ~/.brain/ by setting
    // BRAIN_HOME to the temp dir. BrainStores resolves its unified DB
    // via brain_home(), so this ensures we get a fresh isolated store.
    let guard = tempfile::Builder::new()
        .prefix(".brain_home_")
        .tempdir_in(tmp.path())
        .expect("BRAIN_HOME tempdir");
    // Lock serializes BRAIN_HOME mutations across concurrent tokio threads.
    let _lock = BRAIN_HOME_LOCK.lock().unwrap();
    unsafe { std::env::set_var("BRAIN_HOME", guard.path()) };
    let stores = match BrainStores::from_path(&sqlite_path, Some(&lance_path)) {
        Ok(s) => s,
        Err(e) => {
            unsafe { std::env::remove_var("BRAIN_HOME") };
            drop(_lock);
            panic!("open BrainStores from temp paths: {e}");
        }
    };
    unsafe { std::env::remove_var("BRAIN_HOME") };
    drop(_lock);
    drop(guard);

    #[cfg(not(feature = "embed"))]
    let dispatcher = BrainStoresDispatcher::new(stores);
    #[cfg(feature = "embed")]
    let dispatcher = BrainStoresDispatcher::new(stores, None);

    let config = DaemonConfig::new(&sock_path);
    let server = UnixSocketServer::bind(&config, dispatcher).expect("bind");
    let shutdown = server.shutdown_handle();
    let handle = thread::spawn(move || server.run());
    thread::sleep(Duration::from_millis(20));

    (
        sock_path,
        common::ServerGuard {
            shutdown: Some(shutdown),
            handle: Some(handle),
        },
    )
}

#[test]
fn tasks_list_empty_db_via_real_stores() {
    let tmp = TempDir::new().expect("tempdir");
    let (sock_path, _guard) = spawn_brain_stores_server(&tmp);

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect");
    let mut client = DaemonClient::connect(transport).expect("handshake");

    let resp = client
        .call(Request::TasksList {
            params: TasksListParams::default(),
        })
        .expect("TasksList call");

    match resp {
        Response::TasksList { tasks } => {
            assert!(
                tasks.is_empty(),
                "expected empty task list against empty DB, got {} tasks",
                tasks.len()
            );
        }
        other => panic!("expected Response::TasksList, got {other:?}"),
    }
}

#[test]
fn tasks_list_rejects_unknown_status_filter_via_real_stores() {
    let tmp = TempDir::new().expect("tempdir");
    let (sock_path, _guard) = spawn_brain_stores_server(&tmp);

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect");
    let mut client = DaemonClient::connect(transport).expect("handshake");

    let result = client.call(Request::TasksList {
        params: TasksListParams {
            status: Some("bogus".into()),
            ..TasksListParams::default()
        },
    });

    // BrainStoresDispatcher returns RpcError::Protocol for unknown status.
    match result {
        Err(RpcError::Protocol { message }) => {
            assert!(
                message.contains("bogus"),
                "expected 'bogus' in Protocol error message"
            );
        }
        other => panic!("expected Protocol error for bogus status filter, got {other:?}"),
    }
}
