//! Integration tests for `connect_or_spawn` driving a `FakeSpawner`.
//!
//! These tests exercise the full "missing socket → spawn → poll → connect"
//! flow without depending on a real `brain-daemon` binary — the
//! `FakeSpawner`'s behavior closure binds the listener in-process, which
//! is enough to satisfy `UnixSocketTransport::connect`.

#![cfg(unix)]

use std::os::unix::net::UnixListener;
use std::sync::{Arc, Mutex};

use brain_rpc::testing::FakeSpawner;
use brain_rpc::{RpcError, connect_or_spawn};
use tempfile::TempDir;

#[test]
fn fast_path_skips_spawn_when_socket_already_present() {
    let tmp = TempDir::new().unwrap();
    let sock_path = tmp.path().join("brain.sock");

    // Pre-bind the socket so connect_or_spawn finds it on the first try.
    let listener = UnixListener::bind(&sock_path).expect("bind");
    let _listener_keepalive = Arc::new(listener); // hold for the duration of the test

    let spawner = FakeSpawner::new("/usr/bin/true", |_| {
        panic!("spawn should NOT be called when the socket is already listening");
    });

    let transport = connect_or_spawn(&sock_path, &spawner);
    assert!(transport.is_ok(), "expected fast-path success");
    assert_eq!(
        spawner.spawn_calls(),
        0,
        "spawn must not be invoked on the fast path"
    );
}

#[test]
fn slow_path_spawns_and_reconnects() {
    let tmp = TempDir::new().unwrap();
    let sock_path = tmp.path().join("brain.sock");
    let listener_slot: Arc<Mutex<Option<UnixListener>>> = Arc::new(Mutex::new(None));
    let listener_slot_for_spawner = Arc::clone(&listener_slot);

    let spawner = FakeSpawner::new("/usr/bin/true", move |path| {
        // Simulate the daemon coming up: bind the socket on first call.
        let listener = UnixListener::bind(path).map_err(|e| RpcError::Transport {
            message: format!("fake bind: {e}"),
        })?;
        *listener_slot_for_spawner.lock().unwrap() = Some(listener);
        Ok(())
    });

    let transport = connect_or_spawn(&sock_path, &spawner);
    assert!(
        transport.is_ok(),
        "expected slow-path success after spawn, got {:?}",
        transport.err()
    );
    assert_eq!(
        spawner.spawn_calls(),
        1,
        "spawn should be called exactly once"
    );
}

#[test]
fn returns_transport_error_when_spawn_returns_error() {
    let tmp = TempDir::new().unwrap();
    let sock_path = tmp.path().join("brain.sock");

    let spawner = FakeSpawner::new("/usr/bin/true", |_| {
        Err(RpcError::Transport {
            message: "fake spawn failure".into(),
        })
    });

    match connect_or_spawn(&sock_path, &spawner) {
        Ok(_) => panic!("expected error from failing spawner"),
        Err(RpcError::Transport { message }) => {
            assert_eq!(message, "fake spawn failure");
        }
        Err(other) => panic!("expected Transport error, got {other:?}"),
    }
    assert_eq!(spawner.spawn_calls(), 1);
}

#[test]
fn returns_transport_error_when_daemon_never_binds() {
    let tmp = TempDir::new().unwrap();
    let sock_path = tmp.path().join("brain.sock");

    // Spawn succeeds but never actually binds — the polling loop times out.
    let spawner = FakeSpawner::new("/usr/bin/true", |_| Ok(()));

    // This test exercises the 2s polling timeout — accept the duration.
    match connect_or_spawn(&sock_path, &spawner) {
        Ok(_) => panic!("expected timeout error"),
        Err(RpcError::Transport { message }) => {
            assert!(
                message.contains("did not start accepting"),
                "error should mention timeout, got: {message}"
            );
            assert!(
                message.contains(&sock_path.display().to_string()),
                "error should mention socket path, got: {message}"
            );
        }
        Err(other) => panic!("expected Transport error, got {other:?}"),
    }
    assert_eq!(spawner.spawn_calls(), 1);
}
