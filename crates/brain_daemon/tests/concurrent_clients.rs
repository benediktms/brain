//! Integration test: multiple `DaemonClient`s issuing `TasksList`
//! concurrently against the same `brain-daemon` subprocess.
//!
//! `Arc<BrainStoresDispatcher>` is shared across the daemon's per-
//! connection worker threads. `BrainStores` wraps `Db` which contains
//! `Arc<Mutex<Connection>>` for the writer and a `Vec<Mutex<Connection>>`
//! pool for readers — `Send + Sync` in aggregate, but nothing in the
//! existing test suite actually exercises the cross-thread path under
//! load. This test fires N parallel reads through the wire and asserts
//! they all complete cleanly.
//!
//! Picked up from the brn-2fe.27 reviewer audit (tester high-priority
//! finding: "no concurrent-clients test").

#![cfg(unix)]

mod common;

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use brain_rpc::{DaemonClient, Request, Response, TasksListParams, UnixSocketTransport};
use common::{ChildGuard, brain_daemon_binary};
use tempfile::TempDir;

const CONCURRENT_CLIENTS: usize = 8;
const CALLS_PER_CLIENT: usize = 4;

fn wait_for_socket(path: &std::path::Path, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if UnixSocketTransport::connect(path).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(40));
    }
    false
}

#[test]
fn many_clients_can_round_trip_tasks_list_in_parallel() {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("lance");

    let child = Command::new(brain_daemon_binary())
        .arg("--socket-path")
        .arg(&sock_path)
        .arg("--sqlite-db")
        .arg(&sqlite_path)
        .arg("--lance-db")
        .arg(&lance_path)
        .env("BRAIN_HOME", tmp.path())
        .env_remove("BRAIN_SQLITE_DB")
        .env_remove("BRAIN_DB")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn brain-daemon");
    let _guard = ChildGuard(child);

    assert!(
        wait_for_socket(&sock_path, Duration::from_secs(5)),
        "daemon never bound socket"
    );

    // Fan out: spawn N threads, each opens its own DaemonClient and
    // issues M TasksList calls. We're checking that the daemon's
    // accept-loop + per-connection threading + shared
    // BrainStoresDispatcher all hold up under simultaneous load.
    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for client_idx in 0..CONCURRENT_CLIENTS {
        let sock = sock_path.clone();
        let handle = thread::spawn(move || -> Result<(), String> {
            let transport = UnixSocketTransport::connect(&sock)
                .map_err(|e| format!("client {client_idx} connect: {e}"))?;
            let mut client = DaemonClient::connect(transport)
                .map_err(|e| format!("client {client_idx} handshake: {e}"))?;
            for call_idx in 0..CALLS_PER_CLIENT {
                let resp = client
                    .call(Request::TasksList {
                        params: TasksListParams::default(),
                    })
                    .map_err(|e| format!("client {client_idx} call {call_idx} TasksList: {e}"))?;
                match resp {
                    Response::TasksList { tasks } => {
                        // Empty DB → empty list, every time.
                        if !tasks.is_empty() {
                            return Err(format!(
                                "client {client_idx} call {call_idx}: expected empty list, got {} tasks",
                                tasks.len()
                            ));
                        }
                    }
                    other => {
                        return Err(format!(
                            "client {client_idx} call {call_idx}: unexpected response {other:?}"
                        ));
                    }
                }
            }
            Ok(())
        });
        handles.push(handle);
    }

    let mut failures = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => failures.push(format!("client {idx}: {msg}")),
            Err(panic) => failures.push(format!("client {idx} panicked: {panic:?}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} client(s) failed:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}
