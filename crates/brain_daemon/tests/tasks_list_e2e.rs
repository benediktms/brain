//! End-to-end integration: spawn the actual `brain-daemon` binary with
//! a temp SQLite database, connect via the real `brain_rpc::DaemonClient`,
//! send a `Request::TasksList`, and verify the response shape.
//!
//! This is the proof that the wire works end-to-end through the
//! `BrainStoresDispatcher` path — handshake + real-data request +
//! response — against a real subprocess. The CLI's `--remote` flag
//! uses exactly this code path on the client side; if this test
//! passes, the CLI integration is shaped correctly.
//!
//! The seeded database is empty (we don't write tasks here — the
//! task-create wire variants land in a follow-up ticket). An empty
//! `Vec<TaskSummary>` is still a meaningful round-trip: it proves
//! framing, serialization, dispatcher, and stores all line up.

#![cfg(unix)]

mod common;

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use brain_rpc::{DaemonClient, Request, Response, TasksListParams, UnixSocketTransport};
use common::{ChildGuard, brain_daemon_binary};
use tempfile::TempDir;

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
fn brain_daemon_serves_tasks_list_against_empty_db() {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("lance");

    let binary = brain_daemon_binary();
    assert!(
        binary.exists(),
        "brain-daemon binary missing at {} — cargo should build it for the test",
        binary.display()
    );

    // BRAIN_HOME isolates the daemon from the developer's real
    // ~/.brain/brain.db — BrainStores::from_path consults brain_home
    // first and only falls back to the --sqlite-db arg when that
    // unified DB doesn't exist.
    let child = Command::new(&binary)
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
        "daemon never bound socket at {}",
        sock_path.display()
    );

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let mut client = DaemonClient::connect(transport).expect("handshake");

    let resp = client
        .call(Request::TasksList {
            params: TasksListParams::default(),
        })
        .expect("TasksList rpc");

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
fn brain_daemon_rejects_unknown_status_filter() {
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn brain-daemon");
    let _guard = ChildGuard(child);

    assert!(wait_for_socket(&sock_path, Duration::from_secs(5)));

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect");
    let mut client = DaemonClient::connect(transport).expect("handshake");

    let result = client.call(Request::TasksList {
        params: TasksListParams {
            status: Some("bogus".into()),
            ..TasksListParams::default()
        },
    });

    // MVP wire-protocol gap: brain_daemon's handle_connection closes
    // the connection when the dispatcher returns an RpcError (there's
    // no Response::Error variant yet), so the client sees a transport-
    // closed read failure rather than the dispatcher's specific
    // Protocol error. Either way, the call must fail — that's what
    // this test asserts. Tightening the error variant lands when the
    // wire-level error envelope ticket lands.
    assert!(
        result.is_err(),
        "expected an error for bogus status filter; got {result:?}"
    );
}
