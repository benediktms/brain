//! Smoke test: spawn the actual `target/debug/brain-daemon` binary,
//! connect via the real `brain_rpc::DaemonClient` over a
//! `UnixSocketTransport`, round-trip Ping/Pong, then kill the
//! subprocess.
//!
//! This is the only test in the suite that exercises the *binary
//! boundary* — `Command::new(...).spawn()` against the compiled
//! executable. The other integration tests in `tests/round_trip.rs`
//! drive `UnixSocketServer` in-process. Both layers matter:
//! in-process tests catch logic regressions cheaply; this test
//! catches binary-entry-point regressions (arg parsing, exit codes,
//! stdout/stderr).

#![cfg(unix)]

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use brain_rpc::{DaemonClient, Request, Response, UnixSocketTransport};
use tempfile::TempDir;

/// Path to the brain-daemon binary built by cargo for this test run.
/// Cargo guarantees the binary exists at `target/<profile>/brain-daemon`
/// when running `cargo test -p brain-daemon`, because the [[bin]] is in
/// the same crate.
fn brain_daemon_binary() -> std::path::PathBuf {
    // CARGO_BIN_EXE_<name> is set by cargo for tests in a crate that
    // has [[bin]] targets — the standard, documented way to locate
    // them. Falls back to a manual lookup if the env var is missing
    // (some test runners don't propagate it).
    if let Some(path) = option_env!("CARGO_BIN_EXE_brain-daemon") {
        return std::path::PathBuf::from(path);
    }
    // Fallback: derive from current_exe (test binary in target/debug/deps).
    let me = std::env::current_exe().expect("current_exe");
    me.parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("brain-daemon"))
        .expect("derive brain-daemon path")
}

/// Wait up to `budget` for `UnixSocketTransport::connect` to succeed.
/// Returns the connected transport or the last error.
fn wait_for_socket(
    sock_path: &std::path::Path,
    budget: Duration,
) -> Result<UnixSocketTransport, brain_rpc::RpcError> {
    let start = Instant::now();
    let mut last_err = None;
    while start.elapsed() < budget {
        match UnixSocketTransport::connect(sock_path) {
            Ok(t) => return Ok(t),
            Err(e) => last_err = Some(e),
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(last_err.expect("at least one attempt was made"))
}

#[test]
fn binary_serves_ping_pong() {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let binary = brain_daemon_binary();
    assert!(
        binary.exists(),
        "brain-daemon binary missing at {} — cargo should build it for the test",
        binary.display()
    );

    let child = Command::new(&binary)
        .arg("--socket-path")
        .arg(&sock_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn brain-daemon");
    // Hold the child in a Drop-guard so the subprocess is killed even
    // if an assertion below panics. Cargo's test harness doesn't reap
    // orphaned children for us; without this, repeated test failures
    // would leak daemon processes.
    let _guard = ChildGuard(child);

    // The binary prints "brain-daemon listening on ..." after bind.
    // We don't parse stdout; just poll the socket. 2s budget is more
    // than enough for a freshly-compiled binary to bind a socket on
    // a developer machine or CI runner.
    let transport =
        wait_for_socket(&sock_path, Duration::from_secs(2)).expect("daemon never opened socket");

    let mut client = DaemonClient::connect(transport).expect("handshake");
    let resp = client.call(Request::Ping).expect("ping");
    assert_eq!(resp, Response::Pong);
}

/// Drop-guard that kills + reaps an owned subprocess. Used by tests
/// that spawn `brain-daemon` so assertion panics don't leak children.
struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn binary_rejects_missing_socket_path() {
    let binary = brain_daemon_binary();
    let output = Command::new(&binary)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn brain-daemon");

    assert!(!output.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--socket-path is required"),
        "stderr should explain the missing arg, got: {stderr}"
    );
}

#[test]
fn binary_rejects_unknown_argument() {
    let binary = brain_daemon_binary();
    let output = Command::new(&binary)
        .arg("--bogus")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn brain-daemon");

    assert!(!output.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown argument"),
        "stderr should name the unknown arg, got: {stderr}"
    );
}
