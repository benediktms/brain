//! Shared helpers for brain_daemon integration tests.
//!
//! Integration tests in `tests/*.rs` are compiled as separate crates,
//! so they cannot share helper modules through the usual `mod` graph.
//! The convention is `tests/common/mod.rs` (a *subdirectory* — cargo
//! treats it as a non-test helper module rather than its own test
//! binary) plus a `mod common;` declaration at the top of each test
//! file that consumes it.
//!
//! Helpers live here once they appear in 3+ test files. Two-copy
//! duplication stays inline.

#![cfg(unix)]
#![allow(dead_code)] // Not every test file uses every helper.

/// Locate the `brain-daemon` binary built by cargo for this test run.
///
/// `CARGO_BIN_EXE_<name>` is the documented way; the parent/parent
/// fallback covers test runners that don't propagate it.
pub fn brain_daemon_binary() -> std::path::PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_brain-daemon") {
        return std::path::PathBuf::from(path);
    }
    let me = std::env::current_exe().expect("current_exe");
    me.parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("brain-daemon"))
        .expect("derive brain-daemon path")
}

/// Drop-guard that kills + reaps an owned subprocess. Used by tests
/// that spawn `brain-daemon` so an assertion panic doesn't leak the
/// child process across tests.
pub struct ChildGuard(pub std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Drop-guard that signals server shutdown and joins the server thread.
/// Used by tests that run `UnixSocketServer` in-process.
pub struct ServerGuard {
    pub shutdown: Option<brain_daemon::ShutdownHandle>,
    pub handle: Option<std::thread::JoinHandle<Result<(), brain_rpc::RpcError>>>,
    /// Guards the BRAIN_HOME env var and lock for the server lifetime.
    /// Stored as separate Option fields so `ServerGuard` remains `Send`.
    pub brain_home_lock: Option<std::sync::MutexGuard<'static, ()>>,
    /// Arc<TempDir> so both the thread and ServerGuard can own references.
    pub brain_home_guard: Option<std::sync::Arc<tempfile::TempDir>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(s) = self.shutdown.take() {
            s.request();
        }
        if let Some(h) = self.handle.take() {
            // The accept loop polls every 50ms, so join completes quickly after shutdown.
            let _ = std::thread::JoinHandle::join(h);
        }
        // Drop the BRAIN_HOME lock and env guard only after the server thread
        // has been joined — brain_home() is resolved at dispatch time, not
        // startup, so the guards must live for the entire server lifetime.
        drop(self.brain_home_lock.take());
        drop(self.brain_home_guard.take());
    }
}
