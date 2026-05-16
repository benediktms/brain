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
