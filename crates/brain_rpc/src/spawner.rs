//! `DaemonSpawner` port + `StdProcessSpawner` adapter + `connect_or_spawn`
//! helper.
//!
//! # Hexagonal role
//!
//! This module is mixed-purity by design: the [`DaemonSpawner`] *trait* is
//! a port (framework-free trait), and [`StdProcessSpawner`] is its
//! production *adapter* (the only place `std::process` / `std::env`
//! appear in the crate). The crate audit (`just audit-rpc`) excludes
//! this file from the port-layer I/O check for the same reason it
//! excludes [`crate::unix`] — it's an adapter file by intent.
//!
//! [`connect_or_spawn`] is a higher-level helper that composes the
//! transport adapter and the spawner port to give the CLI/MCP a one-call
//! "find or start the daemon" path.
//!
//! # Binary resolution order
//!
//! `StdProcessSpawner` resolves the `brain-daemon` binary by checking,
//! in order:
//!
//! 1. An explicit `hint` passed via [`StdProcessSpawner::with_hint`]
//!    (highest priority — testing override / advanced use).
//! 2. The `BRAIN_DAEMON_BIN` environment variable (explicit env override).
//! 3. A sibling of [`std::env::current_exe`] named `brain-daemon`
//!    (default for cargo-install / homebrew side-by-side installs).
//! 4. `brain-daemon` found anywhere on `$PATH` (system-wide install fallback).
//!
//! If none of the above resolve to a file, `binary_path` returns
//! [`RpcError::NotFound`] so the caller can surface a clear "daemon
//! binary not installed" error.

#![cfg(unix)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::domain::RpcError;
use crate::unix::UnixSocketTransport;

/// Abstraction for starting the daemon process out-of-band when the
/// socket isn't already accepting connections.
///
/// The trait is intentionally minimal: implementations decide *how* to
/// produce a running daemon. [`StdProcessSpawner`] forks-and-execs the
/// `brain-daemon` binary; tests use
/// [`crate::testing::FakeSpawner`] to side-step process creation
/// entirely.
pub trait DaemonSpawner {
    /// Start the daemon. Returns `Ok(())` once the process has been
    /// spawned (not necessarily once the socket is accepting — the
    /// caller polls for that).
    fn spawn(&self, socket_path: &Path) -> Result<(), RpcError>;

    /// Return the path where the daemon should write its PID file.
    /// If `None`, the daemon is spawned without `--pid-file`.
    fn pid_path(&self) -> Option<PathBuf> {
        None
    }

    /// Resolve the path to the daemon binary.
    ///
    /// Implementations may or may not validate that the path is
    /// executable. [`StdProcessSpawner`] validates as follows:
    ///
    /// - Explicit `hint` (set via [`StdProcessSpawner::with_hint`]) is
    ///   trusted verbatim — callers passing a hint own their input.
    /// - `BRAIN_DAEMON_BIN` is **validated**: the value must point at an
    ///   existing file, otherwise [`RpcError::NotFound`] is returned
    ///   here (not deferred to [`Self::spawn`]). This makes a stale or
    ///   mistyped env var surface as a clear "daemon binary not
    ///   installed" error rather than as an opaque transport failure
    ///   one call later.
    /// - Discovery-based resolution (`current_exe` sibling, `$PATH`)
    ///   already checks `is_file` and returns [`RpcError::NotFound`] if
    ///   nothing resolves.
    fn binary_path(&self) -> Result<PathBuf, RpcError>;
}

/// Production [`DaemonSpawner`]: locates the `brain-daemon` binary via
/// the documented resolution order and spawns it as a detached child
/// process with all stdio redirected to `/dev/null`.
pub struct StdProcessSpawner {
    hint: Option<PathBuf>,
    pid_path: Option<PathBuf>,
}

impl StdProcessSpawner {
    /// Construct a spawner that uses the default resolution order
    /// (env var → current_exe sibling → `$PATH`).
    pub fn new() -> Self {
        Self {
            hint: None,
            pid_path: None,
        }
    }

    /// Construct a spawner with an explicit binary path. Bypasses
    /// discovery entirely — useful for tests and unusual deployments.
    pub fn with_hint(hint: impl Into<PathBuf>) -> Self {
        Self {
            hint: Some(hint.into()),
            pid_path: None,
        }
    }

    /// Set the PID file path. The daemon will be spawned with
    /// `--pid-file <path>` so it is visible to `brain daemon stop/status`.
    pub fn with_pid_path(mut self, pid_path: impl Into<PathBuf>) -> Self {
        self.pid_path = Some(pid_path.into());
        self
    }
}

impl Default for StdProcessSpawner {
    fn default() -> Self {
        Self::new()
    }
}

impl DaemonSpawner for StdProcessSpawner {
    fn spawn(&self, socket_path: &Path) -> Result<(), RpcError> {
        let binary = self.binary_path()?;
        let mut cmd = Command::new(&binary);
        cmd.arg("--socket-path").arg(socket_path);
        if let Some(ref pid_path) = self.pid_path {
            cmd.arg("--pid-file").arg(pid_path);
        }
        cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        cmd.spawn()
            .map_err(|e| RpcError::Transport {
                message: format!("spawn({}): {e}", binary.display()),
            })?;
        Ok(())
    }

    fn pid_path(&self) -> Option<PathBuf> {
        self.pid_path.clone()
    }

    fn binary_path(&self) -> Result<PathBuf, RpcError> {
        let env = std::env::var_os("BRAIN_DAEMON_BIN");
        let current_exe_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf));
        let path_var = std::env::var_os("PATH");
        let path_dirs: Vec<PathBuf> = path_var
            .as_ref()
            .map(|v| std::env::split_paths(v).collect())
            .unwrap_or_default();

        resolve_binary(
            self.hint.as_deref(),
            env.as_deref(),
            current_exe_dir.as_deref(),
            &path_dirs,
            "brain-daemon",
        )
    }
}

/// Pure binary-resolution algorithm. Extracted from
/// [`StdProcessSpawner::binary_path`] so it can be unit-tested without
/// mutating global env state (cargo-nextest parallel test execution
/// makes env mutation racy).
///
/// `pub(crate)` so tests can reach it; not part of the public API.
pub(crate) fn resolve_binary(
    hint: Option<&Path>,
    env_override: Option<&std::ffi::OsStr>,
    current_exe_dir: Option<&Path>,
    path_dirs: &[PathBuf],
    name: &str,
) -> Result<PathBuf, RpcError> {
    // 1. Explicit hint — trust the caller.
    if let Some(h) = hint {
        return Ok(h.to_path_buf());
    }

    // 2. BRAIN_DAEMON_BIN — trust the env, but only if it points at
    //    an actual file. A stale or mistyped value should surface
    //    here ("daemon binary not installed") rather than at
    //    `Command::spawn()` time ("No such file or directory") where
    //    the error is one level removed from the misconfiguration.
    if let Some(env) = env_override
        && !env.is_empty()
    {
        let path = PathBuf::from(OsString::from(env));
        if path.is_file() {
            return Ok(path);
        }
        return Err(RpcError::NotFound {
            id: format!(
                "BRAIN_DAEMON_BIN={} (path does not exist or is not a file)",
                path.display()
            ),
        });
    }

    // 3. Sibling of current_exe, only if it actually exists as a file.
    if let Some(dir) = current_exe_dir {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    // 4. PATH lookup — first existing file in the order PATH lists.
    for dir in path_dirs {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(RpcError::NotFound {
        id: format!("{name} binary"),
    })
}

/// Top-level convenience: connect to the daemon, spawning it if it
/// isn't already running.
///
/// 1. Try [`UnixSocketTransport::connect`]. If it succeeds, return the
///    transport (fast path; daemon already up).
/// 2. Otherwise call [`DaemonSpawner::spawn`] and poll the socket
///    every 50 ms for up to 2 s (40 attempts), returning the first
///    successful connection.
/// 3. If the daemon never starts answering within the budget, return
///    [`RpcError::Transport`] with the socket path and the timeout for
///    operator triage.
pub fn connect_or_spawn<S: DaemonSpawner>(
    socket_path: &Path,
    spawner: &S,
) -> Result<UnixSocketTransport, RpcError> {
    // Fast path.
    if let Ok(transport) = UnixSocketTransport::connect(socket_path) {
        return Ok(transport);
    }

    // Slow path.
    spawner.spawn(socket_path)?;

    let attempts: u32 = 40;
    let interval = Duration::from_millis(50);
    for _ in 0..attempts {
        std::thread::sleep(interval);
        if let Ok(transport) = UnixSocketTransport::connect(socket_path) {
            return Ok(transport);
        }
    }

    Err(RpcError::Transport {
        message: format!(
            "daemon did not start accepting on {} within {}ms",
            socket_path.display(),
            attempts as u64 * interval.as_millis() as u64
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── resolve_binary: pure-function tests, no global state ──────────

    #[test]
    fn resolve_returns_hint_when_present() {
        let hint = PathBuf::from("/explicit/hint");
        let resolved = resolve_binary(Some(&hint), None, None, &[], "brain-daemon").unwrap();
        assert_eq!(resolved, hint);
    }

    #[test]
    fn resolve_uses_env_when_hint_absent() {
        // env path must point at a real file now (is_file check). Build
        // one in a temp dir.
        let tmp = tempfile::tempdir().unwrap();
        let binary = tmp.path().join("brain-daemon");
        std::fs::write(&binary, b"#!/bin/sh\nexit 0\n").unwrap();
        let env: OsString = binary.clone().into();
        let resolved =
            resolve_binary(None, Some(env.as_os_str()), None, &[], "brain-daemon").unwrap();
        assert_eq!(resolved, binary);
    }

    #[test]
    fn resolve_returns_not_found_when_env_path_missing() {
        // BRAIN_DAEMON_BIN points at a nonexistent file → NotFound,
        // not a silent passthrough. Catches typos at resolve time
        // instead of at spawn time.
        let env = OsString::from("/nonexistent/brain-daemon-not-there");
        let result = resolve_binary(None, Some(env.as_os_str()), None, &[], "brain-daemon");
        match result {
            Err(RpcError::NotFound { id }) => {
                assert!(
                    id.contains("BRAIN_DAEMON_BIN"),
                    "error should mention the env var name; got: {id}"
                );
                assert!(
                    id.contains("/nonexistent/brain-daemon-not-there"),
                    "error should mention the bad path; got: {id}"
                );
            }
            other => panic!("expected NotFound for missing-file env, got {other:?}"),
        }
    }

    #[test]
    fn resolve_ignores_empty_env() {
        // PATH lookup falls through to error because no path entries
        // and no sibling. Validates that an empty env var doesn't
        // accidentally satisfy step 2.
        let env = OsString::from("");
        let result = resolve_binary(None, Some(env.as_os_str()), None, &[], "brain-daemon");
        match result {
            Err(RpcError::NotFound { id }) => assert!(id.contains("brain-daemon")),
            other => panic!("expected NotFound for empty env, got {other:?}"),
        }
    }

    #[test]
    fn resolve_uses_current_exe_sibling_when_file_exists() {
        // Build a temp dir with a fake binary inside.
        let tmp = tempfile::tempdir().unwrap();
        let binary_path = tmp.path().join("brain-daemon");
        std::fs::write(&binary_path, b"#!/bin/sh\nexit 0\n").unwrap();

        let resolved = resolve_binary(None, None, Some(tmp.path()), &[], "brain-daemon").unwrap();
        assert_eq!(resolved, binary_path);
    }

    #[test]
    fn resolve_skips_current_exe_sibling_when_file_missing() {
        let tmp = tempfile::tempdir().unwrap();
        // No file at tmp/brain-daemon.
        let result = resolve_binary(None, None, Some(tmp.path()), &[], "brain-daemon");
        match result {
            Err(RpcError::NotFound { id }) => assert!(id.contains("brain-daemon")),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn resolve_finds_binary_in_path_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let dir_a = tmp.path().join("a");
        let dir_b = tmp.path().join("b");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();
        let binary_in_b = dir_b.join("brain-daemon");
        std::fs::write(&binary_in_b, b"#!/bin/sh\nexit 0\n").unwrap();

        let path_dirs = vec![dir_a, dir_b];
        let resolved = resolve_binary(None, None, None, &path_dirs, "brain-daemon").unwrap();
        assert_eq!(resolved, binary_in_b);
    }

    #[test]
    fn resolve_returns_first_match_in_path() {
        let tmp = tempfile::tempdir().unwrap();
        let dir_a = tmp.path().join("a");
        let dir_b = tmp.path().join("b");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();
        // Binary exists in BOTH; the test asserts first-wins.
        let binary_in_a = dir_a.join("brain-daemon");
        let binary_in_b = dir_b.join("brain-daemon");
        std::fs::write(&binary_in_a, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::write(&binary_in_b, b"#!/bin/sh\nexit 0\n").unwrap();

        let path_dirs = vec![dir_a, dir_b];
        let resolved = resolve_binary(None, None, None, &path_dirs, "brain-daemon").unwrap();
        assert_eq!(resolved, binary_in_a);
    }

    #[test]
    fn resolve_returns_not_found_when_nothing_resolves() {
        let result = resolve_binary(None, None, None, &[], "brain-daemon");
        match result {
            Err(RpcError::NotFound { id }) => assert!(id.contains("brain-daemon")),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    // ── StdProcessSpawner integration ─────────────────────────────────

    #[test]
    fn spawn_succeeds_for_existing_binary() {
        // /usr/bin/true exists on macOS and Linux and ignores its args.
        let spawner = StdProcessSpawner::with_hint("/usr/bin/true");
        spawner
            .spawn(Path::new("/tmp/brain-rpc-test.sock"))
            .unwrap();
    }

    #[test]
    fn spawn_returns_transport_error_for_missing_binary() {
        let spawner = StdProcessSpawner::with_hint("/nonexistent/path/brain-daemon");
        match spawner.spawn(Path::new("/tmp/brain-rpc-test.sock")) {
            Err(RpcError::Transport { message }) => {
                assert!(
                    message.contains("/nonexistent/path/brain-daemon"),
                    "error should mention binary path; got: {message}"
                );
            }
            other => panic!("expected Transport error, got {other:?}"),
        }
    }
}
