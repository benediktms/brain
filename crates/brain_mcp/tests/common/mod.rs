//! Shared test helpers for brain_mcp integration tests.
//!
//! Re-exported as `mod common;` from each integration test file.
//! Provides daemon spawning, McpContext construction, and a thin
//! dispatch wrapper so test bodies stay declarative.

#![cfg(unix)]

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use brain_daemon::{DaemonConfig, DefaultDispatcher, UnixSocketServer};
use brain_rpc::{DaemonClient, UnixSocketTransport};
use tempfile::TempDir;

use brain_mcp::protocol::ToolCallResult;
use brain_mcp::{McpContext, ToolRegistry};

// ── server spawning ──────────────────────────────────────────────────────────

/// Wait for the server socket to be ready by polling with exponential backoff.
/// Gives the accept loop time to start before clients attempt to connect.
pub(crate) fn wait_for_server_ready(
    sock_path: &std::path::Path,
    budget: Duration,
) -> std::io::Result<()> {
    let start = std::time::Instant::now();
    let mut last_err = None;
    while start.elapsed() < budget {
        match std::os::unix::net::UnixStream::connect(sock_path) {
            Ok(_) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(last_err.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout waiting for server")
    }))
}

/// Spawn `brain_daemon::UnixSocketServer` with `DefaultDispatcher` on a
/// fresh temp-dir socket. Returns:
///
/// - the `TempDir` (caller must keep it alive for the test duration),
/// - the socket path,
/// - a `ServerGuard` that signals shutdown and joins the thread on drop.
pub fn spawn_daemon() -> (TempDir, std::path::PathBuf, ServerGuard) {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let config = DaemonConfig::new(&sock_path);
    let server = UnixSocketServer::bind(&config, DefaultDispatcher).expect("bind UnixSocketServer");
    let shutdown = server.shutdown_handle();
    let handle = thread::spawn(move || server.run());
    // Give the accept loop time to start before clients attempt to connect.
    wait_for_server_ready(&sock_path, Duration::from_millis(500))
        .expect("server socket not ready within 500ms");
    (
        tmp,
        sock_path,
        ServerGuard {
            shutdown: Some(shutdown),
            handle: Some(handle),
        },
    )
}

// ── ServerGuard ──────────────────────────────────────────────────────────────

/// RAII guard: signals daemon shutdown and joins the server thread when dropped.
pub struct ServerGuard {
    shutdown: Option<brain_daemon::ShutdownHandle>,
    handle: Option<thread::JoinHandle<Result<(), brain_rpc::RpcError>>>,
}

impl ServerGuard {
    /// Construct a guard from the shutdown handle + join handle a
    /// custom spawn helper returns. Used by per-test spawn helpers
    /// that wire alternate dispatchers (e.g. tools_smoke spawns a
    /// `BrainStoresDispatcher` instead of `DefaultDispatcher`).
    pub fn new(
        shutdown: brain_daemon::ShutdownHandle,
        handle: thread::JoinHandle<Result<(), brain_rpc::RpcError>>,
    ) -> Self {
        Self {
            shutdown: Some(shutdown),
            handle: Some(handle),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(s) = self.shutdown.take() {
            s.request();
        }
        if let Some(h) = self.handle.take() {
            // 200 ms is generous — the accept loop polls every 50 ms.
            // Cleanup failure is not surfaced as a test failure: the
            // test body's assertions are the authoritative verdict.
            let _ = h.join();
        }
    }
}

// ── McpContext construction ──────────────────────────────────────────────────

/// Connect an `McpContext` to the daemon socket at `sock_path`.
///
/// Constructs a `DaemonClient<UnixSocketTransport>` via the standard
/// handshake, wraps it in an `McpContext`, and returns it behind `Arc`
/// so it can be shared across `dispatch` calls in one test body.
pub async fn connect_mcp_context(sock_path: &std::path::Path) -> Arc<McpContext> {
    let transport = UnixSocketTransport::connect(sock_path).expect("connect transport");
    let client = DaemonClient::connect(transport).expect("connect client (handshake)");
    Arc::new(McpContext::new(client, "default".to_string()))
}

// ── dispatch wrapper ─────────────────────────────────────────────────────────

/// Thin wrapper around `ToolRegistry::dispatch`.
///
/// Calling `registry.dispatch(name, params, ctx).await` directly is fine,
/// but this alias keeps test bodies readable when driving many calls.
pub async fn dispatch(
    registry: &ToolRegistry,
    ctx: &McpContext,
    name: &str,
    params: serde_json::Value,
) -> ToolCallResult {
    registry.dispatch(name, params, ctx).await
}
