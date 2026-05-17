/// Shared helpers for CLI commands that talk to the daemon over RPC.
///
/// This module centralises the socket-path resolution logic so that every
/// `*_remote` function in the records sub-families can share one definition
/// instead of each duplicating ~25 lines.
use std::path::PathBuf;

use anyhow::Result;

/// Resolve the Unix socket path used to reach `brain-daemon`.
///
/// Resolution order:
/// 1. `$BRAIN_SOCKET_PATH` env var (explicit override).
/// 2. `$BRAIN_HOME/brain-rpc.sock`.
/// 3. `$HOME/.brain/brain-rpc.sock`.
///
/// The filename is `brain-rpc.sock`, **not** `brain.sock` — the legacy
/// `brain_lib::ipc::IpcServer` already occupies that path. Using the same
/// path would route this crate's newline-JSON wire format into the legacy
/// JSON-RPC dispatcher and surface a useless `RpcError::Protocol` error.
pub fn default_socket_path() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("BRAIN_SOCKET_PATH") {
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("BRAIN_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|h| PathBuf::from(h).join(".brain")))
        .map_err(|_| anyhow::anyhow!("neither BRAIN_HOME nor HOME is set"))?;
    Ok(home.join("brain-rpc.sock"))
}

/// Connect to the daemon, spawning it if not already running.
///
/// Returns a connected [`brain_rpc::DaemonClient`] ready for typed method
/// calls. On failure the error message includes both the socket path and a
/// hint about the binary that was (or would have been) spawned.
pub fn connect_daemon() -> Result<brain_rpc::DaemonClient<brain_rpc::UnixSocketTransport>> {
    let socket_path = default_socket_path()?;
    let spawner = brain_rpc::StdProcessSpawner::new();
    let binary_hint = {
        use brain_rpc::DaemonSpawner as _;
        spawner
            .binary_path()
            .map(|p| format!("resolved to: {}", p.display()))
            .unwrap_or_else(|re| {
                format!(
                    "not found — checked: $BRAIN_DAEMON_BIN, sibling of current_exe, $PATH ({re})"
                )
            })
    };
    let transport =
        brain_rpc::connect_or_spawn(&socket_path, &spawner).map_err(|e| {
            anyhow::anyhow!(
                "could not connect to or start brain-daemon at {}: {e}\n  brain-daemon binary: {binary_hint}",
                socket_path.display(),
            )
        })?;
    brain_rpc::DaemonClient::connect(transport)
        .map_err(|e| anyhow::anyhow!("daemon handshake failed: {e}"))
}
