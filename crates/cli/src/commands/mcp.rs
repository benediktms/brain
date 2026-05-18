use std::path::PathBuf;
use std::process;

use anyhow::Result;
use tracing::info;

/// Start the MCP stdio server by spawning the `brain-mcp` binary.
///
/// The binary is located via the same resolution that `rpc_client.rs` uses:
/// - `$BRAIN_DAEMON_BIN` env var
/// - sibling of the current executable
/// - `$PATH`
///
/// Socket path resolution (in order):
/// 1. `$BRAIN_SOCKET_PATH` env var
/// 2. `$BRAIN_HOME/brain-rpc.sock`
/// 3. `$HOME/.brain/brain-rpc.sock`
pub async fn run() -> Result<()> {
    let socket_path = resolve_socket_path()?;
    let brain_mcp = resolve_brain_mcp_binary()?;

    info!(path = %brain_mcp.display(), socket = %socket_path.display(), "spawning brain-mcp");

    let mut child = process::Command::new(&brain_mcp)
        .arg("--socket-path")
        .arg(&socket_path)
        .stdin(process::Stdio::inherit())
        .stdout(process::Stdio::inherit())
        .stderr(process::Stdio::inherit())
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn brain-mcp {}: {e}", brain_mcp.display()))?;

    let status = child.wait()?;
    if !status.success() {
        return Err(anyhow::anyhow!("brain-mcp exited with status {status}"));
    }

    Ok(())
}

/// Resolve the Unix socket path for brain-daemon.
///
/// Mirrors [`crate::commands::rpc_client::default_socket_path`] — socket
/// resolution is duplicated here so this command has no internal RPC
/// dependency and can run even when the daemon is unavailable.
fn resolve_socket_path() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("BRAIN_SOCKET_PATH") {
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("BRAIN_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|h| PathBuf::from(h).join(".brain")))
        .map_err(|_| anyhow::anyhow!("neither BRAIN_HOME nor HOME is set"))?;
    Ok(home.join("brain-rpc.sock"))
}

/// Locate the `brain-mcp` binary.
///
/// Resolution order:
/// 1. `$BRAIN_DAEMON_BIN` env var (intentionally shared with daemon binary)
/// 2. Sibling of the current `brain` executable
/// 3. `$PATH`
fn resolve_brain_mcp_binary() -> Result<PathBuf> {
    // Check BRAIN_DAEMON_BIN first (shared env var with daemon binary lookup).
    if let Ok(p) = std::env::var("BRAIN_DAEMON_BIN") {
        let p = PathBuf::from(p);
        if p.exists() {
            return Ok(p);
        }
    }

    // Try sibling of current executable.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join("brain-mcp");
            if sibling.exists() {
                return Ok(sibling);
            }
        }
    }

    // Fall back to PATH lookup via shell command.
    let output = std::process::Command::new("sh")
        .args(["-c", "command -v brain-mcp"])
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run command -v: {e}"))?;
    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    anyhow::bail!("brain-mcp not found in $PATH")
}
