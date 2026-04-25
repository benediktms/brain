/// UDS IPC server.
///
/// Listens on a Unix Domain Socket, accepts multiple concurrent connections,
/// and dispatches newline-delimited JSON-RPC 2.0 requests through [`BrainRouter`].
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::mcp::protocol::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};

use super::router::BrainRouter;

/// IPC server bound to a Unix Domain Socket.
pub struct IpcServer {
    listener: UnixListener,
    router: Arc<BrainRouter>,
    shutdown: CancellationToken,
    socket_path: PathBuf,
    /// Inode of the socket file at bind time — used to avoid deleting a
    /// replacement socket created by a new daemon during restart.
    socket_inode: u64,
}

impl IpcServer {
    /// Return the default daemon socket path: `~/.brain/brain.sock`.
    pub fn default_socket_path() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        PathBuf::from(home).join(".brain").join("brain.sock")
    }

    /// Bind to `socket_path` with stale socket detection.
    ///
    /// - If socket file exists and a connection succeeds → another daemon is
    ///   listening → returns an error.
    /// - If socket file exists but connection fails → stale socket from a
    ///   crashed daemon → removes it and proceeds to bind.
    /// - Otherwise binds normally.
    ///
    /// Sets socket file permissions to 0o600 (owner-only) after binding.
    pub fn bind(socket_path: &Path, router: Arc<BrainRouter>) -> crate::error::Result<Self> {
        use std::os::unix::fs::PermissionsExt;

        if socket_path.exists() {
            // Attempt a synchronous probe to distinguish live vs stale.
            // We use a std blocking connect: this is called outside the tokio
            // runtime (from watch.rs before spawn), so std is safe here.
            // If we are already inside a runtime context the connect is fast
            // and won't block the executor for long (it either succeeds or
            // fails with ECONNREFUSED immediately).
            match std::os::unix::net::UnixStream::connect(socket_path) {
                Ok(_) => {
                    // Connection succeeded → another daemon is alive.
                    return Err(crate::error::BrainCoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::AddrInUse,
                        "daemon already running on this socket",
                    )));
                }
                Err(_) => {
                    // Connection failed → stale socket from crashed daemon.
                    warn!(path = ?socket_path, "removing stale socket file");
                    std::fs::remove_file(socket_path).map_err(crate::error::BrainCoreError::Io)?;
                }
            }
        }

        let listener = UnixListener::bind(socket_path).map_err(crate::error::BrainCoreError::Io)?;

        // Restrict socket access to owner only.
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
            .map_err(crate::error::BrainCoreError::Io)?;

        info!(path = ?socket_path, "IPC server bound");

        let socket_inode = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(socket_path).map(|m| m.ino()).unwrap_or(0)
        };

        Ok(Self {
            listener,
            router,
            shutdown: CancellationToken::new(),
            socket_path: socket_path.to_path_buf(),
            socket_inode,
        })
    }

    /// Return the bound socket path.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Remove the socket file only if it still belongs to this server instance.
    ///
    /// Compares the current file's inode against the inode recorded at bind
    /// time. If a new daemon has already replaced the socket, the inodes
    /// differ and we skip deletion.
    ///
    /// Returns `true` if the file was removed, `false` if it was missing or
    /// owned by a different server (inode mismatch).
    pub fn remove_socket(&self) -> bool {
        remove_socket_if_owned(&self.socket_path, self.socket_inode)
    }

    /// Return a clone of the cancellation token.
    ///
    /// Callers may call `token.cancel()` to trigger graceful shutdown.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Accept connections until the cancellation token is triggered.
    pub async fn run(&self) {
        info!("IPC accept loop started");
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    info!("IPC server shutting down");
                    break;
                }
                accept = self.listener.accept() => {
                    match accept {
                        Ok((stream, _addr)) => {
                            let router = Arc::clone(&self.router);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, router).await {
                                    warn!(error = %e, "IPC connection error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "IPC accept error");
                        }
                    }
                }
            }
        }
    }
}

/// Handle a single client connection: read lines, dispatch, write responses.
async fn handle_connection(
    stream: tokio::net::UnixStream,
    router: Arc<BrainRouter>,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let reader = BufReader::new(read_half);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        debug!(line = %line, "IPC request received");

        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => dispatch_request(req, &router).await,
            Err(e) => {
                warn!(error = %e, "IPC parse error");
                r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
                    .to_string()
            }
        };

        write_half.write_all(response.as_bytes()).await?;
        write_half.write_all(b"\n").await?;
        write_half.flush().await?;
    }

    Ok(())
}

/// Route a single parsed JSON-RPC request through the BrainRouter.
async fn dispatch_request(req: JsonRpcRequest, router: &BrainRouter) -> String {
    let id = req.id.clone();

    if req.method != "tools/call" {
        return serialize_error(&JsonRpcError::method_not_found(id, &req.method));
    }

    let tool_name = match req.params.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return serialize_error(&JsonRpcError::invalid_params(id, "missing tool name"));
        }
    };

    let mut arguments = req
        .params
        .get("arguments")
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));

    // Extract and remove the `__ipc_brain` routing field from arguments.
    // The tool-level `brain` param (cross-brain targeting) passes through untouched.
    let brain_name = if let Some(obj) = arguments.as_object_mut() {
        obj.remove("__ipc_brain")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    } else {
        None
    };

    let result = router
        .dispatch(brain_name.as_deref(), &tool_name, arguments)
        .await;

    let response = JsonRpcResponse::new(id, serde_json::to_value(result).unwrap_or_default());
    serde_json::to_string(&response).unwrap_or_else(|e| {
        error!(error = %e, "IPC response serialization failed");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal: serialization failed"}}"#.to_string()
    })
}

/// Remove `socket_path` only if its inode matches `expected_inode`.
///
/// Returns `true` if removed, `false` if missing or owned by another daemon.
pub fn remove_socket_if_owned(socket_path: &Path, expected_inode: u64) -> bool {
    use std::os::unix::fs::MetadataExt;
    // Note: narrow TOCTOU between metadata() and remove_file() exists
    // but is acceptable — the inode guard catches the common case
    // (seconds-scale overlap during restart), not the theoretical
    // microsecond race.
    let current_ino = match std::fs::metadata(socket_path) {
        Ok(m) => m.ino(),
        Err(_) => return false, // already gone
    };
    if current_ino != expected_inode {
        info!(
            path = ?socket_path,
            expected = expected_inode,
            actual = current_ino,
            "socket inode changed — new daemon already bound, skipping removal"
        );
        return false;
    }
    std::fs::remove_file(socket_path).is_ok()
}

fn serialize_error(err: &JsonRpcError) -> String {
    serde_json::to_string(err).unwrap_or_else(|e| {
        error!(error = %e, "IPC error serialization failed");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal: error serialization failed"}}"#.to_string()
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    use super::*;
    use crate::ipc::router::BrainRouter;
    use crate::mcp::tools::tests::create_test_context;

    async fn start_test_server(socket_path: &Path) -> CancellationToken {
        let (_dir, ctx) = create_test_context().await;
        ctx.stores
            .db_for_tests()
            .ensure_brain_registered("test-brain", "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());
        let server = IpcServer::bind(socket_path, router).expect("bind failed");
        let token = server.cancellation_token();
        let token2 = token.clone();
        tokio::spawn(async move {
            server.run().await;
        });
        token2
    }

    #[tokio::test]
    async fn test_ipc_tools_call_status() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("test.sock");
        let token = start_test_server(&sock).await;

        // Brief yield so the server accept loop is ready.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut stream = UnixStream::connect(&sock).await.expect("connect failed");
        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        });
        let mut payload = serde_json::to_vec(&req).unwrap();
        payload.push(b'\n');
        stream.write_all(&payload).await.unwrap();

        let mut buf = String::new();
        let mut tmp_buf = vec![0u8; 4096];
        let n = stream.read(&mut tmp_buf).await.unwrap();
        buf.push_str(&String::from_utf8_lossy(&tmp_buf[..n]));

        let parsed: serde_json::Value = serde_json::from_str(buf.trim()).unwrap();
        assert_eq!(parsed["id"], 1);
        assert!(parsed["result"].is_object(), "expected result object");

        token.cancel();
    }

    #[tokio::test]
    async fn test_ipc_unknown_method_returns_error() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("test2.sock");
        let token = start_test_server(&sock).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut stream = UnixStream::connect(&sock).await.expect("connect failed");
        let req = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        });
        let mut payload = serde_json::to_vec(&req).unwrap();
        payload.push(b'\n');
        stream.write_all(&payload).await.unwrap();

        let mut tmp_buf = vec![0u8; 4096];
        let n = stream.read(&mut tmp_buf).await.unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(String::from_utf8_lossy(&tmp_buf[..n]).trim()).unwrap();
        assert!(parsed["error"].is_object());
        assert_eq!(parsed["error"]["code"], -32601);

        token.cancel();
    }

    // ── Daemon restart race condition tests ─────────────────────────

    #[tokio::test]
    async fn test_socket_path_accessor_returns_bound_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("accessor.sock");

        let (_dir, ctx) = create_test_context().await;
        ctx.stores
            .db_for_tests()
            .ensure_brain_registered("test-brain", "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());
        let server = IpcServer::bind(&sock, router).expect("bind failed");
        assert_eq!(server.socket_path(), sock);
        server.cancellation_token().cancel();
    }

    #[tokio::test]
    async fn test_remove_socket_deletes_own_socket() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("own.sock");

        let (_dir, ctx) = create_test_context().await;
        ctx.stores
            .db_for_tests()
            .ensure_brain_registered("test-brain", "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());
        let server = IpcServer::bind(&sock, router).expect("bind failed");
        server.cancellation_token().cancel();

        assert!(sock.exists(), "socket file should exist after bind");
        assert!(
            server.remove_socket(),
            "remove_socket should return true for own socket"
        );
        assert!(!sock.exists(), "socket file should be deleted");
    }

    #[tokio::test]
    async fn test_remove_socket_refuses_to_delete_replaced_socket() {
        // Simulate the race: server A binds, then server B replaces the
        // socket at the same path.  A's remove_socket() must NOT delete
        // B's socket.
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("race.sock");

        // Server A binds.
        let (_dir_a, ctx_a) = create_test_context().await;
        ctx_a
            .stores
            .db_for_tests()
            .ensure_brain_registered("test-brain", "test-brain")
            .unwrap();
        let router_a = BrainRouter::new(Arc::new(ctx_a), "test-brain".to_string());
        let server_a = IpcServer::bind(&sock, router_a).expect("bind A failed");
        server_a.cancellation_token().cancel();

        // Delete A's socket (simulating early phase-1 cleanup).
        std::fs::remove_file(&sock).unwrap();

        // Server B binds at the same path (new daemon).
        let (_dir_b, ctx_b) = create_test_context().await;
        ctx_b
            .stores
            .db_for_tests()
            .ensure_brain_registered("test-brain", "test-brain")
            .unwrap();
        let router_b = BrainRouter::new(Arc::new(ctx_b), "test-brain".to_string());
        let _server_b = IpcServer::bind(&sock, router_b).expect("bind B failed");

        // Now A's late cleanup runs — must NOT delete B's socket.
        assert!(
            !server_a.remove_socket(),
            "server A's remove_socket must return false when socket belongs to server B"
        );
        assert!(
            sock.exists(),
            "server B's socket must survive server A's late cleanup"
        );
    }

    #[tokio::test]
    async fn test_remove_socket_returns_false_when_file_missing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("missing.sock");

        let (_dir, ctx) = create_test_context().await;
        ctx.stores
            .db_for_tests()
            .ensure_brain_registered("test-brain", "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());
        let server = IpcServer::bind(&sock, router).expect("bind failed");

        // Delete behind the server's back.
        std::fs::remove_file(&sock).unwrap();

        assert!(
            !server.remove_socket(),
            "remove_socket should return false when socket already deleted"
        );
    }

    /// Reproduces the exact race from the bug report: new server binds while
    /// old server's cleanup hasn't run yet.
    #[tokio::test]
    async fn test_new_server_binds_after_old_cancelled() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("handoff.sock");

        // Old server starts.
        let token_a = start_test_server(&sock).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Old server cancelled (simulating SIGTERM → phase 1).
        token_a.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Old server's socket deleted (phase 1 cleanup).
        let _ = std::fs::remove_file(&sock);

        // New server binds at same path.
        let token_b = start_test_server(&sock).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // New server is reachable.
        let stream = UnixStream::connect(&sock).await;
        assert!(stream.is_ok(), "new server must be reachable after handoff");

        token_b.cancel();
    }
}
