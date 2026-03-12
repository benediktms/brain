/// IPC client for the brain daemon.
///
/// Connects to the daemon's Unix Domain Socket and issues newline-delimited
/// JSON-RPC 2.0 requests, returning typed responses.
use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::mcp::protocol::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};

/// Error type for IPC client operations.
#[derive(Debug, thiserror::Error)]
pub enum IpcClientError {
    /// The daemon is not running (socket missing or connection refused).
    #[error("daemon not available at {path}: {source}")]
    DaemonUnavailable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// I/O error on an established connection.
    #[error("IPC I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to serialize the request.
    #[error("request serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),

    /// The server returned a JSON-RPC error response.
    #[error("JSON-RPC error {code}: {message}")]
    Rpc { code: i64, message: String },

    /// The server returned an unexpected or empty response.
    #[error("unexpected response from daemon: {0}")]
    Protocol(String),
}

/// A connected IPC client.
///
/// Each [`IpcClient`] holds a single `UnixStream` connection to the daemon.
/// It is **not** `Clone` — create separate clients for concurrent use.
#[derive(Debug)]
pub struct IpcClient {
    reader: BufReader<tokio::io::ReadHalf<UnixStream>>,
    writer: tokio::io::WriteHalf<UnixStream>,
    next_id: u64,
}

impl IpcClient {
    /// Connect to the daemon's UDS at `socket_path`.
    ///
    /// Returns [`IpcClientError::DaemonUnavailable`] when the socket file is
    /// absent (`ENOENT`) or the daemon is not listening (`ECONNREFUSED`).
    pub async fn connect(socket_path: &Path) -> Result<Self, IpcClientError> {
        let stream = UnixStream::connect(socket_path).await.map_err(|e| {
            IpcClientError::DaemonUnavailable {
                path: socket_path.to_path_buf(),
                source: e,
            }
        })?;

        let (read_half, write_half) = tokio::io::split(stream);
        Ok(Self {
            reader: BufReader::new(read_half),
            writer: write_half,
            next_id: 1,
        })
    }

    /// Send a raw JSON-RPC request and return the parsed response.
    ///
    /// On a JSON-RPC error response the method returns
    /// [`IpcClientError::Rpc`] rather than a success value.
    pub async fn call(
        &mut self,
        request: &JsonRpcRequest,
    ) -> Result<JsonRpcResponse, IpcClientError> {
        // Serialize and send the request.
        let mut payload = serde_json::to_vec(request)?;
        payload.push(b'\n');
        self.writer.write_all(&payload).await?;
        self.writer.flush().await?;

        // Read exactly one response line.
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        let line = line.trim();

        if line.is_empty() {
            return Err(IpcClientError::Protocol("empty response".into()));
        }

        // Try parsing as a success response first.
        if let Ok(resp) = serde_json::from_str::<JsonRpcResponse>(line) {
            return Ok(resp);
        }

        // Try parsing as an error response.
        if let Ok(err) = serde_json::from_str::<JsonRpcError>(line) {
            return Err(IpcClientError::Rpc {
                code: err.error.code,
                message: err.error.message,
            });
        }

        Err(IpcClientError::Protocol(format!(
            "unparseable response: {line}"
        )))
    }

    /// Issue a `tools/call` JSON-RPC request and return the raw `result` value.
    ///
    /// `brain` is injected into `arguments` so the server can route to the
    /// correct brain context.
    pub async fn tools_call(
        &mut self,
        tool_name: &str,
        brain: &str,
        mut arguments: Value,
    ) -> Result<Value, IpcClientError> {
        // Inject the brain routing field.
        if let Some(obj) = arguments.as_object_mut() {
            obj.insert("brain".into(), Value::String(brain.into()));
        }

        let id = self.next_id;
        self.next_id += 1;

        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(Value::Number(id.into())),
            method: "tools/call".into(),
            params: serde_json::json!({
                "name": tool_name,
                "arguments": arguments
            }),
        };

        let response = self.call(&request).await?;
        Ok(response.result)
    }

    // -----------------------------------------------------------------------
    // Convenience typed helpers — each issues a tools/call to the daemon.
    // -----------------------------------------------------------------------

    /// Check whether a daemon is listening at `socket_path`.
    ///
    /// Returns `true` if a connection succeeds, `false` otherwise.
    /// This is an async fn; wrap in `tokio::task::spawn_blocking` if you need
    /// a synchronous check.
    pub async fn is_daemon_available(socket_path: &Path) -> bool {
        UnixStream::connect(socket_path).await.is_ok()
    }

    /// Return the default daemon socket path: `~/.brain/brain.sock`.
    pub fn default_socket_path() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        PathBuf::from(home).join(".brain").join("brain.sock")
    }

    /// Ping the daemon by calling the `status` tool.
    pub async fn ping(&mut self, brain: &str) -> Result<Value, IpcClientError> {
        self.tools_call("status", brain, serde_json::json!({}))
            .await
    }

    // --- Task operations ---

    pub async fn task_create(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("tasks_create", brain, arguments).await
    }

    pub async fn task_fetch(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("tasks_get", brain, arguments).await
    }

    pub async fn task_close(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("tasks_close", brain, arguments).await
    }

    pub async fn task_list(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("tasks_list", brain, arguments).await
    }

    pub async fn task_labels_batch(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("tasks_labels_batch", brain, arguments)
            .await
    }

    // --- Record operations ---

    pub async fn record_get(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("records_get", brain, arguments).await
    }

    pub async fn record_list(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("records_list", brain, arguments).await
    }

    pub async fn record_fetch_content(
        &mut self,
        brain: &str,
        arguments: Value,
    ) -> Result<Value, IpcClientError> {
        self.tools_call("records_fetch_content", brain, arguments)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_to_nonexistent_socket_returns_daemon_unavailable() {
        let path = PathBuf::from("/tmp/brain-ipc-test-nonexistent-99999.sock");
        let err = IpcClient::connect(&path).await.unwrap_err();
        match err {
            IpcClientError::DaemonUnavailable { path: p, .. } => {
                assert_eq!(p, path);
            }
            other => panic!("expected DaemonUnavailable, got {other}"),
        }
    }

    #[tokio::test]
    async fn is_daemon_available_returns_false_for_missing_socket() {
        let path = PathBuf::from("/tmp/brain-ipc-test-missing-88888.sock");
        assert!(!IpcClient::is_daemon_available(&path).await);
    }

    #[test]
    fn default_socket_path_ends_with_brain_sock() {
        let p = IpcClient::default_socket_path();
        assert!(
            p.ends_with("brain.sock"),
            "expected path to end with brain.sock, got {p:?}"
        );
    }
}

#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::ipc::router::BrainRouter;
    use crate::ipc::server::IpcServer;
    use crate::mcp::tools::tests::create_test_context;

    /// Spin up a test IPC server, connect a client, verify ping succeeds.
    #[tokio::test]
    async fn client_ping_via_server() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("client_test.sock");

        let (_dir, ctx) = create_test_context().await;
        let mut map = HashMap::new();
        map.insert("test-brain".to_string(), Arc::new(ctx));
        let router = BrainRouter::new(map);
        let server = IpcServer::bind(&sock, router).expect("bind failed");
        let token = server.cancellation_token();
        tokio::spawn(async move { server.run().await });

        // Brief yield so the accept loop is ready.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut client = IpcClient::connect(&sock).await.expect("connect failed");
        let result = client.ping("test-brain").await.expect("ping failed");
        assert!(result.is_object(), "ping should return an object");

        token.cancel();
    }

    /// is_daemon_available returns true when a server is running.
    #[tokio::test]
    async fn is_daemon_available_true_when_server_running() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("avail_test.sock");

        let (_dir, ctx) = create_test_context().await;
        let mut map = HashMap::new();
        map.insert("test-brain".to_string(), Arc::new(ctx));
        let router = BrainRouter::new(map);
        let server = IpcServer::bind(&sock, router).expect("bind failed");
        let token = server.cancellation_token();
        tokio::spawn(async move { server.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert!(IpcClient::is_daemon_available(&sock).await);
        token.cancel();
    }

    /// tools_call routes through the server's router correctly.
    #[tokio::test]
    async fn client_tools_call_status() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("tools_call_test.sock");

        let (_dir, ctx) = create_test_context().await;
        let mut map = HashMap::new();
        map.insert("my-brain".to_string(), Arc::new(ctx));
        let router = BrainRouter::new(map);
        let server = IpcServer::bind(&sock, router).expect("bind failed");
        let token = server.cancellation_token();
        tokio::spawn(async move { server.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut client = IpcClient::connect(&sock).await.expect("connect failed");
        let result = client
            .tools_call("status", "my-brain", json!({}))
            .await
            .expect("tools_call failed");
        assert!(result.is_object());

        token.cancel();
    }
}
