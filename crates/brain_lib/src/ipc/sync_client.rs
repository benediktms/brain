/// Blocking (non-async) IPC client for the brain daemon.
///
/// Provides a synchronous `tools/call` helper for CLI commands that run outside
/// tokio. Uses `std::os::unix::net::UnixStream` and the same newline-delimited
/// JSON-RPC 2.0 protocol as the async [`super::client::IpcClient`].
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;

use serde_json::Value;

use super::client::IpcClientError;
use crate::mcp::protocol::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};

/// Issue a single blocking `tools/call` to the daemon and return the result.
///
/// Connects, sends, reads one response, then drops the connection. Stateless
/// by design — no connection pooling. Suitable for infrequent CLI calls.
pub fn sync_tools_call(
    tool: &str,
    brain: &str,
    mut arguments: Value,
) -> Result<Value, IpcClientError> {
    let socket_path = super::client::IpcClient::default_socket_path();

    let stream =
        UnixStream::connect(&socket_path).map_err(|e| IpcClientError::DaemonUnavailable {
            path: socket_path.clone(),
            source: e,
        })?;

    // Inject the brain routing field.
    if let Some(obj) = arguments.as_object_mut() {
        obj.insert("__ipc_brain".into(), Value::String(brain.into()));
    }

    let request = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: Some(Value::Number(1.into())),
        method: "tools/call".into(),
        params: serde_json::json!({
            "name": tool,
            "arguments": arguments
        }),
    };

    let mut payload = serde_json::to_vec(&request)?;
    payload.push(b'\n');

    let mut writer = stream.try_clone()?;
    writer.write_all(&payload)?;
    writer.flush()?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let line = line.trim();

    if line.is_empty() {
        return Err(IpcClientError::Protocol("empty response".into()));
    }

    if let Ok(resp) = serde_json::from_str::<JsonRpcResponse>(line) {
        return Ok(resp.result);
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_tools_call_to_nonexistent_daemon_returns_unavailable() {
        // Override HOME so default_socket_path points to a nonexistent socket.
        let tmp = tempfile::TempDir::new().unwrap();
        let fake_home = tmp.path().to_str().unwrap();

        // SAFETY: this test is not run in parallel with other tests that
        // read HOME (enforced by #[serial] or test isolation).
        let original_home = std::env::var("HOME").ok();
        unsafe { std::env::set_var("HOME", fake_home) };

        let result = sync_tools_call("status", "test", serde_json::json!({}));

        if let Some(h) = original_home {
            unsafe { std::env::set_var("HOME", h) };
        }

        assert!(
            matches!(result, Err(IpcClientError::DaemonUnavailable { .. })),
            "expected DaemonUnavailable, got {result:?}"
        );
    }
}
