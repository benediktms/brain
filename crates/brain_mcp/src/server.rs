//! MCP stdio JSON-RPC server.
//!
//! Reads newline-delimited JSON-RPC 2.0 frames from stdin, dispatches
//! via [`crate::handle::handle_request`], and writes responses to
//! stdout. All `tracing` output goes to stderr — stdout is reserved
//! for the protocol stream. The loop returns `Ok(())` when stdin
//! closes (clean shutdown) or an I/O error occurs.

use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, error, info};

use crate::context::McpContext;
use crate::handle;
use crate::protocol::JsonRpcRequest;
use crate::tools::ToolRegistry;

/// Run the stdio MCP server loop until stdin closes.
pub async fn run_server(ctx: Arc<McpContext>) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let registry = ToolRegistry::new();

    info!("brain_mcp server starting");

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        // Two-step parse so we can split JSON-RPC error codes correctly:
        //   - JSON not parseable → Parse error (-32700)
        //   - JSON valid but doesn't fit JsonRpcRequest → Invalid Request (-32600)
        // We also log only safe metadata (size + method + id) — never the
        // raw payload, which may contain user content.
        let response = match serde_json::from_str::<serde_json::Value>(&line) {
            Err(e) => {
                error!(error = %e, size = line.len(), "JSON-RPC parse error");
                r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
                    .to_string()
            }
            Ok(value) => {
                let method = value
                    .get("method")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let id = value.get("id").cloned();
                debug!(size = line.len(), method = ?method, id = ?id, "received request");
                match serde_json::from_value::<JsonRpcRequest>(value) {
                    Ok(req) => handle::handle_request(req, &ctx, &registry).await,
                    Err(e) => {
                        error!(error = %e, size = line.len(), method = ?method, "invalid JSON-RPC request");
                        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}"#
                            .to_string()
                    }
                }
            }
        };

        if !response.is_empty() {
            stdout.write_all(response.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    info!("brain_mcp server shutting down (stdin closed)");
    Ok(())
}
