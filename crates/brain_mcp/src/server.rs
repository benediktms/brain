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

/// Run the stdio MCP server loop until stdin closes.
pub async fn run_server(ctx: Arc<McpContext>) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    info!("brain_mcp server starting");

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        debug!(line = %line, "received request");

        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => handle::handle_request(req, &ctx).await,
            Err(e) => {
                error!(error = %e, "invalid JSON-RPC request");
                r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
                    .to_string()
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
