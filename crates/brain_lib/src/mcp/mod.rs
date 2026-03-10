/// MCP stdio JSON-RPC server.
///
/// Implements the Model Context Protocol over newline-delimited JSON-RPC
/// on stdin/stdout. All tracing goes to stderr.
pub mod protocol;
pub mod tools;

use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, error, info, warn};

use crate::db::Db;
use crate::embedder::Embed;
use crate::metrics::Metrics;
use crate::store::StoreReader;
use crate::tasks::TaskStore;

use protocol::{
    InitializeResult, JsonRpcError, JsonRpcRequest, JsonRpcResponse, ServerCapabilities,
    ServerInfo, ToolsCapability, ToolsListResult,
};
use tools::ToolRegistry;

/// Shared context for MCP tool handlers.
///
/// `store` and `embedder` are optional — they require the embedding model to
/// be downloaded. When absent, task tools still work but memory/search tools
/// return an error asking the user to download the model via the HuggingFace CLI.
pub struct McpContext {
    pub db: Db,
    pub store: Option<StoreReader>,
    pub embedder: Option<Arc<dyn Embed>>,
    pub tasks: TaskStore,
    pub metrics: Arc<Metrics>,
}

/// Run the MCP server, reading JSON-RPC from stdin and writing to stdout.
///
/// All logging goes to stderr (stdout is reserved for MCP protocol).
/// Returns when stdin is closed.
pub async fn run_server(ctx: Arc<McpContext>) -> crate::error::Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let registry = ToolRegistry::new();

    info!("MCP server starting");

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(crate::error::BrainCoreError::Io)?
    {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        debug!(line = %line, "received request");

        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => handle_request(req, &ctx, &registry).await,
            Err(e) => {
                error!(error = %e, "invalid JSON-RPC request");
                r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
                    .to_string()
            }
        };

        if !response.is_empty() {
            stdout
                .write_all(response.as_bytes())
                .await
                .map_err(crate::error::BrainCoreError::Io)?;
            stdout
                .write_all(b"\n")
                .await
                .map_err(crate::error::BrainCoreError::Io)?;
            stdout
                .flush()
                .await
                .map_err(crate::error::BrainCoreError::Io)?;
        }
    }

    info!("MCP server shutting down (stdin closed)");
    Ok(())
}

/// Handle a single JSON-RPC request and return the serialized response.
async fn handle_request(req: JsonRpcRequest, ctx: &McpContext, registry: &ToolRegistry) -> String {
    let id = req.id.clone();

    match req.method.as_str() {
        "initialize" => {
            let result = InitializeResult {
                protocol_version: "2024-11-05".into(),
                capabilities: ServerCapabilities {
                    tools: ToolsCapability {},
                },
                server_info: ServerInfo {
                    name: "brain".into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                },
            };

            serialize_response(&JsonRpcResponse::new(
                id,
                serde_json::to_value(result).unwrap(),
            ))
        }
        "notifications/initialized" => {
            // No response for notifications
            info!("MCP client initialized");
            String::new()
        }
        "tools/list" => {
            let result = ToolsListResult {
                tools: registry.definitions(),
            };
            serialize_response(&JsonRpcResponse::new(
                id,
                serde_json::to_value(result).unwrap(),
            ))
        }
        "tools/call" => {
            let tool_name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| {
                    warn!("MCP request missing tool name");
                    ""
                });
            let arguments = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));

            let call_start = std::time::Instant::now();
            let result = registry.dispatch(tool_name, arguments, ctx).await;
            if matches!(
                tool_name,
                "memory.search_minimal" | "memory.expand" | "memory.reflect"
            ) {
                ctx.metrics.record_query_latency(call_start.elapsed());
            }
            serialize_response(&JsonRpcResponse::new(
                id,
                serde_json::to_value(result).unwrap(),
            ))
        }
        _ => serialize_error(&JsonRpcError::method_not_found(id, &req.method)),
    }
}

fn serialize_response(resp: &JsonRpcResponse) -> String {
    serde_json::to_string(resp).unwrap_or_else(|e| {
        error!("Failed to serialize MCP response: {e}");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal: response serialization failed"}}"#.to_string()
    })
}

fn serialize_error(err: &JsonRpcError) -> String {
    serde_json::to_string(err).unwrap_or_else(|e| {
        error!("Failed to serialize MCP error: {e}");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal: error serialization failed"}}"#.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn call(method: &str, params: Value) -> String {
        let (_dir, ctx) = tools::tests::create_test_context().await;
        let registry = ToolRegistry::new();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(1)),
            method: method.into(),
            params,
        };
        handle_request(req, &ctx, &registry).await
    }

    #[tokio::test]
    async fn test_initialize() {
        let resp = call("initialize", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();

        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(parsed["result"]["serverInfo"]["name"], "brain");
        assert!(parsed["result"]["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn test_tools_list() {
        let resp = call("tools/list", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();

        let tools = parsed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 13);

        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"memory.search_minimal"));
        assert!(names.contains(&"memory.expand"));
        assert!(names.contains(&"tasks.apply_event"));
        assert!(names.contains(&"tasks.labels_batch"));
        assert!(names.contains(&"tasks.deps_batch"));
        assert!(names.contains(&"tasks.get"));
        assert!(names.contains(&"tasks.list"));
        assert!(names.contains(&"tasks.next"));
    }

    #[tokio::test]
    async fn test_method_not_found() {
        let resp = call("unknown/method", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();

        assert!(parsed["error"].is_object());
        assert_eq!(parsed["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn test_notification_no_response() {
        let resp = call("notifications/initialized", json!({})).await;
        assert!(resp.is_empty());
    }
}
