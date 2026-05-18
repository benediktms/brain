//! Per-request JSON-RPC dispatcher.
//!
//! Owns the three MCP method paths the server understands:
//!
//! - `initialize` — resolves the session brain from the client's
//!   `roots` array by querying the daemon's brain registry via
//!   [`brain_rpc::DaemonClient::brains_list`].
//! - `tools/list` — enumerates definitions from the [`ToolRegistry`].
//!   Tools migrate cluster-by-cluster from `brain_lib::mcp::tools`;
//!   anything not yet migrated falls through `tools/call` with an
//!   "Unknown tool" envelope.
//! - `tools/call` — dispatches to the matching tool via
//!   [`ToolRegistry::dispatch`].
//!
//! Notifications (`notifications/initialized`) produce no response per
//! the JSON-RPC contract.

use serde::Serialize;
use serde_json::Value;
use tracing::{error, info, warn};

use brain_rpc::BrainsListParams;

use crate::context::McpContext;
use crate::protocol::{
    InitializeResult, JsonRpcError, JsonRpcRequest, JsonRpcResponse, ServerCapabilities,
    ServerInfo, ToolsCapability, ToolsListResult,
};
use crate::tools::ToolRegistry;

/// Handle one JSON-RPC request, returning the serialised response (or
/// the empty string for notifications).
pub async fn handle_request(
    req: JsonRpcRequest,
    ctx: &McpContext,
    registry: &ToolRegistry,
) -> String {
    let id = req.id.clone();

    match req.method.as_str() {
        "initialize" => initialize(req, id, ctx).await,
        "notifications/initialized" => {
            info!("MCP client initialized");
            String::new()
        }
        "tools/list" => serialize_typed_response(
            id,
            ToolsListResult {
                tools: registry.definitions(),
            },
            "tools/list",
        ),
        "tools/call" => {
            let tool_name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let arguments = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));
            let result = registry.dispatch(tool_name, arguments, ctx).await;
            serialize_typed_response(id, result, "tools/call")
        }
        _ => serialize_error(&JsonRpcError::method_not_found(id, &req.method)),
    }
}

async fn initialize(req: JsonRpcRequest, id: Option<Value>, ctx: &McpContext) -> String {
    if let Some(roots) = req.params.get("roots") {
        match resolve_brain_from_roots(roots, ctx).await {
            Ok(Some(name)) => {
                info!(brain = %name, "session brain resolved from initialize roots");
                ctx.set_brain_name(name).await;
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "failed to resolve session brain from roots"),
        }
    }

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

    serialize_typed_response(id, result, "initialize")
}

/// Serialize `result` as a JSON-RPC success response, falling back to
/// a `-32603` internal-error envelope (labelled with `context`) if the
/// `serde_json::to_value` step fails. The `context` label appears in
/// both the tracing log and the user-visible error message so failure
/// triage points straight at the failing method.
fn serialize_typed_response<T: Serialize>(id: Option<Value>, result: T, context: &str) -> String {
    match serde_json::to_value(result) {
        Ok(value) => serialize_response(&JsonRpcResponse::new(id, value)),
        Err(e) => {
            error!(error = %e, context = context, "failed to serialize result");
            serialize_error(&JsonRpcError::internal_error(
                id,
                format!("Internal: {context} result serialization failed"),
            ))
        }
    }
}

/// Parse the client's `roots` array and ask the daemon for the
/// registered brain list, then match by path prefix.
///
/// Returns `Ok(Some(name))` on the first prefix match, `Ok(None)` when
/// no candidate brain matches, and an error if the wire call to the
/// daemon failed.
async fn resolve_brain_from_roots(
    roots: &Value,
    ctx: &McpContext,
) -> anyhow::Result<Option<String>> {
    let roots_arr = match roots.as_array() {
        Some(a) if !a.is_empty() => a,
        _ => return Ok(None),
    };

    let root_paths: Vec<std::path::PathBuf> = roots_arr
        .iter()
        .filter_map(|r| r.get("uri").and_then(|u| u.as_str()))
        .map(|uri| {
            let path = uri.strip_prefix("file://").unwrap_or(uri);
            std::path::PathBuf::from(path)
        })
        .collect();

    if root_paths.is_empty() {
        return Ok(None);
    }

    let (brains, _count) = ctx
        .with_client(|c| c.brains_list(BrainsListParams::default()))
        .await
        .map_err(|e| anyhow::anyhow!("brains_list failed: {e}"))?;

    for brain in &brains {
        if brain.archived {
            continue;
        }
        let brain_roots = std::iter::once(brain.root.as_str())
            .chain(brain.extra_roots.iter().map(std::string::String::as_str));
        for brain_root_str in brain_roots {
            let brain_root = std::path::PathBuf::from(brain_root_str);
            for client_root in &root_paths {
                // `Path::starts_with` returns true on full equality,
                // so a separate `==` arm would be dead.
                if client_root.starts_with(&brain_root) {
                    return Ok(Some(brain.name.clone()));
                }
            }
        }
    }

    Ok(None)
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
