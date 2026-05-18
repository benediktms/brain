//! Per-request JSON-RPC dispatcher.
//!
//! Owns the three MCP method paths the server understands at scaffold
//! time:
//!
//! - `initialize` — resolves the session brain from the client's
//!   `roots` array by querying the daemon's brain registry via
//!   [`brain_rpc::DaemonClient::brains_list`].
//! - `tools/list` — returns an empty list at scaffold time; the 51
//!   tool definitions migrate from `brain_lib::mcp::tools::ToolRegistry`
//!   in Phase D.
//! - `tools/call` — returns a placeholder error envelope until tool
//!   bodies migrate in Phase D.
//!
//! Notifications (`notifications/initialized`) produce no response per
//! the JSON-RPC contract.

use serde_json::Value;
use tracing::{error, info, warn};

use brain_rpc::BrainsListParams;

use crate::context::McpContext;
use crate::protocol::{
    InitializeResult, JsonRpcError, JsonRpcRequest, JsonRpcResponse, ServerCapabilities,
    ServerInfo, ToolCallResult, ToolsCapability, ToolsListResult,
};

/// Handle one JSON-RPC request, returning the serialised response (or
/// the empty string for notifications).
pub async fn handle_request(req: JsonRpcRequest, ctx: &McpContext) -> String {
    let id = req.id.clone();

    match req.method.as_str() {
        "initialize" => initialize(req, id, ctx).await,
        "notifications/initialized" => {
            info!("MCP client initialized");
            String::new()
        }
        "tools/list" => {
            // Empty until the 51-tool migration lands in Phase D.
            let result = ToolsListResult { tools: Vec::new() };
            match serde_json::to_value(result) {
                Ok(value) => serialize_response(&JsonRpcResponse::new(id, value)),
                Err(e) => {
                    error!(error = %e, "failed to serialize tools/list result");
                    serialize_error(&JsonRpcError::internal_error(
                        id,
                        "Internal: tools/list result serialization failed",
                    ))
                }
            }
        }
        "tools/call" => {
            // Placeholder: per-tool dispatch wires up in Phase D when
            // each tool body migrates from `ctx.stores.X.Y(...)` to
            // `ctx.with_client(|c| c.<typed>(...))`.
            let tool_name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("<unnamed>");
            let result = ToolCallResult::error(format!(
                "tool '{tool_name}' is not yet migrated to brain_mcp (Phase D pending)"
            ));
            match serde_json::to_value(result) {
                Ok(value) => serialize_response(&JsonRpcResponse::new(id, value)),
                Err(e) => {
                    error!(error = %e, tool = tool_name, "failed to serialize tools/call result");
                    serialize_error(&JsonRpcError::internal_error(
                        id,
                        "Internal: tools/call result serialization failed",
                    ))
                }
            }
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

    match serde_json::to_value(result) {
        Ok(value) => serialize_response(&JsonRpcResponse::new(id, value)),
        Err(e) => {
            error!(error = %e, "failed to serialize initialize result");
            serialize_error(&JsonRpcError::internal_error(
                id,
                "Internal: initialize result serialization failed",
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
                if client_root == &brain_root || client_root.starts_with(&brain_root) {
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
