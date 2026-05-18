//! JSON-RPC 2.0 types for the IPC layer.
//!
//! These are local to the IPC module — they don't need to live in
//! `crate::mcp::protocol` now that the MCP extraction has separated the
//! tool-facing (`brain_mcp`) from the transport-facing (`brain_lib::ipc`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC 2.0 request envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// JSON-RPC 2.0 response envelope (success case).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub result: Value,
}

/// JSON-RPC 2.0 error body (inside error response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcErrorBody {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// JSON-RPC 2.0 error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub error: JsonRpcErrorBody,
}

impl JsonRpcResponse {
    pub fn new(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result,
        }
    }
}

impl JsonRpcError {
    pub fn new(id: Option<Value>, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            error: JsonRpcErrorBody {
                code,
                message: message.into(),
                data: None,
            },
        }
    }

    pub fn method_not_found(id: Option<Value>, method: &str) -> String {
        serde_json::to_string(&JsonRpcError::new(
            id,
            -32601,
            format!("Method not found: {method}"),
        ))
        .unwrap()
    }

    pub fn invalid_params(id: Option<Value>, msg: impl Into<String>) -> String {
        serde_json::to_string(&JsonRpcError::new(id, -32602, msg)).unwrap()
    }

    pub fn parse_error(id: Option<Value>, msg: &str) -> String {
        serde_json::to_string(&JsonRpcError::new(id, -32700, msg)).unwrap()
    }

    pub fn invalid_request(id: Option<Value>, msg: &str) -> String {
        serde_json::to_string(&JsonRpcError::new(id, -32600, msg)).unwrap()
    }

    pub fn internal(id: Option<Value>, msg: impl Into<String>) -> String {
        serde_json::to_string(&JsonRpcError::new(id, -32603, msg)).unwrap()
    }
}
