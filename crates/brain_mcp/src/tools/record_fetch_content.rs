//! `records.fetch_content` MCP tool — thin wrapper over
//! [`brain_rpc::DaemonClient::records_fetch_content`].
//!
//! The daemon resolves the record id (and the optional remote-brain
//! target), reads the content blob from the object store, applies the
//! text-vs-binary heuristic, and returns a typed
//! [`brain_rpc::RecordContent`]. The wire type serialises to the legacy
//! envelope byte-for-byte (the base64 payload travels under the `data`
//! key via `#[serde(rename)]`), so this tool body has no JSON-shape
//! logic of its own.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::RecordsFetchContentParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[allow(dead_code)]
pub(super) struct RecordFetchContent;

#[allow(dead_code)]
#[derive(Deserialize)]
struct Params {
    record_id: String,
    #[serde(default)]
    brain: Option<String>,
}

impl McpTool for RecordFetchContent {
    fn name(&self) -> &'static str {
        "records.fetch_content"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Fetch the content of a record. For text content (media_type starting with 'text/' or 'application/json'), returns decoded UTF-8 text directly in the 'text' field. For binary content, returns base64-encoded data in the 'data' field. The 'encoding' field indicates how to interpret the content ('utf-8' or 'base64'). Includes title and kind metadata. Use the brain parameter to fetch from a remote brain instead of locally.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID (full ID or unique prefix)"
                    },
                    "brain": {
                        "type": "string",
                        "description": "Target brain name or ID. When provided, fetches content from that brain."
                    }
                },
                "required": ["record_id"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let wire_params = RecordsFetchContentParams {
                record_id: parsed.record_id,
                brain: parsed.brain,
            };

            let content = match ctx
                .with_client(|c| c.records_fetch_content(wire_params))
                .await
            {
                Ok(c) => c,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to fetch record content: {err}"));
                }
            };

            json_response(&content)
        })
    }
}
