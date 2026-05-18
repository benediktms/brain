//! `records.fetch_content` MCP tool.
//!
//! # Wire gap
//!
//! There is no typed `records_fetch_content` method on `DaemonClient` —
//! the daemon does not yet expose a content-fetch RPC variant. This tool
//! returns a structured error directing the caller to use the daemon-side
//! `brain records fetch` CLI command until the wire method is added.
//!
//! When the wire method lands (tracked separately), this file should be
//! updated to route through `ctx.with_client(|c| c.records_fetch_content(...))`.
//! The schema below is preserved byte-identical to the legacy tool so that
//! clients do not need a schema update when the implementation is wired.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordFetchContent;

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
        _params: Value,
        _ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            ToolCallResult::error(
                "records.fetch_content is not yet wired in brain_mcp: \
                 no DaemonClient::records_fetch_content wire method exists. \
                 Use the brain CLI (`brain records fetch <id>`) directly until \
                 the RPC variant is added."
                    .to_string(),
            )
        })
    }
}
