//! `records.archive` MCP tool — thin wrapper over `DaemonClient::records_archive`.
//!
//! The daemon resolves the record ID, verifies existence, sets the archived
//! status and emits a `RecordArchived` event. Response shape mirrors the
//! legacy `{record_id, uri, status}` envelope byte-identical.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::RecordsArchiveParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordArchive;

#[derive(Deserialize)]
struct Params {
    record_id: String,
    reason: Option<String>,
}

impl McpTool for RecordArchive {
    fn name(&self) -> &'static str {
        "records.archive"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Archive a record. Creates a RecordArchived event.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to archive (full ID or unique prefix)"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Optional reason for archiving"
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

            let wire_params = RecordsArchiveParams {
                record_id: parsed.record_id,
                reason: parsed.reason,
            };

            let (record_id, uri, status) =
                match ctx.with_client(|c| c.records_archive(wire_params)).await {
                    Ok(r) => r,
                    Err(err) => {
                        return ToolCallResult::error(format!("Failed to archive record: {err}"));
                    }
                };

            json_response(&json!({
                "record_id": record_id,
                "uri": uri,
                "status": status,
            }))
        })
    }
}
