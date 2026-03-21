use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::events::{RecordArchivedPayload, RecordEvent};
use crate::uri::{SynapseUri, resolve_id};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    record_id: String,
    reason: Option<String>,
}

pub(super) struct RecordArchive;

impl RecordArchive {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let record_id_input = resolve_id(&params.record_id);
        let record_id = match ctx.stores.records.resolve_record_id(&record_id_input) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        // Verify record exists before archiving
        match ctx.stores.records.get_record(&record_id) {
            Ok(Some(_)) => {}
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        }

        let payload = RecordArchivedPayload {
            reason: params.reason,
        };

        let event = RecordEvent::from_payload(&record_id, "mcp", payload);

        if let Err(e) = ctx.stores.records.apply_event(&event) {
            return ToolCallResult::error(format!("Failed to archive record: {e}"));
        }

        let compact_id = ctx
            .stores
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        let uri = SynapseUri::for_record(ctx.brain_name(), &compact_id).to_string();

        let result = json!({
            "record_id": compact_id,
            "uri": uri,
            "status": "archived",
        });

        json_response(&result)
    }
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
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}
