use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::queries::RecordFilter;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    kind: Option<String>,
    #[serde(default = "default_status")]
    status: String,
    tag: Option<String>,
    task_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_status() -> String {
    "active".to_string()
}

fn default_limit() -> usize {
    50
}

pub(super) struct RecordList;

impl RecordList {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let filter = RecordFilter {
            kind: params.kind,
            status: Some(params.status),
            tag: params.tag,
            task_id: params.task_id,
            limit: Some(params.limit),
        };

        let records = match ctx.records.list_records(&filter) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to list records: {e}")),
        };

        let compact_ids = ctx.records.compact_record_ids().unwrap_or_default();

        let records_json: Vec<Value> = records
            .iter()
            .map(|r| {
                let compact_id = compact_ids
                    .get(&r.record_id)
                    .cloned()
                    .unwrap_or_else(|| r.record_id.clone());
                json!({
                    "record_id": compact_id,
                    "title": r.title,
                    "kind": r.kind,
                    "status": r.status,
                    "content_hash": r.content_hash,
                    "content_size": r.content_size,
                    "media_type": r.media_type,
                    "task_id": r.task_id,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                })
            })
            .collect();

        let result = json!({
            "records": records_json,
            "count": records_json.len(),
        });

        json_response(&result)
    }
}

impl McpTool for RecordList {
    fn name(&self) -> &'static str {
        "records.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List records with optional filters. Returns compact IDs.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Filter by record kind (e.g. 'artifact', 'snapshot', 'report', 'document')"
                    },
                    "status": {
                        "type": "string",
                        "description": "Filter by status. Defaults to 'active'.",
                        "enum": ["active", "archived"],
                        "default": "active"
                    },
                    "tag": {
                        "type": "string",
                        "description": "Filter by tag"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Filter by associated task ID"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of records to return. Defaults to 50.",
                        "default": 50
                    }
                }
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
