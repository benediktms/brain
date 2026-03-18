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
    brain: Option<String>,
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

        let remote_brain: Option<(String, crate::records::RecordStore)> =
            if let Some(ref brain) = params.brain {
                let (brain_name, bid) = match ctx.resolve_brain_id(brain) {
                    Ok(r) => r,
                    Err(e) => {
                        return ToolCallResult::error(format!("Failed to resolve brain: {e}"));
                    }
                };
                match ctx.records_for_brain(&bid, &brain_name) {
                    Ok(recs) => Some((brain_name, recs)),
                    Err(e) => {
                        return ToolCallResult::error(format!("Failed to open brain stores: {e}"));
                    }
                }
            } else {
                None
            };
        let (records, remote_brain_name): (&crate::records::RecordStore, Option<String>) =
            match remote_brain {
                Some((ref name, ref recs)) => (recs, Some(name.clone())),
                None => (&ctx.records, None),
            };

        let filter = RecordFilter {
            kind: params.kind,
            status: Some(params.status),
            tag: params.tag,
            task_id: params.task_id,
            limit: Some(params.limit),
            brain_id: None,
        };

        let record_list = match records.list_records(&filter) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to list records: {e}")),
        };

        let compact_ids = records.compact_record_ids().unwrap_or_default();

        let records_json: Vec<Value> = record_list
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

        let mut result = json!({
            "records": records_json,
            "count": records_json.len(),
        });

        if let Some(name) = remote_brain_name {
            result["brain"] = json!(name);
        }

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
                    },
                    "brain": {
                        "type": "string",
                        "description": "Target brain name or ID. When provided, lists records from that brain."
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
