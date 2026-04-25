use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::uri::{SynapseUri, resolve_id};

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

#[derive(Deserialize)]
struct Params {
    record_id: String,
    brain: Option<String>,
}

pub(super) struct RecordGet;

impl RecordGet {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let remote_brain: Option<(String, crate::records::RecordStore)> =
            if let Some(ref brain) = params.brain {
                let (bid, brain_name) = match ctx.resolve_brain_id(brain) {
                    Ok(r) => r,
                    Err(e) => {
                        return ToolCallResult::error(format!("Failed to resolve brain: {e}"));
                    }
                };
                match ctx.stores.with_brain_id(&bid, &brain_name) {
                    Ok(s) => Some((brain_name, s.records)),
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
                None => (&ctx.stores.records, None),
            };

        let record_id_input = resolve_id(&params.record_id);
        let record_id = match records.resolve_record_id(&record_id_input) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        let record = match records.get_record(&record_id) {
            Ok(Some(r)) => r,
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        };

        let mut warnings: Vec<Warning> = Vec::new();
        let tags = store_or_warn(
            records.get_record_tags(&record_id),
            "get_record_tags",
            &mut warnings,
        );
        let links = store_or_warn(
            records.get_record_links(&record_id),
            "get_record_links",
            &mut warnings,
        );

        let compact_id = match records.compact_record_id(&record_id) {
            Ok(id) => id,
            Err(_) => record_id.clone(),
        };

        let links_json: Vec<Value> = links
            .iter()
            .map(|l| {
                json!({
                    "task_id": l.task_id,
                    "chunk_id": l.chunk_id,
                    "created_at": l.created_at,
                })
            })
            .collect();

        let mut result = json!({
            "record_id": compact_id,
            "title": record.title,
            "kind": record.kind,
            "status": record.status,
            "description": record.description,
            "content_hash": record.content_hash,
            "content_size": record.content_size,
            "media_type": record.media_type,
            "task_id": record.task_id,
            "actor": record.actor,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "tags": tags,
            "links": links_json,
        });

        let uri_brain = if let Some(ref name) = remote_brain_name {
            result["brain"] = json!(name);
            name.as_str()
        } else {
            ctx.brain_name()
        };

        let uri = SynapseUri::for_record(uri_brain, &compact_id).to_string();
        if let Some(obj) = result.as_object_mut() {
            obj.insert("uri".into(), json!(uri));
        }

        inject_warnings(&mut result, warnings);

        json_response(&result)
    }
}

impl McpTool for RecordGet {
    fn name(&self) -> &'static str {
        "records.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get a record by ID with full details including tags and links. Supports prefix resolution. Use the brain parameter to fetch from a remote brain instead of locally.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to retrieve (full ID or unique prefix)"
                    },
                    "brain": {
                        "type": "string",
                        "description": "Target brain name or ID. When provided, fetches the record from that brain."
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
