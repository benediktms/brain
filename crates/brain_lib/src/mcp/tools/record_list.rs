use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::queries::RecordFilter;
use crate::uri::SynapseUri;

use super::scope::{BRAINS_PARAM_DESCRIPTION, BrainRef, resolve_scope};
use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

#[derive(Deserialize)]
struct Params {
    kind: Option<String>,
    #[serde(default = "default_status")]
    status: String,
    tag: Option<String>,
    task_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
    /// Deprecated: use `brains` instead. When set, treated as `brains: [brain]`.
    brain: Option<String>,
    /// Brains to query. See `BRAINS_PARAM_DESCRIPTION`.
    #[serde(default)]
    brains: Option<Vec<String>>,
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

        // `brain` (singular) is a back-compat alias for `brains: [brain]`.
        let brains_arg: Option<Vec<String>> = match (&params.brains, &params.brain) {
            (Some(bs), _) => Some(bs.clone()),
            (None, Some(b)) => Some(vec![b.clone()]),
            (None, None) => None,
        };

        let scope = match resolve_scope(ctx, brains_arg.as_deref()) {
            Ok(s) => s,
            Err(err) => return err,
        };

        let mut per_brain: Vec<(BrainRef, Arc<McpContext>)> = Vec::new();
        for brain_ref in scope.brains() {
            let scoped_ctx = match ctx.with_brain_id(&brain_ref.brain_id, &brain_ref.brain_name) {
                Ok(c) => c,
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to scope to brain '{}': {e}",
                        brain_ref.brain_name
                    ));
                }
            };
            per_brain.push((brain_ref.clone(), scoped_ctx));
        }

        // For federated reads we drop the per-brain SQL LIMIT — otherwise each
        // brain's result is truncated independently and the merged response is
        // biased toward whichever brain is iterated first. The global limit
        // applies once on the merged set below.
        let per_brain_limit = if scope.is_federated() {
            None
        } else {
            Some(params.limit)
        };
        let filter = RecordFilter {
            kind: params.kind.clone(),
            status: Some(params.status.clone()),
            tag: params.tag.clone(),
            task_id: params.task_id.clone(),
            limit: per_brain_limit,
            brain_id: None,
        };

        let mut warnings: Vec<Warning> = Vec::new();
        let mut all_records: Vec<Value> = Vec::new();

        for (brain_ref, scoped_ctx) in &per_brain {
            let records = &scoped_ctx.stores.records;
            let record_list = match records.list_records(&filter) {
                Ok(r) => r,
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to list records for brain '{}': {e}",
                        brain_ref.brain_name
                    ));
                }
            };
            let compact_ids = store_or_warn(
                records.compact_record_ids(),
                "compact_record_ids",
                &mut warnings,
            );

            for r in record_list {
                let compact_id = compact_ids
                    .get(&r.record_id)
                    .cloned()
                    .unwrap_or_else(|| r.record_id.clone());
                let uri = SynapseUri::for_record(&brain_ref.brain_name, &compact_id).to_string();
                all_records.push(json!({
                    "record_id": compact_id,
                    "uri": uri,
                    "brain": brain_ref.brain_name,
                    "title": r.title,
                    "kind": r.kind,
                    "status": r.status,
                    "content_hash": r.content_hash,
                    "content_size": r.content_size,
                    "media_type": r.media_type,
                    "task_id": r.task_id,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                }));
            }
        }

        // Sort merged results by `updated_at DESC` so the global limit picks
        // the most-recent records across brains, matching the single-brain
        // SQL ORDER BY in `records::queries::list_records`.
        if scope.is_federated() {
            all_records.sort_by(|a, b| {
                let a_ts = a.get("updated_at").and_then(|v| v.as_i64()).unwrap_or(0);
                let b_ts = b.get("updated_at").and_then(|v| v.as_i64()).unwrap_or(0);
                b_ts.cmp(&a_ts)
            });
        }

        // Apply global limit after merging across brains.
        let total = all_records.len();
        let capped = if all_records.len() > params.limit {
            all_records
                .into_iter()
                .take(params.limit)
                .collect::<Vec<_>>()
        } else {
            all_records
        };

        let mut result = json!({
            "records": capped,
            "count": capped.len(),
            "total": total,
        });

        if scope.is_federated() {
            let brain_names: Vec<&str> = per_brain
                .iter()
                .map(|(b, _)| b.brain_name.as_str())
                .collect();
            result["brains"] = json!(brain_names);
        } else {
            result["brain"] = json!(per_brain[0].0.brain_name);
        }

        inject_warnings(&mut result, warnings);
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
            description: "List records with optional filters. Returns compact IDs. Supports cross-brain queries via the `brains` parameter.".into(),
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
                        "description": "DEPRECATED: use `brains` instead. Equivalent to `brains: [brain]`."
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": BRAINS_PARAM_DESCRIPTION
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
