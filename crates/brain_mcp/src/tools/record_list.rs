//! `records.list` MCP tool — routes to kind-specific list methods on
//! `DaemonClient` based on the `kind` filter param.
//!
//! # Routing strategy
//!
//! The wire layer exposes kind-specific list methods (`analyses_list`,
//! `documents_list`, `plans_list`, `snapshots_list`) plus `artifacts_list`
//! for the cross-kind view. When no `kind` filter is given this tool falls
//! back to `artifacts_list` which returns all kinds. When `kind` is set it
//! routes to the appropriate typed list method.
//!
//! # Wire gaps vs legacy
//!
//! - The legacy tool supported federated multi-brain queries via a `brains`
//!   param; the typed wire list methods accept `RecordsListParams` with no
//!   brain selector — cross-brain listing is not yet wired. The `brain` and
//!   `brains` params are accepted for schema compatibility but ignored with
//!   a warning in the response.
//! - Response shape emits the fields available from wire summaries
//!   (`record_id`, `title`, `created_at`, `brain_id`) rather than the richer
//!   legacy shape (`content_hash`, `media_type`, `task_id`, etc.) — those
//!   fields are not in the wire summary types yet.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::{ArtifactsListParams, RecordsListParams};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordList;

#[derive(Deserialize)]
struct Params {
    kind: Option<String>,
    #[serde(default = "default_status")]
    status: String,
    tag: Option<String>,
    task_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
    /// Cross-brain routing is not yet wired at the wire layer; passing
    /// `Some(_)` is rejected up front so callers aren't silently
    /// misled into thinking results came from the named brain.
    brain: Option<String>,
    /// As above for the multi-brain variant.
    brains: Option<Vec<String>>,
}

fn default_status() -> String {
    "active".to_string()
}

fn default_limit() -> u32 {
    50
}

impl McpTool for RecordList {
    fn name(&self) -> &'static str {
        "records.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List records with optional filters. Returns compact IDs. Cross-brain queries are not yet supported on the wire path — requests including `brain` or `brains` are rejected with a clear error; omit those parameters to query the current brain.".into(),
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
                        "description": "Optional list of brain names or IDs to query. Use [\"all\"] to query all registered brains. When omitted, queries only the current brain."
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
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            if parsed.brain.is_some() || parsed.brains.is_some() {
                return ToolCallResult::error(
                    "cross-brain routing is not yet wired for records.list; \
                     omit the `brain`/`brains` parameters to query the current brain",
                );
            }

            // The cross-kind path (no `kind` or an unrecognised `kind`)
            // routes through `artifacts_list`, whose params don't carry
            // `task_id`. Rather than silently drop the filter and return
            // unrelated records, reject the unsupported combination up
            // front whenever the kind isn't one of the per-kind routes.
            let kind_supports_task_id = matches!(
                parsed.kind.as_deref(),
                Some("analysis")
                    | Some("analyses")
                    | Some("document")
                    | Some("documents")
                    | Some("plan")
                    | Some("plans")
                    | Some("snapshot")
                    | Some("snapshots")
            );
            if parsed.task_id.is_some() && !kind_supports_task_id {
                return ToolCallResult::error(
                    "filtering by task_id requires a specific `kind` \
                     (analysis/document/plan/snapshot); the cross-kind path \
                     cannot honour task_id",
                );
            }

            let list_params = RecordsListParams {
                tag: parsed.tag,
                task_id: parsed.task_id,
                status: Some(parsed.status),
                limit: Some(parsed.limit),
            };

            // Route by kind; default to artifacts (cross-kind view).
            let records_json: Vec<Value> = match parsed.kind.as_deref() {
                Some("analysis") | Some("analyses") => {
                    match ctx.with_client(|c| c.analyses_list(list_params)).await {
                        Ok(recs) => recs
                            .into_iter()
                            .map(|r| {
                                json!({
                                    "record_id": r.record_id,
                                    "kind": "analysis",
                                    "title": r.title,
                                    "brain_id": r.brain_id,
                                    "created_at": r.created_at,
                                })
                            })
                            .collect(),
                        Err(e) => {
                            return ToolCallResult::error(format!("Failed to list analyses: {e}"));
                        }
                    }
                }
                Some("document") | Some("documents") => {
                    match ctx.with_client(|c| c.documents_list(list_params)).await {
                        Ok(recs) => recs
                            .into_iter()
                            .map(|r| {
                                json!({
                                    "record_id": r.record_id,
                                    "kind": "document",
                                    "title": r.title,
                                    "brain_id": r.brain_id,
                                    "created_at": r.created_at,
                                })
                            })
                            .collect(),
                        Err(e) => {
                            return ToolCallResult::error(format!("Failed to list documents: {e}"));
                        }
                    }
                }
                Some("plan") | Some("plans") => {
                    match ctx.with_client(|c| c.plans_list(list_params)).await {
                        Ok(recs) => recs
                            .into_iter()
                            .map(|r| {
                                json!({
                                    "record_id": r.record_id,
                                    "kind": "plan",
                                    "title": r.title,
                                    "brain_id": r.brain_id,
                                    "created_at": r.created_at,
                                })
                            })
                            .collect(),
                        Err(e) => {
                            return ToolCallResult::error(format!("Failed to list plans: {e}"));
                        }
                    }
                }
                Some("snapshot") | Some("snapshots") => {
                    match ctx.with_client(|c| c.snapshots_list(list_params)).await {
                        Ok(recs) => recs
                            .into_iter()
                            .map(|r| {
                                json!({
                                    "record_id": r.record_id,
                                    "kind": "snapshot",
                                    "title": r.title,
                                    "brain_id": r.brain_id,
                                    "created_at": r.created_at,
                                })
                            })
                            .collect(),
                        Err(e) => {
                            return ToolCallResult::error(format!("Failed to list snapshots: {e}"));
                        }
                    }
                }
                _ => {
                    // No kind filter or unknown kind: use artifacts cross-kind view.
                    let artifacts_params = ArtifactsListParams {
                        kind: parsed.kind,
                        tag: list_params.tag,
                        status: list_params.status,
                        limit: list_params.limit,
                    };
                    match ctx
                        .with_client(|c| c.artifacts_list(artifacts_params))
                        .await
                    {
                        Ok(recs) => recs
                            .into_iter()
                            .map(|r| {
                                json!({
                                    "record_id": r.record_id,
                                    "kind": r.kind,
                                    "status": r.status,
                                    "title": r.title,
                                    "brain_id": r.brain_id,
                                    "created_at": r.created_at,
                                })
                            })
                            .collect(),
                        Err(e) => {
                            return ToolCallResult::error(format!("Failed to list records: {e}"));
                        }
                    }
                }
            };

            let count = records_json.len();
            json_response(&json!({
                "records": records_json,
                "count": count,
            }))
        })
    }
}
