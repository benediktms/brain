//! `records.link_add` and `records.link_remove` MCP tools — thin wrappers
//! over `DaemonClient::records_link_add` / `records_link_remove`.
//!
//! These are the deprecated record-scoped link shims. The caller provides a
//! `record_id` plus exactly one of `task_id` or `chunk_id`; the tool
//! constructs a `RecordsLinkParams` and delegates to the daemon.
//!
//! The response preserves the legacy `{record_id, task_id, chunk_id, action,
//! deprecated, deprecation_message}` envelope byte-identical so existing
//! clients do not break on migration.
//!
//! Prefer `links.add` / `links.remove` (the generic polymorphic surface) for
//! new callers.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::{RecordsLinkParams, WireEntityRef};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

// ── Shared params ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LinkParams {
    record_id: String,
    task_id: Option<String>,
    chunk_id: Option<String>,
}

// ── RecordsLinkAdd ─────────────────────────────────────────────────────────

pub(super) struct RecordsLinkAdd;

impl McpTool for RecordsLinkAdd {
    fn name(&self) -> &'static str {
        "records.link_add"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Add a link from a record to a task or note chunk. At least one of task_id or chunk_id must be provided. Idempotent — duplicate links are deduplicated by the projection.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to link from (full ID or unique prefix)"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to link to (optional if chunk_id is provided)"
                    },
                    "chunk_id": {
                        "type": "string",
                        "description": "Note chunk ID to link to (optional if task_id is provided)"
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
            let parsed: LinkParams = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            match (parsed.task_id.as_deref(), parsed.chunk_id.as_deref()) {
                (None, None) => {
                    return ToolCallResult::error(
                        "must specify either task_id or chunk_id (got neither)".to_string(),
                    );
                }
                (Some(_), Some(_)) => {
                    return ToolCallResult::error(
                        "specify exactly one of task_id or chunk_id (got both)".to_string(),
                    );
                }
                _ => {}
            }

            let (target_kind, target_id) = if let Some(ref tid) = parsed.task_id {
                ("TASK", tid.clone())
            } else {
                ("CHUNK", parsed.chunk_id.clone().unwrap())
            };

            let wire_params = RecordsLinkParams {
                record_id: parsed.record_id.clone(),
                target: WireEntityRef {
                    kind: target_kind.to_string(),
                    id: target_id,
                },
                link_kind: "covers".to_string(),
            };

            match ctx.with_client(|c| c.records_link_add(wire_params)).await {
                Ok(_created) => {}
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to add link: {err}"));
                }
            }

            json_response(&json!({
                "record_id": parsed.record_id,
                "task_id": parsed.task_id,
                "chunk_id": parsed.chunk_id,
                "action": "linked",
                "deprecated": true,
                "deprecation_message": "records.link_add is deprecated; prefer links.add with from_type=RECORD, to_type=TASK|CHUNK, edge_kind=covers",
            }))
        })
    }
}

// ── RecordsLinkRemove ──────────────────────────────────────────────────────

pub(super) struct RecordsLinkRemove;

impl McpTool for RecordsLinkRemove {
    fn name(&self) -> &'static str {
        "records.link_remove"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove a link from a record to a task or note chunk. At least one of task_id or chunk_id must be provided. Idempotent — removing a link that doesn't exist has no effect.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to unlink from (full ID or unique prefix)"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to unlink (optional if chunk_id is provided)"
                    },
                    "chunk_id": {
                        "type": "string",
                        "description": "Note chunk ID to unlink (optional if task_id is provided)"
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
            let parsed: LinkParams = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            match (parsed.task_id.as_deref(), parsed.chunk_id.as_deref()) {
                (None, None) => {
                    return ToolCallResult::error(
                        "must specify either task_id or chunk_id (got neither)".to_string(),
                    );
                }
                (Some(_), Some(_)) => {
                    return ToolCallResult::error(
                        "specify exactly one of task_id or chunk_id (got both)".to_string(),
                    );
                }
                _ => {}
            }

            let (target_kind, target_id) = if let Some(ref tid) = parsed.task_id {
                ("TASK", tid.clone())
            } else {
                ("CHUNK", parsed.chunk_id.clone().unwrap())
            };

            let wire_params = RecordsLinkParams {
                record_id: parsed.record_id.clone(),
                target: WireEntityRef {
                    kind: target_kind.to_string(),
                    id: target_id,
                },
                link_kind: "covers".to_string(),
            };

            match ctx
                .with_client(|c| c.records_link_remove(wire_params))
                .await
            {
                Ok(_removed) => {}
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to remove link: {err}"));
                }
            }

            json_response(&json!({
                "record_id": parsed.record_id,
                "task_id": parsed.task_id,
                "chunk_id": parsed.chunk_id,
                "action": "unlinked",
                "deprecated": true,
                "deprecation_message": "records.link_remove is deprecated; prefer links.remove with from_type=RECORD, to_type=TASK|CHUNK, edge_kind=covers",
            }))
        })
    }
}
