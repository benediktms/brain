//! `records.get` MCP tool — routes to the appropriate typed show method on
//! `DaemonClient` based on the record ID prefix.
//!
//! # Routing strategy
//!
//! The wire layer exposes kind-specific show methods (`analyses_show`,
//! `documents_show`, `plans_show`, `snapshots_show`). Since the caller
//! passes only a record_id without kind context, this tool tries each show
//! method in order until one returns `Some(record)`. The first match wins.
//! This preserves the legacy behaviour of `records.get` which used a single
//! polymorphic DB lookup.
//!
//! # Wire gap — `brain` param / content / tags / links
//!
//! The legacy tool exposed tags, links, and a `brain` cross-brain param.
//! The typed wire summaries (`AnalysisSummary` etc.) carry only `record_id`,
//! `title`, `created_at`, and `brain_id`. Tags, links, and cross-brain fetch
//! are not yet in the wire protocol. The response shape below emits the
//! available fields and omits `tags`/`links` until those wire variants land.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordGet;

#[derive(Deserialize)]
struct Params {
    record_id: String,
    /// Cross-brain routing is not yet wired in the daemon's show
    /// methods; passing `Some(_)` is rejected up front so callers
    /// aren't silently misled into thinking the record came from the
    /// target brain.
    brain: Option<String>,
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
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            if parsed.brain.is_some() {
                return ToolCallResult::error(
                    "cross-brain routing is not yet wired for records.get; \
                     omit the `brain` parameter to fetch from the current brain",
                );
            }

            let id = parsed.record_id.clone();

            // Try analyses → documents → plans → snapshots, returning early
            // on the first hit. Distinguish Err (transport/daemon failure —
            // surface immediately) from Ok(None) (this kind doesn't have
            // it — try the next kind).
            match ctx.with_client(|c| c.analyses_show(id.clone())).await {
                Ok(Some(r)) => {
                    return json_response(&json!({
                        "record_id": r.record_id,
                        "kind": "analysis",
                        "title": r.title,
                        "brain_id": r.brain_id,
                        "created_at": r.created_at,
                    }));
                }
                Ok(None) => {}
                Err(e) => return ToolCallResult::error(format!("analyses_show: {e}")),
            }

            match ctx.with_client(|c| c.documents_show(id.clone())).await {
                Ok(Some(r)) => {
                    return json_response(&json!({
                        "record_id": r.record_id,
                        "kind": "document",
                        "title": r.title,
                        "brain_id": r.brain_id,
                        "created_at": r.created_at,
                    }));
                }
                Ok(None) => {}
                Err(e) => return ToolCallResult::error(format!("documents_show: {e}")),
            }

            match ctx.with_client(|c| c.plans_show(id.clone())).await {
                Ok(Some(r)) => {
                    return json_response(&json!({
                        "record_id": r.record_id,
                        "kind": "plan",
                        "title": r.title,
                        "brain_id": r.brain_id,
                        "created_at": r.created_at,
                    }));
                }
                Ok(None) => {}
                Err(e) => return ToolCallResult::error(format!("plans_show: {e}")),
            }

            match ctx.with_client(|c| c.snapshots_show(id.clone())).await {
                Ok(Some(r)) => {
                    return json_response(&json!({
                        "record_id": r.record_id,
                        "kind": "snapshot",
                        "title": r.title,
                        "brain_id": r.brain_id,
                        "created_at": r.created_at,
                    }));
                }
                Ok(None) => {}
                Err(e) => return ToolCallResult::error(format!("snapshots_show: {e}")),
            }

            ToolCallResult::error(format!("Record not found: {id}"))
        })
    }
}
