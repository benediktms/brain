//! `tags.aliases_status` MCP tool — thin wrapper over
//! `DaemonClient::tags_aliases_status`.
//!
//! Reshapes the wire `TagAliasesStatusReport` (flat fields) back into
//! the legacy MCP envelope (`{last_run, total_aliases, total_clusters,
//! current_embedder_version, alias_coverage}`). The legacy tool surfaced
//! the *runtime* embedder version separately from the *stamped* version
//! so callers could detect drift; the wire variant only carries the
//! stamped version (`last_run_embedder_version`). The migrated tool
//! emits `current_embedder_version` as an empty string until a
//! follow-up wire extension surfaces the daemon's runtime embedder.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TagsAliasesStatus;

impl McpTool for TagsAliasesStatus {
    fn name(&self) -> &'static str {
        "tags.aliases_status"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Health summary for the synonym-clustering subsystem. \
                Returns the latest tag_cluster_runs row (or null), total_aliases / \
                total_clusters counts for the current brain, and an alias_coverage \
                breakdown of canonical vs raw counts. The runtime embedder version \
                is not yet available via the wire protocol."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn call<'a>(
        &'a self,
        _params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let report = match ctx.with_client(|c| c.tags_aliases_status()).await {
                Ok(r) => r,
                Err(e) => {
                    return ToolCallResult::error(format!("tags_aliases_status: {e}"));
                }
            };

            let last_run = match (
                report.last_run_id.as_ref(),
                report.last_run_started_at.as_ref(),
                report.last_run_embedder_version.as_ref(),
            ) {
                (Some(run_id), Some(started_at), Some(embedder_version)) => json!({
                    "run_id": run_id,
                    "started_at": started_at,
                    "embedder_version": embedder_version,
                }),
                _ => Value::Null,
            };

            let raw_count = report.total_aliases;
            let canonical_count = report.canonical_count;
            let ratio = if raw_count > 0 {
                (canonical_count as f64) / (raw_count as f64)
            } else {
                0.0
            };

            json_response(&json!({
                "last_run": last_run,
                "total_aliases": report.total_aliases,
                "total_clusters": report.total_clusters,
                "current_embedder_version": "",
                "alias_coverage": {
                    "canonical_count": canonical_count,
                    "raw_count": raw_count,
                    "ratio": ratio,
                },
            }))
        })
    }
}
