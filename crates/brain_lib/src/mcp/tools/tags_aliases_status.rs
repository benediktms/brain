//! `tags.aliases_status` — health summary for the synonym-clustering subsystem.
//!
//! Answers "is the alias table fresh? how many tags cluster?" via a small
//! aggregate read. Reports the *current runtime* embedder version (what
//! the next recluster would stamp), not the version stamped on existing
//! rows — mismatch between the two is a signal that a recluster is due.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

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
                total_clusters counts for the current brain, the current runtime \
                embedder version, and an alias_coverage breakdown of canonical vs raw \
                counts."
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
            let last_run = match ctx.stores.latest_tag_cluster_run() {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("latest_tag_cluster_run: {e}")),
            };
            let counts = match ctx.stores.count_tag_aliases() {
                Ok(c) => c,
                Err(e) => return ToolCallResult::error(format!("count_tag_aliases: {e}")),
            };

            let current_embedder_version = ctx
                .embedder()
                .map(|e| e.version().to_string())
                .unwrap_or_default();

            let ratio = if counts.raw_count > 0 {
                (counts.canonical_count as f64) / (counts.raw_count as f64)
            } else {
                0.0
            };

            json_response(&json!({
                "last_run": last_run,
                "total_aliases": counts.raw_count,
                "total_clusters": counts.cluster_count,
                "current_embedder_version": current_embedder_version,
                "alias_coverage": {
                    "canonical_count": counts.canonical_count,
                    "raw_count": counts.raw_count,
                    "ratio": ratio,
                },
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn aliases_status_handles_empty_alias_table() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("tags.aliases_status", json!({}), &ctx)
            .await;
        assert!(result.is_error.is_none(), "{}", result.content[0].text);

        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("valid json");
        assert!(parsed["last_run"].is_null());
        assert_eq!(parsed["total_aliases"], 0);
        assert_eq!(parsed["total_clusters"], 0);
        assert_eq!(parsed["alias_coverage"]["raw_count"], 0);
        assert_eq!(parsed["alias_coverage"]["canonical_count"], 0);
        assert_eq!(parsed["alias_coverage"]["ratio"], 0.0);
        // current_embedder_version comes from MockEmbedder
        assert_eq!(parsed["current_embedder_version"], "mock-v1");
    }
}
