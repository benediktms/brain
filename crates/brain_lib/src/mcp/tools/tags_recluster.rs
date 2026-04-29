//! `tags.recluster` — synchronous trigger for `run_recluster`.
//!
//! Wraps the per-brain reclustering job (`brn-83a.7.2.3`) so a user can
//! kick a run from the MCP toolset or `brain tags recluster` CLI. This is
//! the only `tags.*` tool that **mutates** `tag_aliases`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::tags::{ClusterParams, run_recluster};

#[derive(Deserialize, Default)]
struct Params {
    threshold: Option<f32>,
}

pub(super) struct TagsRecluster;

impl McpTool for TagsRecluster {
    fn name(&self) -> &'static str {
        "tags.recluster"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Run synonym clustering over the calling brain's raw tags. \
                Folds raw tags into canonical clusters via the embedder + cosine threshold, \
                writes the result to tag_aliases, and returns a ReclusterReport. \
                Mutates tag_aliases — the only tags.* tool that writes."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "threshold": {
                        "type": "number",
                        "description": "Cosine similarity threshold for cluster edges. Default: 0.85.",
                        "minimum": 0.0,
                        "maximum": 1.0
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

            let embedder = match ctx.embedder() {
                Some(e) => e,
                None => return ToolCallResult::error(super::MEMORY_UNAVAILABLE.to_string()),
            };

            let mut cluster_params = ClusterParams::default();
            if let Some(threshold) = parsed.threshold {
                if !(0.0..=1.0).contains(&threshold) {
                    return ToolCallResult::error(format!(
                        "threshold must be between 0.0 and 1.0 (got {threshold}); \
                         values outside this range produce all-singleton clusters \
                         and write a misleading 'successful' run row"
                    ));
                }
                cluster_params.cosine_threshold = threshold;
            }

            match run_recluster(&ctx.stores, embedder, cluster_params).await {
                Ok(report) => json_response(&report),
                Err(e) => ToolCallResult::error(format!("recluster failed: {e}")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn recluster_returns_report_shape_on_empty_brain() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry.dispatch("tags.recluster", json!({}), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "tags.recluster should not error: {}",
            result.content[0].text
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("valid json");

        for field in [
            "run_id",
            "source_count",
            "cluster_count",
            "new_aliases",
            "updated_aliases",
            "stale_aliases",
            "duration_ms",
            "embedder_version",
        ] {
            assert!(
                parsed.get(field).is_some(),
                "missing field {field} in {parsed}"
            );
        }
        assert_eq!(parsed["source_count"], 0);
        assert_eq!(parsed["new_aliases"], 0);
    }

    #[tokio::test]
    async fn recluster_respects_threshold_param() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("tags.recluster", json!({ "threshold": 0.9 }), &ctx)
            .await;
        assert!(result.is_error.is_none(), "{}", result.content[0].text);
    }

    #[tokio::test]
    async fn recluster_rejects_threshold_above_one() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("tags.recluster", json!({ "threshold": 1.5 }), &ctx)
            .await;
        assert_eq!(
            result.is_error,
            Some(true),
            "out-of-range threshold must error"
        );
        assert!(
            result.content[0].text.contains("between 0.0 and 1.0"),
            "error must explain the bound: {}",
            result.content[0].text,
        );
    }

    #[tokio::test]
    async fn recluster_rejects_negative_threshold() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("tags.recluster", json!({ "threshold": -0.1 }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true), "negative threshold must error");
    }
}
