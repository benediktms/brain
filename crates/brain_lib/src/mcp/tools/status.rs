use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct Status;

impl McpTool for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get runtime health metrics: indexing/query latency (p50/p95), stale hash prevention count, token usage, queue depth, LanceDB unoptimized rows, stuck files, and uptime.".into(),
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
            let mut snapshot = ctx.metrics.snapshot();

            // Enrich with stuck-file count from SQLite
            let stuck_file_count = match ctx.stores.count_stuck_files() {
                Ok(count) => count,
                Err(err) => {
                    return ToolCallResult::error(format!(
                        "Failed to read stuck file count: {err}"
                    ));
                }
            };

            let stale_hash_count = match ctx.stores.stale_hashes_prevented() {
                Ok(count) => count,
                Err(err) => {
                    return ToolCallResult::error(format!(
                        "Failed to read stale hash count: {err}"
                    ));
                }
            };

            snapshot.dual_store_stuck_files = stuck_file_count;
            snapshot.stale_hashes_prevented = stale_hash_count;

            json_response(&snapshot)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_status_returns_valid_json_with_all_fields() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("status", json!({}), &ctx).await;
        assert!(result.is_error.is_none(), "status should not error");

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");

        // All 7 top-level metric fields present
        assert!(parsed.get("uptime_seconds").is_some());
        assert!(parsed.get("indexing_latency").is_some());
        assert!(parsed.get("query_latency").is_some());
        assert!(parsed.get("stale_hashes_prevented").is_some());
        assert!(parsed.get("tokens").is_some());
        assert!(parsed.get("queue_depth").is_some());
        assert!(parsed.get("lancedb_unoptimized_rows").is_some());
        assert!(parsed.get("dual_store_stuck_files").is_some());

        // Nested latency fields
        assert!(parsed["indexing_latency"]["p50_us"].is_u64());
        assert!(parsed["indexing_latency"]["p95_us"].is_u64());
        assert!(parsed["indexing_latency"]["total_samples"].is_u64());

        // Error counters
        assert!(parsed.get("indexing_errors").is_some());
        assert!(parsed.get("query_errors").is_some());
        assert!(parsed["indexing_errors"].is_u64());
        assert!(parsed["query_errors"].is_u64());

        // Nested token fields
        assert!(parsed["tokens"]["search_minimal_total"].is_u64());
        assert!(parsed["tokens"]["expand_total"].is_u64());
    }
}
