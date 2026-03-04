use rusqlite::OptionalExtension;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;

pub(super) async fn handle(ctx: &McpContext) -> ToolCallResult {
    let mut snapshot = ctx.metrics.snapshot();

    // Enrich with stuck-file count from SQLite
    let stuck_count = ctx
        .db
        .with_read_conn(|conn| {
            let count: u64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM files WHERE indexing_state = 'indexing_started' AND deleted_at IS NULL",
                    [],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            Ok(count)
        })
        .unwrap_or(0);

    snapshot.dual_store_stuck_files = stuck_count;

    ToolCallResult::text(serde_json::to_string_pretty(&snapshot).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_status_returns_valid_json_with_all_fields() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call("status", &json!({}), &ctx));
        assert!(result.is_error.is_none(), "status should not error");

        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();

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

        // Nested token fields
        assert!(parsed["tokens"]["search_minimal_total"].is_u64());
        assert!(parsed["tokens"]["expand_total"].is_u64());
    }
}
