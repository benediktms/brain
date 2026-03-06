use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::query_pipeline::QueryPipeline;

pub(super) async fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    use super::{opt_str, opt_u64, require_str};
    let query = match require_str(params, "query") {
        Ok(q) => q,
        Err(e) => return e,
    };

    let intent = opt_str(params, "intent", "auto");
    let budget_tokens = opt_u64(params, "budget_tokens", 800) as usize;
    let k = opt_u64(params, "k", 10) as usize;

    let store = ctx.store.as_ref().unwrap();
    let embedder = ctx.embedder.as_ref().unwrap();
    let pipeline = QueryPipeline::new(&ctx.db, store, embedder, &ctx.metrics);
    let search_result = match pipeline.search(query, intent, budget_tokens, k).await {
        Ok(r) => r,
        Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
    };

    ctx.metrics
        .record_search_minimal_tokens(search_result.used_tokens_est);

    let results_json: Vec<Value> = search_result
        .results
        .iter()
        .map(|stub| {
            json!({
                "memory_id": stub.memory_id,
                "title": stub.title,
                "summary": stub.summary_2sent,
                "score": stub.hybrid_score,
                "file_path": stub.file_path,
                "heading_path": stub.heading_path,
            })
        })
        .collect();

    let response = json!({
        "budget_tokens": search_result.budget_tokens,
        "used_tokens_est": search_result.used_tokens_est,
        "intent_resolved": format!("{:?}", crate::ranking::resolve_intent(intent)),
        "result_count": search_result.num_results,
        "total_available": search_result.total_available,
        "results": results_json
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_missing_query() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call(
            "memory.search_minimal",
            &json!({}),
            &ctx,
        ));
        assert_eq!(result.is_error, Some(true));
    }
}
