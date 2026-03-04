use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::query_pipeline::QueryPipeline;

pub(super) async fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    use super::{opt_u64, require_str};
    let topic = match require_str(params, "topic") {
        Ok(t) => t,
        Err(e) => return e,
    };

    let budget_tokens = opt_u64(params, "budget_tokens", 2000) as usize;

    let pipeline = QueryPipeline::new(&ctx.db, &ctx.store, &ctx.embedder, &ctx.metrics);
    let reflect_result = match pipeline.reflect(topic.to_string(), budget_tokens).await {
        Ok(r) => r,
        Err(e) => return ToolCallResult::error(format!("Reflect failed: {e}")),
    };

    let episode_sources: Vec<Value> = reflect_result
        .episodes
        .iter()
        .map(|ep| {
            json!({
                "type": "episode",
                "summary_id": ep.summary_id,
                "title": ep.title,
                "content": ep.content,
                "tags": ep.tags,
                "importance": ep.importance,
            })
        })
        .collect();

    let related_chunks_json: Vec<Value> = reflect_result
        .search_result
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
        "topic": reflect_result.topic,
        "budget_tokens": reflect_result.budget_tokens,
        "source_count": episode_sources.len(),
        "episodes": episode_sources,
        "related_chunks": {
            "budget_tokens": reflect_result.search_result.budget_tokens,
            "used_tokens_est": reflect_result.search_result.used_tokens_est,
            "result_count": reflect_result.search_result.num_results,
            "total_available": reflect_result.search_result.total_available,
            "results": related_chunks_json,
        },
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_reflect() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let params = json!({ "topic": "project architecture" });
        let result = rt.block_on(dispatch_tool_call("memory.reflect", &params, &ctx));
        assert!(result.is_error.is_none());
    }
}
