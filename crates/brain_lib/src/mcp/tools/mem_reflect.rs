use serde_json::{Value, json};

use crate::db::summaries::list_episodes;
use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;

use super::mem_search_minimal;

pub(super) async fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let topic = match params.get("topic").and_then(|v| v.as_str()) {
        Some(t) => t,
        None => return ToolCallResult::error("Missing required parameter: topic"),
    };

    let budget_tokens = params
        .get("budget_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(2000) as usize;

    // Gather source material: recent episodes + relevant chunks
    let episodes = ctx
        .db
        .with_conn(|conn| list_episodes(conn, 10))
        .unwrap_or_default();

    // Also search for relevant chunks via search_minimal logic
    let search_params = json!({
        "query": topic,
        "intent": "reflection",
        "budget_tokens": budget_tokens / 2,
        "k": 5
    });
    let search_result = mem_search_minimal::handle(&search_params, ctx).await;

    // Build combined source material
    let episode_sources: Vec<Value> = episodes
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

    let response = json!({
        "topic": topic,
        "budget_tokens": budget_tokens,
        "source_count": episode_sources.len(),
        "episodes": episode_sources,
        "related_chunks": serde_json::from_str::<Value>(
            search_result.content.first().map(|c| c.text.as_str()).unwrap_or("{}")
        ).unwrap_or(json!({})),
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
