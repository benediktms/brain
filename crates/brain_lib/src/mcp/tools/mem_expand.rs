use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::query_pipeline::QueryPipeline;

pub(super) async fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let memory_ids: Vec<String> = match params.get("memory_ids").and_then(|v| v.as_array()) {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        None => return ToolCallResult::error("Missing required parameter: memory_ids"),
    };

    let budget_tokens = params
        .get("budget_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(2000) as usize;

    let pipeline = QueryPipeline::new(&ctx.db, &ctx.store, &ctx.embedder);
    let expand_result = match pipeline.expand(&memory_ids, budget_tokens).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "expand failed");
            return ToolCallResult::error(format!("Expand failed: {e}"));
        }
    };

    let memories_json: Vec<Value> = expand_result
        .memories
        .iter()
        .map(|m| {
            json!({
                "memory_id": m.memory_id,
                "content": m.content,
                "file_path": m.file_path,
                "heading_path": m.heading_path,
                "byte_start": m.byte_start,
                "byte_end": m.byte_end,
                "truncated": m.truncated,
            })
        })
        .collect();

    let response = json!({
        "budget_tokens": expand_result.budget_tokens,
        "used_tokens_est": expand_result.used_tokens_est,
        "memories": memories_json
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_missing_ids() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call("memory.expand", &json!({}), &ctx));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_empty_ids() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });
        let params = json!({ "memory_ids": [], "budget_tokens": 1000 });
        let result = rt.block_on(dispatch_tool_call("memory.expand", &params, &ctx));
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["memories"].as_array().unwrap().len(), 0);
    }
}
