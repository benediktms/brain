use std::collections::HashMap;

use serde_json::{Value, json};
use tracing::error;

use crate::db::chunks::get_chunks_by_ids;
use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::retrieval::expand_results;

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

    // Look up chunks from SQLite
    let rows = match ctx
        .db
        .with_conn(|conn| get_chunks_by_ids(conn, &memory_ids))
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "chunk lookup failed");
            return ToolCallResult::error(format!("Chunk lookup failed: {e}"));
        }
    };

    // Preserve the requested order
    let row_map: HashMap<&str, _> = rows.iter().map(|r| (r.chunk_id.as_str(), r)).collect();
    let ordered_rows: Vec<_> = memory_ids
        .iter()
        .filter_map(|id| row_map.get(id.as_str()).copied())
        .collect();

    // Build ranked results for expand_results (scores don't matter here)
    let ranked: Vec<crate::ranking::RankedResult> = ordered_rows
        .iter()
        .map(|row| crate::ranking::RankedResult {
            chunk_id: row.chunk_id.clone(),
            hybrid_score: 0.0,
            scores: crate::ranking::SignalScores {
                vector: 0.0,
                keyword: 0.0,
                recency: 0.0,
                links: 0.0,
                tag_match: 0.0,
                importance: 0.0,
            },
            file_path: row.file_path.clone(),
            heading_path: row.heading_path.clone(),
            content: row.content.clone(),
            token_estimate: row.token_estimate,
        })
        .collect();

    let expand_result = expand_results(&ranked, budget_tokens);

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
