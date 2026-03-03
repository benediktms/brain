use serde_json::{Value, json};
use tracing::error;

use crate::db::summaries::{Episode, store_episode};
use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;

pub(super) fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    use super::require_str;
    let goal = match require_str(params, "goal") {
        Ok(g) => g,
        Err(e) => return e,
    };
    let actions = match require_str(params, "actions") {
        Ok(a) => a,
        Err(e) => return e,
    };
    let outcome = match require_str(params, "outcome") {
        Ok(o) => o,
        Err(e) => return e,
    };

    let tags: Vec<String> = params
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let importance = params
        .get("importance")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);

    let episode = Episode {
        goal: goal.to_string(),
        actions: actions.to_string(),
        outcome: outcome.to_string(),
        tags: tags.clone(),
        importance,
    };

    match ctx.db.with_conn(|conn| store_episode(conn, &episode)) {
        Ok(summary_id) => {
            let response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "goal": goal,
                "tags": tags,
                "importance": importance
            });
            ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
        }
        Err(e) => {
            error!(error = %e, "failed to store episode");
            ToolCallResult::error(format!("Failed to store episode: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_write_episode() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });

        let params = json!({
            "goal": "Fix the bug",
            "actions": "Debugged and patched",
            "outcome": "Bug fixed",
            "tags": ["debugging"],
            "importance": 0.8
        });

        let result = rt.block_on(dispatch_tool_call("memory.write_episode", &params, &ctx));
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
    }
}
