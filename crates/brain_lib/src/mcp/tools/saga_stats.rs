use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    saga_id: String,
}

pub(super) struct SagaStats;

impl SagaStats {
    fn execute(&self, raw_params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(raw_params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let stats = match ctx.stores.sagas.stats(&params.saga_id) {
            Ok(s) => s,
            Err(e) => return ToolCallResult::error(format!("Failed to compute stats: {e}")),
        };

        let label_histogram: Vec<Value> = stats
            .label_histogram
            .iter()
            .map(|l| json!({ "label": l.label, "count": l.count }))
            .collect();

        let brains: Vec<Value> = stats
            .brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();

        let c = &stats.counts;
        json_response(&json!({
            "saga_id": params.saga_id,
            "stats": {
                "total": c.total,
                "open": c.open,
                "in_progress": c.in_progress,
                "blocked": c.blocked,
                "done": c.done,
                "cancelled": c.cancelled,
                "completion_pct": c.completion_pct,
            },
            "label_histogram": label_histogram,
            "brains": brains,
        }))
    }
}

impl McpTool for SagaStats {
    fn name(&self) -> &'static str {
        "sagas.stats"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Aggregate statistics for a saga's member tasks: counts by status, \
                completion percentage (done / (total - cancelled), null if denominator is 0), \
                label histogram, and contributing brains."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": { "type": "string", "description": "Saga ID (bare 26-char ULID)" }
                },
                "required": ["saga_id"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move { self.execute(params, ctx) })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::tests::create_test_context;
    use super::{McpTool, SagaStats};

    async fn call(
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        SagaStats.call(params, ctx).await
    }

    #[tokio::test]
    async fn test_stats_empty_saga() {
        let (_dir, ctx) = create_test_context().await;
        let create = super::super::saga_create::SagaCreate;
        let r = create.call(json!({ "title": "Empty" }), &ctx).await;
        let p: Value = serde_json::from_str(&r.content[0].text).unwrap();
        let saga_id = p["saga_id"].as_str().unwrap();

        let result = call(json!({ "saga_id": saga_id }), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "should succeed: {:?}",
            result.content
        );
        let s: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(s["stats"]["total"], 0);
        assert_eq!(s["stats"]["done"], 0);
        assert!(s["stats"]["completion_pct"].is_null());
        assert!(s["label_histogram"].as_array().unwrap().is_empty());
        assert!(s["brains"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_stats_missing_saga_id_fails() {
        let (_dir, ctx) = create_test_context().await;
        let result = call(json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_underscore_alias() {
        assert_eq!(SagaStats.underscore_alias(), "sagas_stats");
    }
}
