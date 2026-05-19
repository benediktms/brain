//! `sagas.stats` MCP tool — thin wrapper over `DaemonClient::sagas_stats`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};
use crate::saga_validation::validate_saga_id;

pub(super) struct SagaStats;

#[derive(Deserialize)]
struct Params {
    saga_id: String,
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
                label histogram, and contributing brains. \
                Accepts compact `saga-<hex>` IDs (e.g. `saga-3j5`); 26-char ULIDs are still accepted for back-compat."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "saga_id": {
                        "type": "string",
                        "description": crate::saga_validation::SAGA_ID_PARAM_DESCRIPTION
                    }
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
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            if let Err(msg) = validate_saga_id(&parsed.saga_id) {
                return ToolCallResult::error(format!("Invalid saga_id: {msg}"));
            }

            let (saga_id_short, stats, label_histogram, brains) = match ctx
                .with_client(|c| c.sagas_stats(parsed.saga_id.clone()))
                .await
            {
                Ok(tuple) => tuple,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to compute stats: {err}"));
                }
            };

            let label_histogram_json: Vec<Value> = label_histogram
                .iter()
                .map(|l| json!({ "label": l.label, "count": l.count }))
                .collect();

            let brains_json: Vec<Value> = brains
                .iter()
                .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
                .collect();

            json_response(&json!({
                "saga_id": saga_id_short,
                "stats": {
                    "total": stats.total,
                    "open": stats.open,
                    "in_progress": stats.in_progress,
                    "blocked": stats.blocked,
                    "done": stats.done,
                    "cancelled": stats.cancelled,
                    "completion_pct": stats.completion_pct,
                },
                "label_histogram": label_histogram_json,
                "brains": brains_json,
            }))
        })
    }
}
