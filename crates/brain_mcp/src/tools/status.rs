//! `status` MCP tool — thin wrapper over `DaemonClient::brain_status`.
//!
//! The daemon's `BrainStatus` handler shapes runtime metrics + SQLite
//! counters into the wire-side [`BrainStatusReport`]. This tool flattens
//! the relevant fields back into the legacy top-level JSON envelope —
//! `uptime_seconds`, `indexing_latency` / `query_latency`,
//! `stale_hashes_prevented`, `indexing_errors` / `query_errors`,
//! `queue_depth`, `lancedb_unoptimized_rows`, `lancedb_optimize_failures`,
//! `dual_store_stuck_files` — so existing clients see no shape drift
//! across the migration.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct Status;

impl McpTool for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get runtime health metrics: indexing/query latency (p50/p95), stale hash prevention count, queue depth, LanceDB unoptimized rows, stuck files, and uptime.".into(),
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
            let report = match ctx.with_client(|c| c.brain_status()).await {
                Ok(report) => report,
                Err(err) => {
                    return ToolCallResult::error(format!("BrainStatus rpc failed: {err}"));
                }
            };

            // Flatten the wire-side report into the legacy top-level
            // envelope. `metrics.*` lifts to the top; SQLite counters
            // (`stuck_files`, `stale_hashes_prevented`) carry over via
            // their legacy field names (`dual_store_stuck_files`,
            // `stale_hashes_prevented`).
            json_response(&json!({
                "uptime_seconds": report.metrics.uptime_seconds,
                "indexing_latency": {
                    "p50_us": report.metrics.indexing_latency.p50_us,
                    "p95_us": report.metrics.indexing_latency.p95_us,
                    "total_samples": report.metrics.indexing_latency.total_samples,
                },
                "query_latency": {
                    "p50_us": report.metrics.query_latency.p50_us,
                    "p95_us": report.metrics.query_latency.p95_us,
                    "total_samples": report.metrics.query_latency.total_samples,
                },
                "stale_hashes_prevented": report.stale_hashes_prevented,
                "indexing_errors": report.metrics.indexing_errors,
                "query_errors": report.metrics.query_errors,
                "queue_depth": report.metrics.queue_depth,
                "lancedb_unoptimized_rows": report.metrics.lancedb_unoptimized_rows,
                "lancedb_optimize_failures": report.metrics.lancedb_optimize_failures,
                "dual_store_stuck_files": report.stuck_files,
            }))
        })
    }
}
