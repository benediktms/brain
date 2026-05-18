//! `jobs.status` MCP tool — thin wrapper over `DaemonClient::jobs_status`.
//!
//! The daemon's `JobsStatus` handler returns a `JobsStatusReport`
//! with per-status counts, recent failures (up to 10), and stuck
//! jobs. Filter parameters (`kind`, `status`, `limit`) are accepted
//! for input-schema parity with the legacy tool but are not yet
//! threaded onto the wire — the report is returned as-is. Adding
//! wire-side filter params is a follow-up.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct JobsStatus;

impl McpTool for JobsStatus {
    fn name(&self) -> &'static str {
        "jobs.status"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get job queue health and observability metrics: per-status counts, recent failures, and stuck jobs.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Filter by job kind (e.g. 'summarize_scope', 'consolidate_cluster')"
                    },
                    "status": {
                        "type": "string",
                        "description": "Filter by job status (ready, pending, in_progress, done, failed)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of recent failures to return. Default: 10.",
                        "default": 10
                    }
                }
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            // Filter inputs are accepted for schema parity but not
            // threaded onto the wire yet. Echo them in the response's
            // `filters` envelope so callers see what was requested.
            let kind_filter = params.get("kind").and_then(|v| v.as_str()).map(String::from);
            let status_filter = params
                .get("status")
                .and_then(|v| v.as_str())
                .map(String::from);
            let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10);

            let report = match ctx.with_client(|c| c.jobs_status()).await {
                Ok(r) => r,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to read jobs status: {err}"));
                }
            };

            let jobs_json: Vec<Value> = report
                .recent_failures
                .iter()
                .map(|j| {
                    json!({
                        "job_id": j.job_id,
                        "kind": j.kind,
                        "ref_id": j.ref_id,
                        "attempts": j.attempts,
                        "last_error": j.last_error,
                        "updated_at": j.updated_at,
                    })
                })
                .collect();

            let stuck_jobs_json: Vec<Value> = report
                .stuck_jobs
                .iter()
                .map(|j| {
                    json!({
                        "job_id": j.job_id,
                        "kind": j.kind,
                        "ref_id": j.ref_id,
                        "attempts": j.attempts,
                        "updated_at": j.updated_at,
                    })
                })
                .collect();

            let response = json!({
                "filters": {
                    "status": status_filter,
                    "kind": kind_filter,
                    "limit": limit,
                },
                "counts": {
                    "pending": report.pending,
                    "running": report.running,
                    "completed": report.done,
                    "failed": report.failed,
                    "ready": report.ready,
                },
                "jobs": jobs_json,
                "stuck_jobs": stuck_jobs_json,
            });

            json_response(&response)
        })
    }
}
