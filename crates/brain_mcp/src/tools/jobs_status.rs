//! `jobs.status` MCP tool — thin wrapper over
//! `DaemonClient::jobs_status`.
//!
//! Forwards `kind` / `status` / `limit` filters to the daemon (which
//! owns the listing + filtering loop) and shapes the response into
//! the legacy MCP envelope: `{filters, counts, jobs, stuck_jobs}`.
//! The wire-side [`JobSummary`] now carries per-row `status` and
//! `started_at` so the byte-shape matches the pre-migration tool
//! without any client-side joins.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::JobsStatusParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

fn default_limit() -> u64 {
    10
}

#[derive(Deserialize, Default)]
struct Params {
    kind: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: u64,
}

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
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let wire_params = JobsStatusParams {
                kind: parsed.kind.clone(),
                status: parsed.status.clone(),
                limit: parsed.limit,
            };

            let report = match ctx.with_client(|c| c.jobs_status(wire_params)).await {
                Ok(report) => report,
                Err(err) => return ToolCallResult::error(format!("{err}")),
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
                        "status": j.status,
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
                        "status": j.status,
                        "started_at": j.started_at,
                    })
                })
                .collect();

            json_response(&json!({
                "filters": {
                    "status": report.listing_status,
                    "kind": parsed.kind,
                    "limit": parsed.limit,
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
            }))
        })
    }
}
