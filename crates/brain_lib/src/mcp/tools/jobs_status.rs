use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::db::job::JobStatus;
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::ports::JobQueue;

use super::{McpTool, json_response};

fn default_limit() -> u64 {
    10
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct Params {
    kind: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: u64,
}

pub(super) struct JobsStatus;

impl JobsStatus {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let db = ctx.db();

        let pending = db
            .count_jobs_by_status(&JobStatus::Pending)
            .map_err(|e| format!("failed to count pending: {e}"));
        let running = db
            .count_jobs_by_status(&JobStatus::InProgress)
            .map_err(|e| format!("failed to count in_progress: {e}"));
        let done = db
            .count_jobs_by_status(&JobStatus::Done)
            .map_err(|e| format!("failed to count done: {e}"));
        let failed = db
            .count_jobs_by_status(&JobStatus::Failed)
            .map_err(|e| format!("failed to count failed: {e}"));
        let ready = db
            .count_jobs_by_status(&JobStatus::Ready)
            .map_err(|e| format!("failed to count ready: {e}"));

        let recent_failures = db
            .list_jobs_by_status(&JobStatus::Failed, params.limit as i32)
            .map_err(|e| format!("failed to list recent failures: {e}"));
        let stuck_jobs = db
            .list_stuck_jobs()
            .map_err(|e| format!("failed to list stuck jobs: {e}"));

        match (pending, running, done, failed, ready) {
            (Ok(pending), Ok(running), Ok(done), Ok(failed), Ok(ready)) => {
                let recent_failures_json: Vec<serde_json::Value> = recent_failures
                    .unwrap_or_default()
                    .into_iter()
                    .map(|j| {
                        let ref_id = match &j.payload {
                            crate::db::job::JobPayload::SummarizeScope { summary_id, .. } => {
                                summary_id.clone()
                            }
                            crate::db::job::JobPayload::ConsolidateCluster {
                                suggested_title,
                                ..
                            } => suggested_title.clone(),
                        };
                        json!({
                            "job_id": j.job_id,
                            "kind": j.kind(),
                            "ref_id": ref_id,
                            "attempts": j.attempts,
                            "last_error": j.last_error,
                            "updated_at": j.updated_at,
                        })
                    })
                    .collect();

                let stuck_jobs_json: Vec<serde_json::Value> = stuck_jobs
                    .unwrap_or_default()
                    .into_iter()
                    .map(|j| {
                        json!({
                            "job_id": j.job_id,
                            "kind": j.kind(),
                            "started_at": j.started_at,
                        })
                    })
                    .collect();

                let response = json!({
                    "counts": {
                        "pending": pending,
                        "running": running,
                        "completed": done,
                        "failed": failed,
                        "ready": ready,
                    },
                    "recent_failures": recent_failures_json,
                    "stuck_jobs": stuck_jobs_json,
                });

                json_response(&response)
            }
            _ => ToolCallResult::error(
                "Failed to retrieve job counts. See individual errors above.".to_string(),
            ),
        }
    }
}

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
        Box::pin(async move { self.execute(params, ctx) })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_jobs_status_returns_counts() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("jobs.status", json!({}), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "jobs.status should not error: {}",
            result.content[0].text
        );

        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert!(parsed.get("counts").is_some());
        let counts = &parsed["counts"];
        assert!(counts.get("pending").is_some());
        assert!(counts.get("running").is_some());
        assert!(counts.get("completed").is_some());
        assert!(counts.get("failed").is_some());
        assert!(counts.get("ready").is_some());
        assert!(parsed.get("recent_failures").is_some());
        assert!(parsed.get("stuck_jobs").is_some());
    }

    #[tokio::test]
    async fn test_jobs_status_accepts_limit_param() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("jobs.status", json!({ "limit": 5 }), &ctx)
            .await;
        assert!(result.is_error.is_none());
    }
}
