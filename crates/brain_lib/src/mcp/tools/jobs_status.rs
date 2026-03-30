use std::future::Future;
use std::pin::Pin;

use std::str::FromStr;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use brain_persistence::db::job::JobStatus;

fn default_limit() -> u64 {
    10
}

#[derive(Deserialize)]
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

        let status_filter = if let Some(status_str) = params.status.as_deref() {
            match JobStatus::from_str(status_str) {
                Ok(status) => Some(status),
                Err(err) => return ToolCallResult::error(err),
            }
        } else {
            None
        };

        let listing_status = status_filter.unwrap_or(JobStatus::Failed);
        let kind_filter = params.kind.as_deref().map(|s| s.to_string());

        let pending = match ctx.stores.count_jobs_by_status(&JobStatus::Pending) {
            Ok(v) => v,
            Err(e) => return ToolCallResult::error(format!("failed to count pending: {e}")),
        };
        let running = match ctx.stores.count_jobs_by_status(&JobStatus::InProgress) {
            Ok(v) => v,
            Err(e) => return ToolCallResult::error(format!("failed to count in_progress: {e}")),
        };
        let done = match ctx.stores.count_jobs_by_status(&JobStatus::Done) {
            Ok(v) => v,
            Err(e) => return ToolCallResult::error(format!("failed to count done: {e}")),
        };
        let failed = match ctx.stores.count_jobs_by_status(&JobStatus::Failed) {
            Ok(v) => v,
            Err(e) => return ToolCallResult::error(format!("failed to count failed: {e}")),
        };
        let ready = match ctx.stores.count_jobs_by_status(&JobStatus::Ready) {
            Ok(v) => v,
            Err(e) => return ToolCallResult::error(format!("failed to count ready: {e}")),
        };

        let mut jobs = match ctx
            .stores
            .list_jobs_by_status(&listing_status, params.limit as i32)
        {
            Ok(list) => list,
            Err(e) => return ToolCallResult::error(format!("failed to list jobs: {e}")),
        };

        if let Some(kind) = kind_filter.as_deref() {
            jobs.retain(|job| job.kind() == kind);
        }

        let mut stuck_jobs = match ctx.stores.list_stuck_jobs() {
            Ok(list) => list,
            Err(e) => return ToolCallResult::error(format!("failed to list stuck jobs: {e}")),
        };
        if let Some(kind) = kind_filter.as_deref() {
            stuck_jobs.retain(|job| job.kind() == kind);
        }

        let jobs_json: Vec<serde_json::Value> = jobs
            .into_iter()
            .map(|j| {
                json!({
                    "job_id": j.job_id,
                    "kind": j.kind(),
                    "ref_id": j.payload.ref_id(),
                    "attempts": j.attempts,
                    "last_error": j.last_error,
                    "status": j.status,
                    "updated_at": j.updated_at,
                })
            })
            .collect();

        let stuck_jobs_json: Vec<serde_json::Value> = stuck_jobs
            .into_iter()
            .map(|j| {
                json!({
                    "job_id": j.job_id,
                    "kind": j.kind(),
                    "status": j.status,
                    "started_at": j.started_at,
                })
            })
            .collect();

        let response = json!({
            "filters": {
                "status": listing_status.as_ref(),
                "kind": kind_filter,
                "limit": params.limit,
            },
            "counts": {
                "pending": pending,
                "running": running,
                "completed": done,
                "failed": failed,
                "ready": ready,
            },
            "jobs": jobs_json,
            "stuck_jobs": stuck_jobs_json,
        });

        json_response(&response)
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

        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert!(parsed.get("counts").is_some());
        let counts = &parsed["counts"];
        assert!(counts.get("pending").is_some());
        assert!(counts.get("running").is_some());
        assert!(counts.get("completed").is_some());
        assert!(counts.get("failed").is_some());
        assert!(counts.get("ready").is_some());
        assert!(parsed.get("jobs").is_some());
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

    #[tokio::test]
    async fn test_jobs_status_filters_by_status_and_kind() {
        use brain_persistence::db::job::{JobPayload, JobStatus};
        use brain_persistence::db::jobs::EnqueueJobInput;

        let (_dir, ctx) = create_test_context().await;
        let db = ctx.stores.db();

        let payload = JobPayload::SummarizeScope {
            summary_id: "sum-123".into(),
            scope_type: "directory".into(),
            scope_value: "src".into(),
            content: "hello".into(),
        };
        let input = EnqueueJobInput {
            payload,
            priority: 10,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };
        let job_id = ctx
            .stores
            .enqueue_job(&input)
            .expect("checked in test assertions");
        db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE jobs SET status = 'failed', updated_at = 0 WHERE job_id = ?1",
                rusqlite::params![job_id],
            )
            .expect("checked in test assertions");
            Ok(())
        })
        .expect("checked in test assertions");

        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "jobs.status",
                json!({
                    "status": JobStatus::Failed.as_ref(),
                    "kind": "summarize_scope",
                    "limit": 5,
                }),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "unexpected error: {:?}",
            result.is_error
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let jobs = parsed["jobs"].as_array().expect("jobs array");
        assert_eq!(jobs.len(), 1, "expected exactly one filtered job");
        assert_eq!(jobs[0]["job_id"], job_id);
        assert_eq!(parsed["filters"]["status"], JobStatus::Failed.as_ref());
        assert_eq!(parsed["filters"]["kind"], "summarize_scope");
    }
}
