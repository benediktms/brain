//! Job worker: claims pending jobs from the queue and processes them via an LLM provider.

use tracing::{debug, info, warn};

use crate::db::Db;
use crate::db::jobs::{self, EnqueueParams};
use crate::summarizer::Summarize;

/// Prompt template for scope summarization.
const SUMMARIZE_SCOPE_PROMPT: &str = "\
Summarize the following content concisely in 2-3 sentences. \
Be factual and direct. No markdown formatting.\n\nContent:\n";

/// Prompt template for cluster consolidation.
const CONSOLIDATE_CLUSTER_PROMPT: &str = "\
Synthesize these episodes into a single concise reflection. \
Include key decisions, outcomes, and lessons learned. \
No markdown formatting.\n\nEpisodes:\n";

/// Process pending summarization jobs.
///
/// Claims up to `limit` jobs from the queue, sends each to the summarizer,
/// and marks them as completed or failed. Returns the number of successfully
/// processed jobs.
pub async fn process_jobs(db: &Db, summarizer: &dyn Summarize, limit: i32) -> usize {
    let claimed = match db.with_write_conn(|conn| jobs::claim_jobs(conn, "summarize_scope", limit))
    {
        Ok(jobs) => jobs,
        Err(e) => {
            warn!(error = %e, "failed to claim summarization jobs");
            return 0;
        }
    };

    if claimed.is_empty() {
        return 0;
    }

    debug!(count = claimed.len(), "claimed summarization jobs");

    let mut success_count = 0;

    for job in &claimed {
        let prompt = build_prompt(&job.kind, &job.payload);

        match summarizer.summarize(&prompt).await {
            Ok(result) => {
                if let Err(e) =
                    db.with_write_conn(|conn| jobs::complete_job(conn, &job.job_id, Some(&result)))
                {
                    warn!(job_id = %job.job_id, error = %e, "failed to mark job as completed");
                } else {
                    success_count += 1;
                    debug!(job_id = %job.job_id, kind = %job.kind, "job completed");
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                if let Err(fail_err) =
                    db.with_write_conn(|conn| jobs::fail_job(conn, &job.job_id, &error_msg))
                {
                    warn!(
                        job_id = %job.job_id,
                        original_error = %error_msg,
                        fail_error = %fail_err,
                        "failed to record job failure"
                    );
                } else {
                    warn!(job_id = %job.job_id, error = %error_msg, "job failed — will retry if attempts remain");
                }
            }
        }
    }

    if success_count > 0 {
        info!(
            processed = success_count,
            total_claimed = claimed.len(),
            "summarization jobs processed"
        );
    }

    success_count
}

/// Build the prompt for a given job kind and payload.
fn build_prompt(kind: &str, payload: &str) -> String {
    // Extract content from JSON payload, falling back to raw payload
    let content = serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|v| v.get("content").and_then(|c| c.as_str()).map(String::from))
        .unwrap_or_else(|| payload.to_string());

    match kind {
        "consolidate_cluster" => format!("{CONSOLIDATE_CLUSTER_PROMPT}{content}"),
        _ => format!("{SUMMARIZE_SCOPE_PROMPT}{content}"),
    }
}

/// Enqueue a summarization job for a scope.
pub fn enqueue_scope_summary(
    db: &Db,
    brain_id: &str,
    scope_key: &str,
    content: &str,
) -> crate::error::Result<String> {
    let payload = serde_json::json!({ "content": content }).to_string();
    db.with_write_conn(|conn| {
        jobs::enqueue_job(
            conn,
            &EnqueueParams {
                kind: "summarize_scope",
                brain_id,
                ref_id: Some(scope_key),
                ref_kind: Some("scope"),
                priority: jobs::priority::NORMAL,
                payload: &payload,
                max_attempts: 3,
            },
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::summarizer::MockSummarizer;

    fn setup_db() -> Db {
        Db::open_in_memory().unwrap()
    }

    #[tokio::test]
    async fn test_process_jobs_empty_queue() {
        let db = setup_db();
        let summarizer = MockSummarizer;
        let count = process_jobs(&db, &summarizer, 10).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_process_jobs_round_trip() {
        let db = setup_db();
        let summarizer = MockSummarizer;

        // Enqueue a job
        let job_id =
            enqueue_scope_summary(&db, "", "dir:/src", "fn main() { println!(\"hello\"); }")
                .unwrap();

        // Process it
        let count = process_jobs(&db, &summarizer, 10).await;
        assert_eq!(count, 1);

        // Verify completed
        let job = db
            .with_read_conn(|conn| jobs::get_job(conn, &job_id))
            .unwrap()
            .unwrap();
        assert_eq!(job.status, jobs::JobStatus::Completed);
        assert!(job.result.is_some());
    }

    #[tokio::test]
    async fn test_process_jobs_failure_retries() {
        let db = setup_db();

        // Create a summarizer that always fails
        struct FailingSummarizer;
        #[async_trait::async_trait]
        impl Summarize for FailingSummarizer {
            async fn summarize(&self, _text: &str) -> crate::error::Result<String> {
                Err(crate::error::BrainCoreError::Internal(
                    "intentional failure".to_string(),
                ))
            }
            fn backend_name(&self) -> &'static str {
                "failing"
            }
        }

        let job_id = enqueue_scope_summary(&db, "", "dir:/fail", "content").unwrap();
        let count = process_jobs(&db, &FailingSummarizer, 10).await;
        assert_eq!(count, 0);

        // Job should be back to pending with error recorded
        let job = db
            .with_read_conn(|conn| jobs::get_job(conn, &job_id))
            .unwrap()
            .unwrap();
        assert_eq!(job.status, jobs::JobStatus::Pending); // retryable
        assert!(
            job.last_error
                .as_deref()
                .unwrap()
                .contains("intentional failure")
        );
        assert_eq!(job.attempts, 1);
    }

    #[test]
    fn test_build_prompt_scope() {
        let payload = r#"{"content": "hello world"}"#;
        let prompt = build_prompt("summarize_scope", payload);
        assert!(prompt.contains("hello world"));
        assert!(prompt.starts_with("Summarize the following"));
    }

    #[test]
    fn test_build_prompt_cluster() {
        let payload = r#"{"content": "episode data"}"#;
        let prompt = build_prompt("consolidate_cluster", payload);
        assert!(prompt.contains("episode data"));
        assert!(prompt.starts_with("Synthesize these episodes"));
    }
}
