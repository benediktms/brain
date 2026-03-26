//! Job worker: claims pending jobs from the queue and dispatches to handlers
//! based on the typed [`JobPayload`] variant.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::params;
use tracing::{debug, info, warn};

use crate::db::Db;
use crate::db::jobs::{self, EnqueueJobInput, JobPayload};
use crate::ports::JobQueue;
use crate::summarizer::Summarize;

const SUMMARIZE_SCOPE_PROMPT: &str = "\
Summarize the following content concisely in 2-3 sentences. \
Be factual and direct. No markdown formatting.\n\nContent:\n";

const CONSOLIDATE_CLUSTER_PROMPT: &str = "\
Synthesize these episodes into a single concise reflection. \
Include key decisions, outcomes, and lessons learned. \
No markdown formatting.\n\nEpisodes:\n";

/// Process pending jobs. Claims up to `limit` ready jobs, dispatches each
/// to the appropriate handler based on its payload variant, and marks them
/// as completed or failed. Returns the number of successfully processed jobs.
pub async fn process_jobs(db: &Db, summarizer: &dyn Summarize, limit: i32) -> usize {
    let claimed = match db.claim_ready_jobs(limit) {
        Ok(jobs) => jobs,
        Err(e) => {
            warn!(error = %e, "failed to claim ready jobs");
            return 0;
        }
    };

    if claimed.is_empty() {
        return 0;
    }

    debug!(count = claimed.len(), "claimed ready jobs");

    let mut success_count = 0;

    for job in &claimed {
        let prompt = build_prompt(&job.payload);

        match summarizer.summarize(&prompt).await {
            Ok(result) => {
                if let Err(e) = persist_job_result(db, &job.payload, &result) {
                    warn!(job_id = %job.job_id, error = %e, "failed to persist job result");
                    continue;
                }

                if let Err(e) = db.complete_job(&job.job_id, Some(&result)) {
                    warn!(job_id = %job.job_id, error = %e, "failed to mark job as completed");
                } else {
                    success_count += 1;
                    debug!(job_id = %job.job_id, kind = %job.kind(), "job completed");
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                if let Err(fail_err) = db.fail_job(&job.job_id, &error_msg) {
                    warn!(
                        job_id = %job.job_id,
                        original_error = %error_msg,
                        fail_error = %fail_err,
                        "failed to record job failure"
                    );
                } else {
                    warn!(job_id = %job.job_id, error = %error_msg, "job failed");
                }
            }
        }
    }

    if success_count > 0 {
        info!(
            processed = success_count,
            total_claimed = claimed.len(),
            "jobs processed"
        );
    }

    success_count
}

/// Build the LLM prompt from the typed payload.
fn build_prompt(payload: &JobPayload) -> String {
    match payload {
        JobPayload::SummarizeScope { content, .. } => {
            format!("{SUMMARIZE_SCOPE_PROMPT}{content}")
        }
        JobPayload::ConsolidateCluster {
            suggested_title,
            episodes,
            ..
        } => {
            format!("{CONSOLIDATE_CLUSTER_PROMPT}Title: {suggested_title}\n\n{episodes}")
        }
    }
}

fn persist_job_result(db: &Db, payload: &JobPayload, result: &str) -> crate::error::Result<()> {
    match payload {
        JobPayload::SummarizeScope { summary_id, .. } => {
            let summary_id = summary_id.clone();
            let result = result.to_string();
            let now = now_unix_secs();

            db.with_write_conn(move |conn| {
                conn.execute(
                    "UPDATE derived_summaries
                     SET content = ?1, stale = 0, generated_at = ?2
                     WHERE id = ?3",
                    params![result, now, summary_id],
                )?;
                Ok(())
            })
        }
        JobPayload::ConsolidateCluster { .. } => Ok(()),
    }
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Enqueue a scope summarization job.
pub fn enqueue_scope_summary(
    queue: &dyn JobQueue,
    summary_id: &str,
    scope_type: &str,
    scope_value: &str,
    content: &str,
) -> crate::error::Result<String> {
    let input = EnqueueJobInput {
        payload: JobPayload::SummarizeScope {
            summary_id: summary_id.to_string(),
            scope_type: scope_type.to_string(),
            scope_value: scope_value.to_string(),
            content: content.to_string(),
        },
        priority: jobs::priority::NORMAL,
        retry_config: None, // uses payload default (Fixed{3})
        stuck_threshold_secs: None,
        metadata: serde_json::json!({}),
        scheduled_at: 0,
    };
    queue.enqueue_job(&input)
}

/// Enqueue a cluster consolidation job.
pub fn enqueue_cluster_summary(
    queue: &dyn JobQueue,
    cluster_index: usize,
    suggested_title: &str,
    episode_ids: &[String],
    episodes: &str,
) -> crate::error::Result<String> {
    let input = EnqueueJobInput {
        payload: JobPayload::ConsolidateCluster {
            cluster_index,
            suggested_title: suggested_title.to_string(),
            episode_ids: episode_ids.to_vec(),
            episodes: episodes.to_string(),
        },
        priority: jobs::priority::NORMAL,
        retry_config: None, // uses payload default (Fixed{3})
        stuck_threshold_secs: None,
        metadata: serde_json::json!({}),
        scheduled_at: 0,
    };
    queue.enqueue_job(&input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db::jobs::JobStatus;
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

        db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
                 VALUES (?1, ?2, ?3, '', 0, 0)",
                params!["sum-1", "directory", "src/"],
            )?;
            Ok(())
        })
        .unwrap();

        let job_id = enqueue_scope_summary(
            &db,
            "sum-1",
            "directory",
            "src/",
            "fn main() { println!(\"hello\"); }",
        )
        .unwrap();

        let count = process_jobs(&db, &summarizer, 10).await;
        assert_eq!(count, 1);

        let job = db
            .with_read_conn(|conn| jobs::get_job(conn, &job_id))
            .unwrap()
            .unwrap();
        assert_eq!(job.status, JobStatus::Done);
        assert!(job.result.is_some());
    }

    #[tokio::test]
    async fn test_process_jobs_failure_retries() {
        let db = setup_db();

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

        let job_id = enqueue_scope_summary(&db, "sum-1", "directory", "fail/", "content").unwrap();
        let count = process_jobs(&db, &FailingSummarizer, 10).await;
        assert_eq!(count, 0);

        let job = db
            .with_read_conn(|conn| jobs::get_job(conn, &job_id))
            .unwrap()
            .unwrap();
        // After claim (attempts=1) + fail with Fixed{3}, reschedules to ready
        assert_eq!(job.status, JobStatus::Ready);
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
        let payload = JobPayload::SummarizeScope {
            summary_id: "sum-1".into(),
            scope_type: "directory".into(),
            scope_value: "src/".into(),
            content: "hello world".into(),
        };
        let prompt = build_prompt(&payload);
        assert!(prompt.contains("hello world"));
        assert!(prompt.starts_with("Summarize the following"));
    }

    #[test]
    fn test_build_prompt_cluster() {
        let payload = JobPayload::ConsolidateCluster {
            cluster_index: 0,
            suggested_title: "Episodes".into(),
            episode_ids: vec!["ep-1".into()],
            episodes: "episode data".into(),
        };
        let prompt = build_prompt(&payload);
        assert!(prompt.contains("episode data"));
        assert!(prompt.starts_with("Synthesize these episodes"));
    }

    #[tokio::test]
    async fn test_process_jobs_http_round_trip() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{ "type": "text", "text": "Mock summary result" }],
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "cache_creation_input_tokens": null,
                    "cache_read_input_tokens": null
                }
            })))
            .mount(&mock_server)
            .await;

        let provider = crate::llm::AnthropicProvider::new(
            "test-key".to_string(),
            mock_server.uri(),
            "claude-haiku-4-5-20251001".to_string(),
        );

        let db = setup_db();
        db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
                 VALUES (?1, ?2, ?3, '', 0, 0)",
                params!["sum-1", "directory", "src/"],
            )?;
            Ok(())
        })
        .unwrap();
        let job_id =
            enqueue_scope_summary(&db, "sum-1", "directory", "src/", "some content").unwrap();

        let count = process_jobs(&db, &provider, 10).await;
        assert_eq!(count, 1, "expected 1 successful job");

        let job = db
            .with_read_conn(|conn| jobs::get_job(conn, &job_id))
            .unwrap()
            .unwrap();
        assert_eq!(job.status, JobStatus::Done);
        assert_eq!(job.result.as_deref(), Some("Mock summary result"));
        let stored: String = db
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT content FROM derived_summaries WHERE id = ?1",
                    params!["sum-1"],
                    |row| row.get(0),
                )
                .map_err(|e| crate::error::BrainCoreError::Database(e.to_string()))
            })
            .unwrap();
        assert_eq!(stored, "Mock summary result");
    }

    #[tokio::test]
    async fn test_process_jobs_http_failure() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(500).set_body_raw("Internal error", "text/plain"))
            .mount(&mock_server)
            .await;

        let provider = crate::llm::AnthropicProvider::new(
            "test-key".to_string(),
            mock_server.uri(),
            "claude-haiku-4-5-20251001".to_string(),
        );

        let db = setup_db();
        let job_id =
            enqueue_scope_summary(&db, "sum-1", "directory", "src/", "some content").unwrap();

        let count = process_jobs(&db, &provider, 10).await;
        assert_eq!(count, 0, "expected 0 successes on HTTP failure");

        let job = db
            .with_read_conn(|conn| jobs::get_job(conn, &job_id))
            .unwrap()
            .unwrap();
        assert_eq!(
            job.status,
            JobStatus::Ready,
            "job should be rescheduled for retry"
        );
        assert!(job.last_error.is_some(), "error should be recorded");
        assert_eq!(job.attempts, 1);
    }
}
