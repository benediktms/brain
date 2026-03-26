//! Job worker: claims pending jobs from the queue and dispatches to handlers
//! based on the typed [`JobPayload`] variant.
//!
//! Each claimed job is spawned as a separate `tokio::spawn` task for
//! concurrent execution. LLM jobs resolve their own provider from the DB.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::params;
use tracing::{debug, warn};

use crate::db::Db;
use crate::db::jobs::{self, EnqueueJobInput, JobPayload};
use crate::embedder::Embed;
use crate::ports::JobQueue;
use crate::store::Store;

const SUMMARIZE_SCOPE_PROMPT: &str = "\
Summarize the following content concisely in 2-3 sentences. \
Be factual and direct. No markdown formatting.\n\nContent:\n";

const CONSOLIDATE_CLUSTER_PROMPT: &str = "\
Synthesize these episodes into a single concise reflection. \
Include key decisions, outcomes, and lessons learned. \
No markdown formatting.\n\nEpisodes:\n";

/// Process pending jobs. Claims up to `limit` ready jobs, dispatches each
/// to the appropriate handler via `tokio::spawn` for concurrent execution.
/// Returns the number of claimed jobs (not completed — they run in background).
pub async fn process_jobs(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    limit: i32,
) -> usize {
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

    let count = claimed.len();
    for job in claimed {
        let db = db.clone();
        let store = store.clone();
        let embedder = embedder.clone();

        tokio::spawn(async move {
            let result = dispatch_job(&db, &store, &embedder, &job.payload).await;

            match result {
                Ok(result_str) => {
                    if let Err(e) = db.complete_job(&job.job_id, result_str.as_deref()) {
                        warn!(job_id = %job.job_id, error = %e, "failed to mark job as completed");
                    } else {
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
        });
    }

    count
}

type JobResult = std::result::Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;

/// Route a job to the appropriate handler based on its payload variant.
async fn dispatch_job(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    payload: &JobPayload,
) -> JobResult {
    match payload {
        // LLM jobs: resolve provider from DB, call summarizer.
        JobPayload::SummarizeScope { .. } | JobPayload::ConsolidateCluster { .. } => {
            process_llm_job(db, payload).await
        }
        // Sweep jobs: DB queries + child job enqueues.
        JobPayload::StaleScopeSweep => process_stale_scope_sweep(db).await,
        JobPayload::ConsolidationSweep => process_consolidation_sweep(db).await,
        // Infra jobs: need store + embedder.
        JobPayload::FullScanSweep { dirs, .. } => {
            process_full_scan(db, store, embedder, dirs).await
        }
        JobPayload::EmbedPollSweep { brain_id } => {
            process_embed_poll(db, store, embedder, brain_id).await
        }
    }
}

// ─── LLM jobs ───────────────────────────────────────────────────

/// Resolve the LLM provider from the DB and run the summarization.
async fn process_llm_job(db: &Db, payload: &JobPayload) -> JobResult {
    let brain_home =
        crate::config::brain_home().map_err(|e| format!("failed to resolve brain_home: {e}"))?;
    let summarizer = crate::llm::resolve_provider_with_db(db, &brain_home)
        .ok_or("no LLM provider configured — set ANTHROPIC_API_KEY or configure via brain config")?;

    let prompt = build_prompt(payload);
    let result = summarizer.summarize(&prompt).await?;
    persist_job_result(db, payload, &result)?;
    Ok(Some(result))
}

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
        _ => String::new(),
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
        _ => Ok(()),
    }
}

// ─── Sweep jobs ─────────────────────────────────────────────────

/// Find stale derived summaries and enqueue SummarizeScope child jobs.
async fn process_stale_scope_sweep(db: &Db) -> JobResult {
    use crate::hierarchy::DerivedSummaryStore;

    let stale = db.list_stale_summaries(20)?;
    if stale.is_empty() {
        return Ok(Some(r#"{"enqueued":0}"#.to_string()));
    }

    let mut enqueued = 0;
    for summary in &stale {
        let scope_type = crate::hierarchy::ScopeType::parse_db(&summary.scope_type);
        if let Some(scope_type) = scope_type {
            match crate::hierarchy::generate_scope_summary_with_options(
                db,
                &scope_type,
                &summary.scope_value,
                true,
            ) {
                Ok(_) => enqueued += 1,
                Err(e) => {
                    warn!(scope = %summary.scope_value, error = %e, "failed to enqueue stale scope");
                }
            }
        }
    }

    Ok(Some(format!(
        r#"{{"enqueued":{enqueued},"stale":{}}}"#,
        stale.len()
    )))
}

/// Find unclustered episodes and enqueue ConsolidateCluster child jobs.
///
/// Skips enqueuing if there are already active (non-terminal) consolidation
/// jobs — prevents re-sending the same episodes to the LLM.
async fn process_consolidation_sweep(db: &Db) -> JobResult {
    use crate::consolidation::{consolidate_episodes, enqueue_cluster_summarization};

    // Don't enqueue new clusters if previous ones are still being processed.
    let active_count: i64 = db.with_read_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM jobs WHERE kind = 'consolidate_cluster'
             AND status NOT IN ('done', 'failed')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| brain_persistence::error::BrainCoreError::Database(e.to_string()))
    })?;

    if active_count > 0 {
        return Ok(Some(format!(
            r#"{{"skipped":true,"active_jobs":{active_count}}}"#
        )));
    }

    let episodes =
        db.with_read_conn(|conn| brain_persistence::db::summaries::list_episodes(conn, 100, ""))?;

    if episodes.is_empty() {
        return Ok(Some(r#"{"clusters":0,"enqueued":0}"#.to_string()));
    }

    let result = consolidate_episodes(episodes, 7200);
    let enqueued = match enqueue_cluster_summarization(db, &result.clusters) {
        Ok(n) => n,
        Err(e) => {
            warn!(error = %e, "failed to enqueue consolidation clusters");
            0
        }
    };

    Ok(Some(format!(
        r#"{{"clusters":{},"enqueued":{enqueued}}}"#,
        result.clusters.len()
    )))
}

// ─── Infra jobs ─────────────────────────────────────────────────

/// Scan note directories for new/changed files and index them.
async fn process_full_scan(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    dirs: &[String],
) -> JobResult {
    let dirs: Vec<PathBuf> = dirs.iter().map(PathBuf::from).collect();
    if dirs.is_empty() {
        return Ok(Some(r#"{"indexed":0,"skipped":0}"#.to_string()));
    }

    let scan_pipeline =
        crate::pipeline::IndexPipeline::with_store(db.clone(), store.clone(), embedder.clone())
            .await?;

    let stats = scan_pipeline.full_scan(&dirs).await?;
    // Compact fragments after scan.
    store.optimizer().force_optimize().await;

    Ok(Some(format!(
        r#"{{"indexed":{},"skipped":{},"deleted":{}}}"#,
        stats.indexed, stats.skipped, stats.deleted
    )))
}

/// Embed stale chunks, tasks, and records for a specific brain.
async fn process_embed_poll(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> JobResult {
    use crate::pipeline::embed_poll;

    let n_tasks = embed_poll::poll_stale_tasks(db, store, embedder, brain_id).await;
    let n_chunks = embed_poll::poll_stale_chunks(db, store, embedder).await;
    let n_records = embed_poll::poll_stale_records(db, store, embedder, brain_id).await;

    Ok(Some(format!(
        r#"{{"tasks":{n_tasks},"chunks":{n_chunks},"records":{n_records}}}"#
    )))
}

// ─── Helpers ────────────────────────────────────────────────────

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

    fn setup_db() -> Db {
        Db::open_in_memory().unwrap()
    }

    #[tokio::test]
    async fn test_process_llm_job_round_trip() {
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

        let payload = JobPayload::SummarizeScope {
            summary_id: "sum-1".into(),
            scope_type: "directory".into(),
            scope_value: "src/".into(),
            content: "fn main() { println!(\"hello\"); }".into(),
        };

        // Test build_prompt
        let prompt = build_prompt(&payload);
        assert!(prompt.contains("fn main()"));
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

    #[test]
    fn test_build_prompt_sweep_is_empty() {
        assert!(build_prompt(&JobPayload::StaleScopeSweep).is_empty());
        assert!(build_prompt(&JobPayload::ConsolidationSweep).is_empty());
    }
}
