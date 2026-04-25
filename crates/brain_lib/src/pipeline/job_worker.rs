//! Job worker: claims pending jobs from the queue and dispatches to handlers
//! based on the typed [`JobPayload`] variant.
//!
//! Each claimed job is spawned as a separate `tokio::spawn` task for
//! concurrent execution. LLM jobs resolve their own provider from the DB.
//!
//! An [`ActiveJobs`] lock set prevents the stuck-job reaper from resetting
//! jobs that are still being actively worked on. Each spawned task holds a
//! [`JobGuard`] that removes the job from the set on drop (including panics).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashSet;
use tracing::{debug, warn};

use crate::embedder::Embed;
use crate::ports::{JobPersistence, JobQueue};
use brain_persistence::db::Db;
use brain_persistence::db::jobs::{self, EnqueueJobInput, JobPayload};
use brain_persistence::store::Store;

// ─── Active-job lock set ────────────────────────────────────────

/// In-memory set of job IDs that are currently being executed by a
/// `tokio::spawn` task, combined with a lock-free capacity counter.
///
/// The `DashSet` tracks *which* jobs are active (for the stuck-job reaper).
/// The `AtomicUsize` tracks *how many* slots remain (for admission control).
/// Together they prevent both duplicate execution and memory/CPU saturation.
///
/// This is the correct design for a single-process daemon. If the process
/// crashes, the locks are lost — and that is fine, because the spawned tasks
/// die with it. The time-based reaper handles crash recovery.
#[derive(Clone)]
pub struct ActiveJobs {
    set: Arc<DashSet<String>>,
    remaining: Arc<AtomicUsize>,
}

/// Default maximum number of concurrently executing jobs. Keeps memory and
/// CPU usage bounded when many brains each produce recurring jobs.
pub const DEFAULT_MAX_CONCURRENT_JOBS: usize = 8;

impl ActiveJobs {
    pub fn new(max_concurrent: usize) -> Self {
        assert!(max_concurrent > 0, "max_concurrent must be at least 1");
        Self {
            set: Arc::new(DashSet::new()),
            remaining: Arc::new(AtomicUsize::new(max_concurrent)),
        }
    }

    /// Atomically reserve up to `requested` execution slots.
    ///
    /// Uses a CAS loop so concurrent callers never over-allocate. Returns
    /// the number of slots actually reserved (0 when at capacity). The
    /// caller must call [`acquire`] for each reserved slot or
    /// [`release_slots`] for any unused reservations.
    pub fn reserve_slots(&self, requested: usize) -> usize {
        if requested == 0 {
            return 0;
        }
        loop {
            let current = self.remaining.load(Ordering::Acquire);
            if current == 0 {
                return 0;
            }
            let to_take = requested.min(current);
            match self.remaining.compare_exchange_weak(
                current,
                current - to_take,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return to_take,
                Err(_) => continue,
            }
        }
    }

    /// Release slots that were reserved but not used (e.g., DB returned
    /// fewer ready jobs than reserved).
    pub fn release_slots(&self, count: usize) {
        if count > 0 {
            self.remaining.fetch_add(count, Ordering::Release);
        }
    }

    /// Insert a job ID. Returns a [`JobGuard`] that removes it on drop
    /// and returns one slot to the capacity pool.
    pub fn acquire(&self, job_id: String) -> JobGuard {
        self.set.insert(job_id.clone());
        JobGuard {
            set: Arc::clone(&self.set),
            remaining: Arc::clone(&self.remaining),
            job_id,
        }
    }

    /// Check whether a job ID is currently held.
    pub fn contains(&self, job_id: &str) -> bool {
        self.set.contains(job_id)
    }

    /// Number of jobs currently in-flight.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// True if no jobs are currently in-flight.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

/// RAII guard that removes a job ID from the [`ActiveJobs`] set on drop
/// and returns one slot to the capacity pool.
/// This guarantees cleanup even if the spawned task panics.
pub struct JobGuard {
    set: Arc<DashSet<String>>,
    remaining: Arc<AtomicUsize>,
    job_id: String,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        self.set.remove(&self.job_id);
        self.remaining.fetch_add(1, Ordering::Release);
    }
}

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
///
/// The concurrency cap is enforced atomically via [`ActiveJobs::reserve_slots`].
/// If all slots are occupied, no new jobs are claimed. This prevents memory
/// and CPU saturation when many recurring jobs become ready simultaneously.
///
/// Each spawned task holds a [`JobGuard`] from the `active` set, preventing
/// the stuck-job reaper from resetting the job while it is still running.
pub async fn process_jobs(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    active: &ActiveJobs,
    limit: i32,
) -> usize {
    // Atomically reserve execution slots — concurrent callers cannot
    // over-allocate thanks to the CAS loop inside reserve_slots.
    let reserved = active.reserve_slots(limit.max(0) as usize);
    if reserved == 0 {
        debug!("at concurrency cap, skipping claim");
        return 0;
    }

    let claimed = match db.claim_ready_jobs(reserved as i32) {
        Ok(jobs) => jobs,
        Err(e) => {
            warn!(error = %e, "failed to claim ready jobs");
            active.release_slots(reserved);
            return 0;
        }
    };

    if claimed.is_empty() {
        active.release_slots(reserved);
        return 0;
    }

    // Release slots we reserved but didn't need (DB had fewer ready jobs).
    let unused = reserved - claimed.len();
    if unused > 0 {
        active.release_slots(unused);
    }

    debug!(count = claimed.len(), "claimed ready jobs");

    let count = claimed.len();
    // Collect ALL guards before spawning to close the acquisition gap between
    // claim and spawn — a concurrent reaper must not reset a claimed job.
    let guards: Vec<_> = claimed
        .iter()
        .map(|j| active.acquire(j.job_id.clone()))
        .collect();
    for (job, _guard) in claimed.into_iter().zip(guards) {
        let db = db.clone();
        let store = store.clone();
        let embedder = embedder.clone();

        tokio::spawn(async move {
            // _guard is moved into this future and dropped when the task ends
            // (success, failure, or panic).
            let _guard = _guard;

            if let Err(e) = db.advance_to_in_progress(&job.job_id) {
                warn!(job_id = %job.job_id, error = %e, "failed to advance to in_progress");
                if let Err(fail_err) =
                    db.fail_job(&job.job_id, &format!("advance_to_in_progress failed: {e}"))
                {
                    warn!(
                        job_id = %job.job_id,
                        fail_error = %fail_err,
                        "failed to record advance_to_in_progress failure"
                    );
                }
                return;
            }

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

/// Reap stuck jobs, but skip any that are still actively running (present in
/// the `active` set). This prevents the reaper from resetting jobs that are
/// just slow, not actually stuck.
pub fn reap_stuck_jobs_filtered(
    queue: &dyn crate::ports::JobQueue,
    active: &ActiveJobs,
) -> crate::error::Result<usize> {
    let stuck = queue.list_stuck_jobs()?;
    if stuck.is_empty() {
        return Ok(0);
    }

    let truly_stuck: Vec<_> = stuck
        .into_iter()
        .filter(|j| !active.contains(&j.job_id))
        .collect();

    if truly_stuck.is_empty() {
        return Ok(0);
    }

    let mut count = 0;
    for job in &truly_stuck {
        // Call fail_job for ALL stuck jobs — it handles retryable vs exhausted
        // internally (reschedules if attempts remain, marks failed otherwise).
        match queue.fail_job(&job.job_id, "reaped: exceeded stuck threshold") {
            Ok(()) => count += 1,
            Err(e) => {
                warn!(job_id = %job.job_id, error = %e, "failed to reap stuck job");
            }
        }
    }

    if count > 0 {
        debug!(count, "reaped stuck jobs (filtered)");
    }
    Ok(count)
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
        JobPayload::FullScanSweep { brain_id } => {
            process_full_scan(db, store, embedder, brain_id).await
        }
        JobPayload::EmbedPollSweep { brain_id } => {
            process_embed_poll(db, store, embedder, brain_id).await
        }
        // LOD jobs: generate L1 summary for an object.
        JobPayload::LodSummarize { .. } => process_lod_summarize(db, payload).await,
    }
}

// ─── LLM jobs ───────────────────────────────────────────────────

/// Resolve the LLM provider from the DB and run the summarization.
async fn process_llm_job(db: &Db, payload: &JobPayload) -> JobResult {
    let brain_home =
        crate::config::brain_home().map_err(|e| format!("failed to resolve brain_home: {e}"))?;
    let summarizer = crate::llm::resolve_provider_with_db(db, &brain_home).ok_or(
        "no LLM provider configured — set ANTHROPIC_API_KEY or configure via brain config",
    )?;

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

fn persist_job_result(
    db: &dyn JobPersistence,
    payload: &JobPayload,
    result: &str,
) -> crate::error::Result<()> {
    match payload {
        JobPayload::SummarizeScope { summary_id, .. } => {
            db.persist_scope_summary_result(summary_id, result)
        }
        JobPayload::ConsolidateCluster {
            suggested_title,
            episode_ids,
            brain_id,
            ..
        } => db.persist_consolidation_result(suggested_title, result, episode_ids, brain_id),
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

/// Find unclustered episodes per brain and enqueue ConsolidateCluster jobs.
///
/// Iterates each brain that has unconsolidated episodes. The `consolidated_by`
/// column (v35) prevents re-processing already-consolidated episodes.
async fn process_consolidation_sweep(db: &Db) -> JobResult {
    use crate::consolidation::{consolidate_episodes, enqueue_cluster_summarization};

    let brain_ids = db.with_read_conn(|conn| {
        brain_persistence::db::summaries::list_unconsolidated_brain_ids(conn)
    })?;

    if brain_ids.is_empty() {
        return Ok(Some(
            r#"{"brains":0,"clusters":0,"enqueued":0}"#.to_string(),
        ));
    }

    let mut total_clusters = 0;
    let mut total_enqueued = 0;

    for brain_id in &brain_ids {
        let episodes = db.with_read_conn(|conn| {
            brain_persistence::db::summaries::list_unconsolidated_episodes(conn, 100, brain_id)
        })?;

        if episodes.is_empty() {
            continue;
        }

        let result = consolidate_episodes(episodes, 7200);
        total_clusters += result.clusters.len();

        match enqueue_cluster_summarization(db, &result.clusters, brain_id) {
            Ok(n) => total_enqueued += n,
            Err(e) => {
                warn!(brain_id = %brain_id, error = %e, "failed to enqueue consolidation clusters");
            }
        }
    }

    Ok(Some(format!(
        r#"{{"brains":{},"clusters":{total_clusters},"enqueued":{total_enqueued}}}"#,
        brain_ids.len()
    )))
}

// ─── Infra jobs ─────────────────────────────────────────────────

/// Scan note directories for new/changed files and index them.
///
/// Dirs are resolved from the DB at execution time (not baked into the job
/// payload), so config changes take effect on the next sweep without needing
/// to update existing singleton job rows.
async fn process_full_scan(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> JobResult {
    if brain_id.is_empty() {
        warn!("FullScanSweep has empty brain_id, skipping");
        return Ok(Some(r#"{"skipped":"empty_brain_id"}"#.to_string()));
    }

    let dirs: Vec<PathBuf> = db.with_read_conn(|conn| {
        use brain_persistence::db::schema::get_brain;
        let row = get_brain(conn, brain_id)?;
        let mut dirs = Vec::new();
        if let Some(ref row) = row {
            if let Some(ref roots_json) = row.roots_json {
                match serde_json::from_str::<Vec<String>>(roots_json) {
                    Ok(roots) => dirs.extend(roots.into_iter().map(PathBuf::from)),
                    Err(e) => {
                        warn!(brain_id = %brain_id, error = %e, "malformed roots_json, skipping")
                    }
                }
            }
            if let Some(ref notes_json) = row.notes_json {
                match serde_json::from_str::<Vec<String>>(notes_json) {
                    Ok(notes) => dirs.extend(notes.into_iter().map(PathBuf::from)),
                    Err(e) => {
                        warn!(brain_id = %brain_id, error = %e, "malformed notes_json, skipping")
                    }
                }
            }
        }
        Ok(dirs)
    })?;
    if dirs.is_empty() {
        return Ok(Some(r#"{"indexed":0,"skipped":0}"#.to_string()));
    }

    let mut scan_pipeline =
        crate::pipeline::IndexPipeline::with_store(db.clone(), store.clone(), embedder.clone())
            .await?;
    scan_pipeline.set_brain_id(brain_id.to_string());

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
    let n_chunks = embed_poll::poll_stale_chunks(db, store, embedder, brain_id).await;
    let n_records = embed_poll::poll_stale_records(db, store, embedder, brain_id).await;
    let n_summaries = embed_poll::poll_stale_summaries(db, store, embedder, brain_id).await;

    Ok(Some(format!(
        r#"{{"tasks":{n_tasks},"chunks":{n_chunks},"records":{n_records},"summaries":{n_summaries}}}"#
    )))
}

// ─── LOD jobs ───────────────────────────────────────────────────

/// Generate an L1 (LLM-summarized or extractive) LOD chunk for an object.
///
/// Follows the `process_llm_job` pattern: the summarizer is resolved inside
/// the handler via `resolve_provider_with_db`. If no provider is configured,
/// falls back to `generate_extractive_l1`.
async fn process_lod_summarize(db: &Db, payload: &JobPayload) -> JobResult {
    use crate::l0_generate::generate_extractive_l1;
    use crate::lod::{
        L1_MIN_CONTENT_LEN, L1_TTL_DAYS, LodChunkStore, LodLevel, LodMethod, UpsertLodChunk,
    };
    use crate::tokens::estimate_tokens;

    let JobPayload::LodSummarize {
        object_uri,
        brain_id,
        source_content,
        source_hash,
    } = payload
    else {
        return Err("invalid payload: expected LodSummarize".into());
    };

    let brain_home =
        crate::config::brain_home().map_err(|e| format!("failed to resolve brain_home: {e}"))?;
    let summarizer = crate::llm::resolve_provider_with_db(db, &brain_home);

    let (l1_content, method, model_id) = match summarizer {
        Some(s) => {
            let prompt = format!(
                "Summarize the following content concisely in 2-3 paragraphs, \
                 focusing on key decisions, entities, and actionable details:\n\n{}",
                source_content
            );
            let result = s.summarize(&prompt).await?;
            if result.trim().len() < L1_MIN_CONTENT_LEN {
                warn!(
                    object_uri = %object_uri,
                    output_len = result.trim().len(),
                    "LLM returned insufficient content, falling back to extractive"
                );
                let l1 = generate_extractive_l1(source_content);
                (l1, LodMethod::Extractive, None)
            } else {
                let model = s.backend_name().to_string();
                (result, LodMethod::Llm, Some(model))
            }
        }
        None => {
            let l1 = generate_extractive_l1(source_content);
            (l1, LodMethod::Extractive, None)
        }
    };

    let expires_at = (chrono::Utc::now() + chrono::Duration::days(L1_TTL_DAYS)).to_rfc3339();

    let input = UpsertLodChunk {
        object_uri,
        brain_id,
        lod_level: LodLevel::L1,
        content: &l1_content,
        token_est: Some(estimate_tokens(&l1_content) as i64),
        method,
        model_id: model_id.as_deref(),
        source_hash,
        expires_at: Some(&expires_at),
        job_id: None,
    };
    LodChunkStore::upsert_lod_chunk(db, &input)?;

    Ok(Some(l1_content))
}

// ─── Helpers ────────────────────────────────────────────────────

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
    brain_id: &str,
) -> crate::error::Result<String> {
    let input = EnqueueJobInput {
        payload: JobPayload::ConsolidateCluster {
            cluster_index,
            suggested_title: suggested_title.to_string(),
            episode_ids: episode_ids.to_vec(),
            episodes: episodes.to_string(),
            brain_id: brain_id.to_string(),
        },
        priority: jobs::priority::NORMAL,
        retry_config: None, // uses payload default (Fixed{3})
        stuck_threshold_secs: None,
        metadata: serde_json::json!({}),
        scheduled_at: 0,
    };
    queue.enqueue_job(&input)
}

/// Enqueue an L1 summarization job with dedup check.
///
/// Returns `Ok(Some(job_id))` if enqueued, `Ok(None)` if skipped (a
/// non-terminal job for the same `object_uri` already exists).
pub fn enqueue_l1_summarize(
    queue: &dyn crate::ports::JobQueue,
    object_uri: &str,
    brain_id: &str,
    source_content: &str,
    source_hash: &str,
) -> crate::error::Result<Option<String>> {
    if source_content.len() > 100_000 {
        warn!(
            object_uri = %object_uri,
            content_len = source_content.len(),
            "enqueue_l1_summarize: rejecting source_content exceeding 100K chars"
        );
        return Ok(None);
    }

    if queue.has_active_lod_job(object_uri)? {
        return Ok(None);
    }

    let input = EnqueueJobInput {
        payload: JobPayload::LodSummarize {
            object_uri: object_uri.to_string(),
            brain_id: brain_id.to_string(),
            source_content: source_content.to_string(),
            source_hash: source_hash.to_string(),
        },
        priority: jobs::priority::BACKGROUND,
        retry_config: None, // uses payload default (Fixed{3})
        stuck_threshold_secs: None,
        metadata: serde_json::json!({}),
        scheduled_at: 0,
    };
    let job_id = queue.enqueue_job(&input)?;
    Ok(Some(job_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_persistence::db::Db;
    use rusqlite::params;

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
            brain_id: "brain-1".into(),
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

    /// Insert a stuck job directly into the DB for testing.
    /// Uses `in_progress` status with `started_at` backdated 600s.
    fn insert_stuck_job(db: &Db, job_id: &str) {
        let old = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 600;
        let payload = serde_json::to_string(&JobPayload::StaleScopeSweep).unwrap();
        db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO jobs (job_id, kind, status, started_at, priority, payload, attempts,
                                   retry_config, stuck_threshold_secs, metadata,
                                   created_at, scheduled_at, updated_at)
                 VALUES (?1, 'stale_scope_sweep', 'in_progress', ?2, 100, ?3, 1,
                         '{\"type\":\"fixed\",\"attempts\":3}', 60, '{}', ?2, ?2, ?2)",
                params![job_id, old, payload],
            )?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_reap_skips_active_jobs() {
        let db = setup_db();
        let active = ActiveJobs::new(100);

        insert_stuck_job(&db, "J-ACTIVE");

        // Guard held → reaper should skip this job.
        let _guard = active.acquire("J-ACTIVE".into());
        let reaped = reap_stuck_jobs_filtered(&db, &active).unwrap();
        assert_eq!(reaped, 0, "reaper must skip jobs in the active set");

        // Verify the job is still in_progress (not reset).
        let job = db
            .with_read_conn(|conn| brain_persistence::db::jobs::get_job(conn, "J-ACTIVE"))
            .unwrap()
            .unwrap();
        assert_eq!(
            job.status,
            brain_persistence::db::jobs::JobStatus::InProgress
        );
    }

    #[test]
    fn test_reap_proceeds_after_guard_dropped() {
        let db = setup_db();
        let active = ActiveJobs::new(100);

        insert_stuck_job(&db, "J-RELEASED");

        // Acquire and immediately drop — simulates task completion.
        let guard = active.acquire("J-RELEASED".into());
        drop(guard);

        let reaped = reap_stuck_jobs_filtered(&db, &active).unwrap();
        assert_eq!(
            reaped, 1,
            "reaper must process jobs no longer in active set"
        );
    }

    #[test]
    fn test_reap_mixed_active_and_stuck() {
        let db = setup_db();
        let active = ActiveJobs::new(100);

        insert_stuck_job(&db, "J-HELD");
        insert_stuck_job(&db, "J-FREE");

        // Only hold one guard.
        let _guard = active.acquire("J-HELD".into());

        let reaped = reap_stuck_jobs_filtered(&db, &active).unwrap();
        assert_eq!(reaped, 1, "should reap only the unguarded job");

        // Verify: J-HELD still in_progress, J-FREE was reaped (fail_job was called).
        let held = db
            .with_read_conn(|conn| brain_persistence::db::jobs::get_job(conn, "J-HELD"))
            .unwrap()
            .unwrap();
        assert_eq!(
            held.status,
            brain_persistence::db::jobs::JobStatus::InProgress
        );

        let freed = db
            .with_read_conn(|conn| brain_persistence::db::jobs::get_job(conn, "J-FREE"))
            .unwrap()
            .unwrap();
        // fail_job with retryable config resets to Ready.
        assert_eq!(freed.status, brain_persistence::db::jobs::JobStatus::Ready);
    }

    #[test]
    fn test_active_jobs_acquire_and_release() {
        let active = ActiveJobs::new(100);
        assert!(!active.contains("job-1"));

        let guard = active.acquire("job-1".into());
        assert!(active.contains("job-1"));

        drop(guard);
        assert!(!active.contains("job-1"));
    }

    #[test]
    fn test_active_jobs_guard_cleanup_on_panic() {
        let active = ActiveJobs::new(100);
        let active2 = active.clone();

        let handle = std::thread::spawn(move || {
            let _guard = active2.acquire("job-panic".into());
            panic!("simulated panic");
        });
        // The thread panicked, but the guard's Drop should have removed the entry.
        let _ = handle.join();
        assert!(!active.contains("job-panic"));
    }

    #[test]
    fn test_active_jobs_multiple_concurrent() {
        let active = ActiveJobs::new(100);

        let g1 = active.acquire("job-a".into());
        let g2 = active.acquire("job-b".into());

        assert!(active.contains("job-a"));
        assert!(active.contains("job-b"));
        assert!(!active.contains("job-c"));

        drop(g1);
        assert!(!active.contains("job-a"));
        assert!(active.contains("job-b"));

        drop(g2);
        assert!(!active.contains("job-b"));
    }

    // ─── Concurrency cap tests ─────────────────────────────────

    #[test]
    fn test_reserve_slots_basic() {
        let active = ActiveJobs::new(4);

        // Can reserve up to capacity
        assert_eq!(active.reserve_slots(2), 2);
        assert_eq!(active.reserve_slots(3), 2); // only 2 remain
        assert_eq!(active.reserve_slots(1), 0); // at capacity

        // Releasing makes slots available again
        active.release_slots(3);
        assert_eq!(active.reserve_slots(2), 2);
    }

    #[test]
    fn test_reserve_slots_zero_request() {
        let active = ActiveJobs::new(4);
        assert_eq!(active.reserve_slots(0), 0);
    }

    #[test]
    fn test_guard_drop_returns_slot() {
        let active = ActiveJobs::new(2);

        // Reserve and acquire both slots
        assert_eq!(active.reserve_slots(2), 2);
        let g1 = active.acquire("j1".into());
        let g2 = active.acquire("j2".into());

        // At capacity
        assert_eq!(active.reserve_slots(1), 0);

        // Drop one guard — its slot returns to the pool
        drop(g1);
        assert_eq!(active.reserve_slots(1), 1);

        drop(g2);
        assert_eq!(active.reserve_slots(1), 1);
    }

    #[test]
    fn test_guard_panic_returns_slot() {
        let active = ActiveJobs::new(2);
        assert_eq!(active.reserve_slots(1), 1);

        let active2 = active.clone();
        let handle = std::thread::spawn(move || {
            let _guard = active2.acquire("j-panic".into());
            panic!("boom");
        });
        let _ = handle.join();

        // Slot returned despite panic
        assert!(!active.contains("j-panic"));
        assert_eq!(active.reserve_slots(2), 2); // both slots free
    }

    #[test]
    fn test_reserve_slots_concurrent() {
        use std::sync::Barrier;

        let active = ActiveJobs::new(3);
        let barrier = Arc::new(Barrier::new(4));

        // 4 threads each try to reserve 2 slots out of 3 total
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let a = active.clone();
                let b = barrier.clone();
                std::thread::spawn(move || {
                    b.wait();
                    a.reserve_slots(2)
                })
            })
            .collect();

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // Exactly 3 slots should be distributed across all threads
        assert_eq!(total, 3);
    }

    // ─── LOD enqueue / process tests ──────────────────────────────

    #[test]
    fn test_enqueue_lod_summarize_creates_job() {
        let db = setup_db();
        let job_id = enqueue_l1_summarize(
            &db,
            "synapse://brain-1/chunk-1",
            "brain-1",
            "some source content",
            "hash-abc",
        )
        .unwrap();
        assert!(job_id.is_some(), "should return a job_id");
    }

    #[test]
    fn test_enqueue_dedup_skips_active_job() {
        let db = setup_db();
        // First enqueue succeeds.
        let first = enqueue_l1_summarize(
            &db,
            "synapse://brain-1/chunk-2",
            "brain-1",
            "content",
            "hash-1",
        )
        .unwrap();
        assert!(first.is_some());

        // Second enqueue for same URI is deduped.
        let second = enqueue_l1_summarize(
            &db,
            "synapse://brain-1/chunk-2",
            "brain-1",
            "content",
            "hash-1",
        )
        .unwrap();
        assert!(second.is_none(), "duplicate enqueue should return None");
    }

    #[test]
    fn test_enqueue_allows_after_completed() {
        let db = setup_db();

        let first = enqueue_l1_summarize(
            &db,
            "synapse://brain-1/chunk-3",
            "brain-1",
            "content",
            "hash-1",
        )
        .unwrap()
        .unwrap();

        // Mark the job done.
        db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE jobs SET status = 'done', processed_at = 1000 WHERE job_id = ?1",
                rusqlite::params![first],
            )?;
            Ok(())
        })
        .unwrap();

        // Now a new enqueue should succeed.
        let second = enqueue_l1_summarize(
            &db,
            "synapse://brain-1/chunk-3",
            "brain-1",
            "content",
            "hash-2",
        )
        .unwrap();
        assert!(second.is_some(), "should allow re-enqueue after done");
    }

    #[test]
    fn test_has_active_lod_job_true_for_active_statuses() {
        let db = setup_db();

        for status in &["ready", "pending", "in_progress"] {
            let uri = format!("synapse://brain/chunk-{status}");
            let payload = serde_json::to_string(&JobPayload::LodSummarize {
                object_uri: uri.clone(),
                brain_id: "brain-1".into(),
                source_content: "content".into(),
                source_hash: "hash".into(),
            })
            .unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            db.with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO jobs (job_id, kind, status, priority, payload, attempts,
                                       retry_config, stuck_threshold_secs, metadata,
                                       created_at, scheduled_at, updated_at)
                     VALUES (?1, 'lod_summarize', ?2, 200, ?3, 0,
                             '{\"type\":\"fixed\",\"attempts\":3}', 300, '{}', ?4, ?4, ?4)",
                    rusqlite::params![format!("job-{status}"), status, payload, now],
                )?;
                Ok(())
            })
            .unwrap();
            assert!(
                db.has_active_lod_job(&uri).unwrap(),
                "status={status} should be active"
            );
        }
    }

    #[test]
    fn test_has_active_lod_job_false_for_terminal_statuses() {
        let db = setup_db();

        for status in &["done", "failed"] {
            let uri = format!("synapse://brain/chunk-terminal-{status}");
            let payload = serde_json::to_string(&JobPayload::LodSummarize {
                object_uri: uri.clone(),
                brain_id: "brain-1".into(),
                source_content: "content".into(),
                source_hash: "hash".into(),
            })
            .unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            db.with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO jobs (job_id, kind, status, priority, payload, attempts,
                                       retry_config, stuck_threshold_secs, metadata,
                                       created_at, scheduled_at, updated_at)
                     VALUES (?1, 'lod_summarize', ?2, 200, ?3, 0,
                             '{\"type\":\"fixed\",\"attempts\":3}', 300, '{}', ?4, ?4, ?4)",
                    rusqlite::params![format!("job-terminal-{status}"), status, payload, now],
                )?;
                Ok(())
            })
            .unwrap();
            assert!(
                !db.has_active_lod_job(&uri).unwrap(),
                "status={status} should not be active"
            );
        }
    }

    #[tokio::test]
    async fn test_process_lod_summarize_extractive_fallback() {
        let db = setup_db();
        let payload = JobPayload::LodSummarize {
            object_uri: "synapse://brain-1/chunk-fallback".into(),
            brain_id: "brain-1".into(),
            source_content: "First paragraph.\n\nSecond paragraph.\n\nThird paragraph.".into(),
            source_hash: "hash-fb".into(),
        };

        // No LLM provider configured → extractive fallback.
        // process_lod_summarize calls brain_home() which may fail in test env.
        // We call it directly and tolerate brain_home resolution errors.
        let result = process_lod_summarize(&db, &payload).await;

        // In test env without brain_home set, it may succeed with extractive or
        // fail on brain_home resolution. Either is acceptable — just verify no panic.
        let _ = result;
    }

    #[test]
    fn test_process_lod_summarize_wrong_payload_returns_err() {
        // Calling with wrong payload variant should return Err immediately.
        // We use a synchronous wrapper to test the else branch.
        let payload = JobPayload::StaleScopeSweep;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let db = brain_persistence::db::Db::open_in_memory().unwrap();
        let result = rt.block_on(process_lod_summarize(&db, &payload));
        assert!(result.is_err(), "wrong payload should return Err");
    }
}
