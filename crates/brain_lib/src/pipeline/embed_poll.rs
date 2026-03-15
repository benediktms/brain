//! Job-based embedding pipeline for tasks and chunks.
//!
//! Claims `embed_task` and `embed_chunk` jobs from the `jobs` table, processes
//! them in batches (embed → LanceDB upsert → mark embedded), then completes
//! the jobs. Falls back gracefully on errors, failing individual jobs so they
//! can be retried.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::db::Db;
use crate::db::jobs::{self, Job};
use crate::embedder::{Embed, embed_batch_async};
use crate::store::Store;
use crate::tasks::capsule::{build_outcome_capsule, build_task_capsule, store_task_capsule};
use crate::tasks::queries::get_labels_for_tasks;

// ── Tasks ───────────────────────────────────────────────────────────────────

/// Claim and process `embed_task` jobs from the jobs table.
///
/// Builds task + outcome capsules, batch-embeds them, upserts to LanceDB and
/// SQLite FTS, then marks the tasks as embedded and completes the jobs.
///
/// `brain_id` — when non-empty, filters jobs to this brain only.
/// `db` must be the database containing both `tasks` and `jobs` tables.
///
/// Returns the number of tasks successfully embedded.
pub async fn poll_embed_task_jobs(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize {
    debug!("embed_poll: claiming embed_task jobs");

    // ── 1. Claim jobs ────────────────────────────────────────────────────
    let brain_filter = if brain_id.is_empty() {
        None
    } else {
        Some(brain_id)
    };

    let claimed_jobs: Vec<Job> = match db.claim_jobs("embed_task", brain_filter, 256) {
        Ok(j) => j,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to claim embed_task jobs");
            return 0;
        }
    };

    if claimed_jobs.is_empty() {
        debug!("embed_poll: no embed_task jobs to process");
        return 0;
    }

    info!(count = claimed_jobs.len(), "embed_poll: processing embed_task jobs");

    // ── 2. Fetch task rows by ref_id ─────────────────────────────────────
    let task_ids: Vec<&str> = claimed_jobs
        .iter()
        .filter_map(|j| j.ref_id.as_deref())
        .collect();

    #[derive(Debug)]
    struct TaskPollRow {
        task_id: String,
        title: String,
        description: Option<String>,
        status: String,
        priority: i32,
        blocked_reason: Option<String>,
    }

    let rows: Vec<TaskPollRow> = match db.with_read_conn(|conn| {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders: Vec<String> = (1..=task_ids.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "SELECT task_id, title, description, status, priority, blocked_reason
             FROM tasks WHERE task_id IN ({})",
            placeholders.join(", ")
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::types::ToSql> =
            task_ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok(TaskPollRow {
                task_id: row.get(0)?,
                title: row.get(1)?,
                description: row.get(2)?,
                status: row.get(3)?,
                priority: row.get(4)?,
                blocked_reason: row.get(5)?,
            })
        })?;
        crate::db::collect_rows(rows)
    }) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to fetch tasks for jobs");
            // Fail all jobs so they retry
            for job in &claimed_jobs {
                let _ = db.fail_job(&job.job_id, &format!("fetch error: {e}"), false);
            }
            return 0;
        }
    };

    // Build a lookup from task_id → TaskPollRow
    let task_map: std::collections::HashMap<&str, &TaskPollRow> =
        rows.iter().map(|r| (r.task_id.as_str(), r)).collect();

    // ── 3. Fetch labels ──────────────────────────────────────────────────
    let task_id_refs: Vec<&str> = rows.iter().map(|r| r.task_id.as_str()).collect();
    let label_map: std::collections::HashMap<String, Vec<String>> =
        match db.with_read_conn(|conn| get_labels_for_tasks(conn, &task_id_refs)) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "embed_poll: failed to fetch labels");
                std::collections::HashMap::new()
            }
        };

    // ── 4. Build capsule texts ───────────────────────────────────────────
    struct CapsuleEntry<'a> {
        job: &'a Job,
        task_id: String,
        title: String,
        file_id: String,
        capsule_text: String,
    }

    let mut capsules: Vec<CapsuleEntry<'_>> = Vec::new();
    let mut missing_job_ids: Vec<&str> = Vec::new();

    for job in &claimed_jobs {
        let task_id = match job.ref_id.as_deref() {
            Some(id) => id,
            None => {
                let _ = db.fail_job(&job.job_id, "missing ref_id", true);
                continue;
            }
        };

        let row = match task_map.get(task_id) {
            Some(r) => *r,
            None => {
                // Task was deleted since job was enqueued
                missing_job_ids.push(&job.job_id);
                continue;
            }
        };

        let labels = label_map.get(task_id).cloned().unwrap_or_default();
        let capsule_text = build_task_capsule(
            &row.title,
            row.description.as_deref(),
            &labels,
            row.priority,
        );
        let file_id = format!("task:{}", row.task_id);
        capsules.push(CapsuleEntry {
            job,
            task_id: row.task_id.clone(),
            title: row.title.clone(),
            file_id,
            capsule_text,
        });

        // Outcome capsule for terminal tasks
        let status = row.status.as_str();
        if status == "done" || status == "cancelled" {
            let reason = if status == "cancelled" {
                row.blocked_reason.as_deref()
            } else {
                None
            };
            let outcome_text = build_outcome_capsule(&row.title, reason);
            let outcome_file_id = format!("task-outcome:{}", row.task_id);
            capsules.push(CapsuleEntry {
                job,
                task_id: row.task_id.clone(),
                title: row.title.clone(),
                file_id: outcome_file_id,
                capsule_text: outcome_text,
            });
        }
    }

    // Complete jobs for deleted tasks (nothing to embed)
    if !missing_job_ids.is_empty() {
        let _ = db.complete_jobs(&missing_job_ids);
    }

    if capsules.is_empty() {
        return 0;
    }

    // ── 5. Batch embed ───────────────────────────────────────────────────
    let texts: Vec<String> = capsules.iter().map(|c| c.capsule_text.clone()).collect();
    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed task capsules");
            for cap in &capsules {
                let _ = db.fail_job(&cap.job.job_id, &format!("embed error: {e}"), false);
            }
            return 0;
        }
    };

    // ── 6. Upsert to LanceDB + SQLite FTS ───────────────────────────────
    let mut embedded_task_ids: HashSet<String> = HashSet::new();
    let mut completed_job_ids: HashSet<String> = HashSet::new();

    for (entry, embedding) in capsules.iter().zip(embeddings.iter()) {
        if let Err(e) = store
            .upsert_chunks(
                &entry.file_id,
                &entry.title,
                &[(0, entry.capsule_text.as_str())],
                std::slice::from_ref(embedding),
            )
            .await
        {
            warn!(
                task_id = %entry.task_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for task capsule"
            );
            let _ = db.fail_job(&entry.job.job_id, &format!("upsert error: {e}"), false);
            continue;
        }

        if let Err(e) = store_task_capsule(db, &entry.file_id, &entry.capsule_text) {
            warn!(
                task_id = %entry.task_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: SQLite FTS upsert failed for task capsule"
            );
            // Non-fatal: LanceDB succeeded, continue
        }

        embedded_task_ids.insert(entry.task_id.clone());
        completed_job_ids.insert(entry.job.job_id.clone());
    }

    // ── 7. Mark embedded + complete jobs ─────────────────────────────────
    if !embedded_task_ids.is_empty() {
        let ids_ref: Vec<&str> = embedded_task_ids.iter().map(|s| s.as_str()).collect();
        if let Err(e) = db.with_write_conn(|conn| mark_tasks_embedded(conn, &ids_ref)) {
            warn!(error = %e, "embed_poll: failed to mark tasks as embedded");
        }
    }

    if !completed_job_ids.is_empty() {
        let ids_ref: Vec<&str> = completed_job_ids.iter().map(|s| s.as_str()).collect();
        if let Err(e) = db.complete_jobs(&ids_ref) {
            warn!(error = %e, "embed_poll: failed to complete jobs");
        }
    }

    let count = embedded_task_ids.len();
    info!(count, "embed_poll: tasks embedded");
    count
}

/// Set `embedded_at = now()` on a batch of tasks.
fn mark_tasks_embedded(conn: &rusqlite::Connection, task_ids: &[&str]) -> crate::error::Result<()> {
    if task_ids.is_empty() {
        return Ok(());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let placeholders: Vec<String> = (2..=task_ids.len() + 1).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "UPDATE tasks SET embedded_at = ?1 WHERE task_id IN ({})",
        placeholders.join(", ")
    );
    let mut params: Vec<&dyn rusqlite::types::ToSql> = Vec::with_capacity(task_ids.len() + 1);
    let ts_ref: &dyn rusqlite::types::ToSql = &now;
    params.push(ts_ref);
    for id in task_ids {
        params.push(id as &dyn rusqlite::types::ToSql);
    }
    conn.execute(&sql, params.as_slice())?;
    Ok(())
}

// ── Chunks ──────────────────────────────────────────────────────────────────

/// Claim and process `embed_chunk` jobs from the jobs table.
///
/// Batch-embeds chunks, upserts to LanceDB, marks them as embedded, and
/// completes the jobs.
///
/// Returns the number of chunks successfully embedded.
pub async fn poll_embed_chunk_jobs(db: &Db, store: &Store, embedder: &Arc<dyn Embed>) -> usize {
    debug!("embed_poll: claiming embed_chunk jobs");

    let claimed_jobs: Vec<Job> = match db.claim_jobs("embed_chunk", None, 256) {
        Ok(j) => j,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to claim embed_chunk jobs");
            return 0;
        }
    };

    if claimed_jobs.is_empty() {
        debug!("embed_poll: no embed_chunk jobs to process");
        return 0;
    }

    info!(count = claimed_jobs.len(), "embed_poll: processing embed_chunk jobs");

    // ── Fetch chunk rows ─────────────────────────────────────────────────
    let chunk_ids: Vec<&str> = claimed_jobs
        .iter()
        .filter_map(|j| j.ref_id.as_deref())
        .collect();

    #[derive(Debug)]
    struct ChunkPollRow {
        chunk_id: String,
        file_id: String,
        file_path: String,
        chunk_ord: i32,
        content: String,
    }

    let rows: Vec<ChunkPollRow> = match db.with_read_conn(|conn| {
        if chunk_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders: Vec<String> = (1..=chunk_ids.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "SELECT c.chunk_id, c.file_id, COALESCE(f.path, c.file_id), c.chunk_ord, c.content
             FROM chunks c
             LEFT JOIN files f ON f.file_id = c.file_id
             WHERE c.chunk_id IN ({})",
            placeholders.join(", ")
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::types::ToSql> =
            chunk_ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok(ChunkPollRow {
                chunk_id: row.get(0)?,
                file_id: row.get(1)?,
                file_path: row.get(2)?,
                chunk_ord: row.get(3)?,
                content: row.get(4)?,
            })
        })?;
        crate::db::collect_rows(rows)
    }) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to fetch chunks for jobs");
            for job in &claimed_jobs {
                let _ = db.fail_job(&job.job_id, &format!("fetch error: {e}"), false);
            }
            return 0;
        }
    };

    // Build chunk_id → row lookup + job lookup
    let chunk_map: std::collections::HashMap<&str, &ChunkPollRow> =
        rows.iter().map(|r| (r.chunk_id.as_str(), r)).collect();
    let job_by_ref: std::collections::HashMap<&str, &Job> = claimed_jobs
        .iter()
        .filter_map(|j| j.ref_id.as_deref().map(|r| (r, j)))
        .collect();

    // Complete jobs for deleted chunks
    let mut missing_job_ids: Vec<&str> = Vec::new();
    for job in &claimed_jobs {
        if let Some(ref_id) = job.ref_id.as_deref() {
            if !chunk_map.contains_key(ref_id) {
                missing_job_ids.push(&job.job_id);
            }
        }
    }
    if !missing_job_ids.is_empty() {
        let _ = db.complete_jobs(&missing_job_ids);
    }

    if rows.is_empty() {
        return 0;
    }

    // ── Batch embed ─────────────────────────────────────────────────────
    let texts: Vec<String> = rows.iter().map(|r| r.content.clone()).collect();
    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed chunks");
            for job in &claimed_jobs {
                let _ = db.fail_job(&job.job_id, &format!("embed error: {e}"), false);
            }
            return 0;
        }
    };

    // ── Group by file_id for upsert ─────────────────────────────────────
    use std::collections::HashMap;

    struct FileGroup<'a> {
        file_path: &'a str,
        chunks: Vec<(usize, &'a str)>,
        embeddings: Vec<Vec<f32>>,
        chunk_ids: Vec<&'a str>,
    }

    let mut file_groups: HashMap<&str, FileGroup<'_>> = HashMap::new();

    for (row, embedding) in rows.iter().zip(embeddings.iter()) {
        let group = file_groups
            .entry(row.file_id.as_str())
            .or_insert_with(|| FileGroup {
                file_path: row.file_path.as_str(),
                chunks: Vec::new(),
                embeddings: Vec::new(),
                chunk_ids: Vec::new(),
            });
        group
            .chunks
            .push((row.chunk_ord as usize, row.content.as_str()));
        group.embeddings.push(embedding.clone());
        group.chunk_ids.push(row.chunk_id.as_str());
    }

    // ── Upsert each file group ──────────────────────────────────────────
    let mut embedded_chunk_ids: Vec<&str> = Vec::new();
    let mut failed_chunk_ids: HashSet<&str> = HashSet::new();

    for (file_id, group) in &file_groups {
        if let Err(e) = store
            .upsert_chunks(file_id, group.file_path, &group.chunks, &group.embeddings)
            .await
        {
            warn!(
                file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for file chunks"
            );
            for cid in &group.chunk_ids {
                failed_chunk_ids.insert(cid);
            }
            continue;
        }
        embedded_chunk_ids.extend_from_slice(&group.chunk_ids);
    }

    // ── Mark embedded + complete/fail jobs ───────────────────────────────
    if !embedded_chunk_ids.is_empty() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if let Err(e) = db.with_write_conn(|conn| {
            crate::db::chunks::mark_chunks_embedded(conn, &embedded_chunk_ids, now)
        }) {
            warn!(error = %e, "embed_poll: failed to mark chunks as embedded");
        }

        // Complete corresponding jobs
        let completed_job_ids: Vec<&str> = embedded_chunk_ids
            .iter()
            .filter_map(|cid| job_by_ref.get(cid).map(|j| j.job_id.as_str()))
            .collect();
        if !completed_job_ids.is_empty() {
            let _ = db.complete_jobs(&completed_job_ids);
        }
    }

    // Fail jobs for chunks that failed upsert
    for cid in &failed_chunk_ids {
        if let Some(job) = job_by_ref.get(cid) {
            let _ = db.fail_job(&job.job_id, "LanceDB upsert failed", false);
        }
    }

    let count = embedded_chunk_ids.len();
    info!(count, "embed_poll: chunks embedded");
    count
}

// ── Legacy compatibility wrappers ────────────────────────────────────────────

/// Poll for tasks whose capsule is stale (updated_at > embedded_at or embedded_at IS NULL).
///
/// This is the legacy polling path. It auto-enqueues jobs for stale tasks,
/// then delegates to the job-based processor.
///
/// Returns the number of tasks successfully embedded.
pub async fn poll_stale_tasks(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize {
    // Auto-enqueue jobs for any tasks that are stale but don't have active jobs
    if let Err(e) = db.with_write_conn(|conn| {
        enqueue_stale_task_jobs(conn, brain_id)
    }) {
        warn!(error = %e, "embed_poll: failed to enqueue stale task jobs");
    }

    poll_embed_task_jobs(db, store, embedder, brain_id).await
}

/// Poll for file chunks that have not yet been embedded into LanceDB.
///
/// This is the legacy polling path. It auto-enqueues jobs for stale chunks,
/// then delegates to the job-based processor.
///
/// Returns the number of chunks successfully embedded.
pub async fn poll_stale_chunks(db: &Db, store: &Store, embedder: &Arc<dyn Embed>) -> usize {
    // Auto-enqueue jobs for any chunks that are stale but don't have active jobs
    if let Err(e) = db.with_write_conn(|conn| {
        enqueue_stale_chunk_jobs(conn)
    }) {
        warn!(error = %e, "embed_poll: failed to enqueue stale chunk jobs");
    }

    poll_embed_chunk_jobs(db, store, embedder).await
}

/// Enqueue `embed_task` jobs for tasks that are stale (embedded_at < updated_at or NULL).
fn enqueue_stale_task_jobs(conn: &rusqlite::Connection, brain_id: &str) -> crate::error::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    if brain_id.is_empty() {
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority,
                               created_at, scheduled_at, updated_at)
             SELECT 'job-' || hex(randomblob(8)), 'embed_task', 'pending', brain_id,
                    task_id, 'task', 100, ?1, ?1, ?1
             FROM tasks
             WHERE (updated_at > COALESCE(embedded_at, 0) OR embedded_at IS NULL)
             LIMIT 256
             ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
             DO NOTHING",
            [now],
        )?;
    } else {
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority,
                               created_at, scheduled_at, updated_at)
             SELECT 'job-' || hex(randomblob(8)), 'embed_task', 'pending', brain_id,
                    task_id, 'task', 100, ?1, ?1, ?1
             FROM tasks
             WHERE (updated_at > COALESCE(embedded_at, 0) OR embedded_at IS NULL)
               AND brain_id = ?2
             LIMIT 256
             ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
             DO NOTHING",
            rusqlite::params![now, brain_id],
        )?;
    }
    Ok(())
}

/// Enqueue `embed_chunk` jobs for chunks that have not been embedded.
fn enqueue_stale_chunk_jobs(conn: &rusqlite::Connection) -> crate::error::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority,
                           created_at, scheduled_at, updated_at)
         SELECT 'job-' || hex(randomblob(8)), 'embed_chunk', 'pending', '',
                chunk_id, 'chunk', 100, ?1, ?1, ?1
         FROM chunks
         WHERE embedded_at IS NULL
         LIMIT 256
         ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
         DO NOTHING",
        [now],
    )?;
    Ok(())
}

// ── Self-heal ────────────────────────────────────────────────────────────────

/// Check if LanceDB is accessible. If not, bulk-enqueue re-embed jobs for all
/// tasks and chunks so they will be re-embedded on the next poll cycle.
///
/// `db` is the per-brain database (contains `chunks` and `jobs`).
/// `unified_db` is the unified database (contains `tasks` and `jobs`).
/// In single-brain mode, both point to the same database.
///
/// Returns `true` if a reset occurred (LanceDB was missing/inaccessible).
pub async fn self_heal_if_lance_missing(db: &Db, unified_db: &Db, store: &Store) -> bool {
    if store.current_schema_matches_expected().await {
        return false;
    }

    warn!("LanceDB not found — enqueuing re-embed jobs for full re-embed");

    // Enqueue embed_task jobs for all tasks
    if let Err(e) = unified_db.with_write_conn(|conn| {
        jobs::enqueue_embed_tasks_for_brain(conn, "")
    }) {
        warn!(error = %e, "embed_poll: failed to enqueue task re-embed jobs");
    }

    // Also reset embedded_at so the stale-task scanner picks up anything
    // that the bulk enqueue might have missed (e.g. ON CONFLICT DO NOTHING)
    if let Err(e) = unified_db.with_write_conn(|conn| {
        conn.execute_batch("UPDATE tasks SET embedded_at = NULL;")?;
        Ok(())
    }) {
        warn!(error = %e, "embed_poll: failed to reset tasks.embedded_at");
    }

    // Enqueue embed_chunk jobs for all chunks
    if let Err(e) = db.with_write_conn(|conn| {
        jobs::enqueue_embed_chunks_all(conn)
    }) {
        warn!(error = %e, "embed_poll: failed to enqueue chunk re-embed jobs");
    }

    if let Err(e) = db.with_write_conn(|conn| {
        conn.execute_batch("UPDATE chunks SET embedded_at = NULL;")?;
        Ok(())
    }) {
        warn!(error = %e, "embed_poll: failed to reset chunks.embedded_at");
    }

    true
}
