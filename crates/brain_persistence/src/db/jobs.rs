//! Job queue CRUD for async operations (summarization, consolidation, etc.).
//!
//! Jobs are direct CRUD (not event-sourced). The `kind` column is derived from
//! the [`JobPayload`] variant discriminant. The poll index supports efficient
//! claim queries ordered by priority and schedule time.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Result;

pub use super::job::{Job, JobPayload, JobStatus, RetryStrategy};

/// Priority constants for job scheduling.
pub mod priority {
    pub const CRITICAL: i32 = 0;
    pub const SELF_HEAL: i32 = 50;
    pub const NORMAL: i32 = 100;
    pub const BACKGROUND: i32 = 200;
}

/// Column list for SELECT/RETURNING queries. 16 columns.
const JOB_COLUMNS: &str = "\
    job_id, kind, status, priority, payload, \
    retry_config, stuck_threshold_secs, \
    result, attempts, last_error, metadata, \
    created_at, scheduled_at, started_at, processed_at, updated_at";

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}

/// Map a row (selected via `JOB_COLUMNS`) to a [`Job`].
fn row_to_job(row: &rusqlite::Row) -> rusqlite::Result<Job> {
    let status_str: String = row.get(2)?;
    let status: JobStatus = status_str.parse().map_err(|e: String| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, e.into())
    })?;

    let payload_str: String = row.get(4)?;
    let payload: JobPayload = serde_json::from_str(&payload_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let retry_config_str: String = row.get(5)?;
    let retry_config: RetryStrategy = serde_json::from_str(&retry_config_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let metadata_str: String = row.get(10)?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(e))
    })?;

    Ok(Job {
        job_id: row.get(0)?,
        payload,
        status,
        priority: row.get(3)?,
        retry_config,
        stuck_threshold_secs: row.get(6)?,
        result: row.get(7)?,
        attempts: row.get::<_, i32>(8)? as u32,
        last_error: row.get(9)?,
        metadata,
        created_at: row.get(11)?,
        scheduled_at: row.get(12)?,
        started_at: row.get(13)?,
        processed_at: row.get(14)?,
        updated_at: row.get(15)?,
    })
}

// ─── Enqueue ─────────────────────────────────────────────────────

/// Input for enqueueing a job.
///
/// `kind` and `retry_config` defaults are derived from the payload variant
/// unless overridden.
#[derive(Debug, Clone)]
pub struct EnqueueJobInput {
    pub payload: JobPayload,
    pub priority: i32,
    /// Override the default retry strategy for this kind. `None` = use payload default.
    pub retry_config: Option<RetryStrategy>,
    pub stuck_threshold_secs: Option<i64>,
    pub metadata: serde_json::Value,
    /// When to schedule (0 = immediately).
    pub scheduled_at: i64,
}

/// Enqueue a job. Returns the new job_id.
pub fn enqueue_job(conn: &Connection, input: &EnqueueJobInput) -> Result<String> {
    let job_id = ulid::Ulid::new().to_string();
    let now = now_secs();
    let scheduled_at = if input.scheduled_at == 0 {
        now
    } else {
        input.scheduled_at
    };

    let kind = input.payload.kind();
    let retry_config = input
        .retry_config
        .clone()
        .unwrap_or_else(|| input.payload.default_retry_strategy());
    let stuck_threshold = input
        .stuck_threshold_secs
        .unwrap_or_else(|| input.payload.default_stuck_threshold_secs());

    let payload_json = serde_json::to_string(&input.payload)
        .map_err(|e| crate::error::BrainCoreError::Internal(format!("serialize payload: {e}")))?;
    let retry_json = serde_json::to_string(&retry_config).map_err(|e| {
        crate::error::BrainCoreError::Internal(format!("serialize retry_config: {e}"))
    })?;
    let metadata_json = serde_json::to_string(&input.metadata)
        .map_err(|e| crate::error::BrainCoreError::Internal(format!("serialize metadata: {e}")))?;

    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, priority, payload,
                            retry_config, stuck_threshold_secs,
                            attempts, metadata, created_at, scheduled_at, updated_at)
         VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10)",
        params![
            job_id,
            kind,
            input.priority,
            payload_json,
            retry_json,
            stuck_threshold,
            metadata_json,
            now,
            scheduled_at,
            now,
        ],
    )?;

    Ok(job_id)
}

// ─── Claim / advance ─────────────────────────────────────────────

/// Atomically claim up to `limit` ready jobs.
/// Sets status to 'pending', increments attempts, records started_at.
/// Returns jobs sorted by priority then scheduled_at.
pub fn claim_ready_jobs(conn: &Connection, limit: i32) -> Result<Vec<Job>> {
    let now = now_secs();

    let mut stmt = conn.prepare(&format!(
        "UPDATE jobs SET status = 'pending',
                          started_at = ?1,
                          attempts = attempts + 1,
                          updated_at = ?1
         WHERE job_id IN (
             SELECT job_id FROM jobs
             WHERE status = 'ready'
               AND scheduled_at <= ?1
             ORDER BY priority ASC, scheduled_at ASC
             LIMIT ?2
         )
         RETURNING {JOB_COLUMNS}"
    ))?;

    let mut jobs: Vec<Job> = stmt
        .query_map(params![now, limit], row_to_job)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    jobs.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(a.scheduled_at.cmp(&b.scheduled_at))
    });

    Ok(jobs)
}

/// Advance a job from `pending` to `in_progress` just before dispatch.
pub fn advance_to_in_progress(conn: &Connection, job_id: &str) -> Result<()> {
    let now = now_secs();
    conn.execute(
        "UPDATE jobs SET status = 'in_progress', updated_at = ?1
         WHERE job_id = ?2 AND status = 'pending'",
        params![now, job_id],
    )?;
    Ok(())
}

// ─── Complete / fail ─────────────────────────────────────────────

/// Mark a job as done with an optional result. Sets processed_at.
pub fn complete_job(conn: &Connection, job_id: &str, result: Option<&str>) -> Result<()> {
    let now = now_secs();
    conn.execute(
        "UPDATE jobs SET status = 'done', result = ?1, processed_at = ?2, updated_at = ?2
         WHERE job_id = ?3 AND status IN ('pending', 'in_progress')",
        params![result, now, job_id],
    )?;
    Ok(())
}

/// Handle a job failure. If retries remain, reschedule to `ready` with
/// exponential backoff. If exhausted, mark as `failed` and set processed_at.
pub fn fail_job(conn: &Connection, job_id: &str, error_msg: &str) -> Result<()> {
    let now = now_secs();

    let (attempts, retry_config_str): (i32, String) = conn.query_row(
        "SELECT attempts, retry_config FROM jobs WHERE job_id = ?1",
        [job_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    let retry_config: RetryStrategy =
        serde_json::from_str(&retry_config_str).unwrap_or(RetryStrategy::NoRetry);

    if retry_config.is_exhausted(attempts as u32) {
        // Terminal failure.
        conn.execute(
            "UPDATE jobs SET status = 'failed', last_error = ?1, processed_at = ?2, updated_at = ?2
             WHERE job_id = ?3 AND status IN ('pending', 'in_progress')",
            params![error_msg, now, job_id],
        )?;
    } else {
        // Reschedule with exponential backoff: 30s, 60s, 120s, ... capped at 1h.
        let exponent = (attempts - 1).clamp(0, 6) as u32;
        let backoff = std::cmp::min(30_i64 * (1_i64 << exponent), 3600);
        let next_scheduled = now + backoff;

        conn.execute(
            "UPDATE jobs SET status = 'ready',
                              last_error = ?1,
                              scheduled_at = ?2,
                              started_at = NULL,
                              updated_at = ?3
             WHERE job_id = ?4 AND status IN ('pending', 'in_progress')",
            params![error_msg, next_scheduled, now, job_id],
        )?;
    }

    Ok(())
}

// ─── Maintenance ─────────────────────────────────────────────────

/// Reap stuck jobs: reset in_progress/pending jobs that exceeded their
/// `stuck_threshold_secs` back to `ready`. Only reaps retryable jobs.
pub fn reap_stuck_jobs(conn: &Connection) -> Result<usize> {
    let stuck = list_stuck_jobs(conn)?;

    let retryable_ids: Vec<&str> = stuck
        .iter()
        .filter(|j| j.retry_config.is_retryable())
        .map(|j| j.job_id.as_str())
        .collect();

    if retryable_ids.is_empty() {
        return Ok(0);
    }

    let now = now_secs();
    let mut count = 0;

    for job_id in &retryable_ids {
        count += conn.execute(
            "UPDATE jobs SET status = 'ready',
                              started_at = NULL,
                              updated_at = ?1
             WHERE job_id = ?2
               AND status IN ('in_progress', 'pending')",
            params![now, job_id],
        )?;
    }

    Ok(count)
}

/// Delete old completed jobs older than `age_secs`.
///
/// Excludes recurring singleton kinds (listed in `protected_kinds`) from
/// deletion — those rows are recycled via `reschedule_terminal_job`, not GC'd.
pub fn gc_completed_jobs(
    conn: &Connection,
    age_secs: i64,
    protected_kinds: &[&str],
) -> Result<usize> {
    let cutoff = now_secs() - age_secs;

    if protected_kinds.is_empty() {
        let count = conn.execute(
            "DELETE FROM jobs WHERE status IN ('done', 'failed') AND processed_at < ?1",
            [cutoff],
        )?;
        return Ok(count);
    }

    // Build a NOT IN clause for protected kinds.
    let placeholders: Vec<String> = (0..protected_kinds.len())
        .map(|i| format!("?{}", i + 2))
        .collect();
    let sql = format!(
        "DELETE FROM jobs WHERE status IN ('done', 'failed') AND processed_at < ?1 AND kind NOT IN ({})",
        placeholders.join(", ")
    );

    let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params_vec.push(Box::new(cutoff));
    for kind in protected_kinds {
        params_vec.push(Box::new(kind.to_string()));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params_vec.iter().map(|p| p.as_ref()).collect();

    let count = conn.execute(&sql, param_refs.as_slice())?;
    Ok(count)
}

// ─── Queries ─────────────────────────────────────────────────────

/// Get a single job by ID.
pub fn get_job(conn: &Connection, job_id: &str) -> Result<Option<Job>> {
    let job = conn
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE job_id = ?1"),
            [job_id],
            row_to_job,
        )
        .optional()?;
    Ok(job)
}

/// List jobs with optional status filter.
pub fn list_jobs(
    conn: &Connection,
    status_filter: Option<&JobStatus>,
    limit: i32,
) -> Result<Vec<Job>> {
    let mut sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE 1=1");
    let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(status) = status_filter {
        params_vec.push(Box::new(status.as_ref().to_string()));
        sql.push_str(&format!(" AND status = ?{}", params_vec.len()));
    }

    params_vec.push(Box::new(limit));
    sql.push_str(&format!(
        " ORDER BY priority ASC, scheduled_at ASC LIMIT ?{}",
        params_vec.len()
    ));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params_vec.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let jobs = stmt
        .query_map(param_refs.as_slice(), row_to_job)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(jobs)
}

// ─── Status counts ────────────────────────────────────────────────

/// Count jobs by status.
pub fn count_jobs_by_status(conn: &Connection, status: &JobStatus) -> Result<i64> {
    let status_str = status.as_ref().to_string();
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE status = ?1",
        [&status_str],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// List recent jobs filtered by status, ordered by most recent first.
pub fn list_jobs_by_status(conn: &Connection, status: &JobStatus, limit: i32) -> Result<Vec<Job>> {
    let status_str = status.as_ref().to_string();
    let mut stmt = conn.prepare(&format!(
        "SELECT {JOB_COLUMNS} FROM jobs
         WHERE status = ?1
         ORDER BY updated_at DESC
         LIMIT ?2"
    ))?;
    let jobs = stmt
        .query_map(params![status_str, limit], row_to_job)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(jobs)
}

/// List stuck jobs (in_progress with started_at past threshold, or pending with
/// updated_at past threshold) that are still retryable.
pub fn list_stuck_jobs(conn: &Connection) -> Result<Vec<Job>> {
    let now = now_secs();
    let mut stmt = conn.prepare(&format!(
        "SELECT {JOB_COLUMNS} FROM jobs
         WHERE (status = 'in_progress' AND started_at < ?1 - stuck_threshold_secs)
            OR (status = 'pending' AND updated_at < ?1 - stuck_threshold_secs)
         ORDER BY started_at ASC
         LIMIT 50"
    ))?;
    let jobs = stmt
        .query_map(params![now], row_to_job)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(jobs)
}

/// Reset a failed job back to `ready`. Returns `true` if a row was updated.
pub fn retry_failed_job(conn: &Connection, job_id: &str) -> Result<bool> {
    let now = now_secs();
    let rows = conn.execute(
        "UPDATE jobs
             SET status = 'ready',
                 last_error = NULL,
                 result = NULL,
                 started_at = NULL,
                 processed_at = NULL,
                 scheduled_at = ?1,
                 updated_at = ?1
         WHERE job_id = ?2 AND status = 'failed'",
        params![now, job_id],
    )?;
    Ok(rows > 0)
}

// ─── Singleton / dedup ──────────────────────────────────────────

/// Get a job by its `kind` column. Prefers active (non-terminal) jobs;
/// falls back to the most recently updated row.
pub fn get_job_by_kind(conn: &Connection, kind: &str) -> Result<Option<Job>> {
    let job = conn
        .query_row(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE kind = ?1
                 ORDER BY CASE WHEN status IN ('done', 'failed') THEN 1 ELSE 0 END,
                          updated_at DESC
                 LIMIT 1"
            ),
            [kind],
            row_to_job,
        )
        .optional()?;
    Ok(job)
}

/// If no job of this `kind` (and brain scope) exists, insert one in `ready` state.
/// Returns `Some(job_id)` if inserted, `None` if a row already exists.
///
/// For per-brain jobs (payload contains `brain_id`), uniqueness is scoped by
/// `kind + json_extract(payload, '$.brain_id')`. For global jobs, just `kind`.
///
/// Uses a single INSERT ... WHERE NOT EXISTS for atomicity under
/// SQLite's single-writer serialization.
pub fn ensure_singleton_job(conn: &Connection, input: &EnqueueJobInput) -> Result<Option<String>> {
    let job_id = ulid::Ulid::new().to_string();
    let now = now_secs();
    let scheduled_at = if input.scheduled_at == 0 {
        now
    } else {
        input.scheduled_at
    };

    let kind = input.payload.kind();
    let brain_id = input.payload.brain_id();
    let retry_config = input
        .retry_config
        .clone()
        .unwrap_or_else(|| input.payload.default_retry_strategy());
    let stuck_threshold = input
        .stuck_threshold_secs
        .unwrap_or_else(|| input.payload.default_stuck_threshold_secs());

    let payload_json = serde_json::to_string(&input.payload)
        .map_err(|e| crate::error::BrainCoreError::Internal(format!("serialize payload: {e}")))?;
    let retry_json = serde_json::to_string(&retry_config).map_err(|e| {
        crate::error::BrainCoreError::Internal(format!("serialize retry_config: {e}"))
    })?;
    let metadata_json = serde_json::to_string(&input.metadata)
        .map_err(|e| crate::error::BrainCoreError::Internal(format!("serialize metadata: {e}")))?;

    // Use named parameter :brain_scope for the NOT EXISTS subquery so it
    // doesn't depend on the positional parameter count of the INSERT.
    let exists_sql = if brain_id.is_some() {
        "SELECT 1 FROM jobs WHERE kind = :kind AND json_extract(payload, '$.brain_id') = :brain_scope"
    } else {
        "SELECT 1 FROM jobs WHERE kind = :kind"
    };

    let sql = format!(
        "INSERT INTO jobs (job_id, kind, status, priority, payload,
                            retry_config, stuck_threshold_secs,
                            attempts, metadata, created_at, scheduled_at, updated_at)
         SELECT :job_id, :kind, 'ready', :priority, :payload, :retry, :stuck, 0, :meta, :now, :sched, :now
         WHERE NOT EXISTS ({exists_sql})"
    );

    let mut named_params: Vec<(&str, Box<dyn rusqlite::types::ToSql>)> = vec![
        (":job_id", Box::new(job_id.clone())),
        (":kind", Box::new(kind.to_string())),
        (":priority", Box::new(input.priority)),
        (":payload", Box::new(payload_json)),
        (":retry", Box::new(retry_json)),
        (":stuck", Box::new(stuck_threshold)),
        (":meta", Box::new(metadata_json)),
        (":now", Box::new(now)),
        (":sched", Box::new(scheduled_at)),
    ];
    if let Some(bid) = brain_id {
        named_params.push((":brain_scope", Box::new(bid.to_string())));
    }

    let param_refs: Vec<(&str, &dyn rusqlite::types::ToSql)> =
        named_params.iter().map(|(k, v)| (*k, v.as_ref())).collect();
    let rows = conn.execute(&sql, param_refs.as_slice())?;

    if rows > 0 { Ok(Some(job_id)) } else { Ok(None) }
}

/// Ensure a singleton job exists and is schedulable. Combines
/// `ensure_singleton_job` + `reschedule_terminal_job` into one call
/// so both can run under a single `with_write_conn` mutex acquisition.
///
/// 1. If no row exists for this kind (+ brain scope) → insert as `ready`.
/// 2. If a row exists in terminal state → reset to `ready`.
/// 3. If a row exists and is active → no-op.
pub fn reconcile_singleton_job(conn: &Connection, input: &EnqueueJobInput) -> Result<()> {
    reconcile_singleton_job_with_delay(conn, input, 0)
}

/// Like [`reconcile_singleton_job`] but schedules the rescheduled job
/// `delay_secs` into the future (so it won't be claimed immediately).
pub fn reconcile_singleton_job_with_delay(
    conn: &Connection,
    input: &EnqueueJobInput,
    delay_secs: i64,
) -> Result<()> {
    let inserted = ensure_singleton_job(conn, input)?;
    if inserted.is_none() {
        // Row already exists — try to reschedule if terminal.
        reschedule_terminal_job(
            conn,
            input.payload.kind(),
            input.payload.brain_id(),
            delay_secs,
        )?;
    }
    Ok(())
}

/// If the singleton job for `kind` (+ brain scope) is in a terminal state
/// (done/failed), reset it to `ready`. Returns `true` if a row was reset.
///
/// Uses `UPDATE ... WHERE status IN ('done','failed')` as the mutex —
/// concurrent callers get `rows_affected=0` and skip.
/// `in_progress` jobs are never touched.
pub fn reschedule_terminal_job(
    conn: &Connection,
    kind: &str,
    brain_id: Option<&str>,
    delay_secs: i64,
) -> Result<bool> {
    let now = now_secs();
    let scheduled_at = now + delay_secs;

    let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
        if let Some(bid) = brain_id {
            (
                "UPDATE jobs SET status = 'ready',
                              attempts = 0,
                              result = NULL,
                              last_error = NULL,
                              started_at = NULL,
                              processed_at = NULL,
                              scheduled_at = ?1,
                              updated_at = ?2
             WHERE kind = ?3 AND json_extract(payload, '$.brain_id') = ?4
               AND status IN ('done', 'failed')"
                    .to_string(),
                vec![
                    Box::new(scheduled_at) as Box<dyn rusqlite::types::ToSql>,
                    Box::new(now),
                    Box::new(kind.to_string()),
                    Box::new(bid.to_string()),
                ],
            )
        } else {
            (
                "UPDATE jobs SET status = 'ready',
                              attempts = 0,
                              result = NULL,
                              last_error = NULL,
                              started_at = NULL,
                              processed_at = NULL,
                              scheduled_at = ?1,
                              updated_at = ?2
             WHERE kind = ?3 AND status IN ('done', 'failed')"
                    .to_string(),
                vec![
                    Box::new(scheduled_at) as Box<dyn rusqlite::types::ToSql>,
                    Box::new(now),
                    Box::new(kind.to_string()),
                ],
            )
        };

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params_vec.iter().map(|p| p.as_ref()).collect();
    let rows = conn.execute(&sql, param_refs.as_slice())?;
    Ok(rows > 0)
}

/// Enqueue an on-demand singleton job. If a non-terminal job of the same
/// `kind` already exists, returns its `job_id` instead of inserting.
/// Returns `(job_id, was_created)`.
///
/// Uses `INSERT ... SELECT ... WHERE NOT EXISTS` for atomicity under
/// SQLite's single-writer serialization, then falls back to a SELECT
/// if the INSERT matched zero rows (another writer won the race).
pub fn enqueue_dedup_job(conn: &Connection, input: &EnqueueJobInput) -> Result<(String, bool)> {
    let job_id = ulid::Ulid::new().to_string();
    let now = now_secs();
    let scheduled_at = if input.scheduled_at == 0 {
        now
    } else {
        input.scheduled_at
    };

    let kind = input.payload.kind();
    let retry_config = input
        .retry_config
        .clone()
        .unwrap_or_else(|| input.payload.default_retry_strategy());
    let stuck_threshold = input
        .stuck_threshold_secs
        .unwrap_or_else(|| input.payload.default_stuck_threshold_secs());

    let payload_json = serde_json::to_string(&input.payload)
        .map_err(|e| crate::error::BrainCoreError::Internal(format!("serialize payload: {e}")))?;
    let retry_json = serde_json::to_string(&retry_config).map_err(|e| {
        crate::error::BrainCoreError::Internal(format!("serialize retry_config: {e}"))
    })?;
    let metadata_json = serde_json::to_string(&input.metadata)
        .map_err(|e| crate::error::BrainCoreError::Internal(format!("serialize metadata: {e}")))?;

    let rows = conn.execute(
        "INSERT INTO jobs (job_id, kind, status, priority, payload,
                            retry_config, stuck_threshold_secs,
                            attempts, metadata, created_at, scheduled_at, updated_at)
         SELECT ?1, ?2, 'ready', ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10
         WHERE NOT EXISTS (
             SELECT 1 FROM jobs WHERE kind = ?2 AND status NOT IN ('done', 'failed')
         )",
        params![
            job_id,
            kind,
            input.priority,
            payload_json,
            retry_json,
            stuck_threshold,
            metadata_json,
            now,
            scheduled_at,
            now,
        ],
    )?;

    if rows > 0 {
        return Ok((job_id, true));
    }

    // Another job of this kind is active — return it.
    let existing: String = conn.query_row(
        "SELECT job_id FROM jobs WHERE kind = ?1 AND status NOT IN ('done', 'failed') LIMIT 1",
        [kind],
        |row| row.get(0),
    )?;
    Ok((existing, false))
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE jobs (
                 job_id              TEXT PRIMARY KEY,
                 kind                TEXT NOT NULL,
                 status              TEXT NOT NULL DEFAULT 'ready'
                                     CHECK (status IN ('ready', 'pending', 'in_progress', 'done', 'failed')),
                 priority            INTEGER NOT NULL DEFAULT 100,
                 payload             TEXT NOT NULL DEFAULT '{}',
                 retry_config        TEXT NOT NULL DEFAULT '{\"type\":\"no_retry\"}',
                 stuck_threshold_secs INTEGER NOT NULL DEFAULT 300,
                 result              TEXT,
                 attempts            INTEGER NOT NULL DEFAULT 0,
                 last_error          TEXT,
                 metadata            TEXT NOT NULL DEFAULT '{}',
                 created_at          INTEGER NOT NULL,
                 scheduled_at        INTEGER NOT NULL,
                 started_at          INTEGER,
                 processed_at        INTEGER,
                 updated_at          INTEGER NOT NULL
             );

             CREATE INDEX idx_jobs_poll ON jobs(status, priority, scheduled_at);
             CREATE INDEX idx_jobs_kind ON jobs(kind, status);",
        )
        .unwrap();
        conn
    }

    fn make_payload() -> JobPayload {
        JobPayload::SummarizeScope {
            summary_id: "sum-1".into(),
            scope_type: "directory".into(),
            scope_value: "src/".into(),
            content: "fn main() {}".into(),
        }
    }

    fn enq(conn: &Connection, payload: JobPayload, prio: i32) -> String {
        let input = EnqueueJobInput {
            payload,
            priority: prio,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };
        enqueue_job(conn, &input).unwrap()
    }

    fn set_status(conn: &Connection, job_id: &str, status: &str) {
        conn.execute(
            "UPDATE jobs SET status = ?1, updated_at = 0 WHERE job_id = ?2",
            params![status, job_id],
        )
        .unwrap();
    }

    #[test]
    fn test_enqueue_creates_ready_job() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready);
        assert_eq!(job.kind(), "summarize_scope");
        assert_eq!(job.attempts, 0);
        assert_eq!(job.retry_config, RetryStrategy::Fixed { attempts: 3 });
    }

    #[test]
    fn test_claim_returns_job_and_sets_pending() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);

        let claimed = claim_ready_jobs(&conn, 10).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].job_id, job_id);
        assert_eq!(claimed[0].status, JobStatus::Pending);
        assert_eq!(claimed[0].attempts, 1);
        assert!(claimed[0].started_at.is_some());
    }

    #[test]
    fn test_claim_respects_priority_ordering() {
        let conn = setup_db();
        let now = now_secs();
        let payload_low = serde_json::to_string(&JobPayload::SummarizeScope {
            summary_id: "sum-low".into(),
            scope_type: "directory".into(),
            scope_value: "low/".into(),
            content: "".into(),
        })
        .unwrap();
        let payload_high = serde_json::to_string(&JobPayload::SummarizeScope {
            summary_id: "sum-high".into(),
            scope_type: "directory".into(),
            scope_value: "high/".into(),
            content: "".into(),
        })
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-LOW', 'summarize_scope', 'ready', 200, ?1, '{\"type\":\"fixed\",\"attempts\":3}', '{}', ?2, ?2, ?2)",
            params![payload_low, now],
        ).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-HIGH', 'summarize_scope', 'ready', 0, ?1, '{\"type\":\"fixed\",\"attempts\":3}', '{}', ?2, ?2, ?2)",
            params![payload_high, now],
        ).unwrap();

        let claimed = claim_ready_jobs(&conn, 10).unwrap();
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].job_id, "J-HIGH");
        assert_eq!(claimed[1].job_id, "J-LOW");
    }

    #[test]
    fn test_claim_skips_future_scheduled() {
        let conn = setup_db();
        let now = now_secs();
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-FUTURE', 'summarize_scope', 'ready', 100, ?1, '{\"type\":\"fixed\",\"attempts\":3}', '{}', ?2, ?3, ?2)",
            params![payload, now, now + 3600],
        ).unwrap();

        let claimed = claim_ready_jobs(&conn, 10).unwrap();
        assert!(claimed.is_empty());
    }

    #[test]
    fn test_retry_failed_job_resets_state() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        set_status(&conn, &job_id, "failed");

        let reset = retry_failed_job(&conn, &job_id).unwrap();
        assert!(reset);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready);
        assert!(job.last_error.is_none());
        assert!(job.processed_at.is_none());
    }

    #[test]
    fn test_retry_failed_job_noop_for_non_failed() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        set_status(&conn, &job_id, "in_progress");

        let reset = retry_failed_job(&conn, &job_id).unwrap();
        assert!(!reset);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::InProgress);
    }

    #[test]
    fn test_advance_to_in_progress() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        claim_ready_jobs(&conn, 1).unwrap();

        advance_to_in_progress(&conn, &job_id).unwrap();

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::InProgress);
    }

    #[test]
    fn test_complete_sets_done_and_processed_at() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        claim_ready_jobs(&conn, 1).unwrap();
        advance_to_in_progress(&conn, &job_id).unwrap();

        complete_job(&conn, &job_id, Some(r#"{"summary":"done"}"#)).unwrap();

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Done);
        assert_eq!(job.result.as_deref(), Some(r#"{"summary":"done"}"#));
        assert!(job.processed_at.is_some());
    }

    #[test]
    fn test_fail_with_retries_reschedules_to_ready() {
        let conn = setup_db();
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J1', 'summarize_scope', 'pending', 100, ?1, 1,
                     '{\"type\":\"fixed\",\"attempts\":3}', '{}', 1000, 1000, 1000)",
            [&payload],
        )
        .unwrap();

        fail_job(&conn, "J1", "timeout").unwrap();

        let job = get_job(&conn, "J1").unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready, "should reschedule to ready");
        assert_eq!(job.last_error.as_deref(), Some("timeout"));
        assert!(job.started_at.is_none());
        assert!(job.scheduled_at > now_secs() - 5);
    }

    #[test]
    fn test_fail_exhausted_retries_marks_failed() {
        let conn = setup_db();
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J1', 'summarize_scope', 'pending', 100, ?1, 3,
                     '{\"type\":\"fixed\",\"attempts\":3}', '{}', 1000, 1000, 1000)",
            [&payload],
        )
        .unwrap();

        fail_job(&conn, "J1", "fatal error").unwrap();

        let job = get_job(&conn, "J1").unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Failed);
        assert!(job.processed_at.is_some());
    }

    #[test]
    fn test_fail_no_retry_marks_failed_immediately() {
        let conn = setup_db();
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J1', 'summarize_scope', 'pending', 100, ?1, 1,
                     '{\"type\":\"no_retry\"}', '{}', 1000, 1000, 1000)",
            [&payload],
        )
        .unwrap();

        fail_job(&conn, "J1", "no retry").unwrap();

        let job = get_job(&conn, "J1").unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Failed);
    }

    #[test]
    fn test_fail_infinite_always_reschedules() {
        let conn = setup_db();
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J1', 'summarize_scope', 'pending', 100, ?1, 999,
                     '{\"type\":\"infinite\"}', '{}', 1000, 1000, 1000)",
            [&payload],
        )
        .unwrap();

        fail_job(&conn, "J1", "transient error").unwrap();

        let job = get_job(&conn, "J1").unwrap().unwrap();
        assert_eq!(
            job.status,
            JobStatus::Ready,
            "Infinite should always reschedule"
        );
    }

    #[test]
    fn test_reap_stuck_in_progress() {
        let conn = setup_db();
        let old = now_secs() - 600;
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, started_at, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-STUCK', 'summarize_scope', 'in_progress', ?1, 100, ?2, 1,
                     '{\"type\":\"fixed\",\"attempts\":3}', '{}', ?1, ?1, ?1)",
            params![old, payload],
        )
        .unwrap();

        let reaped = reap_stuck_jobs(&conn).unwrap();
        assert_eq!(reaped, 1);

        let job = get_job(&conn, "J-STUCK").unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready);
        assert!(job.started_at.is_none());
    }

    #[test]
    fn test_reap_skips_no_retry_jobs() {
        let conn = setup_db();
        let old = now_secs() - 600;
        let payload = serde_json::to_string(&make_payload()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, started_at, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-NORETRY', 'summarize_scope', 'in_progress', ?1, 100, ?2, 1,
                     '{\"type\":\"no_retry\"}', '{}', ?1, ?1, ?1)",
            params![old, payload],
        )
        .unwrap();

        let reaped = reap_stuck_jobs(&conn).unwrap();
        assert_eq!(reaped, 0);

        let job = get_job(&conn, "J-NORETRY").unwrap().unwrap();
        assert_eq!(job.status, JobStatus::InProgress);
    }

    #[test]
    fn test_gc_completed_jobs() {
        let conn = setup_db();
        let now = now_secs();
        let old = now - 86400 * 2;
        let payload = serde_json::to_string(&make_payload()).unwrap();

        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, processed_at, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-OLD', 'summarize_scope', 'done', ?1, 100, ?2, 1,
                     '{\"type\":\"fixed\",\"attempts\":3}', '{}', ?1, ?1, ?1)",
            params![old, payload],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, processed_at, priority, payload, attempts,
                               retry_config, metadata, created_at, scheduled_at, updated_at)
             VALUES ('J-RECENT', 'summarize_scope', 'done', ?1, 100, ?2, 1,
                     '{\"type\":\"fixed\",\"attempts\":3}', '{}', ?1, ?1, ?1)",
            params![now, payload],
        )
        .unwrap();

        let deleted = gc_completed_jobs(&conn, 86400, &[]).unwrap();
        assert_eq!(deleted, 1);
        assert!(get_job(&conn, "J-OLD").unwrap().is_none());
        assert!(get_job(&conn, "J-RECENT").unwrap().is_some());
    }

    #[test]
    fn test_list_jobs_with_status_filter() {
        let conn = setup_db();
        enq(&conn, make_payload(), priority::NORMAL);
        let id2 = enq(
            &conn,
            JobPayload::ConsolidateCluster {
                cluster_index: 0,
                suggested_title: "Episodes".into(),
                episode_ids: vec!["ep1".into()],
                episodes: "ep1".into(),
                brain_id: "brain-1".into(),
            },
            priority::NORMAL,
        );

        conn.execute("UPDATE jobs SET status = 'done' WHERE job_id = ?1", [&id2])
            .unwrap();

        let all = list_jobs(&conn, None, 50).unwrap();
        assert_eq!(all.len(), 2);

        let done = list_jobs(&conn, Some(&JobStatus::Done), 50).unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].kind(), "consolidate_cluster");
    }

    #[test]
    fn test_payload_roundtrip_through_db() {
        let conn = setup_db();
        let payload = JobPayload::SummarizeScope {
            summary_id: "sum-tag".into(),
            scope_type: "tag".into(),
            scope_value: "rust".into(),
            content: "fn hello() {}".into(),
        };
        let job_id = enq(&conn, payload, priority::NORMAL);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        match &job.payload {
            JobPayload::SummarizeScope {
                summary_id,
                scope_type,
                scope_value,
                content,
            } => {
                assert_eq!(summary_id, "sum-tag");
                assert_eq!(scope_type, "tag");
                assert_eq!(scope_value, "rust");
                assert_eq!(content, "fn hello() {}");
            }
            _ => panic!("unexpected payload variant"),
        }
    }

    // ─── Singleton / dedup tests ─────────────────────────────────

    #[test]
    fn test_get_job_by_kind() {
        let conn = setup_db();
        assert!(get_job_by_kind(&conn, "summarize_scope").unwrap().is_none());

        enq(&conn, make_payload(), priority::NORMAL);
        let job = get_job_by_kind(&conn, "summarize_scope").unwrap().unwrap();
        assert_eq!(job.kind(), "summarize_scope");
    }

    #[test]
    fn test_ensure_singleton_job_inserts_first_time() {
        let conn = setup_db();
        let input = EnqueueJobInput {
            payload: make_payload(),
            priority: priority::NORMAL,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };

        let result = ensure_singleton_job(&conn, &input).unwrap();
        assert!(result.is_some(), "should insert on first call");

        let result2 = ensure_singleton_job(&conn, &input).unwrap();
        assert!(result2.is_none(), "should skip on second call");
    }

    #[test]
    fn test_ensure_singleton_job_skips_even_when_terminal() {
        let conn = setup_db();
        let input = EnqueueJobInput {
            payload: make_payload(),
            priority: priority::NORMAL,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };

        let job_id = ensure_singleton_job(&conn, &input).unwrap().unwrap();
        set_status(&conn, &job_id, "done");

        // Still skips — singleton checks for ANY row of this kind
        let result = ensure_singleton_job(&conn, &input).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_reschedule_terminal_job_resets_done() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        set_status(&conn, &job_id, "done");

        let reset = reschedule_terminal_job(&conn, "summarize_scope", None, 0).unwrap();
        assert!(reset);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready);
        assert_eq!(job.attempts, 0);
        assert!(job.result.is_none());
        assert!(job.last_error.is_none());
    }

    #[test]
    fn test_reschedule_terminal_job_resets_failed() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        set_status(&conn, &job_id, "failed");

        let reset = reschedule_terminal_job(&conn, "summarize_scope", None, 0).unwrap();
        assert!(reset);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Ready);
    }

    #[test]
    fn test_reschedule_terminal_job_ignores_in_progress() {
        let conn = setup_db();
        let job_id = enq(&conn, make_payload(), priority::NORMAL);
        set_status(&conn, &job_id, "in_progress");

        let reset = reschedule_terminal_job(&conn, "summarize_scope", None, 0).unwrap();
        assert!(!reset, "should not reset in_progress job");

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::InProgress);
    }

    #[test]
    fn test_reschedule_terminal_job_ignores_ready() {
        let conn = setup_db();
        enq(&conn, make_payload(), priority::NORMAL);

        let reset = reschedule_terminal_job(&conn, "summarize_scope", None, 0).unwrap();
        assert!(!reset, "should not reset already-ready job");
    }

    #[test]
    fn test_enqueue_dedup_job_returns_existing_active() {
        let conn = setup_db();
        let input = EnqueueJobInput {
            payload: make_payload(),
            priority: priority::NORMAL,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };

        let (id1, created1) = enqueue_dedup_job(&conn, &input).unwrap();
        assert!(created1);

        let (id2, created2) = enqueue_dedup_job(&conn, &input).unwrap();
        assert!(!created2);
        assert_eq!(id1, id2, "should return existing active job");
    }

    #[test]
    fn test_enqueue_dedup_job_creates_after_terminal() {
        let conn = setup_db();
        let input = EnqueueJobInput {
            payload: make_payload(),
            priority: priority::NORMAL,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };

        let (id1, _) = enqueue_dedup_job(&conn, &input).unwrap();
        set_status(&conn, &id1, "done");

        let (id2, created) = enqueue_dedup_job(&conn, &input).unwrap();
        assert!(created, "should create new job after terminal");
        assert_ne!(id1, id2);
    }
}
