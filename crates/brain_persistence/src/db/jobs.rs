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
    let status: JobStatus = status_str.parse().unwrap_or(JobStatus::Ready);

    let payload_str: String = row.get(4)?;
    let payload: JobPayload = serde_json::from_str(&payload_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let retry_config_str: String = row.get(5)?;
    let retry_config: RetryStrategy =
        serde_json::from_str(&retry_config_str).unwrap_or(RetryStrategy::NoRetry);

    let metadata_str: String = row.get(10)?;
    let metadata: serde_json::Value =
        serde_json::from_str(&metadata_str).unwrap_or_else(|_| serde_json::json!({}));

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
         WHERE job_id = ?3",
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
             WHERE job_id = ?3",
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
             WHERE job_id = ?4",
            params![error_msg, next_scheduled, now, job_id],
        )?;
    }

    Ok(())
}

// ─── Maintenance ─────────────────────────────────────────────────

/// Reap stuck jobs: reset in_progress/pending jobs that exceeded their
/// `stuck_threshold_secs` back to `ready`. Only reaps retryable jobs.
pub fn reap_stuck_jobs(conn: &Connection) -> Result<usize> {
    let now = now_secs();

    let r1 = conn.execute(
        "UPDATE jobs SET status = 'ready',
                          started_at = NULL,
                          updated_at = ?1
         WHERE status = 'in_progress'
           AND processed_at IS NULL
           AND started_at < ?1 - stuck_threshold_secs
           AND retry_config != '{\"type\":\"noRetry\"}'",
        params![now],
    )?;

    let r2 = conn.execute(
        "UPDATE jobs SET status = 'ready',
                          updated_at = ?1
         WHERE status = 'pending'
           AND started_at IS NULL
           AND updated_at < ?1 - stuck_threshold_secs
           AND retry_config != '{\"type\":\"noRetry\"}'",
        params![now],
    )?;

    Ok(r1 + r2)
}

/// Delete old completed jobs older than `age_secs`.
pub fn gc_completed_jobs(conn: &Connection, age_secs: i64) -> Result<usize> {
    let cutoff = now_secs() - age_secs;
    let count = conn.execute(
        "DELETE FROM jobs WHERE status = 'done' AND processed_at < ?1",
        [cutoff],
    )?;
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
                 retry_config        TEXT NOT NULL DEFAULT '{\"type\":\"noRetry\"}',
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
            scope_type: "directory".into(),
            scope_value: "low/".into(),
            content: "".into(),
        })
        .unwrap();
        let payload_high = serde_json::to_string(&JobPayload::SummarizeScope {
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
                     '{\"type\":\"noRetry\"}', '{}', 1000, 1000, 1000)",
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
                     '{\"type\":\"noRetry\"}', '{}', ?1, ?1, ?1)",
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

        let deleted = gc_completed_jobs(&conn, 86400).unwrap();
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
                episodes: "ep1".into(),
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
            scope_type: "tag".into(),
            scope_value: "rust".into(),
            content: "fn hello() {}".into(),
        };
        let job_id = enq(&conn, payload, priority::NORMAL);

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        match &job.payload {
            JobPayload::SummarizeScope {
                scope_type,
                scope_value,
                content,
            } => {
                assert_eq!(scope_type, "tag");
                assert_eq!(scope_value, "rust");
                assert_eq!(content, "fn hello() {}");
            }
            _ => panic!("unexpected payload variant"),
        }
    }
}
