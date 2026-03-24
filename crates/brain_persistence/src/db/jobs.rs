//! Job queue CRUD for async operations (summarization, consolidation, etc.).
//!
//! Jobs are direct CRUD (not event-sourced). The dedup index on `(kind, ref_id)`
//! prevents duplicate active jobs for the same target. The poll index supports
//! efficient `claim_jobs` queries ordered by priority and schedule time.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Result;

/// Priority constants for job scheduling.
pub mod priority {
    pub const CRITICAL: i32 = 0;
    pub const SELF_HEAL: i32 = 50;
    pub const NORMAL: i32 = 100;
    pub const BACKGROUND: i32 = 200;
}

/// A row from the `jobs` table.
#[derive(Debug, Clone)]
pub struct JobRow {
    pub job_id: String,
    pub kind: String,
    pub status: String,
    pub brain_id: String,
    pub ref_id: Option<String>,
    pub ref_kind: Option<String>,
    pub priority: i32,
    pub payload: String,
    pub result: Option<String>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub updated_at: i64,
}

const JOB_COLUMNS: &str = "job_id, kind, status, brain_id, ref_id, ref_kind, priority, \
     payload, result, attempts, max_attempts, last_error, \
     created_at, scheduled_at, started_at, completed_at, updated_at";

fn row_to_job(row: &rusqlite::Row) -> rusqlite::Result<JobRow> {
    Ok(JobRow {
        job_id: row.get(0)?,
        kind: row.get(1)?,
        status: row.get(2)?,
        brain_id: row.get(3)?,
        ref_id: row.get(4)?,
        ref_kind: row.get(5)?,
        priority: row.get(6)?,
        payload: row.get(7)?,
        result: row.get(8)?,
        attempts: row.get(9)?,
        max_attempts: row.get(10)?,
        last_error: row.get(11)?,
        created_at: row.get(12)?,
        scheduled_at: row.get(13)?,
        started_at: row.get(14)?,
        completed_at: row.get(15)?,
        updated_at: row.get(16)?,
    })
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}

/// Parameters for enqueuing a job.
pub struct EnqueueParams<'a> {
    pub kind: &'a str,
    pub brain_id: &'a str,
    pub ref_id: Option<&'a str>,
    pub ref_kind: Option<&'a str>,
    pub priority: i32,
    pub payload: &'a str,
    pub max_attempts: i32,
}

/// Enqueue a job. If a pending/running job already exists for the same
/// `(kind, ref_id)`, upgrades its priority to the minimum of old and new.
/// Returns the job_id (either newly created or existing).
pub fn enqueue_job(conn: &Connection, p: &EnqueueParams) -> Result<String> {
    let job_id = ulid::Ulid::new().to_string();
    let now = now_secs();

    // ON CONFLICT targets the partial unique index idx_jobs_dedup(kind, ref_id)
    // WHERE status IN ('pending', 'running'). On collision, upgrade priority
    // to the minimum (higher urgency wins).
    let changed = conn.execute(
        "INSERT INTO jobs (job_id, kind, brain_id, ref_id, ref_kind, priority, payload,
                           max_attempts, created_at, scheduled_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, ?9)
         ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
         DO UPDATE SET priority = MIN(jobs.priority, excluded.priority),
                       updated_at = excluded.updated_at",
        params![
            job_id,
            p.kind,
            p.brain_id,
            p.ref_id,
            p.ref_kind,
            p.priority,
            p.payload,
            p.max_attempts,
            now
        ],
    )?;

    if changed == 1 {
        // Check if this was an insert or an update by seeing if our job_id exists
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = ?1)",
            [&job_id],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(job_id);
        }
    }

    // If we hit ON CONFLICT, the existing job was updated. Find its ID.
    if let Some(existing_id) = conn
        .query_row(
            "SELECT job_id FROM jobs WHERE kind = ?1 AND ref_id = ?2 AND status IN ('pending', 'running')",
            params![p.kind, p.ref_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    {
        return Ok(existing_id);
    }

    Ok(job_id)
}

/// Atomically claim up to `limit` pending jobs of the given `kind`.
/// Sets status to 'running', increments attempts, records started_at.
pub fn claim_jobs(conn: &Connection, kind: &str, limit: i32) -> Result<Vec<JobRow>> {
    let now = now_secs();

    // Two-step claim: SELECT candidates, then UPDATE. The RETURNING clause
    // doesn't preserve subquery ordering, so we sort after.
    let mut stmt = conn.prepare(&format!(
        "UPDATE jobs SET status = 'running',
                         started_at = ?1,
                         attempts = attempts + 1,
                         updated_at = ?1
         WHERE job_id IN (
             SELECT job_id FROM jobs
             WHERE status = 'pending'
               AND scheduled_at <= ?1
               AND kind = ?2
             ORDER BY priority ASC, scheduled_at ASC
             LIMIT ?3
         )
         RETURNING {JOB_COLUMNS}"
    ))?;

    let mut jobs: Vec<JobRow> = stmt
        .query_map(params![now, kind, limit], row_to_job)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    jobs.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(a.scheduled_at.cmp(&b.scheduled_at))
    });

    Ok(jobs)
}

/// Mark a job as completed with an optional result payload.
pub fn complete_job(conn: &Connection, job_id: &str, result: Option<&str>) -> Result<()> {
    let now = now_secs();
    conn.execute(
        "UPDATE jobs SET status = 'completed', result = ?1, completed_at = ?2, updated_at = ?2
         WHERE job_id = ?3",
        params![result, now, job_id],
    )?;
    Ok(())
}

/// Handle a job failure. If retries remain, reschedule with exponential backoff.
/// If max attempts reached, mark as failed.
pub fn fail_job(conn: &Connection, job_id: &str, error_msg: &str) -> Result<()> {
    let now = now_secs();

    let (attempts, max_attempts): (i32, i32) = conn.query_row(
        "SELECT attempts, max_attempts FROM jobs WHERE job_id = ?1",
        [job_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    if attempts < max_attempts {
        // Exponential backoff: min(30 * 2^(attempts-1), 3600) seconds
        let backoff = std::cmp::min(30_i64 * (1_i64 << (attempts - 1).min(6)), 3600);
        let next_scheduled = now + backoff;

        conn.execute(
            "UPDATE jobs SET status = 'pending',
                             last_error = ?1,
                             scheduled_at = ?2,
                             started_at = NULL,
                             updated_at = ?3
             WHERE job_id = ?4",
            params![error_msg, next_scheduled, now, job_id],
        )?;
    } else {
        conn.execute(
            "UPDATE jobs SET status = 'failed',
                             last_error = ?1,
                             completed_at = ?2,
                             updated_at = ?2
             WHERE job_id = ?3",
            params![error_msg, now, job_id],
        )?;
    }

    Ok(())
}

/// Reap stuck jobs: reset running jobs older than `threshold_secs` back to pending.
/// Returns the number of reaped jobs.
pub fn reap_stuck_jobs(conn: &Connection, threshold_secs: i64) -> Result<usize> {
    let cutoff = now_secs() - threshold_secs;
    let count = conn.execute(
        "UPDATE jobs SET status = 'pending',
                         started_at = NULL,
                         updated_at = ?1
         WHERE status = 'running' AND started_at < ?2",
        params![now_secs(), cutoff],
    )?;
    Ok(count)
}

/// Delete completed and dead jobs older than `age_secs`.
/// Returns the number of deleted jobs.
pub fn gc_completed_jobs(conn: &Connection, age_secs: i64) -> Result<usize> {
    let cutoff = now_secs() - age_secs;
    let count = conn.execute(
        "DELETE FROM jobs WHERE status IN ('completed', 'dead') AND completed_at < ?1",
        [cutoff],
    )?;
    Ok(count)
}

/// Get a single job by ID.
pub fn get_job(conn: &Connection, job_id: &str) -> Result<Option<JobRow>> {
    let row = conn
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE job_id = ?1"),
            [job_id],
            row_to_job,
        )
        .optional()?;
    Ok(row)
}

/// List jobs with optional filters. Returns up to `limit` rows.
pub fn list_jobs(
    conn: &Connection,
    status_filter: Option<&str>,
    brain_id_filter: Option<&str>,
    limit: i32,
) -> Result<Vec<JobRow>> {
    let mut sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE 1=1");
    let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(status) = status_filter {
        param_values.push(Box::new(status.to_string()));
        sql.push_str(&format!(" AND status = ?{}", param_values.len()));
    }
    if let Some(brain_id) = brain_id_filter {
        param_values.push(Box::new(brain_id.to_string()));
        sql.push_str(&format!(" AND brain_id = ?{}", param_values.len()));
    }

    param_values.push(Box::new(limit));
    sql.push_str(&format!(
        " ORDER BY priority ASC, scheduled_at ASC LIMIT ?{}",
        param_values.len()
    ));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        param_values.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let jobs = stmt
        .query_map(param_refs.as_slice(), row_to_job)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(jobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE jobs (
                 job_id        TEXT PRIMARY KEY,
                 kind          TEXT NOT NULL,
                 status        TEXT NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead')),
                 brain_id      TEXT NOT NULL DEFAULT '',
                 ref_id        TEXT,
                 ref_kind      TEXT,
                 priority      INTEGER NOT NULL DEFAULT 100,
                 payload       TEXT NOT NULL DEFAULT '{}',
                 result        TEXT,
                 attempts      INTEGER NOT NULL DEFAULT 0,
                 max_attempts  INTEGER NOT NULL DEFAULT 3,
                 last_error    TEXT,
                 created_at    INTEGER NOT NULL,
                 scheduled_at  INTEGER NOT NULL,
                 started_at    INTEGER,
                 completed_at  INTEGER,
                 updated_at    INTEGER NOT NULL
             );

             CREATE INDEX idx_jobs_poll ON jobs(status, priority, scheduled_at);
             CREATE INDEX idx_jobs_brain_status ON jobs(brain_id, status);
             CREATE UNIQUE INDEX idx_jobs_dedup ON jobs(kind, ref_id)
                 WHERE status IN ('pending', 'running');",
        )
        .unwrap();
        conn
    }

    fn enq(conn: &Connection, kind: &str, ref_id: Option<&str>, prio: i32, max: i32) -> String {
        enqueue_job(
            conn,
            &EnqueueParams {
                kind,
                brain_id: "",
                ref_id,
                ref_kind: None,
                priority: prio,
                payload: "{}",
                max_attempts: max,
            },
        )
        .unwrap()
    }

    #[test]
    fn test_enqueue_creates_pending_job() {
        let conn = setup_db();
        let job_id = enqueue_job(
            &conn,
            &EnqueueParams {
                kind: "summarize_scope",
                brain_id: "brain-1",
                ref_id: Some("scope-1"),
                ref_kind: Some("scope"),
                priority: priority::NORMAL,
                payload: "{}",
                max_attempts: 3,
            },
        )
        .unwrap();

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.kind, "summarize_scope");
        assert_eq!(job.brain_id, "brain-1");
        assert_eq!(job.ref_id.as_deref(), Some("scope-1"));
        assert_eq!(job.priority, priority::NORMAL);
        assert_eq!(job.attempts, 0);
        assert_eq!(job.max_attempts, 3);
    }

    #[test]
    fn test_claim_returns_job_and_sets_running() {
        let conn = setup_db();
        let job_id = enq(&conn, "test_kind", Some("ref-1"), priority::NORMAL, 3);

        let claimed = claim_jobs(&conn, "test_kind", 10).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].job_id, job_id);
        assert_eq!(claimed[0].status, "running");
        assert_eq!(claimed[0].attempts, 1);
        assert!(claimed[0].started_at.is_some());
    }

    #[test]
    fn test_claim_respects_priority_ordering() {
        let conn = setup_db();
        let now = now_secs();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, ref_id, created_at, scheduled_at, updated_at)
             VALUES ('J-LOW', 'test', 'pending', 200, 'r1', ?1, ?1, ?1)",
            [now],
        ).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, ref_id, created_at, scheduled_at, updated_at)
             VALUES ('J-HIGH', 'test', 'pending', 0, 'r2', ?1, ?1, ?1)",
            [now],
        ).unwrap();

        let claimed = claim_jobs(&conn, "test", 10).unwrap();
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].job_id, "J-HIGH");
        assert_eq!(claimed[1].job_id, "J-LOW");
    }

    #[test]
    fn test_claim_skips_future_scheduled() {
        let conn = setup_db();
        let now = now_secs();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, priority, created_at, scheduled_at, updated_at)
             VALUES ('J-FUTURE', 'test', 'pending', 100, ?1, ?2, ?1)",
            params![now, now + 3600],
        ).unwrap();

        let claimed = claim_jobs(&conn, "test", 10).unwrap();
        assert!(claimed.is_empty());
    }

    #[test]
    fn test_complete_sets_status() {
        let conn = setup_db();
        let job_id = enq(&conn, "test", Some("r1"), 100, 3);
        claim_jobs(&conn, "test", 1).unwrap();

        complete_job(&conn, &job_id, Some("{\"summary\":\"done\"}")).unwrap();

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, "completed");
        assert_eq!(job.result.as_deref(), Some("{\"summary\":\"done\"}"));
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_fail_with_retries_reschedules() {
        let conn = setup_db();
        let job_id = enq(&conn, "test", Some("r1"), 100, 3);
        claim_jobs(&conn, "test", 1).unwrap();

        let before = now_secs();
        fail_job(&conn, &job_id, "timeout").unwrap();

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.last_error.as_deref(), Some("timeout"));
        assert!(job.started_at.is_none());
        assert!(job.scheduled_at >= before + 30);
    }

    #[test]
    fn test_fail_exhausted_retries_marks_failed() {
        let conn = setup_db();
        let job_id = enq(&conn, "test", Some("r1"), 100, 1);
        claim_jobs(&conn, "test", 1).unwrap();

        fail_job(&conn, &job_id, "fatal error").unwrap();

        let job = get_job(&conn, &job_id).unwrap().unwrap();
        assert_eq!(job.status, "failed");
        assert_eq!(job.last_error.as_deref(), Some("fatal error"));
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_dedup_upgrades_priority() {
        let conn = setup_db();
        let id1 = enq(&conn, "test", Some("ref-1"), priority::BACKGROUND, 3);
        let id2 = enq(&conn, "test", Some("ref-1"), priority::CRITICAL, 3);

        assert_eq!(id1, id2);
        let job = get_job(&conn, &id1).unwrap().unwrap();
        assert_eq!(job.priority, priority::CRITICAL);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_reap_stuck_jobs() {
        let conn = setup_db();
        let old = now_secs() - 600;
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, started_at, created_at, scheduled_at, updated_at)
             VALUES ('J-STUCK', 'test', 'running', ?1, ?1, ?1, ?1)",
            [old],
        ).unwrap();

        let reaped = reap_stuck_jobs(&conn, 300).unwrap();
        assert_eq!(reaped, 1);

        let job = get_job(&conn, "J-STUCK").unwrap().unwrap();
        assert_eq!(job.status, "pending");
        assert!(job.started_at.is_none());
    }

    #[test]
    fn test_gc_completed_jobs() {
        let conn = setup_db();
        let now = now_secs();
        let old = now - 86400 * 2;
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, completed_at, created_at, scheduled_at, updated_at)
             VALUES ('J-OLD', 'test', 'completed', ?1, ?1, ?1, ?1)",
            [old],
        ).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, completed_at, created_at, scheduled_at, updated_at)
             VALUES ('J-RECENT', 'test', 'completed', ?1, ?1, ?1, ?1)",
            [now],
        ).unwrap();

        let deleted = gc_completed_jobs(&conn, 86400).unwrap();
        assert_eq!(deleted, 1);
        assert!(get_job(&conn, "J-OLD").unwrap().is_none());
        assert!(get_job(&conn, "J-RECENT").unwrap().is_some());
    }

    #[test]
    fn test_list_jobs_with_filters() {
        let conn = setup_db();
        enqueue_job(
            &conn,
            &EnqueueParams {
                kind: "kind_a",
                brain_id: "brain-1",
                ref_id: Some("r1"),
                ref_kind: None,
                priority: 100,
                payload: "{}",
                max_attempts: 3,
            },
        )
        .unwrap();
        enqueue_job(
            &conn,
            &EnqueueParams {
                kind: "kind_b",
                brain_id: "brain-2",
                ref_id: Some("r2"),
                ref_kind: None,
                priority: 100,
                payload: "{}",
                max_attempts: 3,
            },
        )
        .unwrap();

        let all = list_jobs(&conn, None, None, 50).unwrap();
        assert_eq!(all.len(), 2);

        let b1 = list_jobs(&conn, None, Some("brain-1"), 50).unwrap();
        assert_eq!(b1.len(), 1);
        assert_eq!(b1[0].brain_id, "brain-1");

        let pending = list_jobs(&conn, Some("pending"), None, 50).unwrap();
        assert_eq!(pending.len(), 2);

        let running = list_jobs(&conn, Some("running"), None, 50).unwrap();
        assert!(running.is_empty());
    }
}
