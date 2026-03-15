//! Generic background job queue backed by the `jobs` SQLite table.
//!
//! Jobs are enqueued with a `kind` + optional `ref_id`, claimed in priority
//! order by the daemon poll loop, and marked completed/failed on finish.
//! A partial unique index prevents duplicate active jobs for the same object.

use rusqlite::Connection;
use tracing::warn;
use ulid::Ulid;

use super::Db;
use crate::error::Result;

/// A claimed job ready for processing.
#[derive(Debug, Clone)]
pub struct Job {
    pub job_id: String,
    pub kind: String,
    pub brain_id: String,
    pub ref_id: Option<String>,
    pub ref_kind: Option<String>,
    pub payload: String,
    pub attempts: i32,
}

/// Priority levels for jobs. Lower = higher priority.
pub mod priority {
    /// Critical / self-heal work.
    pub const CRITICAL: i32 = 0;
    /// Self-heal re-embed (above normal, below critical).
    pub const SELF_HEAL: i32 = 50;
    /// Normal embedding work.
    pub const NORMAL: i32 = 100;
    /// Background maintenance.
    pub const BACKGROUND: i32 = 200;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn generate_job_id() -> String {
    Ulid::new().to_string()
}

// ── Enqueue ──────────────────────────────────────────────────────────────────

/// Enqueue a job, deduplicating against active (pending/running) jobs.
///
/// If a job with the same `(kind, ref_id)` is already pending or running,
/// the priority is upgraded to the minimum of the two.  Returns the job_id
/// of the new or updated job.
pub fn enqueue_job(
    conn: &Connection,
    kind: &str,
    brain_id: &str,
    ref_id: Option<&str>,
    ref_kind: Option<&str>,
    priority: i32,
    payload: &str,
) -> Result<String> {
    let job_id = generate_job_id();
    let now = now_ts();

    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority, payload,
                           created_at, scheduled_at, updated_at)
         VALUES (?1, ?2, 'pending', ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8)
         ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
         DO UPDATE SET priority = MIN(jobs.priority, excluded.priority),
                       updated_at = excluded.updated_at",
        rusqlite::params![job_id, kind, brain_id, ref_id, ref_kind, priority, payload, now],
    )?;

    Ok(job_id)
}

/// Bulk-enqueue embed jobs for all tasks in a brain (used by self-heal).
///
/// Uses `INSERT ... SELECT ... ON CONFLICT DO NOTHING` so existing active
/// jobs are not duplicated.
pub fn enqueue_embed_tasks_for_brain(conn: &Connection, brain_id: &str) -> Result<usize> {
    let now = now_ts();
    let count = if brain_id.is_empty() {
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority,
                               created_at, scheduled_at, updated_at)
             SELECT 'job-' || hex(randomblob(8)), 'embed_task', 'pending', brain_id,
                    task_id, 'task', ?1, ?2, ?2, ?2
             FROM tasks
             ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
             DO NOTHING",
            rusqlite::params![priority::SELF_HEAL, now],
        )?
    } else {
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority,
                               created_at, scheduled_at, updated_at)
             SELECT 'job-' || hex(randomblob(8)), 'embed_task', 'pending', brain_id,
                    task_id, 'task', ?1, ?2, ?2, ?2
             FROM tasks
             WHERE brain_id = ?3
             ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
             DO NOTHING",
            rusqlite::params![priority::SELF_HEAL, now, brain_id],
        )?
    };
    Ok(count)
}

/// Bulk-enqueue embed jobs for all chunks (used by self-heal).
pub fn enqueue_embed_chunks_all(conn: &Connection) -> Result<usize> {
    let now = now_ts();
    let count = conn.execute(
        "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, priority,
                           created_at, scheduled_at, updated_at)
         SELECT 'job-' || hex(randomblob(8)), 'embed_chunk', 'pending', '',
                chunk_id, 'chunk', ?1, ?2, ?2, ?2
         FROM chunks
         ON CONFLICT (kind, ref_id) WHERE status IN ('pending', 'running')
         DO NOTHING",
        rusqlite::params![priority::SELF_HEAL, now],
    )?;
    Ok(count)
}

// ── Claim ────────────────────────────────────────────────────────────────────

/// Claim up to `limit` pending jobs of a given `kind`, optionally scoped to
/// a `brain_id`. Returns the claimed jobs with status set to `running`.
pub fn claim_jobs(
    conn: &Connection,
    kind: &str,
    brain_id: Option<&str>,
    limit: usize,
) -> Result<Vec<Job>> {
    let now = now_ts();

    let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = match brain_id {
        Some(bid) => (
            "UPDATE jobs
             SET status = 'running', started_at = ?1, attempts = attempts + 1, updated_at = ?1
             WHERE job_id IN (
                 SELECT job_id FROM jobs
                 WHERE status = 'pending' AND scheduled_at <= ?1 AND kind = ?2 AND brain_id = ?3
                 ORDER BY priority ASC, scheduled_at ASC
                 LIMIT ?4
             )
             RETURNING job_id, kind, brain_id, ref_id, ref_kind, payload, attempts",
            vec![
                Box::new(now),
                Box::new(kind.to_string()),
                Box::new(bid.to_string()),
                Box::new(limit as i64),
            ],
        ),
        None => (
            "UPDATE jobs
             SET status = 'running', started_at = ?1, attempts = attempts + 1, updated_at = ?1
             WHERE job_id IN (
                 SELECT job_id FROM jobs
                 WHERE status = 'pending' AND scheduled_at <= ?1 AND kind = ?2
                 ORDER BY priority ASC, scheduled_at ASC
                 LIMIT ?3
             )
             RETURNING job_id, kind, brain_id, ref_id, ref_kind, payload, attempts",
            vec![
                Box::new(now),
                Box::new(kind.to_string()),
                Box::new(limit as i64),
            ],
        ),
    };

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(Job {
            job_id: row.get(0)?,
            kind: row.get(1)?,
            brain_id: row.get(2)?,
            ref_id: row.get(3)?,
            ref_kind: row.get(4)?,
            payload: row.get(5)?,
            attempts: row.get(6)?,
        })
    })?;

    super::collect_rows(rows)
}

// ── Complete / Fail ──────────────────────────────────────────────────────────

/// Mark a job as successfully completed.
pub fn complete_job(conn: &Connection, job_id: &str) -> Result<()> {
    let now = now_ts();
    conn.execute(
        "UPDATE jobs SET status = 'completed', completed_at = ?1, last_error = NULL, updated_at = ?1
         WHERE job_id = ?2",
        rusqlite::params![now, job_id],
    )?;
    Ok(())
}

/// Mark a job as failed. If retries remain and `permanent` is false, reschedule
/// it as pending with exponential backoff. Otherwise mark it as `failed`.
pub fn fail_job(conn: &Connection, job_id: &str, error: &str, permanent: bool) -> Result<()> {
    let now = now_ts();

    if permanent {
        conn.execute(
            "UPDATE jobs SET status = 'failed', last_error = ?1, completed_at = ?2, updated_at = ?2
             WHERE job_id = ?3",
            rusqlite::params![error, now, job_id],
        )?;
        return Ok(());
    }

    // Check current attempts vs max_attempts
    let (attempts, max_attempts): (i32, i32) = conn.query_row(
        "SELECT attempts, max_attempts FROM jobs WHERE job_id = ?1",
        [job_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    if attempts >= max_attempts {
        conn.execute(
            "UPDATE jobs SET status = 'failed', last_error = ?1, completed_at = ?2, updated_at = ?2
             WHERE job_id = ?3",
            rusqlite::params![error, now, job_id],
        )?;
    } else {
        // Backoff: min(30 * 2^(attempts-1), 3600)
        let backoff = std::cmp::min(30 * (1i64 << (attempts - 1).max(0)), 3600);
        let scheduled_at = now + backoff;
        conn.execute(
            "UPDATE jobs SET status = 'pending', last_error = ?1, scheduled_at = ?2, updated_at = ?3
             WHERE job_id = ?4",
            rusqlite::params![error, scheduled_at, now, job_id],
        )?;
    }

    Ok(())
}

/// Mark a batch of jobs as completed in a single statement.
pub fn complete_jobs_batch(conn: &Connection, job_ids: &[&str]) -> Result<()> {
    if job_ids.is_empty() {
        return Ok(());
    }
    let now = now_ts();
    let placeholders: Vec<String> = (2..=job_ids.len() + 1).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "UPDATE jobs SET status = 'completed', completed_at = ?1, last_error = NULL, updated_at = ?1
         WHERE job_id IN ({})",
        placeholders.join(", ")
    );
    let mut params: Vec<&dyn rusqlite::types::ToSql> = Vec::with_capacity(job_ids.len() + 1);
    params.push(&now);
    for id in job_ids {
        params.push(id as &dyn rusqlite::types::ToSql);
    }
    conn.execute(&sql, params.as_slice())?;
    Ok(())
}

// ── Reaper ───────────────────────────────────────────────────────────────────

/// Reset jobs stuck in `running` for longer than `timeout_secs` back to
/// `pending` for retry. This handles daemon crashes.
pub fn reap_stuck_jobs(conn: &Connection, timeout_secs: i64) -> Result<usize> {
    let now = now_ts();
    let cutoff = now - timeout_secs;
    let count = conn.execute(
        "UPDATE jobs SET status = 'pending', scheduled_at = ?1, updated_at = ?1
         WHERE status = 'running' AND started_at < ?2",
        rusqlite::params![now, cutoff],
    )?;
    if count > 0 {
        warn!(count, "reaped stuck jobs back to pending");
    }
    Ok(count)
}

// ── GC ───────────────────────────────────────────────────────────────────────

/// Delete completed and dead jobs older than `retention_secs`.
pub fn gc_old_jobs(conn: &Connection, retention_secs: i64) -> Result<usize> {
    let cutoff = now_ts() - retention_secs;
    let count = conn.execute(
        "DELETE FROM jobs WHERE status IN ('completed', 'dead') AND completed_at < ?1",
        [cutoff],
    )?;
    Ok(count)
}

// ── Observability ────────────────────────────────────────────────────────────

/// Count pending jobs grouped by kind.
pub fn pending_job_counts(conn: &Connection) -> Result<Vec<(String, i64)>> {
    let mut stmt =
        conn.prepare("SELECT kind, COUNT(*) FROM jobs WHERE status = 'pending' GROUP BY kind")?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    super::collect_rows(rows)
}

// ── Db convenience wrappers ──────────────────────────────────────────────────

impl Db {
    /// Enqueue a job via the write connection.
    pub fn enqueue_job(
        &self,
        kind: &str,
        brain_id: &str,
        ref_id: Option<&str>,
        ref_kind: Option<&str>,
        priority: i32,
    ) -> Result<String> {
        self.with_write_conn(|conn| enqueue_job(conn, kind, brain_id, ref_id, ref_kind, priority, "{}"))
    }

    /// Claim a batch of jobs via the write connection.
    pub fn claim_jobs(
        &self,
        kind: &str,
        brain_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Job>> {
        self.with_write_conn(|conn| claim_jobs(conn, kind, brain_id, limit))
    }

    /// Mark a batch of jobs as completed via the write connection.
    pub fn complete_jobs(&self, job_ids: &[&str]) -> Result<()> {
        self.with_write_conn(|conn| complete_jobs_batch(conn, job_ids))
    }

    /// Fail a job via the write connection.
    pub fn fail_job(&self, job_id: &str, error: &str, permanent: bool) -> Result<()> {
        self.with_write_conn(|conn| fail_job(conn, job_id, error, permanent))
    }

    /// Reap stuck jobs via the write connection.
    pub fn reap_stuck_jobs(&self, timeout_secs: i64) -> Result<usize> {
        self.with_write_conn(|conn| reap_stuck_jobs(conn, timeout_secs))
    }

    /// GC old completed/dead jobs.
    pub fn gc_old_jobs(&self, retention_secs: i64) -> Result<usize> {
        self.with_write_conn(|conn| gc_old_jobs(conn, retention_secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Db {
        Db::open_in_memory().unwrap()
    }

    #[test]
    fn test_enqueue_and_claim() {
        let db = setup_db();

        // Enqueue a job
        let job_id = db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        assert!(job_id.starts_with("job-"));

        // Claim it
        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].ref_id.as_deref(), Some("t1"));
        assert_eq!(jobs[0].kind, "embed_task");
        assert_eq!(jobs[0].attempts, 1);

        // Claiming again should return nothing
        let jobs2 = db.claim_jobs("embed_task", None, 10).unwrap();
        assert!(jobs2.is_empty());
    }

    #[test]
    fn test_dedup_upgrades_priority() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        // Enqueue again with higher priority
        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::SELF_HEAL).unwrap();

        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        assert_eq!(jobs.len(), 1);

        // Verify priority was upgraded by checking we get it at all (it was MIN'd)
        // We can check the DB directly
        db.with_read_conn(|conn| {
            let prio: i32 = conn.query_row(
                "SELECT priority FROM jobs WHERE ref_id = 't1'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(prio, priority::SELF_HEAL);
            Ok(())
        }).unwrap();
    }

    #[test]
    fn test_complete_job() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        assert_eq!(jobs.len(), 1);

        db.complete_jobs(&[jobs[0].job_id.as_str()]).unwrap();

        // Job should be completed, not claimable
        let jobs2 = db.claim_jobs("embed_task", None, 10).unwrap();
        assert!(jobs2.is_empty());

        // New job for same ref_id should be allowed now
        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        let jobs3 = db.claim_jobs("embed_task", None, 10).unwrap();
        assert_eq!(jobs3.len(), 1);
    }

    #[test]
    fn test_fail_job_retries() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        let job_id = &jobs[0].job_id;

        // Fail with retry (not permanent, attempt 1 of 3)
        db.fail_job(job_id, "transient error", false).unwrap();

        // Job should be back to pending (but scheduled in the future)
        db.with_read_conn(|conn| {
            let status: String = conn.query_row(
                "SELECT status FROM jobs WHERE job_id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )?;
            assert_eq!(status, "pending");
            Ok(())
        }).unwrap();
    }

    #[test]
    fn test_fail_job_permanent() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        let job_id = &jobs[0].job_id;

        db.fail_job(job_id, "permanent error", true).unwrap();

        db.with_read_conn(|conn| {
            let status: String = conn.query_row(
                "SELECT status FROM jobs WHERE job_id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )?;
            assert_eq!(status, "failed");
            Ok(())
        }).unwrap();
    }

    #[test]
    fn test_fail_job_exhausts_retries() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();

        // Exhaust all 3 attempts
        for i in 0..3 {
            // Need to make the job claimable (set scheduled_at to past)
            if i > 0 {
                db.with_write_conn(|conn| {
                    conn.execute(
                        "UPDATE jobs SET scheduled_at = 0 WHERE ref_id = 't1' AND status = 'pending'",
                        [],
                    )?;
                    Ok(())
                }).unwrap();
            }

            let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
            if jobs.is_empty() {
                break;
            }
            db.fail_job(&jobs[0].job_id, "transient error", false).unwrap();
        }

        db.with_read_conn(|conn| {
            let status: String = conn.query_row(
                "SELECT status FROM jobs WHERE ref_id = 't1'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(status, "failed");
            Ok(())
        }).unwrap();
    }

    #[test]
    fn test_reap_stuck_jobs() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        assert_eq!(jobs.len(), 1);

        // Simulate a stuck job by setting started_at far in the past
        db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE jobs SET started_at = 0 WHERE job_id = ?1",
                [jobs[0].job_id.as_str()],
            )?;
            Ok(())
        }).unwrap();

        let reaped = db.reap_stuck_jobs(300).unwrap();
        assert_eq!(reaped, 1);

        // Job should be claimable again
        // But scheduled_at was set to now by reaper, so we need to adjust
        db.with_write_conn(|conn| {
            conn.execute("UPDATE jobs SET scheduled_at = 0 WHERE ref_id = 't1'", [])?;
            Ok(())
        }).unwrap();

        let jobs2 = db.claim_jobs("embed_task", None, 10).unwrap();
        assert_eq!(jobs2.len(), 1);
    }

    #[test]
    fn test_gc_old_jobs() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        let jobs = db.claim_jobs("embed_task", None, 10).unwrap();
        db.complete_jobs(&[jobs[0].job_id.as_str()]).unwrap();

        // Set completed_at far in the past
        db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE jobs SET completed_at = 0 WHERE job_id = ?1",
                [jobs[0].job_id.as_str()],
            )?;
            Ok(())
        }).unwrap();

        let deleted = db.gc_old_jobs(86400 * 7).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_claim_with_brain_id_filter() {
        let db = setup_db();

        db.enqueue_job("embed_task", "brain-a", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        db.enqueue_job("embed_task", "brain-b", Some("t2"), Some("task"), priority::NORMAL).unwrap();

        // Claim only brain-a jobs
        let jobs = db.claim_jobs("embed_task", Some("brain-a"), 10).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].ref_id.as_deref(), Some("t1"));

        // brain-b job still available
        let jobs2 = db.claim_jobs("embed_task", Some("brain-b"), 10).unwrap();
        assert_eq!(jobs2.len(), 1);
        assert_eq!(jobs2[0].ref_id.as_deref(), Some("t2"));
    }

    #[test]
    fn test_priority_ordering() {
        let db = setup_db();

        // Enqueue low-priority first, then high-priority
        db.enqueue_job("embed_task", "", Some("t-low"), Some("task"), priority::BACKGROUND).unwrap();
        db.enqueue_job("embed_task", "", Some("t-high"), Some("task"), priority::CRITICAL).unwrap();
        db.enqueue_job("embed_task", "", Some("t-normal"), Some("task"), priority::NORMAL).unwrap();

        // Claim one at a time — should get highest priority first
        let jobs = db.claim_jobs("embed_task", None, 1).unwrap();
        assert_eq!(jobs[0].ref_id.as_deref(), Some("t-high"));

        let jobs = db.claim_jobs("embed_task", None, 1).unwrap();
        assert_eq!(jobs[0].ref_id.as_deref(), Some("t-normal"));

        let jobs = db.claim_jobs("embed_task", None, 1).unwrap();
        assert_eq!(jobs[0].ref_id.as_deref(), Some("t-low"));
    }

    #[test]
    fn test_pending_job_counts() {
        let db = setup_db();

        db.enqueue_job("embed_task", "", Some("t1"), Some("task"), priority::NORMAL).unwrap();
        db.enqueue_job("embed_task", "", Some("t2"), Some("task"), priority::NORMAL).unwrap();
        db.enqueue_job("embed_chunk", "", Some("c1"), Some("chunk"), priority::NORMAL).unwrap();

        let counts = db.with_read_conn(|conn| pending_job_counts(conn)).unwrap();
        let map: std::collections::HashMap<String, i64> = counts.into_iter().collect();
        assert_eq!(map.get("embed_task"), Some(&2));
        assert_eq!(map.get("embed_chunk"), Some(&1));
    }
}
