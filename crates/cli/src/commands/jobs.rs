use std::path::Path;

use anyhow::Result;
use brain_lib::db::job::JobStatus;
use brain_lib::ports::JobQueue;
use brain_lib::stores::BrainStores;

pub fn run_status(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;
    let db = stores.db();

    let pending = db.count_jobs_by_status(&JobStatus::Pending)?;
    let running = db.count_jobs_by_status(&JobStatus::InProgress)?;
    let done = db.count_jobs_by_status(&JobStatus::Done)?;
    let failed = db.count_jobs_by_status(&JobStatus::Failed)?;
    let ready = db.count_jobs_by_status(&JobStatus::Ready)?;

    let recent_failures = db.list_jobs_by_status(&JobStatus::Failed, 10)?;
    let stuck_jobs = db.list_stuck_jobs()?;

    if json {
        let recent_failures_json: Vec<serde_json::Value> = recent_failures
            .iter()
            .map(|j| {
                let ref_id = match &j.payload {
                    brain_lib::db::job::JobPayload::SummarizeScope { summary_id, .. } => {
                        summary_id.clone()
                    }
                    brain_lib::db::job::JobPayload::ConsolidateCluster {
                        suggested_title, ..
                    } => suggested_title.clone(),
                };
                serde_json::json!({
                    "job_id": j.job_id,
                    "kind": j.kind(),
                    "ref_id": ref_id,
                    "attempts": j.attempts,
                    "last_error": j.last_error,
                    "updated_at": j.updated_at,
                })
            })
            .collect();

        let stuck_jobs_json: Vec<serde_json::Value> = stuck_jobs
            .iter()
            .map(|j| {
                serde_json::json!({
                    "job_id": j.job_id,
                    "kind": j.kind(),
                    "started_at": j.started_at,
                })
            })
            .collect();

        let output = serde_json::json!({
            "counts": {
                "pending": pending,
                "running": running,
                "completed": done,
                "failed": failed,
                "ready": ready,
            },
            "recent_failures": recent_failures_json,
            "stuck_jobs": stuck_jobs_json,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("Job Queue");
        println!("  pending:   {pending}");
        println!("  running:   {running}");
        println!("  ready:     {ready}");
        println!("  completed: {done}");
        println!("  failed:    {failed}");
        println!();

        if !recent_failures.is_empty() {
            println!("Recent Failures");
            for job in &recent_failures {
                let ref_id = match &job.payload {
                    brain_lib::db::job::JobPayload::SummarizeScope { summary_id, .. } => {
                        summary_id.as_str()
                    }
                    brain_lib::db::job::JobPayload::ConsolidateCluster {
                        suggested_title, ..
                    } => suggested_title.as_str(),
                };
                println!(
                    "  [{:>3} attempts] {} — {}: {}",
                    job.attempts,
                    job.job_id,
                    job.kind(),
                    ref_id
                );
                if let Some(ref err) = job.last_error {
                    println!("    Error: {}", err);
                }
            }
            println!();
        }

        if !stuck_jobs.is_empty() {
            println!("Stuck Jobs");
            for job in &stuck_jobs {
                println!(
                    "  {} — {} (started {:?})",
                    job.job_id,
                    job.kind(),
                    job.started_at
                );
            }
            println!();
        }

        if recent_failures.is_empty() && stuck_jobs.is_empty() {
            println!("No recent failures or stuck jobs.");
        }
    }

    Ok(())
}

pub fn run_retry(sqlite_db: &Path, lance_db: Option<&Path>, job_id: &str) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;
    let db = stores.db();

    let job = db
        .with_read_conn(|conn| brain_lib::db::jobs::get_job(conn, job_id))
        .map_err(|e| anyhow::anyhow!("failed to get job: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("job not found: {job_id}"))?;

    if job.status == JobStatus::Ready || job.status == JobStatus::Pending {
        anyhow::bail!(
            "job {} is already in {} state — no retry needed",
            job_id,
            job.status
        );
    }

    db.with_write_conn(|conn| {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs() as i64;
        conn.execute(
            "UPDATE jobs SET status = 'ready', last_error = NULL, started_at = NULL, \
             scheduled_at = ?1, updated_at = ?1 WHERE job_id = ?2",
            rusqlite::params![now, job_id],
        )
        .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))?;
        Ok(())
    })?;

    println!("Retried job {job_id} — it has been reset to pending.");

    Ok(())
}

pub fn run_gc(sqlite_db: &Path, lance_db: Option<&Path>, older_than_days: u32) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;
    let db = stores.db();

    let age_secs = older_than_days as i64 * 86400;
    let deleted = db.gc_completed_jobs(age_secs)?;

    println!(
        "Deleted {deleted} completed job{} older than {} day{}.",
        if deleted == 1 { "" } else { "s" },
        older_than_days,
        if older_than_days == 1 { "" } else { "s" }
    );

    Ok(())
}
