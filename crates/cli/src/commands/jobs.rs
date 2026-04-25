use std::path::Path;

use anyhow::Result;
use brain_lib::ports::JobQueue;
use brain_lib::stores::BrainStores;
use brain_persistence::db::job::JobStatus;

pub fn run_status(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;

    let pending = stores.count_jobs_by_status(&JobStatus::Pending)?;
    let running = stores.count_jobs_by_status(&JobStatus::InProgress)?;
    let done = stores.count_jobs_by_status(&JobStatus::Done)?;
    let failed = stores.count_jobs_by_status(&JobStatus::Failed)?;
    let ready = stores.count_jobs_by_status(&JobStatus::Ready)?;

    let recent_jobs = stores.list_jobs_by_status(&JobStatus::Failed, 10)?;
    let stuck_jobs = stores.list_stuck_jobs()?;

    if json {
        let recent_failures_json: Vec<serde_json::Value> = recent_jobs
            .iter()
            .map(|j| {
                serde_json::json!({
                    "job_id": j.job_id,
                    "kind": j.kind(),
                    "ref_id": j.payload.ref_id(),
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

        if !recent_jobs.is_empty() {
            println!("Recent Failures");
            for job in &recent_jobs {
                println!(
                    "  [{:>3} attempts] {} — {}: {}",
                    job.attempts,
                    job.job_id,
                    job.kind(),
                    job.payload.ref_id()
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

        if recent_jobs.is_empty() && stuck_jobs.is_empty() {
            println!("No recent failures or stuck jobs.");
        }
    }

    Ok(())
}

pub fn run_retry(sqlite_db: &Path, lance_db: Option<&Path>, job_id: &str) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;

    let job = stores
        .get_job(job_id)
        .map_err(|e| anyhow::anyhow!("failed to get job: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("job not found: {job_id}"))?;

    if job.status != JobStatus::Failed {
        anyhow::bail!(
            "job {} is in '{}' state — only failed jobs can be retried",
            job_id,
            job.status
        );
    }

    let reset = stores.retry_failed_job(job_id)?;
    if !reset {
        anyhow::bail!("job {job_id} was not reset; it may have changed state concurrently");
    }

    println!("Retried job {job_id} — it has been reset to pending.");

    Ok(())
}

pub fn run_gc(sqlite_db: &Path, lance_db: Option<&Path>, older_than_days: u32) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;

    let age_secs = older_than_days as i64 * 86400;
    let protected = brain_lib::pipeline::recurring_jobs::protected_kinds();
    let deleted = stores.gc_completed_jobs(age_secs, &protected)?;

    println!(
        "Deleted {deleted} completed job{} older than {} day{}.",
        if deleted == 1 { "" } else { "s" },
        older_than_days,
        if older_than_days == 1 { "" } else { "s" }
    );

    Ok(())
}
