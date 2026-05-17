use std::path::Path;

use anyhow::Result;
use brain_lib::stores::BrainStores;

use crate::commands::rpc_client;

fn run_remote(json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let report = client
        .brain_status()
        .map_err(|e| anyhow::anyhow!("BrainStatus rpc failed: {e}"))?;

    if json {
        let output = serde_json::json!({
            "brain_name": report.brain_name,
            "brain_id": report.brain_id,
            "tasks": {
                "open": report.tasks_open,
                "in_progress": report.tasks_in_progress,
                "blocked": report.tasks_blocked,
                "done": report.tasks_done,
            },
            "index": {
                "stuck_files": report.stuck_files,
                "stale_hashes_prevented": report.stale_hashes_prevented,
            },
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("Brain:  {} [{}]", report.brain_name, report.brain_id);
        println!();
        println!("Tasks");
        println!("  open:        {}", report.tasks_open);
        println!("  in_progress: {}", report.tasks_in_progress);
        println!("  blocked:     {}", report.tasks_blocked);
        println!("  done:        {}", report.tasks_done);
        println!();
        println!("Index");
        println!("  stuck_files:           {}", report.stuck_files);
        println!("  stale_hashes_prevented:{}", report.stale_hashes_prevented);
    }

    Ok(())
}

/// Show brain health status: task counts, index stats, brain ID.
pub fn run(sqlite_db: &Path, lance_db: Option<&Path>, json: bool, remote: bool) -> Result<()> {
    if remote {
        return run_remote(json);
    }
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;

    let brain_name = stores.brain_name.clone();
    let brain_id = stores.brain_id.clone();

    // Task counts — each list call is cheap (indexed query).
    let open = stores.tasks.list_open()?.len();
    let in_progress = stores.tasks.list_in_progress()?.len();
    let blocked = stores.tasks.list_blocked()?.len();
    let done = stores.tasks.list_done()?.len();

    // Index stats from SQLite.
    let stuck_files = stores.count_stuck_files()?;
    let stale_hashes_prevented = stores.stale_hashes_prevented()?;

    if json {
        let output = serde_json::json!({
            "brain_name": brain_name,
            "brain_id": brain_id,
            "tasks": {
                "open": open,
                "in_progress": in_progress,
                "blocked": blocked,
                "done": done,
            },
            "index": {
                "stuck_files": stuck_files,
                "stale_hashes_prevented": stale_hashes_prevented,
            },
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("Brain:  {} [{}]", brain_name, brain_id);
        println!();
        println!("Tasks");
        println!("  open:        {open}");
        println!("  in_progress: {in_progress}");
        println!("  blocked:     {blocked}");
        println!("  done:        {done}");
        println!();
        println!("Index");
        println!("  stuck_files:           {stuck_files}");
        println!("  stale_hashes_prevented:{stale_hashes_prevented}");
    }

    Ok(())
}
