use std::path::Path;

use anyhow::Result;
use brain_lib::stores::BrainStores;

/// Show brain health status: task counts, index stats, brain ID.
pub fn run(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<()> {
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
