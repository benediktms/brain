use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use brain_lib::stores::BrainStores;
use brain_lib::tasks::import_beads::import_beads_issues;

pub fn run(jsonl_path: Option<PathBuf>, sqlite_db: PathBuf, dry_run: bool) -> Result<()> {
    let path = jsonl_path.unwrap_or_else(|| PathBuf::from(".beads/issues.jsonl"));

    if !path.exists() {
        bail!(
            "Beads issues file not found: {}\nHint: run from the project root or pass --path",
            path.display()
        );
    }

    let stores = BrainStores::from_path(&sqlite_db)?;
    let store = stores.tasks;

    if dry_run {
        println!("Dry run — no events will be written.\n");
    }

    let report =
        import_beads_issues(&path, &store, dry_run).context("Failed to import beads issues")?;

    println!("{report}");

    if dry_run {
        println!("(dry run — nothing was written)");
    } else {
        println!("Import complete.");
    }

    Ok(())
}
