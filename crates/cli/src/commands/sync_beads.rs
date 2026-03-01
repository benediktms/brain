use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use notify_debouncer_full::{new_debouncer, notify::RecursiveMode};

use brain_lib::db::Db;
use brain_lib::tasks::TaskStore;

fn open_store(sqlite_db: &PathBuf) -> Result<TaskStore> {
    let db = Db::open(sqlite_db).context("Failed to open SQLite database")?;
    let tasks_dir = sqlite_db
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("tasks");
    TaskStore::new(&tasks_dir, db).context("Failed to open task store")
}

fn do_sync(store: &TaskStore, path: &PathBuf) -> Result<()> {
    let report = store
        .sync_from_beads(path)
        .context("Failed to sync from beads")?;
    println!("{report}");
    Ok(())
}

pub fn run(jsonl_path: Option<PathBuf>, sqlite_db: PathBuf, watch: bool) -> Result<()> {
    let path = jsonl_path.unwrap_or_else(|| PathBuf::from(".beads/issues.jsonl"));

    if !path.exists() {
        bail!(
            "Beads issues file not found: {}\nHint: run from the project root or pass --path",
            path.display()
        );
    }

    let store = open_store(&sqlite_db)?;

    // Initial sync
    println!("Syncing from {}...", path.display());
    do_sync(&store, &path)?;
    println!("Sync complete.");

    if !watch {
        return Ok(());
    }

    // Watch mode: re-sync on file changes
    println!(
        "\nWatching {} for changes (Ctrl-C to stop)...",
        path.display()
    );

    let (tx, rx) = mpsc::channel();

    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)
        .context("Failed to create file watcher")?;

    // Watch the parent directory (notify requires watching dirs for some backends)
    let watch_path = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    debouncer
        .watch(watch_path, RecursiveMode::NonRecursive)
        .context("Failed to watch beads directory")?;

    let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());

    loop {
        match rx.recv() {
            Ok(Ok(events)) => {
                // Check if any event affects our file
                let dominated = events.iter().any(|e| {
                    e.paths
                        .iter()
                        .any(|p| p.canonicalize().unwrap_or_else(|_| p.clone()) == canonical)
                });
                if dominated {
                    println!("\nChange detected, re-syncing...");
                    match do_sync(&store, &path) {
                        Ok(()) => println!("Sync complete."),
                        Err(e) => eprintln!("Sync error: {e:#}"),
                    }
                }
            }
            Ok(Err(errs)) => {
                for e in errs {
                    eprintln!("Watch error: {e}");
                }
            }
            Err(e) => {
                bail!("Watch channel closed: {e}");
            }
        }
    }
}
