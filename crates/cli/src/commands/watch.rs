use anyhow::Result;
use std::path::PathBuf;

use crate::commands::rpc_client::connect_daemon;
use crate::markdown_table::MarkdownTable;

/// `brain watch` is a thin RPC client to the file-watcher supervisor
/// running inside brain-daemon. Auto-spawns the daemon when needed.
///
/// - `brain watch <path>` dispatches `WatchAdd`. Renders the brain that the
///   path was attached to.
/// - `brain watch` (no path) dispatches `WatchList`. Renders the current
///   watch set as a markdown table.
pub fn run(notes_path: Option<PathBuf>) -> Result<()> {
    let mut client = connect_daemon()?;

    match notes_path {
        Some(path) => {
            let path_str = path.to_string_lossy().into_owned();
            let (added_path, brain_name) = client
                .watch_add(path_str)
                .map_err(|e| anyhow::anyhow!("watch_add failed: {e}"))?;
            println!("watching {brain_name}: {added_path}");
        }
        None => {
            let watches = client
                .watch_list()
                .map_err(|e| anyhow::anyhow!("watch_list failed: {e}"))?;
            if watches.is_empty() {
                println!("no paths currently watched");
                return Ok(());
            }
            let mut table = MarkdownTable::new(vec!["BRAIN", "DIR", "ACTIVE"]);
            for w in &watches {
                table.add_row(vec![
                    w.brain_name.clone(),
                    w.note_dir.clone(),
                    if w.watching { "yes" } else { "no" }.to_string(),
                ]);
            }
            print!("{table}");
        }
    }
    Ok(())
}
