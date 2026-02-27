use std::path::PathBuf;

use tracing::{info, warn};
use walkdir::WalkDir;

/// A scanned note file path.
pub struct ScannedFile {
    pub path: PathBuf,
}

/// Walk `dirs`, collect all `*.md` file paths.
/// Skips hidden directories and unreadable entries with a warning.
pub fn scan_brain(dirs: &[PathBuf]) -> Vec<ScannedFile> {
    let mut files = Vec::new();

    for dir in dirs {
        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_entry(|e| !is_hidden(e))
        {
            let entry = match entry {
                Ok(e) => e,
                Err(err) => {
                    warn!("skipping unreadable entry: {err}");
                    continue;
                }
            };

            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.into_path();
            if path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }

            files.push(ScannedFile { path });
        }
    }

    info!(file_count = files.len(), "brain scan complete");
    files
}

fn is_hidden(entry: &walkdir::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}
