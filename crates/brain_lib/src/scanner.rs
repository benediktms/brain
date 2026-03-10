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

pub(crate) fn is_hidden(entry: &walkdir::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::{is_hidden, scan_brain};

    /// Create a TempDir with a visible (non-dot) name so that `filter_entry` / `is_hidden`
    /// does not prune the root of the walk on macOS (where `TempDir::new()` uses `.tmp*` names).
    fn tempdir() -> TempDir {
        tempfile::Builder::new()
            .prefix("brain_test_")
            .tempdir()
            .expect("tempdir")
    }

    /// Build a small fixture tree:
    ///   <root>/a.md
    ///   <root>/d.txt
    ///   <root>/.hidden/b.md
    ///   <root>/visible/c.md
    fn make_fixture() -> TempDir {
        let dir = tempdir();
        let root = dir.path();

        fs::write(root.join("a.md"), "# A").unwrap();
        fs::write(root.join("d.txt"), "not markdown").unwrap();

        fs::create_dir(root.join(".hidden")).unwrap();
        fs::write(root.join(".hidden").join("b.md"), "# B").unwrap();

        fs::create_dir(root.join("visible")).unwrap();
        fs::write(root.join("visible").join("c.md"), "# C").unwrap();

        dir
    }

    // Helper: collect paths from scan results as sorted strings for stable comparison.
    fn sorted_paths(files: Vec<super::ScannedFile>) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = files.into_iter().map(|f| f.path).collect();
        paths.sort();
        paths
    }

    #[test]
    fn scan_includes_md_in_visible_dirs() {
        let dir = make_fixture();
        let results = scan_brain(&[dir.path().to_path_buf()]);
        let paths = sorted_paths(results);

        // Should find a.md and visible/c.md — not b.md (hidden dir) or d.txt
        assert_eq!(
            paths.len(),
            2,
            "expected exactly 2 .md files, got: {paths:?}"
        );

        let names: Vec<&str> = paths
            .iter()
            .filter_map(|p| p.file_name()?.to_str())
            .collect();
        assert!(names.contains(&"a.md"), "a.md should be included");
        assert!(names.contains(&"c.md"), "visible/c.md should be included");
    }

    #[test]
    fn scan_excludes_hidden_directories() {
        let dir = make_fixture();
        let results = scan_brain(&[dir.path().to_path_buf()]);
        let paths = sorted_paths(results);

        let has_hidden = paths.iter().any(|p| {
            p.components().any(|c| {
                c.as_os_str()
                    .to_str()
                    .map(|s| s.starts_with('.'))
                    .unwrap_or(false)
            })
        });
        assert!(!has_hidden, ".hidden/b.md must not appear in scan results");
    }

    #[test]
    fn scan_handles_nested_structure() {
        let dir = tempdir();
        let root = dir.path();

        // Two levels of nesting
        fs::create_dir_all(root.join("a/b")).unwrap();
        fs::write(root.join("a/b/deep.md"), "# deep").unwrap();
        fs::write(root.join("a/mid.md"), "# mid").unwrap();
        fs::write(root.join("top.md"), "# top").unwrap();

        let results = scan_brain(&[root.to_path_buf()]);
        assert_eq!(
            results.len(),
            3,
            "should find all 3 nested .md files, got {:?}",
            sorted_paths(results)
        );
    }

    #[test]
    fn scan_excludes_non_md_files() {
        let dir = make_fixture();
        let results = scan_brain(&[dir.path().to_path_buf()]);
        let has_txt = results.iter().any(|f| {
            f.path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e != "md")
                .unwrap_or(false)
        });
        assert!(!has_txt, "non-.md files must be excluded");
    }

    #[test]
    fn scan_empty_directory_returns_empty() {
        let dir = tempdir();
        let results = scan_brain(&[dir.path().to_path_buf()]);
        assert!(results.is_empty(), "empty dir should yield no results");
    }

    #[test]
    fn scan_multiple_dirs_are_combined() {
        let dir1 = tempdir();
        let dir2 = tempdir();
        fs::write(dir1.path().join("x.md"), "# X").unwrap();
        fs::write(dir2.path().join("y.md"), "# Y").unwrap();

        let results = scan_brain(&[dir1.path().to_path_buf(), dir2.path().to_path_buf()]);
        assert_eq!(results.len(), 2, "both dirs should be scanned");
    }

    #[test]
    fn is_hidden_detects_dot_names() {
        // We construct real DirEntries by walking a temp tree.
        let dir = tempdir();
        fs::create_dir(dir.path().join(".git")).unwrap();
        fs::create_dir(dir.path().join(".hidden")).unwrap();
        fs::create_dir(dir.path().join("visible")).unwrap();
        fs::write(dir.path().join(".hidden_file.md"), "").unwrap();

        let entries: Vec<walkdir::DirEntry> = walkdir::WalkDir::new(dir.path())
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .collect();

        for entry in &entries {
            let name = entry.file_name().to_str().unwrap_or("");
            if name.starts_with('.') {
                assert!(
                    is_hidden(entry),
                    "is_hidden should return true for `{name}`"
                );
            } else {
                assert!(
                    !is_hidden(entry),
                    "is_hidden should return false for `{name}`"
                );
            }
        }
    }

    #[test]
    fn scan_does_not_panic_on_nonexistent_dir() {
        // A directory that does not exist should produce no results, not a panic.
        let results = scan_brain(&[PathBuf::from("/tmp/__brain_nonexistent_dir_xyz__")]);
        assert!(
            results.is_empty(),
            "nonexistent dir should yield no results"
        );
    }
}
