use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tracing::{info, warn};

use crate::error::{BrainCoreError, Result};

/// Expand a leading `~` or `~/` to the user's home directory.
///
/// Does NOT handle `~user` syntax — only bare `~`.
pub fn expand_tilde(path: &Path) -> Result<PathBuf> {
    let s = path.to_string_lossy();
    if s == "~" || s.starts_with("~/") {
        let home = dirs::home_dir()
            .ok_or_else(|| BrainCoreError::Config("cannot determine home directory".into()))?;
        if s == "~" {
            Ok(home)
        } else {
            Ok(home.join(&s[2..]))
        }
    } else {
        Ok(path.to_path_buf())
    }
}

/// Normalize a single path: expand tilde, resolve relative paths against `base`,
/// then canonicalize (resolves symlinks, validates existence).
pub fn normalize_path(path: &Path, base: &Path) -> Result<PathBuf> {
    let expanded = expand_tilde(path)?;
    let absolute = if expanded.is_absolute() {
        expanded
    } else {
        base.join(&expanded)
    };
    std::fs::canonicalize(&absolute).map_err(|e| {
        BrainCoreError::Config(format!(
            "note path '{}' does not exist or is inaccessible: {e}",
            absolute.display()
        ))
    })
}

/// True if `child` is a strict descendant of `parent` (component-boundary safe).
fn is_subpath(child: &Path, parent: &Path) -> bool {
    child != parent && child.starts_with(parent)
}

/// Normalize, deduplicate, and consolidate overlapping note paths.
///
/// Parent paths subsume their children. Logs consolidation at info level.
/// Fails if any path doesn't exist.
pub fn normalize_note_paths(paths: &[PathBuf], base: &Path) -> Result<Vec<PathBuf>> {
    // 1. Normalize each path
    let normalized: Vec<PathBuf> = paths
        .iter()
        .map(|p| normalize_path(p, base))
        .collect::<Result<Vec<_>>>()?;

    // 2. Dedup exact matches
    let unique: BTreeSet<PathBuf> = normalized.into_iter().collect();

    // 3. Sort by component count ascending (shortest first = potential parents)
    let mut sorted: Vec<PathBuf> = unique.into_iter().collect();
    sorted.sort_by_key(|p| p.components().count());

    // 4. Keep only paths not subsumed by an already-accepted parent
    let mut accepted: Vec<PathBuf> = Vec::with_capacity(sorted.len());
    for path in sorted {
        if accepted.iter().any(|parent| is_subpath(&path, parent)) {
            info!(
                path = %path.display(),
                "subsumed by parent directory, skipping"
            );
        } else {
            accepted.push(path);
        }
    }

    Ok(accepted)
}

/// Like [`normalize_note_paths`] but skips non-existent paths with a warning
/// instead of failing. For defensive runtime use.
pub fn normalize_note_paths_lenient(paths: &[PathBuf], base: &Path) -> Vec<PathBuf> {
    // 1. Normalize each path, skipping failures
    let normalized: Vec<PathBuf> = paths
        .iter()
        .filter_map(|p| match normalize_path(p, base) {
            Ok(np) => Some(np),
            Err(e) => {
                warn!(path = %p.display(), error = %e, "skipping inaccessible note path");
                None
            }
        })
        .collect();

    // 2. Dedup exact matches
    let unique: BTreeSet<PathBuf> = normalized.into_iter().collect();

    // 3. Sort by component count ascending
    let mut sorted: Vec<PathBuf> = unique.into_iter().collect();
    sorted.sort_by_key(|p| p.components().count());

    // 4. Keep only paths not subsumed by an already-accepted parent
    let mut accepted: Vec<PathBuf> = Vec::with_capacity(sorted.len());
    for path in sorted {
        if accepted.iter().any(|parent| is_subpath(&path, parent)) {
            info!(
                path = %path.display(),
                "subsumed by parent directory, skipping"
            );
        } else {
            accepted.push(path);
        }
    }

    accepted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs as unix_fs;
    use tempfile::TempDir;

    #[test]
    fn test_expand_tilde() {
        let home = dirs::home_dir().unwrap();
        assert_eq!(expand_tilde(Path::new("~")).unwrap(), home);
        assert_eq!(
            expand_tilde(Path::new("~/Documents")).unwrap(),
            home.join("Documents")
        );
        // Non-tilde paths pass through unchanged
        assert_eq!(
            expand_tilde(Path::new("/absolute/path")).unwrap(),
            PathBuf::from("/absolute/path")
        );
        assert_eq!(
            expand_tilde(Path::new("relative/path")).unwrap(),
            PathBuf::from("relative/path")
        );
    }

    #[test]
    fn test_relative_path_resolved() {
        let tmp = TempDir::new().unwrap();
        let notes = tmp.path().join("notes");
        std::fs::create_dir(&notes).unwrap();

        let result = normalize_path(Path::new("notes"), tmp.path()).unwrap();
        assert_eq!(result, std::fs::canonicalize(&notes).unwrap());
    }

    #[test]
    fn test_missing_path_errors() {
        let tmp = TempDir::new().unwrap();
        let result = normalize_path(Path::new("nonexistent"), tmp.path());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("does not exist or is inaccessible"), "{err}");
    }

    #[test]
    fn test_parent_subsumes_children() {
        let tmp = TempDir::new().unwrap();
        let a = tmp.path().join("a");
        let ab = a.join("b");
        let abc = ab.join("c");
        std::fs::create_dir_all(&abc).unwrap();

        let paths = vec![a.clone(), ab.clone(), abc.clone()];
        let result = normalize_note_paths(&paths, tmp.path()).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], std::fs::canonicalize(&a).unwrap());
    }

    #[test]
    fn test_child_covered_by_parent() {
        let tmp = TempDir::new().unwrap();
        let a = tmp.path().join("a");
        let a_sub = a.join("sub");
        std::fs::create_dir_all(&a_sub).unwrap();

        // Reverse order: child first, parent second
        let paths = vec![a_sub.clone(), a.clone()];
        let result = normalize_note_paths(&paths, tmp.path()).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], std::fs::canonicalize(&a).unwrap());
    }

    #[test]
    fn test_symlink_deduplication() {
        let tmp = TempDir::new().unwrap();
        let real_dir = tmp.path().join("real");
        std::fs::create_dir(&real_dir).unwrap();
        let link_dir = tmp.path().join("link");
        unix_fs::symlink(&real_dir, &link_dir).unwrap();

        let paths = vec![real_dir.clone(), link_dir.clone()];
        let result = normalize_note_paths(&paths, tmp.path()).unwrap();

        // Both resolve to the same canonical path
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], std::fs::canonicalize(&real_dir).unwrap());
    }

    #[test]
    fn test_duplicate_exact_paths() {
        let tmp = TempDir::new().unwrap();
        let a = tmp.path().join("a");
        std::fs::create_dir(&a).unwrap();

        let paths = vec![a.clone(), a.clone()];
        let result = normalize_note_paths(&paths, tmp.path()).unwrap();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_is_subpath_boundary() {
        // /tmp/abc is NOT a subpath of /tmp/ab
        assert!(!is_subpath(Path::new("/tmp/abc"), Path::new("/tmp/ab")));
        // But /tmp/ab/c IS a subpath of /tmp/ab
        assert!(is_subpath(Path::new("/tmp/ab/c"), Path::new("/tmp/ab")));
        // A path is not a subpath of itself
        assert!(!is_subpath(Path::new("/tmp/ab"), Path::new("/tmp/ab")));
    }

    #[test]
    fn test_lenient_skips_missing() {
        let tmp = TempDir::new().unwrap();
        let existing = tmp.path().join("exists");
        std::fs::create_dir(&existing).unwrap();

        let paths = vec![
            existing.clone(),
            tmp.path().join("missing1"),
            tmp.path().join("missing2"),
        ];
        let result = normalize_note_paths_lenient(&paths, tmp.path());

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], std::fs::canonicalize(&existing).unwrap());
    }
}
