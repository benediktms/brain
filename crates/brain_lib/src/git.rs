//! Git repository discovery — resolve a path to its shared `.git` directory.
//!
//! All worktrees of the same git repository (main checkout and linked worktrees
//! created via `git worktree add`) share one on-disk `.git` directory. This
//! shared directory is the "common-dir". Resolving any working-tree path to its
//! common-dir gives a stable identity key for "this is repo X" — independent of
//! the worktree's filesystem location.
//!
//! Used by `brain init`'s Case C auto-attach: when a fresh worktree's
//! common-dir matches a registered brain root's common-dir, the worktree is
//! attached to that brain instead of registering a new one.

use lru::LruCache;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// Resolve the canonicalized common-dir (shared `.git` directory) for a path.
///
/// Returns `Ok(None)` for paths that are not inside a git repository — this is
/// expected, not exceptional. Returns `Err` only on filesystem I/O failures
/// during canonicalization.
///
/// For a main checkout, the common-dir is `<repo>/.git`.
/// For a linked worktree, it is the parent repository's `.git` (resolved via
/// the `commondir` pointer file that git places in the worktree's per-worktree
/// git directory).
pub fn common_dir(path: &Path) -> io::Result<Option<PathBuf>> {
    let (gix_path, _trust) = match gix_discover::upwards(path) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let (git_dir, _work_tree) = gix_path.into_repository_and_work_tree_directories();

    // Linked worktrees have a `commondir` pointer file inside the worktree's
    // per-worktree git dir; main checkouts do not. The pointer's contents are
    // either an absolute path or a path relative to the worktree's git dir.
    let commondir_pointer = git_dir.join("commondir");
    let common = if commondir_pointer.exists() {
        let pointer = std::fs::read_to_string(&commondir_pointer)?;
        let pointer_path = PathBuf::from(pointer.trim());
        if pointer_path.is_absolute() {
            pointer_path
        } else {
            git_dir.join(pointer_path)
        }
    } else {
        git_dir.clone()
    };

    Ok(Some(std::fs::canonicalize(&common)?))
}

const DEFAULT_GIT_CACHE_CAP: usize = 4096;

fn cache() -> &'static Mutex<LruCache<PathBuf, Option<PathBuf>>> {
    static CACHE: OnceLock<Mutex<LruCache<PathBuf, Option<PathBuf>>>> = OnceLock::new();
    CACHE.get_or_init(|| {
        let cap = std::env::var("BRAIN_GIT_CACHE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_GIT_CACHE_CAP);
        let cap =
            NonZeroUsize::new(cap).unwrap_or(NonZeroUsize::new(DEFAULT_GIT_CACHE_CAP).unwrap());
        Mutex::new(LruCache::new(cap))
    })
}

/// Cached variant of [`common_dir`]. Memoizes by canonicalized input path.
/// Bounded LRU; capacity defaults to 4096, tunable via `BRAIN_GIT_CACHE_CAP`.
/// Process-lifetime cache; not persisted.
pub fn common_dir_cached(path: &Path) -> io::Result<Option<PathBuf>> {
    // Canonicalize the input so symlinked/`..`-laden equivalents share a cache slot.
    // Fall back to the raw path if canonicalization fails (e.g. path doesn't exist).
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    {
        let mut guard = cache().lock().unwrap();
        if let Some(hit) = guard.get(&key) {
            return Ok(hit.clone());
        }
    }
    let resolved = common_dir(&key)?;
    cache().lock().unwrap().put(key, resolved.clone());
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::TempDir;

    fn git(args: &[&str], cwd: &Path) {
        let status = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .expect("git not available on PATH");
        assert!(status.success(), "git {args:?} failed");
    }

    fn init_repo(path: &Path) {
        git(&["init", "-q", "-b", "main"], path);
        // Need at least one commit so `git worktree add` works.
        git(&["config", "user.email", "test@test"], path);
        git(&["config", "user.name", "test"], path);
        std::fs::write(path.join("readme"), "x").unwrap();
        git(&["add", "."], path);
        git(&["commit", "-q", "-m", "init"], path);
    }

    #[test]
    fn common_dir_returns_none_for_non_git_dir() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(common_dir(tmp.path()).unwrap(), None);
    }

    #[test]
    fn common_dir_returns_dot_git_for_main_checkout() {
        let tmp = TempDir::new().unwrap();
        init_repo(tmp.path());

        let resolved = common_dir(tmp.path()).unwrap().unwrap();
        let expected = std::fs::canonicalize(tmp.path().join(".git")).unwrap();
        assert_eq!(resolved, expected);
    }

    #[test]
    fn common_dir_matches_across_main_and_linked_worktree() {
        let main = TempDir::new().unwrap();
        init_repo(main.path());

        let wt = TempDir::new().unwrap();
        let wt_path = wt.path().join("linked");
        git(
            &[
                "worktree",
                "add",
                "--detach",
                wt_path.to_str().unwrap(),
                "HEAD",
            ],
            main.path(),
        );

        let main_common = common_dir(main.path()).unwrap().unwrap();
        let wt_common = common_dir(&wt_path).unwrap().unwrap();
        assert_eq!(
            main_common, wt_common,
            "main and linked worktree must resolve to the same common-dir"
        );
    }

    #[test]
    fn common_dir_cached_returns_same_value_on_repeated_calls() {
        let tmp = TempDir::new().unwrap();
        init_repo(tmp.path());

        let first = common_dir_cached(tmp.path()).unwrap();
        let second = common_dir_cached(tmp.path()).unwrap();
        assert_eq!(first, second);
        assert!(first.is_some());
    }

    #[test]
    fn lru_cache_bounded_eviction() {
        use lru::LruCache;
        use std::num::NonZeroUsize;
        use std::path::PathBuf;

        // Verify LruCache itself: inserting N+1 items into a capacity-N cache
        // keeps len == N (the oldest entry is evicted).
        let n: usize = 4;
        let mut cache: LruCache<PathBuf, Option<PathBuf>> =
            LruCache::new(NonZeroUsize::new(n).unwrap());

        for i in 0..=n {
            cache.put(PathBuf::from(format!("/fake/path/{i}")), None);
        }

        assert_eq!(
            cache.len(),
            n,
            "cache must not exceed capacity after N+1 insertions"
        );
        // The first entry (i=0) must have been evicted.
        assert!(
            cache.peek(&PathBuf::from("/fake/path/0")).is_none(),
            "oldest entry must be evicted"
        );
    }
}
