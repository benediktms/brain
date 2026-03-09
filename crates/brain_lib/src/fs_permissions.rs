//! Filesystem permission utilities for securing `~/.brain` directories.
//!
//! All directories under `~/.brain` contain personal knowledge and task data.
//! They should be owner-only (`0o700`) to prevent other users from reading them.

use std::fs;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::Path;

use tracing::warn;

use crate::error::{BrainCoreError, Result};

/// Expected Unix permission mode for private directories.
const PRIVATE_DIR_MODE: u32 = 0o700;

/// Bits that must NOT be set (group/other read, write, or execute).
const OPEN_BITS: u32 = 0o077;

/// Create a directory (and parents) with `0o700` permissions.
///
/// Each component in the path that needs creating gets `0o700`. Already-existing
/// directories are left unchanged — call [`check_dir_permissions`] to audit them.
pub fn create_private_dir(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }

    // Build parent chain so we can set mode on each created segment.
    let mut to_create = Vec::new();
    let mut current = path.to_path_buf();
    while !current.exists() {
        to_create.push(current.clone());
        if !current.pop() {
            break;
        }
    }

    for dir in to_create.into_iter().rev() {
        fs::DirBuilder::new()
            .mode(PRIVATE_DIR_MODE)
            .create(&dir)
            .map_err(BrainCoreError::Io)?;
    }
    Ok(())
}

/// Check whether `path` has permissions that are too open.
///
/// Returns `Ok(())` if permissions are restrictive (no group/other bits set).
/// Logs a warning and returns `Ok(())` if the directory doesn't exist yet.
/// Returns `Err` only on I/O failures reading metadata.
///
/// When permissions are too open, logs a warning with remediation advice.
pub fn check_dir_permissions(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(true);
    }

    let metadata = fs::metadata(path).map_err(BrainCoreError::Io)?;
    let mode = metadata.permissions().mode();

    if mode & OPEN_BITS != 0 {
        warn!(
            path = %path.display(),
            current_mode = format!("{:#o}", mode & 0o777),
            expected_mode = format!("{:#o}", PRIVATE_DIR_MODE),
            "directory has overly broad permissions — other users may read your data. \
             Fix with: chmod 700 {}",
            path.display()
        );
        return Ok(false);
    }

    Ok(true)
}

/// Create a private directory and verify its permissions.
///
/// Combines [`create_private_dir`] and [`check_dir_permissions`]. Use this
/// for all `~/.brain` directory creation to ensure consistent security.
pub fn ensure_private_dir(path: &Path) -> Result<()> {
    create_private_dir(path)?;
    check_dir_permissions(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn test_create_private_dir_sets_700() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("private");

        create_private_dir(&dir).unwrap();

        let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "expected 0o700, got {:#o}", mode);
    }

    #[test]
    fn test_create_private_dir_nested() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("a").join("b").join("c");

        create_private_dir(&dir).unwrap();

        // All created segments should be 0o700
        for segment in ["a", "a/b", "a/b/c"] {
            let p = tmp.path().join(segment);
            let mode = fs::metadata(&p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}: expected 0o700, got {:#o}", segment, mode);
        }
    }

    #[test]
    fn test_create_private_dir_idempotent() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("private");

        create_private_dir(&dir).unwrap();
        create_private_dir(&dir).unwrap(); // should not error
    }

    #[test]
    fn test_check_permissions_ok() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("secure");
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();

        assert!(check_dir_permissions(&dir).unwrap());
    }

    #[test]
    fn test_check_permissions_too_open() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("open");
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(!check_dir_permissions(&dir).unwrap());
    }

    #[test]
    fn test_check_permissions_nonexistent_ok() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("nonexistent");

        assert!(check_dir_permissions(&dir).unwrap());
    }

    #[test]
    fn test_ensure_private_dir_creates_and_checks() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("ensured");

        ensure_private_dir(&dir).unwrap();

        assert!(dir.is_dir());
        let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }
}
