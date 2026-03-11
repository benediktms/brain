pub mod paths;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BrainCoreError, Result};

// ---------------------------------------------------------------------------
// Global config (~/.brain/config.toml)
// ---------------------------------------------------------------------------

/// Top-level global configuration stored at `$BRAIN_HOME/config.toml`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// Default model directory (optional override).
    pub model_dir: Option<PathBuf>,
    /// Default log level.
    pub log_level: Option<String>,
    /// Registered brains keyed by name.
    #[serde(default)]
    pub brains: HashMap<String, BrainEntry>,
}

/// An entry for a registered brain inside the global config.
#[derive(Debug, Serialize, Deserialize)]
pub struct BrainEntry {
    /// Absolute path to the project root.
    pub root: PathBuf,
    /// Note directory paths (absolute).
    #[serde(default)]
    pub notes: Vec<PathBuf>,
}

// ---------------------------------------------------------------------------
// Per-project config (.brain/brain.toml)
// ---------------------------------------------------------------------------

/// Project-local configuration stored at `<project>/.brain/brain.toml`.
#[derive(Debug, Serialize, Deserialize)]
pub struct BrainToml {
    /// Human-readable brain name.
    pub name: String,
    /// Relative paths to note directories.
    #[serde(default)]
    pub notes: Vec<PathBuf>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return the brain home directory (`$BRAIN_HOME` or `~/.brain`).
pub fn brain_home() -> Result<PathBuf> {
    if let Ok(val) = std::env::var("BRAIN_HOME") {
        return Ok(PathBuf::from(val));
    }
    dirs::home_dir()
        .map(|h| h.join(".brain"))
        .ok_or_else(|| BrainCoreError::Config("cannot determine home directory".into()))
}

/// Load the global config, returning a default if the file does not exist.
pub fn load_global_config() -> Result<GlobalConfig> {
    let path = brain_home()?.join("config.toml");
    if !path.exists() {
        return Ok(GlobalConfig::default());
    }
    let text = std::fs::read_to_string(&path).map_err(BrainCoreError::Io)?;
    toml::from_str(&text)
        .map_err(|e| BrainCoreError::Config(format!("failed to parse {}: {e}", path.display())))
}

/// Persist the global config to disk, creating parent directories as needed.
pub fn save_global_config(cfg: &GlobalConfig) -> Result<()> {
    let home = brain_home()?;
    crate::fs_permissions::ensure_private_dir(&home)?;
    let path = home.join("config.toml");
    let text = toml::to_string_pretty(cfg)
        .map_err(|e| BrainCoreError::Config(format!("failed to serialize config: {e}")))?;
    std::fs::write(&path, text).map_err(BrainCoreError::Io)?;
    Ok(())
}

/// Check `~/.brain` permissions at startup.
///
/// Warns if the brain home directory exists with overly broad permissions.
/// Returns `true` if permissions are OK (or the directory doesn't exist yet).
pub fn check_brain_home_permissions() -> Result<bool> {
    let home = brain_home()?;
    crate::fs_permissions::check_dir_permissions(&home)
}

/// Load a project-local `brain.toml`.
pub fn load_brain_toml(brain_dir: &Path) -> Result<BrainToml> {
    let path = brain_dir.join("brain.toml");
    let text = std::fs::read_to_string(&path).map_err(BrainCoreError::Io)?;
    toml::from_str(&text)
        .map_err(|e| BrainCoreError::Config(format!("failed to parse {}: {e}", path.display())))
}

/// Write a project-local `brain.toml`.
pub fn save_brain_toml(brain_dir: &Path, cfg: &BrainToml) -> Result<()> {
    std::fs::create_dir_all(brain_dir).map_err(BrainCoreError::Io)?;
    let path = brain_dir.join("brain.toml");
    let text = toml::to_string_pretty(cfg)
        .map_err(|e| BrainCoreError::Config(format!("failed to serialize brain.toml: {e}")))?;
    std::fs::write(&path, text).map_err(BrainCoreError::Io)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Brain path resolution
// ---------------------------------------------------------------------------

/// Resolved paths for a discovered brain project.
#[derive(Debug)]
pub struct ResolvedPaths {
    pub model_dir: PathBuf,
    pub lance_db: PathBuf,
    pub sqlite_db: PathBuf,
}

/// Walk up from `start` looking for `.brain/brain.toml`.
/// Returns the directory containing `.brain/` if found.
pub fn find_brain_root(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        if dir.join(".brain").join("brain.toml").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Resolve brain paths from the marker file and global config.
/// Returns `None` if no `.brain/brain.toml` marker is found.
pub fn resolve_brain_paths(start: &Path) -> Result<Option<ResolvedPaths>> {
    let home = brain_home()?;
    resolve_brain_paths_with_home(start, &home)
}

/// Internal: resolve brain paths given an explicit `home` directory.
/// Separated to make unit tests independent of the `BRAIN_HOME` env var.
pub(crate) fn resolve_brain_paths_with_home(
    start: &Path,
    home: &Path,
) -> Result<Option<ResolvedPaths>> {
    let root = match find_brain_root(start) {
        Some(r) => r,
        None => return Ok(None),
    };
    let brain_toml = load_brain_toml(&root.join(".brain"))?;
    let brain_data = home.join("brains").join(&brain_toml.name);

    Ok(Some(ResolvedPaths {
        model_dir: home.join("models").join("bge-small-en-v1.5"),
        lance_db: brain_data.join("lancedb"),
        sqlite_db: brain_data.join("brain.db"),
    }))
}

/// Resolve paths for a brain identified by `name` (from the registry).
///
/// Unlike [`resolve_brain_paths`], this does not require being inside the
/// project directory — it derives paths purely from the name.
pub fn resolve_paths_for_brain(name: &str) -> Result<ResolvedPaths> {
    let home = brain_home()?;
    Ok(resolve_paths_for_brain_with_home(name, &home))
}

/// Internal: resolve paths for a named brain given an explicit `home` directory.
///
/// Separated to make unit tests independent of the `BRAIN_HOME` env var.
/// Unlike [`resolve_brain_paths_with_home`], this returns the struct directly
/// since home is provided and no fallible discovery is needed.
pub fn resolve_paths_for_brain_with_home(name: &str, home: &Path) -> ResolvedPaths {
    let brain_data = home.join("brains").join(name);
    ResolvedPaths {
        model_dir: home.join("models").join("bge-small-en-v1.5"),
        lance_db: brain_data.join("lancedb"),
        sqlite_db: brain_data.join("brain.db"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Create `.brain/brain.toml` with `name = "<name>"` inside `dir`.
    fn make_brain_marker(dir: &Path, name: &str) {
        let brain_dir = dir.join(".brain");
        fs::create_dir_all(&brain_dir).unwrap();
        fs::write(brain_dir.join("brain.toml"), format!("name = \"{name}\"\n")).unwrap();
    }

    // -----------------------------------------------------------------------
    // find_brain_root
    // -----------------------------------------------------------------------

    #[test]
    fn find_brain_root_discovers_marker_at_start() {
        let tmp = TempDir::new().unwrap();
        make_brain_marker(tmp.path(), "test-brain");

        let result = find_brain_root(tmp.path());
        assert_eq!(result, Some(tmp.path().to_path_buf()));
    }

    #[test]
    fn find_brain_root_walks_up_tree() {
        let tmp = TempDir::new().unwrap();
        make_brain_marker(tmp.path(), "test-brain");

        // Create a deeply nested subdirectory
        let deep = tmp.path().join("a").join("b").join("c");
        fs::create_dir_all(&deep).unwrap();

        let result = find_brain_root(&deep);
        assert_eq!(result, Some(tmp.path().to_path_buf()));
    }

    #[test]
    fn find_brain_root_returns_none_when_no_marker() {
        let tmp = TempDir::new().unwrap();
        // No .brain/brain.toml created

        let result = find_brain_root(tmp.path());
        assert!(result.is_none());
    }

    #[test]
    fn find_brain_root_nested_returns_nearest() {
        let tmp = TempDir::new().unwrap();
        // Outer brain at root
        make_brain_marker(tmp.path(), "outer");

        // Inner brain in a subdirectory
        let inner = tmp.path().join("sub").join("project");
        fs::create_dir_all(&inner).unwrap();
        make_brain_marker(&inner, "inner");

        // Starting from inside inner project — should find inner, not outer
        let deep = inner.join("src");
        fs::create_dir_all(&deep).unwrap();

        let result = find_brain_root(&deep);
        assert_eq!(result, Some(inner));
    }

    #[test]
    fn find_brain_root_empty_dir_returns_none() {
        let tmp = TempDir::new().unwrap();
        let empty = tmp.path().join("empty");
        fs::create_dir_all(&empty).unwrap();

        let result = find_brain_root(&empty);
        assert!(result.is_none());
    }

    #[test]
    fn find_brain_root_dot_brain_dir_without_toml_is_ignored() {
        let tmp = TempDir::new().unwrap();
        // Create .brain dir but NOT the brain.toml inside it
        fs::create_dir_all(tmp.path().join(".brain")).unwrap();

        let result = find_brain_root(tmp.path());
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // resolve_brain_paths_with_home
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_brain_paths_returns_none_when_no_marker() {
        let tmp = TempDir::new().unwrap();
        let fake_home = TempDir::new().unwrap();

        let result = resolve_brain_paths_with_home(tmp.path(), fake_home.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn resolve_brain_paths_returns_correct_paths() {
        let tmp = TempDir::new().unwrap();
        make_brain_marker(tmp.path(), "my-brain");
        let fake_home = TempDir::new().unwrap();

        let result = resolve_brain_paths_with_home(tmp.path(), fake_home.path())
            .unwrap()
            .unwrap();

        let home = fake_home.path();
        assert_eq!(
            result.model_dir,
            home.join("models").join("bge-small-en-v1.5")
        );
        assert_eq!(
            result.lance_db,
            home.join("brains").join("my-brain").join("lancedb")
        );
        assert_eq!(
            result.sqlite_db,
            home.join("brains").join("my-brain").join("brain.db")
        );
    }

    #[test]
    fn resolve_brain_paths_uses_nearest_brain_root() {
        let tmp = TempDir::new().unwrap();
        // Outer brain at root
        make_brain_marker(tmp.path(), "outer");

        // Inner brain in a subdirectory
        let inner = tmp.path().join("inner");
        fs::create_dir_all(&inner).unwrap();
        make_brain_marker(&inner, "inner-brain");

        let deep = inner.join("src");
        fs::create_dir_all(&deep).unwrap();

        let fake_home = TempDir::new().unwrap();
        let result = resolve_brain_paths_with_home(&deep, fake_home.path())
            .unwrap()
            .unwrap();

        // Should use the inner brain name, not outer
        assert!(
            result.lance_db.to_string_lossy().contains("inner-brain"),
            "expected inner-brain in path, got: {}",
            result.lance_db.display()
        );
    }

    // -----------------------------------------------------------------------
    // resolve_paths_for_brain_with_home
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_paths_for_brain_with_home_returns_correct_paths() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        let result = resolve_paths_for_brain_with_home("my-brain", home);

        assert_eq!(result.model_dir, home.join("models").join("bge-small-en-v1.5"));
        assert_eq!(result.lance_db, home.join("brains").join("my-brain").join("lancedb"));
        assert_eq!(result.sqlite_db, home.join("brains").join("my-brain").join("brain.db"));
    }

    #[test]
    fn resolve_paths_for_brain_with_home_different_names_produce_different_paths() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        let result_a = resolve_paths_for_brain_with_home("brain-a", home);
        let result_b = resolve_paths_for_brain_with_home("brain-b", home);

        assert_ne!(result_a.lance_db, result_b.lance_db);
        assert_ne!(result_a.sqlite_db, result_b.sqlite_db);
        // model_dir is shared
        assert_eq!(result_a.model_dir, result_b.model_dir);
    }

    #[test]
    fn resolve_paths_for_brain_with_home_name_is_in_paths() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        let result = resolve_paths_for_brain_with_home("special-brain", home);

        assert!(
            result.lance_db.to_string_lossy().contains("special-brain"),
            "expected brain name in lancedb path, got: {}",
            result.lance_db.display()
        );
        assert!(
            result.sqlite_db.to_string_lossy().contains("special-brain"),
            "expected brain name in sqlite_db path, got: {}",
            result.sqlite_db.display()
        );
    }
}
