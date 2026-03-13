use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use brain_lib::db::Db;

/// Resolved database handles for CLI commands.
///
/// - `per_brain`: the per-brain DB at `sqlite_db` — for `brain_meta` lookups (prefix)
/// - `unified`: `$BRAIN_HOME/brain.db` — for task/record data
/// - `brain_id`: stable brain identifier resolved from the config registry
///
/// Falls back to the per-brain DB when no separate unified DB exists
/// (pre-migration installations or when `sqlite_db` IS the unified DB).
pub struct ResolvedDbs {
    pub per_brain: Db,
    pub unified: Db,
    pub brain_id: String,
    pub brain_home: PathBuf,
}

pub fn resolve_dbs(sqlite_db: &Path) -> Result<ResolvedDbs> {
    resolve_dbs_inner(sqlite_db, None)
}

/// Inner implementation that accepts an optional brain_home override for testing.
fn resolve_dbs_inner(sqlite_db: &Path, brain_home_override: Option<&Path>) -> Result<ResolvedDbs> {
    let per_brain = Db::open(sqlite_db).context("Failed to open SQLite database")?;

    // Derive brain_home from path convention: $BRAIN_HOME/brains/<name>/brain.db
    let brain_data_dir = sqlite_db.parent().unwrap_or(Path::new("."));
    let brain_name = brain_data_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    // Step 1: Try path-based derivation ($BRAIN_HOME/brains/<name>/brain.db → parent.parent)
    let path_derived_home = brain_data_dir
        .parent()
        .and_then(|p| p.parent())
        .filter(|p| p.join("brain.db").exists())
        .map(|p| p.to_path_buf());

    // Step 2: Resolve brain_home — override > path convention > config > data dir
    let brain_home = if let Some(ovr) = brain_home_override {
        ovr.to_path_buf()
    } else if let Some(ref derived) = path_derived_home {
        derived.clone()
    } else {
        brain_lib::config::brain_home().unwrap_or_else(|_| brain_data_dir.to_path_buf())
    };

    // Step 3: Open unified DB only if it exists and is a different file from sqlite_db
    let unified_db_path = brain_home.join("brain.db");
    let unified = if unified_db_path.exists()
        && std::fs::canonicalize(&unified_db_path).ok() != std::fs::canonicalize(sqlite_db).ok()
    {
        Db::open(&unified_db_path).context("Failed to open unified database")?
    } else {
        per_brain.clone()
    };

    // Resolve brain_id from config registry
    let brain_id = if !brain_name.is_empty() {
        brain_lib::config::resolve_brain_entry(brain_name)
            .and_then(|(name, entry)| brain_lib::config::resolve_brain_id(&entry, &name))
            .unwrap_or_default()
    } else {
        String::new()
    };

    Ok(ResolvedDbs {
        per_brain,
        unified,
        brain_id,
        brain_home,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: create the standard brain directory structure and return the per-brain DB path.
    ///
    /// Layout:
    /// ```text
    /// <root>/
    ///   brain.db          ← unified DB (only if `create_unified` is true)
    ///   brains/
    ///     <name>/
    ///       brain.db      ← per-brain DB
    ///       tasks/
    ///       records/
    /// ```
    fn make_brain_dirs(root: &Path, name: &str, create_unified: bool) -> PathBuf {
        let brain_data = root.join("brains").join(name);
        std::fs::create_dir_all(brain_data.join("tasks")).unwrap();
        std::fs::create_dir_all(brain_data.join("records")).unwrap();

        // Create per-brain DB by opening it (initializes schema)
        let per_brain_path = brain_data.join("brain.db");
        Db::open(&per_brain_path).unwrap();

        if create_unified {
            let unified_path = root.join("brain.db");
            Db::open(&unified_path).unwrap();
        }

        per_brain_path
    }

    #[test]
    fn resolve_with_unified_db_opens_both() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "my-project", true);

        let resolved = resolve_dbs_inner(&sqlite_db, Some(tmp.path())).unwrap();

        // brain_home should point to the root (parent of brains/)
        assert_eq!(resolved.brain_home, tmp.path());
        // brain_id will be empty since there's no real config registry
        assert!(resolved.brain_id.is_empty());
    }

    #[test]
    fn resolve_without_unified_db_falls_back_to_per_brain() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "my-project", false);

        // Pass brain_home_override pointing to the temp root (no unified DB there)
        let resolved = resolve_dbs_inner(&sqlite_db, Some(tmp.path())).unwrap();

        // No unified DB at root → unified should be a clone of per_brain
        // (We can't directly compare Db instances, but we verify no error occurred
        // and brain_home is the override we provided)
        assert_eq!(resolved.brain_home, tmp.path());
        assert!(resolved.brain_id.is_empty());
    }

    #[test]
    fn resolve_when_sqlite_is_unified_db() {
        // When sqlite_db points directly to the unified DB ($BRAIN_HOME/brain.db),
        // canonicalize comparison should detect they're the same file → clone fallback
        let tmp = TempDir::new().unwrap();
        let unified_path = tmp.path().join("brain.db");
        Db::open(&unified_path).unwrap();

        let resolved = resolve_dbs_inner(&unified_path, Some(tmp.path())).unwrap();

        // brain_home.join("brain.db") == sqlite_db → should not double-open
        assert_eq!(resolved.brain_home, tmp.path());
        assert!(resolved.brain_id.is_empty());
    }

    #[test]
    fn resolve_flat_directory_no_convention() {
        // sqlite_db in a flat directory with no brains/<name>/ convention
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("brain.db");
        Db::open(&db_path).unwrap();

        // Override brain_home to the same temp dir
        let resolved = resolve_dbs_inner(&db_path, Some(tmp.path())).unwrap();

        // Same file → unified falls back to per_brain clone
        assert_eq!(resolved.brain_home, tmp.path());
        assert!(resolved.brain_id.is_empty());
    }

    #[test]
    fn brain_home_derived_from_path_convention() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "test-brain", true);

        // No override — path convention should succeed because unified DB exists
        let resolved = resolve_dbs_inner(&sqlite_db, None).unwrap();

        // Path convention: $ROOT/brains/test-brain/brain.db → brain_home = $ROOT
        assert_eq!(
            std::fs::canonicalize(&resolved.brain_home).unwrap(),
            std::fs::canonicalize(tmp.path()).unwrap()
        );
    }

    #[test]
    fn brain_home_uses_override_when_path_derivation_fails() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "orphan", false);

        // No unified DB at root → path derivation fails, override takes effect
        let override_dir = TempDir::new().unwrap();
        let resolved = resolve_dbs_inner(&sqlite_db, Some(override_dir.path())).unwrap();

        assert_eq!(resolved.brain_home, override_dir.path());
    }

    #[test]
    fn unified_db_not_opened_when_nonexistent() {
        // Verify that when no unified DB exists at brain_home, we get a fallback
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "solo", false);
        let empty_home = TempDir::new().unwrap();

        let resolved = resolve_dbs_inner(&sqlite_db, Some(empty_home.path())).unwrap();

        // brain_home points to the empty override (no brain.db there)
        assert_eq!(resolved.brain_home, empty_home.path());
        // No error — unified silently falls back to per_brain clone
        assert!(resolved.brain_id.is_empty());
    }
}
