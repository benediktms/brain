pub mod paths;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};

use crate::embedder::Embed;
use crate::error::{BrainCoreError, Result};
use crate::store::{Store, StoreReader};

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
///
/// Serialization always writes `roots = [...]`. For backward compatibility,
/// deserialization accepts either the old `root = "..."` scalar form or the
/// new `roots = [...]` array form.
#[derive(Debug, Clone, Serialize)]
pub struct BrainEntry {
    /// Root paths for this brain. Index 0 is the primary root.
    pub roots: Vec<PathBuf>,
    /// Note directory paths (absolute).
    #[serde(default)]
    pub notes: Vec<PathBuf>,
    /// Stable brain ID (8-char Nano ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Alternate names for this brain.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

impl BrainEntry {
    /// Return the primary root path (first element of `roots`).
    ///
    /// # Panics
    /// Panics if `roots` is empty, which should never happen for a valid entry.
    pub fn primary_root(&self) -> &Path {
        &self.roots[0]
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible deserialization for BrainEntry
// ---------------------------------------------------------------------------

impl<'de> Deserialize<'de> for BrainEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct BrainEntryRaw {
            // New format: roots = [...]
            roots: Option<Vec<PathBuf>>,
            // Old format: root = "..."
            root: Option<PathBuf>,
            #[serde(default)]
            notes: Vec<PathBuf>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            id: Option<String>,
            #[serde(default, skip_serializing_if = "Vec::is_empty")]
            aliases: Vec<String>,
            // Accept but ignore legacy extra_roots field.
            #[serde(default)]
            extra_roots: Vec<PathBuf>,
        }

        let raw = BrainEntryRaw::deserialize(deserializer)?;

        let roots = if let Some(r) = raw.roots {
            r
        } else if let Some(r) = raw.root {
            let mut v = vec![r];
            v.extend(raw.extra_roots);
            v
        } else {
            return Err(serde::de::Error::missing_field("roots"));
        };

        Ok(BrainEntry {
            roots,
            notes: raw.notes,
            id: raw.id,
            aliases: raw.aliases,
        })
    }
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
    /// Stable brain ID (8-char Nano ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
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

/// Generate a new 8-character Nano ID for use as a stable brain identifier.
pub fn generate_brain_id() -> String {
    nanoid::nanoid!(8)
}

/// Get or lazily generate a brain ID, persisting it to brain.toml
/// and syncing it to the global registry (`~/.brain/config.toml`).
pub fn get_or_generate_brain_id(brain_dir: &Path) -> Result<String> {
    let mut toml = load_brain_toml(brain_dir)?;
    if let Some(ref id) = toml.id {
        // Ensure the global registry is in sync even if the ID already exists locally.
        sync_id_to_registry(&toml.name, id);
        return Ok(id.clone());
    }
    let id = generate_brain_id();
    toml.id = Some(id.clone());
    save_brain_toml(brain_dir, &toml)?;
    sync_id_to_registry(&toml.name, &id);
    Ok(id)
}

/// Best-effort: update the brain's `id` field in the global registry.
fn sync_id_to_registry(brain_name: &str, id: &str) {
    let Ok(mut global) = load_global_config() else {
        return;
    };
    if let Some(entry) = global.brains.get_mut(brain_name)
        && entry.id.as_deref() != Some(id)
    {
        entry.id = Some(id.to_string());
        let _ = save_global_config(&global);
    }
}

// ---------------------------------------------------------------------------
// Brain registry helpers
// ---------------------------------------------------------------------------

/// Internal: resolve a brain entry by name or ID from an already-loaded config.
///
/// Separated from [`resolve_brain_entry`] so unit tests can bypass the
/// `BRAIN_HOME` env var and pass a config directly.
pub fn resolve_brain_entry_from_config(
    name_or_id: &str,
    config: &GlobalConfig,
) -> Result<(String, BrainEntry)> {
    // Exact name match first.
    if let Some(entry) = config.brains.get(name_or_id) {
        return Ok((name_or_id.to_string(), entry.clone()));
    }

    // Scan by ID field.
    for (name, entry) in &config.brains {
        if entry.id.as_deref() == Some(name_or_id) {
            return Ok((name.clone(), entry.clone()));
        }
    }

    // Scan by alias.
    for (name, entry) in &config.brains {
        if entry.aliases.iter().any(|a| a == name_or_id) {
            return Ok((name.clone(), entry.clone()));
        }
    }

    let available: Vec<&str> = config.brains.keys().map(String::as_str).collect();
    Err(BrainCoreError::Config(format!(
        "brain '{}' not found in registry; available: [{}]",
        name_or_id,
        available.join(", ")
    )))
}

/// Resolve a brain entry by name or ID from the global registry.
///
/// Tries an exact name match first, then scans entries for a matching `id`
/// field. Returns `(name, entry)` on success.
pub fn resolve_brain_entry(name_or_id: &str) -> Result<(String, BrainEntry)> {
    let config = load_global_config()?;
    resolve_brain_entry_from_config(name_or_id, &config)
}

/// Resolve the stable brain ID from a registry entry.
///
/// Uses the `id` field if already set; otherwise generates and persists a new
/// 8-char Nano ID via [`get_or_generate_brain_id`].
pub fn resolve_brain_id(entry: &BrainEntry, _name: &str) -> Result<String> {
    if let Some(ref id) = entry.id {
        Ok(id.clone())
    } else {
        get_or_generate_brain_id(&entry.primary_root().join(".brain"))
    }
}

/// Open a [`crate::tasks::TaskStore`] for a remote brain identified by `name`.
///
/// Resolves the brain's data paths, creates the database directory if needed,
/// and opens the SQLite database.
pub fn open_remote_task_store(name: &str, _entry: &BrainEntry) -> Result<crate::tasks::TaskStore> {
    let paths = resolve_paths_for_brain(name)?;

    // Ensure the parent directory of the database file exists.
    if let Some(parent) = paths.sqlite_db.parent() {
        std::fs::create_dir_all(parent).map_err(BrainCoreError::Io)?;
    }

    let db = crate::db::Db::open(&paths.sqlite_db)?;

    let tasks_dir = paths
        .sqlite_db
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("tasks");

    let store = crate::tasks::TaskStore::new(&tasks_dir, db)?;
    Ok(store)
}

/// Open all stores for a brain identified by name or ID.
///
/// Delegates to [`crate::stores::BrainStores::from_brain`] for the actual
/// store construction. Returns `(brain_name, brain_id, tasks, records, objects)`.
pub fn open_brain_stores(
    name_or_id: &str,
) -> Result<(
    String,
    String,
    crate::tasks::TaskStore,
    crate::records::RecordStore,
    crate::records::objects::ObjectStore,
)> {
    let stores = crate::stores::BrainStores::from_brain(name_or_id)?;
    Ok((
        stores.brain_name,
        stores.brain_id,
        stores.tasks,
        stores.records,
        stores.objects,
    ))
}

// ---------------------------------------------------------------------------
// Remote search context
// ---------------------------------------------------------------------------

/// Context needed to perform search on a remote brain.
///
/// The `db` field has been removed — callers pass a shared `Db` reference
/// to `FederatedPipeline` so that a single unified SQLite connection is
/// reused across all brains. Only the per-brain LanceDB store is kept here.
pub struct RemoteSearchContext {
    pub brain_name: String,
    pub brain_id: String,
    pub store: Option<StoreReader>,
}

/// Open a remote brain's search context (StoreReader) by name or ID.
///
/// Returns `None` if the brain is not found in the registry.
/// Errors if the brain is found but its vector store cannot be opened.
///
/// No separate SQLite connection is opened — callers should pass the shared
/// unified `Db` to `FederatedPipeline` instead.
pub async fn open_remote_search_context(
    brain_home: &Path,
    brain_key: &str,
    _model_dir: &Path,
    _embedder: &Arc<dyn Embed>,
) -> Result<Option<RemoteSearchContext>> {
    let config = {
        let path = brain_home.join("config.toml");
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path).map_err(BrainCoreError::Io)?;
        toml::from_str::<GlobalConfig>(&text).map_err(|e| {
            BrainCoreError::Config(format!("failed to parse {}: {e}", path.display()))
        })?
    };

    let (name, entry) = match resolve_brain_entry_from_config(brain_key, &config) {
        Ok(pair) => pair,
        Err(_) => return Ok(None),
    };

    let brain_id = entry.id.clone().unwrap_or_default();

    let paths = resolve_paths_for_brain_with_home(&name, brain_home);

    let store = if paths.lance_db.exists() {
        let s = Store::open_or_create(&paths.lance_db).await?;
        Some(StoreReader::from_store(&s))
    } else {
        None
    };

    Ok(Some(RemoteSearchContext {
        brain_name: name,
        brain_id,
        store,
    }))
}

/// Find a registered brain whose `roots` list contains `path`.
///
/// Returns `(name, entry)` for the first match, or `None` if no brain
/// has `path` in its roots.
pub fn find_brain_by_path<'a>(
    config: &'a GlobalConfig,
    path: &Path,
) -> Option<(String, &'a BrainEntry)> {
    for (name, entry) in &config.brains {
        if entry.roots.iter().any(|r| r == path) {
            return Some((name.clone(), entry));
        }
    }
    None
}

/// Find a registered brain whose `id` matches `brain_id`.
///
/// Returns `(name, entry)` for the first match, or `None` if no brain
/// has that ID.
pub fn find_brain_by_id<'a>(
    config: &'a GlobalConfig,
    brain_id: &str,
) -> Option<(String, &'a BrainEntry)> {
    for (name, entry) in &config.brains {
        if entry.id.as_deref() == Some(brain_id) {
            return Some((name.clone(), entry));
        }
    }
    None
}

/// Return all brain `(name, id)` pairs from the global registry.
///
/// The `id` field is the stable 8-char Nano ID stored in the config entry,
/// or an empty string when no ID has been assigned yet.
pub fn list_brain_keys(brain_home: &Path) -> Result<Vec<(String, String)>> {
    let path = brain_home.join("config.toml");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&path).map_err(BrainCoreError::Io)?;
    let config: GlobalConfig = toml::from_str(&text)
        .map_err(|e| BrainCoreError::Config(format!("failed to parse {}: {e}", path.display())))?;
    let mut pairs: Vec<(String, String)> = config
        .brains
        .into_iter()
        .map(|(name, entry)| (name, entry.id.unwrap_or_default()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(pairs)
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
    /// Path to the Flan-T5-small summarizer model directory.
    /// `None` if the model has not been downloaded yet.
    pub summarizer_model_dir: Option<PathBuf>,
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
    let summarizer_model_dir = home.join("models").join("flan-t5-small");

    Ok(Some(ResolvedPaths {
        model_dir: home.join("models").join("bge-small-en-v1.5"),
        lance_db: brain_data.join("lancedb"),
        sqlite_db: brain_data.join("brain.db"),
        summarizer_model_dir: if summarizer_model_dir.is_dir() {
            Some(summarizer_model_dir)
        } else {
            None
        },
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
    let summarizer_model_dir = home.join("models").join("flan-t5-small");
    ResolvedPaths {
        model_dir: home.join("models").join("bge-small-en-v1.5"),
        lance_db: brain_data.join("lancedb"),
        sqlite_db: brain_data.join("brain.db"),
        summarizer_model_dir: if summarizer_model_dir.is_dir() {
            Some(summarizer_model_dir)
        } else {
            None
        },
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

    // -----------------------------------------------------------------------
    // resolve_brain_entry_from_config
    // -----------------------------------------------------------------------

    fn make_global_config_with_brain(name: &str, id: Option<&str>, root: &Path) -> GlobalConfig {
        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            name.to_string(),
            BrainEntry {
                roots: vec![root.to_path_buf()],
                notes: vec![],
                id: id.map(str::to_string),
                aliases: vec![],
            },
        );
        cfg
    }

    #[test]
    fn resolve_brain_entry_by_name() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("infra", Some("abc12345"), tmp.path());

        let (name, entry) = resolve_brain_entry_from_config("infra", &cfg).unwrap();
        assert_eq!(name, "infra");
        assert_eq!(entry.primary_root(), tmp.path());
        assert_eq!(entry.id, Some("abc12345".to_string()));
    }

    #[test]
    fn resolve_brain_entry_by_id() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("infra", Some("abc12345"), tmp.path());

        let (name, entry) = resolve_brain_entry_from_config("abc12345", &cfg).unwrap();
        assert_eq!(name, "infra");
        assert_eq!(entry.primary_root(), tmp.path());
    }

    #[test]
    fn resolve_brain_entry_not_found() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("infra", Some("abc12345"), tmp.path());

        let err = resolve_brain_entry_from_config("nonexistent", &cfg).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent"),
            "error should mention the requested name: {msg}"
        );
        assert!(
            msg.contains("infra"),
            "error should list available brains: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // open_remote_task_store
    // -----------------------------------------------------------------------

    #[test]
    fn open_remote_task_store_creates_functional_store() {
        let brain_home_tmp = TempDir::new().unwrap();
        let project_tmp = TempDir::new().unwrap();

        // Point BRAIN_HOME at our temp dir.
        // SAFETY: test is single-threaded; no concurrent env access.
        unsafe {
            std::env::set_var("BRAIN_HOME", brain_home_tmp.path());
        }

        let entry = BrainEntry {
            roots: vec![project_tmp.path().to_path_buf()],
            notes: vec![],
            id: Some("test1234".to_string()),
            aliases: vec![],
        };

        let store = open_remote_task_store("test-brain", &entry).unwrap();

        // The store should be functional — listing tasks on an empty store succeeds.
        let tasks = store.list_all().unwrap();
        assert!(tasks.is_empty());

        // Clean up env var.
        unsafe {
            std::env::remove_var("BRAIN_HOME");
        }
    }

    // -----------------------------------------------------------------------
    // Brain ID helpers
    // -----------------------------------------------------------------------

    #[test]
    fn generate_brain_id_returns_8_chars() {
        let id = generate_brain_id();
        assert_eq!(id.len(), 8);
    }

    #[test]
    fn brain_toml_round_trip_with_id() {
        let tmp = TempDir::new().unwrap();
        let brain_dir = tmp.path().join(".brain");
        fs::create_dir_all(&brain_dir).unwrap();

        let toml_cfg = BrainToml {
            name: "test-brain".to_string(),
            notes: vec![],
            id: Some("abcd1234".to_string()),
        };
        save_brain_toml(&brain_dir, &toml_cfg).unwrap();
        let loaded = load_brain_toml(&brain_dir).unwrap();
        assert_eq!(loaded.id, Some("abcd1234".to_string()));
    }

    #[test]
    fn brain_toml_round_trip_without_id() {
        let tmp = TempDir::new().unwrap();
        let brain_dir = tmp.path().join(".brain");
        fs::create_dir_all(&brain_dir).unwrap();

        let toml_cfg = BrainToml {
            name: "test-brain".to_string(),
            notes: vec![],
            id: None,
        };
        save_brain_toml(&brain_dir, &toml_cfg).unwrap();
        let loaded = load_brain_toml(&brain_dir).unwrap();
        assert_eq!(loaded.id, None);
    }

    #[test]
    fn get_or_generate_brain_id_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let brain_dir = tmp.path().join(".brain");
        fs::create_dir_all(&brain_dir).unwrap();
        fs::write(brain_dir.join("brain.toml"), "name = \"test\"\n").unwrap();

        let id1 = get_or_generate_brain_id(&brain_dir).unwrap();
        assert_eq!(id1.len(), 8);

        let id2 = get_or_generate_brain_id(&brain_dir).unwrap();
        assert_eq!(id1, id2, "second call should return same ID");
    }

    // -----------------------------------------------------------------------
    // list_brain_keys
    // -----------------------------------------------------------------------

    #[test]
    fn list_brain_keys_empty_when_no_config() {
        let fake_home = TempDir::new().unwrap();
        let keys = list_brain_keys(fake_home.path()).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn list_brain_keys_returns_name_and_id() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            "alpha".to_string(),
            BrainEntry {
                roots: vec![home.to_path_buf()],
                notes: vec![],
                id: Some("id000001".to_string()),
                aliases: vec![],
            },
        );
        cfg.brains.insert(
            "beta".to_string(),
            BrainEntry {
                roots: vec![home.to_path_buf()],
                notes: vec![],
                id: None,
                aliases: vec![],
            },
        );
        let text = toml::to_string_pretty(&cfg).unwrap();
        fs::write(home.join("config.toml"), text).unwrap();

        let keys = list_brain_keys(home).unwrap();
        // Sorted alphabetically
        assert_eq!(keys[0], ("alpha".to_string(), "id000001".to_string()));
        assert_eq!(keys[1], ("beta".to_string(), "".to_string()));
    }

    // -----------------------------------------------------------------------
    // open_remote_search_context
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn open_remote_search_context_returns_none_when_not_found() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        // Write an empty config so the file exists but has no brains
        fs::write(home.join("config.toml"), "[brains]\n").unwrap();

        // Dummy embedder
        let embedder: Arc<dyn crate::embedder::Embed> = Arc::new(DummyEmbedder);
        let model_dir = home.join("models");

        let result = open_remote_search_context(home, "nonexistent", &model_dir, &embedder)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn open_remote_search_context_opens_db_without_lancedb() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        // Register "test-brain" in config
        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            "test-brain".to_string(),
            BrainEntry {
                roots: vec![home.to_path_buf()],
                notes: vec![],
                id: Some("testid01".to_string()),
                aliases: vec![],
            },
        );
        let text = toml::to_string_pretty(&cfg).unwrap();
        fs::write(home.join("config.toml"), text).unwrap();

        let embedder: Arc<dyn crate::embedder::Embed> = Arc::new(DummyEmbedder);
        let model_dir = home.join("models");

        let ctx = open_remote_search_context(home, "test-brain", &model_dir, &embedder)
            .await
            .unwrap()
            .expect("should find test-brain");

        assert_eq!(ctx.brain_name, "test-brain");
        assert_eq!(ctx.brain_id, "testid01");
        // LanceDB dir doesn't exist, so store should be None
        assert!(ctx.store.is_none());
    }

    #[tokio::test]
    async fn open_remote_search_context_lookup_by_id() {
        let fake_home = TempDir::new().unwrap();
        let home = fake_home.path();

        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            "my-brain".to_string(),
            BrainEntry {
                roots: vec![home.to_path_buf()],
                notes: vec![],
                id: Some("abc12345".to_string()),
                aliases: vec![],
            },
        );
        let text = toml::to_string_pretty(&cfg).unwrap();
        fs::write(home.join("config.toml"), text).unwrap();

        let embedder: Arc<dyn crate::embedder::Embed> = Arc::new(DummyEmbedder);
        let model_dir = home.join("models");

        // Look up by ID instead of name
        let ctx = open_remote_search_context(home, "abc12345", &model_dir, &embedder)
            .await
            .unwrap()
            .expect("should find brain by id");

        assert_eq!(ctx.brain_name, "my-brain");
        assert_eq!(ctx.brain_id, "abc12345");
    }

    // -----------------------------------------------------------------------
    // roots backward compat and serialization
    // -----------------------------------------------------------------------

    #[test]
    fn test_roots_backward_compat() {
        // Old format: root = "..." scalar
        let toml_str = r#"
[brains.my-brain]
root = "/some/path"
"#;
        let cfg: GlobalConfig = toml::from_str(toml_str).unwrap();
        let entry = cfg.brains.get("my-brain").unwrap();
        assert_eq!(entry.roots, vec![PathBuf::from("/some/path")]);
    }

    #[test]
    fn test_roots_new_format() {
        // New format: roots = [...]
        let toml_str = r#"
[brains.my-brain]
roots = ["/path1", "/path2"]
"#;
        let cfg: GlobalConfig = toml::from_str(toml_str).unwrap();
        let entry = cfg.brains.get("my-brain").unwrap();
        assert_eq!(
            entry.roots,
            vec![PathBuf::from("/path1"), PathBuf::from("/path2")]
        );
    }

    #[test]
    fn test_primary_root() {
        let entry = BrainEntry {
            roots: vec![PathBuf::from("/primary"), PathBuf::from("/secondary")],
            notes: vec![],
            id: None,
            aliases: vec![],
        };
        assert_eq!(entry.primary_root(), PathBuf::from("/primary").as_path());
    }

    #[test]
    fn test_roots_serialization() {
        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            "my-brain".to_string(),
            BrainEntry {
                roots: vec![PathBuf::from("/path1"), PathBuf::from("/path2")],
                notes: vec![],
                id: None,
                aliases: vec![],
            },
        );
        let serialized = toml::to_string_pretty(&cfg).unwrap();
        // Should use roots = [...] not root = "..."
        assert!(
            serialized.contains("roots"),
            "serialized TOML should contain 'roots': {serialized}"
        );
        assert!(
            !serialized.contains("root ="),
            "serialized TOML should not contain legacy 'root =' field: {serialized}"
        );
        assert!(
            serialized.contains("/path1"),
            "serialized TOML should contain first path: {serialized}"
        );
        assert!(
            serialized.contains("/path2"),
            "serialized TOML should contain second path: {serialized}"
        );
    }

    #[test]
    fn test_aliases_default_empty() {
        // Config without aliases field — should deserialize to empty vec
        let toml_str = r#"
[brains.my-brain]
roots = ["/some/path"]
"#;
        let cfg: GlobalConfig = toml::from_str(toml_str).unwrap();
        let entry = cfg.brains.get("my-brain").unwrap();
        assert!(
            entry.aliases.is_empty(),
            "aliases should default to empty vec"
        );
    }

    // -----------------------------------------------------------------------
    // alias resolution
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_brain_entry_by_alias() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            "infra".to_string(),
            BrainEntry {
                roots: vec![tmp.path().to_path_buf()],
                notes: vec![],
                id: Some("abc12345".to_string()),
                aliases: vec!["gateway".to_string(), "infra-alias".to_string()],
            },
        );

        let (name, entry) = resolve_brain_entry_from_config("gateway", &cfg).unwrap();
        assert_eq!(name, "infra");
        assert_eq!(entry.primary_root(), tmp.path());

        let (name2, _) = resolve_brain_entry_from_config("infra-alias", &cfg).unwrap();
        assert_eq!(name2, "infra");
    }

    #[test]
    fn resolve_brain_entry_alias_does_not_shadow_name() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = GlobalConfig::default();
        // "infra" registered with an alias that matches another brain's name "gateway"
        cfg.brains.insert(
            "infra".to_string(),
            BrainEntry {
                roots: vec![tmp.path().to_path_buf()],
                notes: vec![],
                id: Some("aaaaaaaa".to_string()),
                aliases: vec!["gateway".to_string()],
            },
        );
        cfg.brains.insert(
            "gateway".to_string(),
            BrainEntry {
                roots: vec![tmp.path().to_path_buf()],
                notes: vec![],
                id: Some("bbbbbbbb".to_string()),
                aliases: vec![],
            },
        );

        // Name match wins over alias match.
        let (name, entry) = resolve_brain_entry_from_config("gateway", &cfg).unwrap();
        assert_eq!(name, "gateway");
        assert_eq!(entry.id, Some("bbbbbbbb".to_string()));
    }

    // -----------------------------------------------------------------------
    // find_brain_by_path / find_brain_by_id
    // -----------------------------------------------------------------------

    #[test]
    fn find_brain_by_path_returns_none_when_empty() {
        let cfg = GlobalConfig::default();
        let path = PathBuf::from("/some/path");
        assert!(find_brain_by_path(&cfg, &path).is_none());
    }

    #[test]
    fn find_brain_by_path_finds_primary_root() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("my-brain", Some("abcd1234"), tmp.path());

        let result = find_brain_by_path(&cfg, tmp.path());
        assert!(result.is_some());
        let (name, _entry) = result.unwrap();
        assert_eq!(name, "my-brain");
    }

    #[test]
    fn find_brain_by_path_finds_extra_root() {
        let tmp = TempDir::new().unwrap();
        let extra = TempDir::new().unwrap();
        let mut cfg = GlobalConfig::default();
        cfg.brains.insert(
            "my-brain".to_string(),
            BrainEntry {
                roots: vec![tmp.path().to_path_buf(), extra.path().to_path_buf()],
                notes: vec![],
                id: Some("abcd1234".to_string()),
                aliases: vec![],
            },
        );

        let result = find_brain_by_path(&cfg, extra.path());
        assert!(result.is_some());
        let (name, _entry) = result.unwrap();
        assert_eq!(name, "my-brain");
    }

    #[test]
    fn find_brain_by_path_returns_none_for_unregistered_path() {
        let tmp = TempDir::new().unwrap();
        let other = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("my-brain", Some("abcd1234"), tmp.path());

        let result = find_brain_by_path(&cfg, other.path());
        assert!(result.is_none());
    }

    #[test]
    fn find_brain_by_id_returns_none_when_empty() {
        let cfg = GlobalConfig::default();
        assert!(find_brain_by_id(&cfg, "abcd1234").is_none());
    }

    #[test]
    fn find_brain_by_id_finds_matching_entry() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("my-brain", Some("abcd1234"), tmp.path());

        let result = find_brain_by_id(&cfg, "abcd1234");
        assert!(result.is_some());
        let (name, _entry) = result.unwrap();
        assert_eq!(name, "my-brain");
    }

    #[test]
    fn find_brain_by_id_returns_none_for_unknown_id() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_global_config_with_brain("my-brain", Some("abcd1234"), tmp.path());

        assert!(find_brain_by_id(&cfg, "xxxxxxxx").is_none());
    }

    /// Minimal no-op embedder for tests that need Arc<dyn Embed>.
    struct DummyEmbedder;

    impl crate::embedder::Embed for DummyEmbedder {
        fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| vec![0.0f32; 384]).collect())
        }

        fn hidden_size(&self) -> usize {
            384
        }
    }
}
