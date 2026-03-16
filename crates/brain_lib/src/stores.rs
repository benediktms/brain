//! Unified store access — hides DB wiring behind a single constructor.
//!
//! Every call site that needs TaskStore + RecordStore + ObjectStore goes through
//! `BrainStores` instead of manually resolving DB handles.

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::config;
use crate::db::Db;
use crate::error::{BrainCoreError, Result};
use crate::records::RecordStore;
use crate::records::objects::ObjectStore;
use crate::tasks::TaskStore;

/// Unified access to all brain stores.
///
/// All stores share a single `db` handle — `~/.brain/brain.db` scoped by
/// `brain_id`.
pub struct BrainStores {
    // Internal — not part of public API.
    db: Db,

    // Public — consumer API.
    pub tasks: TaskStore,
    pub records: RecordStore,
    pub objects: ObjectStore,
    pub brain_id: String,
    pub brain_name: String,
    pub brain_home: PathBuf,
}

impl BrainStores {
    /// Construct from a per-brain `sqlite_db` path.
    ///
    /// Resolves brain_home, opens the unified DB, resolves brain_id from the
    /// config registry, and builds all stores.
    pub fn from_path(sqlite_db: &Path) -> Result<Self> {
        Self::from_path_inner(sqlite_db, None)
    }

    /// Construct from a brain name or ID via the global registry.
    ///
    /// Resolves the brain entry, opens the DB, and builds all stores with
    /// audit trail if a project root exists.
    pub fn from_brain(name_or_id: &str) -> Result<Self> {
        let (name, entry) = config::resolve_brain_entry(name_or_id)?;
        let brain_id = config::resolve_brain_id(&entry, &name)?;
        let paths = config::resolve_paths_for_brain(&name)?;

        // Ensure data directory exists.
        if let Some(parent) = paths.sqlite_db.parent() {
            std::fs::create_dir_all(parent).map_err(BrainCoreError::Io)?;
        }

        let brain_data_dir = paths
            .sqlite_db
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf();

        // Derive brain_home from path convention.
        let brain_home = brain_data_dir
            .parent() // brains/
            .and_then(|p| p.parent()) // $BRAIN_HOME
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| brain_data_dir.clone());

        // Open the unified DB (~/.brain/brain.db) as the single database.
        let unified_db_path = brain_home.join("brain.db");
        let db = Db::open(&unified_db_path)?;

        // Audit trail: project-local JSONL if a root is available.
        let audit_path = entry
            .roots
            .first()
            .map(|root| root.join(".brain").join("tasks").join("events.jsonl"));

        let mut stores = Self::build(db, brain_id, name, &brain_data_dir, brain_home)?;

        if let Some(p) = audit_path {
            stores.tasks = stores.tasks.with_audit_path(p);
        }

        Ok(stores)
    }

    /// Low-level: from a pre-opened Db handle.
    ///
    /// Used by the daemon which already has a Db from IndexPipeline.
    /// `brain_data_dir` is the per-brain data directory (e.g.
    /// `~/.brain/brains/<name>/`).
    pub fn from_dbs(
        db: Db,
        brain_id: &str,
        brain_data_dir: &Path,
        brain_home: &Path,
    ) -> Result<Self> {
        let brain_name = brain_data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self::build(
            db,
            brain_id.to_string(),
            brain_name,
            brain_data_dir,
            brain_home.to_path_buf(),
        )
    }

    /// In-memory stores for testing. Returns `(TempDir, Self)` — caller owns
    /// the TempDir to keep the backing files alive.
    pub fn in_memory() -> Result<(TempDir, Self)> {
        Self::in_memory_with_brain_id("")
    }

    /// In-memory with explicit brain_id for scoped testing.
    pub fn in_memory_with_brain_id(brain_id: &str) -> Result<(TempDir, Self)> {
        let tmp = TempDir::new().map_err(BrainCoreError::Io)?;
        let db_path = tmp.path().join("brain.db");
        let db = Db::open(&db_path)?;

        let brain_id_str = brain_id.to_string();

        let tasks_dir = tmp.path().join("tasks");
        let tasks = if brain_id_str.is_empty() {
            TaskStore::new(&tasks_dir, db.clone())?
        } else {
            TaskStore::with_brain_id(&tasks_dir, db.clone(), &brain_id_str)?
        };

        let records_dir = tmp.path().join("records");
        let records = if brain_id_str.is_empty() {
            RecordStore::new(&records_dir, db.clone())?
        } else {
            RecordStore::with_brain_id(&records_dir, db.clone(), &brain_id_str)?
        };

        let objects_dir = tmp.path().join("objects");
        let objects = ObjectStore::new(&objects_dir)?;

        let brain_home = tmp.path().to_path_buf();
        Ok((
            tmp,
            Self {
                db,
                tasks,
                records,
                objects,
                brain_id: brain_id_str,
                brain_name: "test-brain".to_string(),
                brain_home,
            },
        ))
    }

    /// Access the underlying Db handle.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Set an audit trail path on the TaskStore.
    pub fn with_audit_path(mut self, path: PathBuf) -> Self {
        self.tasks = self.tasks.with_audit_path(path);
        self
    }

    // -- internals --

    fn from_path_inner(sqlite_db: &Path, brain_home_override: Option<&Path>) -> Result<Self> {
        let brain_data_dir = sqlite_db.parent().unwrap_or(Path::new("."));
        let brain_name = brain_data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        // Resolve brain_home: override > path convention > config > data dir.
        let path_derived_home = brain_data_dir
            .parent()
            .and_then(|p| p.parent())
            .filter(|p| p.join("brain.db").exists())
            .map(|p| p.to_path_buf());

        let brain_home = if let Some(ovr) = brain_home_override {
            ovr.to_path_buf()
        } else if let Some(ref derived) = path_derived_home {
            derived.clone()
        } else {
            config::brain_home().unwrap_or_else(|_| brain_data_dir.to_path_buf())
        };

        // Open the unified DB (~/.brain/brain.db) as the single database.
        // Falls back to the path-local brain.db when the unified DB does not yet exist.
        let unified_db_path = brain_home.join("brain.db");
        let db = if unified_db_path.exists() {
            Db::open(&unified_db_path)?
        } else {
            Db::open(sqlite_db)?
        };

        // Resolve brain_id from config registry.
        let brain_id = if !brain_name.is_empty() {
            config::resolve_brain_entry(&brain_name)
                .and_then(|(name, entry)| config::resolve_brain_id(&entry, &name))
                .unwrap_or_default()
        } else {
            String::new()
        };

        Self::build(db, brain_id, brain_name, brain_data_dir, brain_home)
    }

    /// Build all stores from a resolved Db handle and paths.
    fn build(
        db: Db,
        brain_id: String,
        brain_name: String,
        brain_data_dir: &Path,
        brain_home: PathBuf,
    ) -> Result<Self> {
        // Ensure the brain is registered before any writes.
        // The FK constraint on brain_id (v22) requires the brain to exist upfront.
        if !brain_id.is_empty() {
            db.ensure_brain_registered(&brain_id, &brain_name)?;
        }
        let tasks_dir = brain_data_dir.join("tasks");
        let tasks = if brain_id.is_empty() {
            TaskStore::new(&tasks_dir, db.clone())?
        } else {
            TaskStore::with_brain_id(&tasks_dir, db.clone(), &brain_id)?
        };

        let records_dir = brain_data_dir.join("records");
        let records = if brain_id.is_empty() {
            RecordStore::new(&records_dir, db.clone())?
        } else {
            RecordStore::with_brain_id(&records_dir, db.clone(), &brain_id)?
        };

        // ObjectStore: prefer unified brain_home/objects when available.
        let unified_objects = brain_home.join("objects");
        let objects_dir = if unified_objects.exists() {
            unified_objects
        } else {
            brain_data_dir.join("objects")
        };
        let objects = ObjectStore::new(&objects_dir)?;

        Ok(Self {
            db,
            tasks,
            records,
            objects,
            brain_id,
            brain_name,
            brain_home,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create standard brain directory structure and return unified DB path.
    fn make_brain_dirs(root: &Path, name: &str, create_unified: bool) -> PathBuf {
        let brain_data = root.join("brains").join(name);
        std::fs::create_dir_all(brain_data.join("tasks")).unwrap();
        std::fs::create_dir_all(brain_data.join("records")).unwrap();

        let per_brain_path = brain_data.join("brain.db");
        Db::open(&per_brain_path).unwrap();

        if create_unified {
            let unified_path = root.join("brain.db");
            Db::open(&unified_path).unwrap();
        }

        per_brain_path
    }

    #[test]
    fn in_memory_creates_functional_stores() {
        let (_tmp, stores) = BrainStores::in_memory().unwrap();

        // TaskStore round-trip
        let tasks = stores.tasks.list_all().unwrap();
        assert!(tasks.is_empty());

        // RecordStore round-trip
        let filter = crate::records::queries::RecordFilter {
            kind: None,
            status: None,
            tag: None,
            task_id: None,
            limit: None,
            brain_id: None,
        };
        let records = stores.records.list_records(&filter).unwrap();
        assert!(records.is_empty());

        // ObjectStore — write and read back
        let content_ref = stores.objects.write(b"hello").unwrap();
        let data = stores.objects.read(&content_ref.hash).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn from_path_with_unified_db() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "my-project", true);

        let stores = BrainStores::from_path_inner(&sqlite_db, Some(tmp.path())).unwrap();

        assert_eq!(stores.brain_home, tmp.path());
        assert_eq!(stores.brain_name, "my-project");
        // brain_id empty — no real config registry in tests
        assert!(stores.brain_id.is_empty());
    }

    #[test]
    fn from_path_without_unified_db() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "my-project", false);

        let stores = BrainStores::from_path_inner(&sqlite_db, Some(tmp.path())).unwrap();

        // No unified DB → falls back to per-brain path
        assert_eq!(stores.brain_home, tmp.path());
        assert!(stores.brain_id.is_empty());
    }

    #[test]
    fn brain_id_scoping_in_memory() {
        let (_tmp, stores) = BrainStores::in_memory_with_brain_id("test-abc").unwrap();

        assert_eq!(stores.brain_id, "test-abc");
        assert_eq!(stores.brain_name, "test-brain");

        // Stores are functional
        let tasks = stores.tasks.list_all().unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn from_dbs_wires_stores() {
        let tmp = TempDir::new().unwrap();
        let brain_data = tmp.path().join("brains").join("test-brain");
        std::fs::create_dir_all(&brain_data).unwrap();

        let db_path = brain_data.join("brain.db");
        let db = Db::open(&db_path).unwrap();

        let stores = BrainStores::from_dbs(db, "test-id", &brain_data, tmp.path()).unwrap();

        assert_eq!(stores.brain_id, "test-id");
        assert_eq!(stores.brain_name, "test-brain");

        // Stores are functional
        let tasks = stores.tasks.list_all().unwrap();
        assert!(tasks.is_empty());
    }
}
