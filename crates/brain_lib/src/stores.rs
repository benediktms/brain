//! Unified store access — hides DB wiring behind a single constructor.
//!
//! Every call site that needs TaskStore + RecordStore + ObjectStore goes through
//! `BrainStores` instead of manually resolving DB handles.

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::config;
use crate::error::{BrainCoreError, Result};
use crate::records::RecordStore;
use crate::records::objects::ObjectStore;
use crate::tasks::TaskStore;
use brain_persistence::db::Db;

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
    /// Construct from a `sqlite_db` path and optional per-brain `lance_db` path.
    ///
    /// When `lance_db` is provided, `brain_data_dir` and `brain_name` are
    /// derived from it (`lance_db.parent()` = per-brain data dir). This is
    /// required after consolidation because `sqlite_db` now points to the
    /// unified `~/.brain/brain.db`, not a per-brain path.
    ///
    /// Resolves brain_home, opens the unified DB, resolves brain_id from the
    /// config registry, and builds all stores.
    ///
    /// TODO: Legacy — this path-derivation approach predates the unified DB
    /// and registry. Callers should migrate to `from_brain` (name-based) or
    /// `from_dbs` (pre-opened handle) and this method should be removed.
    pub fn from_path(sqlite_db: &Path, lance_db: Option<&Path>) -> Result<Self> {
        let (brain_data_dir, brain_name) = if let Some(ldb) = lance_db {
            let data_dir = ldb.parent().unwrap_or(Path::new(".")).to_path_buf();
            let name = data_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            (data_dir, name)
        } else {
            // Fallback: derive from sqlite_db (legacy / unified DB path → empty name)
            let data_dir = sqlite_db.parent().unwrap_or(Path::new(".")).to_path_buf();
            let name = data_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            (data_dir, name)
        };

        // Hidden directories (e.g. ".brain" from ~/.brain/) are BRAIN_HOME
        // paths, not brain project names — treat them as unscoped.
        let brain_name = if brain_name.starts_with('.') {
            String::new()
        } else {
            brain_name
        };

        Self::from_path_inner(sqlite_db, &brain_data_dir, &brain_name, None)
    }

    /// Construct from a brain name or ID via the global registry.
    ///
    /// Resolves the brain entry, opens the DB, and builds all stores with
    /// audit trail if a project root exists.
    pub fn from_brain(name_or_id: &str) -> Result<Self> {
        let (name, entry) = config::resolve_brain_entry(name_or_id)?;
        let brain_id = config::resolve_brain_id(&entry, &name)?;
        let paths = config::resolve_paths_for_brain(&name)?;

        // brain_home is the parent of sqlite_db (which is now ~/.brain/brain.db).
        let brain_home = paths
            .sqlite_db
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| config::brain_home().unwrap_or_else(|_| PathBuf::from(".")));

        // Per-brain data dir: always derived from brain_home + name, independent
        // of sqlite_db path (which is now unified and not per-brain).
        let brain_data_dir = brain_home.join("brains").join(&name);

        // Ensure data directory and unified DB parent exist.
        std::fs::create_dir_all(&brain_data_dir).map_err(BrainCoreError::Io)?;
        if let Some(parent) = paths.sqlite_db.parent() {
            std::fs::create_dir_all(parent).map_err(BrainCoreError::Io)?;
        }

        // Open the unified DB (~/.brain/brain.db).
        let db = Db::open(&paths.sqlite_db)?;

        Self::build(db, brain_id, name, &brain_data_dir, brain_home)
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

        let tasks = if brain_id_str.is_empty() {
            TaskStore::new(db.clone())
        } else {
            TaskStore::with_brain_id(db.clone(), &brain_id_str, &brain_id_str)?
        };

        let records = if brain_id_str.is_empty() {
            RecordStore::new(db.clone())
        } else {
            RecordStore::with_brain_id(db.clone(), &brain_id_str, &brain_id_str)?
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

    pub(crate) fn inner_db(&self) -> &Db {
        &self.db
    }

    /// Test-only accessor for the underlying `Db` handle.
    ///
    /// Available only when the `test-utils` feature is enabled. Production
    /// code must use the port-trait impls or delegation methods on
    /// `BrainStores` instead.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn db_for_tests(&self) -> &Db {
        &self.db
    }

    // -----------------------------------------------------------------
    // Delegation methods — MCP handlers use these instead of .db()
    // -----------------------------------------------------------------

    // -- BrainRegistry --

    /// Check whether a brain has been archived.
    pub fn is_brain_archived(&self, brain_id: &str) -> Result<bool> {
        use crate::ports::BrainRegistry;
        BrainRegistry::is_brain_archived(&self.db, brain_id)
    }

    /// List all brain rows, optionally filtered to active-only.
    pub fn list_brains(
        &self,
        active_only: bool,
    ) -> Result<Vec<brain_persistence::db::schema::BrainRow>> {
        use crate::ports::BrainRegistry;
        BrainRegistry::list_brains(&self.db, active_only)
    }

    /// Return all active brain `(name, id)` pairs.
    pub fn list_brain_keys(&self) -> Result<Vec<(String, String)>> {
        use crate::ports::BrainRegistry;
        BrainRegistry::list_brain_keys(&self.db)
    }

    /// Resolve a brain by name, brain_id, alias, or root path.
    ///
    /// Returns `(brain_id, name)`. Resolution order: name → id → alias → root path.
    pub fn resolve_brain(&self, input: &str) -> Result<(String, String)> {
        self.db.resolve_brain(input)
    }

    /// Construct a `QueryPipeline` over the unified DB borrowed from this
    /// store. Callers supply the LanceDB-backed search store, embedder, and
    /// metrics handle from their context.
    pub fn query_pipeline<'a, S>(
        &'a self,
        store: &'a S,
        embedder: &'a std::sync::Arc<dyn crate::embedder::Embed>,
        metrics: &'a std::sync::Arc<crate::metrics::Metrics>,
    ) -> crate::query_pipeline::QueryPipeline<'a, S, Db>
    where
        S: crate::ports::ChunkSearcher + Send + Sync,
    {
        crate::query_pipeline::QueryPipeline::new(&self.db, store, embedder, metrics)
    }

    /// Construct a `FederatedPipeline` over the unified DB borrowed from this
    /// store. Per-brain entries are `(brain_name, optional store)`.
    pub fn federated_pipeline<'a, S>(
        &'a self,
        brains: Vec<(String, Option<S>)>,
        embedder: &'a std::sync::Arc<dyn crate::embedder::Embed>,
        metrics: &'a std::sync::Arc<crate::metrics::Metrics>,
    ) -> crate::query_pipeline::FederatedPipeline<'a, S, Db>
    where
        S: crate::ports::ChunkSearcher + Send + Sync,
    {
        crate::query_pipeline::FederatedPipeline {
            db: &self.db,
            brains,
            embedder,
            metrics,
        }
    }

    // -- EpisodeWriter / EpisodeReader --

    /// Store an episode. Returns the `summary_id`.
    pub fn store_episode(
        &self,
        episode: &brain_persistence::db::summaries::Episode,
    ) -> Result<String> {
        use crate::ports::EpisodeWriter;
        EpisodeWriter::store_episode(&self.db, episode)
    }

    /// List recent episodes, newest first.
    pub fn list_episodes(
        &self,
        limit: usize,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>> {
        use crate::ports::EpisodeReader;
        EpisodeReader::list_episodes(&self.db, limit, brain_id)
    }

    /// List recent episodes across multiple brains.
    pub fn list_episodes_multi_brain(
        &self,
        limit: usize,
        brain_ids: &[String],
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>> {
        use crate::ports::EpisodeReader;
        EpisodeReader::list_episodes_multi_brain(&self.db, limit, brain_ids)
    }

    /// Batch-load summaries by ID.
    pub fn get_summaries_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>> {
        use crate::ports::EpisodeReader;
        EpisodeReader::get_summaries_by_ids(&self.db, ids)
    }

    /// Load a single summary by ID. Returns `None` if no row exists.
    pub fn get_summary_by_id(
        &self,
        summary_id: &str,
    ) -> Result<Option<brain_persistence::db::summaries::SummaryRow>> {
        self.db.get_summary_by_id(summary_id)
    }

    /// Batch-load chunk rows by ID.
    pub fn get_chunks_by_ids(
        &self,
        chunk_ids: &[String],
    ) -> Result<Vec<brain_persistence::db::chunks::ChunkRow>> {
        use crate::ports::ChunkMetaReader;
        ChunkMetaReader::get_chunks_by_ids(&self.db, chunk_ids)
    }

    // -- ReflectionWriter --

    /// Store a reflection linked to source summaries. Returns the `summary_id`.
    pub fn store_reflection(
        &self,
        title: &str,
        content: &str,
        source_ids: &[String],
        tags: &[String],
        importance: f64,
        brain_id: &str,
    ) -> Result<String> {
        use crate::ports::ReflectionWriter;
        ReflectionWriter::store_reflection(
            &self.db, title, content, source_ids, tags, importance, brain_id,
        )
    }

    // -- ProcedureWriter --

    /// Store a procedure. Returns the `summary_id`.
    pub fn store_procedure(
        &self,
        title: &str,
        steps: &str,
        tags: &[String],
        importance: f64,
        brain_id: &str,
    ) -> Result<String> {
        use crate::ports::ProcedureWriter;
        ProcedureWriter::store_procedure(&self.db, title, steps, tags, importance, brain_id)
    }

    // -- StatusReader --

    /// Count files stuck in `indexing_started` state.
    pub fn count_stuck_files(&self) -> Result<u64> {
        use crate::ports::StatusReader;
        StatusReader::count_stuck_files(&self.db)
    }
    // -- BrainManager --

    /// Archive a brain by ID.
    pub fn archive_brain(&self, brain_id: &str) -> Result<()> {
        use crate::ports::BrainManager;
        BrainManager::archive_brain(&self.db, brain_id)
    }

    /// Get a brain row by ID.
    pub fn get_brain(
        &self,
        brain_id: &str,
    ) -> Result<Option<brain_persistence::db::schema::BrainRow>> {
        use crate::ports::BrainManager;
        BrainManager::get_brain(&self.db, brain_id)
    }

    // -- JobQueue (extended) --

    /// Get a single job by ID.
    pub fn get_job(&self, job_id: &str) -> Result<Option<brain_persistence::db::job::Job>> {
        use crate::ports::JobQueue;
        JobQueue::get_job(&self.db, job_id)
    }

    /// Update a job's status directly.
    pub fn update_job_status(
        &self,
        job_id: &str,
        status: &brain_persistence::db::job::JobStatus,
    ) -> Result<bool> {
        use crate::ports::JobQueue;
        JobQueue::update_job_status(&self.db, job_id, status)
    }

    /// List recent jobs filtered by optional status.
    pub fn list_jobs(
        &self,
        status: Option<&brain_persistence::db::job::JobStatus>,
        limit: i32,
    ) -> Result<Vec<brain_persistence::db::job::Job>> {
        self.db
            .with_read_conn(move |conn| brain_persistence::db::jobs::list_jobs(conn, status, limit))
    }

    // -- LinkWriter --

    /// Replace all links for a file.
    pub fn replace_links(
        &self,
        file_id: &str,
        links: &[brain_persistence::links::Link],
    ) -> Result<()> {
        use crate::ports::LinkWriter;
        LinkWriter::replace_links(&self.db, file_id, links)
    }

    // -- EmbeddingOps --

    /// Find chunks that need embedding.
    pub fn find_stale_chunks_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::chunks::ChunkPollRow>> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::find_stale_chunks_for_embedding(&self.db, brain_id)
    }

    /// Find summaries that need embedding.
    pub fn find_stale_summaries_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryPollRow>> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::find_stale_summaries_for_embedding(&self.db, brain_id)
    }

    /// Find tasks that need embedding.
    pub fn find_stale_tasks_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::tasks::queries::TaskPollRow>> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::find_stale_tasks_for_embedding(&self.db, brain_id)
    }

    /// Find records that need embedding.
    pub fn find_stale_records_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::records::queries::RecordPollRow>> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::find_stale_records_for_embedding(&self.db, brain_id)
    }

    /// Mark chunks as embedded.
    pub fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()> {
        use crate::ports::ChunkMetaWriter;
        ChunkMetaWriter::mark_chunks_embedded(&self.db, chunk_ids, timestamp)
    }

    /// Mark summaries as embedded.
    pub fn mark_summaries_embedded(&self, summary_ids: &[&str]) -> Result<()> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::mark_summaries_embedded(&self.db, summary_ids)
    }

    /// Mark tasks as embedded.
    pub fn mark_tasks_embedded(&self, task_ids: &[&str]) -> Result<()> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::mark_tasks_embedded(&self.db, task_ids)
    }

    /// Mark records as embedded.
    pub fn mark_records_embedded(&self, record_ids: &[&str]) -> Result<()> {
        use crate::ports::EmbeddingOps;
        EmbeddingOps::mark_records_embedded(&self.db, record_ids)
    }

    // -- MaintenanceOps --

    /// Rebuild the FTS index for summaries.
    pub fn reindex_summaries_fts(&self) -> Result<usize> {
        use crate::ports::MaintenanceOps;
        MaintenanceOps::reindex_summaries_fts(&self.db)
    }

    // -- FtsSearcher --

    /// Full-text search over summaries.
    pub fn search_summaries_fts(
        &self,
        query: &str,
        limit: usize,
        brain_ids: Option<&[String]>,
    ) -> Result<Vec<brain_persistence::db::fts::FtsSummaryResult>> {
        use crate::ports::FtsSearcher;
        FtsSearcher::search_summaries_fts(&self.db, query, limit, brain_ids)
    }

    /// Read the `stale_hashes_prevented` counter.
    pub fn stale_hashes_prevented(&self) -> Result<u64> {
        use crate::ports::StatusReader;
        StatusReader::stale_hashes_prevented(&self.db)
    }

    // -- JobQueue --

    /// Count jobs with the given status.
    pub fn count_jobs_by_status(
        &self,
        status: &brain_persistence::db::job::JobStatus,
    ) -> Result<i64> {
        use crate::ports::JobQueue;
        JobQueue::count_jobs_by_status(&self.db, status)
    }

    /// List recent jobs filtered by status.
    pub fn list_jobs_by_status(
        &self,
        status: &brain_persistence::db::job::JobStatus,
        limit: i32,
    ) -> Result<Vec<brain_persistence::db::job::Job>> {
        use crate::ports::JobQueue;
        JobQueue::list_jobs_by_status(&self.db, status, limit)
    }

    /// List stuck jobs.
    pub fn list_stuck_jobs(&self) -> Result<Vec<brain_persistence::db::job::Job>> {
        use crate::ports::JobQueue;
        JobQueue::list_stuck_jobs(&self.db)
    }

    /// Enqueue a new job. Returns the `job_id`.
    pub fn enqueue_job(
        &self,
        input: &brain_persistence::db::jobs::EnqueueJobInput,
    ) -> Result<String> {
        use crate::ports::JobQueue;
        JobQueue::enqueue_job(&self.db, input)
    }

    // -- ChunkMetaWriter --

    /// Upsert a record capsule chunk into SQLite.
    pub fn upsert_record_chunk(&self, record_file_id: &str, capsule_text: &str) -> Result<()> {
        use crate::ports::ChunkMetaWriter;
        ChunkMetaWriter::upsert_record_chunk(&self.db, record_file_id, capsule_text, &self.brain_id)
    }

    /// Clone this store bundle with a different brain_id.
    ///
    /// All shared resources (Db, ObjectStore root) are re-used. TaskStore and
    /// RecordStore are re-created scoped to `brain_id`.
    pub fn with_brain_id(&self, brain_id: &str, brain_name: &str) -> Result<Self> {
        let tasks = TaskStore::with_brain_id(self.db.clone(), brain_id, brain_name)?;
        let records = RecordStore::with_brain_id(self.db.clone(), brain_id, brain_name)?;
        let objects = ObjectStore::new(self.objects.root())?;
        Ok(Self {
            db: self.db.clone(),
            tasks,
            records,
            objects,
            brain_id: brain_id.to_string(),
            brain_name: brain_name.to_string(),
            brain_home: self.brain_home.clone(),
        })
    }

    // -- internals --

    fn from_path_inner(
        sqlite_db: &Path,
        brain_data_dir: &Path,
        brain_name: &str,
        brain_home_override: Option<&Path>,
    ) -> Result<Self> {
        let brain_name = brain_name.to_string();

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
        let tasks = if brain_id.is_empty() {
            TaskStore::new(db.clone())
        } else {
            TaskStore::with_brain_id(db.clone(), &brain_id, &brain_name)?
        };

        let records = if brain_id.is_empty() {
            RecordStore::new(db.clone())
        } else {
            RecordStore::with_brain_id(db.clone(), &brain_id, &brain_name)?
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
        let brain_data_dir = tmp.path().join("brains").join("my-project");

        let stores = BrainStores::from_path_inner(
            &sqlite_db,
            &brain_data_dir,
            "my-project",
            Some(tmp.path()),
        )
        .unwrap();

        assert_eq!(stores.brain_home, tmp.path());
        assert_eq!(stores.brain_name, "my-project");
        // brain_id empty — no real config registry in tests
        assert!(stores.brain_id.is_empty());
    }

    #[test]
    fn from_path_without_unified_db() {
        let tmp = TempDir::new().unwrap();
        let sqlite_db = make_brain_dirs(tmp.path(), "my-project", false);
        let brain_data_dir = tmp.path().join("brains").join("my-project");

        let stores = BrainStores::from_path_inner(
            &sqlite_db,
            &brain_data_dir,
            "my-project",
            Some(tmp.path()),
        )
        .unwrap();

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
    fn with_brain_id_rescopes_stores() {
        let (_tmp, stores) = BrainStores::in_memory_with_brain_id("brain-a").unwrap();
        assert_eq!(stores.brain_id, "brain-a");

        let rescoped = stores.with_brain_id("brain-b", "other-brain").unwrap();
        assert_eq!(rescoped.brain_id, "brain-b");
        assert_eq!(rescoped.brain_name, "other-brain");

        // Stores are functional
        let tasks = rescoped.tasks.list_all().unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn from_path_rejects_dotfile_brain_name() {
        // Simulates ~/.brain/lancedb where parent is ~/.brain — the directory
        // name ".brain" should not become a brain_name.
        let tmp = TempDir::new().unwrap();
        let dot_brain = tmp.path().join(".brain");
        std::fs::create_dir_all(dot_brain.join("brains").join(".brain")).unwrap();

        let sqlite_db = dot_brain.join("brain.db");
        Db::open(&sqlite_db).unwrap();

        let lance_db = dot_brain.join("lancedb");
        std::fs::create_dir_all(&lance_db).unwrap();

        let stores = BrainStores::from_path(&sqlite_db, Some(&lance_db)).unwrap();
        assert!(
            stores.brain_name.is_empty(),
            "dotfile directory name should not become brain_name, got: {:?}",
            stores.brain_name
        );
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
