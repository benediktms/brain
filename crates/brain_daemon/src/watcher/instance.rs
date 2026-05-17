//! Per-brain state held by the multi-brain watcher supervisor.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::config::resolve_paths_for_brain;
use brain_lib::mcp::McpContext;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::prelude::{Embed, WorkQueue};
use brain_persistence::db::Db;
use brain_persistence::store::Store;
use tracing::warn;

/// Per-brain state held by the multi-brain event loop.
pub struct BrainInstance {
    pub name: String,
    pub pipeline: IndexPipeline,
    pub work_queue: WorkQueue,
    pub note_dirs: Vec<PathBuf>,
    pub mcp_context: Arc<McpContext>,
}

/// Initialise a single [`BrainInstance`] from a brain name and note directories.
///
/// Accepts shared `Db` and `Store` handles — all brains use the unified
/// SQLite DB and unified LanceDB store.
pub async fn init_brain_instance(
    name: &str,
    notes: Vec<PathBuf>,
    embedder: Arc<dyn Embed>,
    brain_id: &str,
    db: Db,
    store: Store,
) -> Result<BrainInstance> {
    let paths = resolve_paths_for_brain(name)?;

    // Validate that at least one note directory exists
    let note_dirs: Vec<PathBuf> = notes
        .into_iter()
        .filter(|p| {
            if p.exists() {
                true
            } else {
                warn!(brain = %name, dir = %p.display(), "note directory does not exist, skipping");
                false
            }
        })
        .collect();

    // Create the pipeline with the shared embedder and shared store.
    // Schema version check is already done in run_multi() before cloning.
    let mut pipeline = IndexPipeline::with_embedder(db, store, embedder).await?;
    pipeline.set_brain_id(brain_id.to_string());

    // Resolve LLM provider: env vars first, then DB-backed credentials.
    let brain_home =
        brain_lib::config::brain_home().unwrap_or_else(|_| std::path::PathBuf::from("."));
    if let Some(provider) =
        brain_lib::llm::resolve_provider_with_db(pipeline.provider_store(), &brain_home)
    {
        pipeline.set_summarizer(Arc::from(provider));
    }

    // Build MCP context from the pipeline's stores + task/record/object stores.
    // Derive brain_data_dir from the per-brain data directory.
    let brain_data_dir = paths
        .sqlite_db
        .parent()
        .map(|h| h.join("brains").join(name))
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let brain_home_path = brain_data_dir
        .parent() // brains/
        .and_then(|p| p.parent()) // $BRAIN_HOME
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let stores = brain_lib::stores::BrainStores::from_dbs(
        pipeline.clone_db_for_spawn(),
        brain_id,
        &brain_data_dir,
        &brain_home_path,
    )?;
    let metrics = Arc::clone(pipeline.metrics());

    // Mirror `McpContext::bootstrap`'s soft-gate: when the pipeline has no
    // embedder configured, the search layer is unavailable and tools fall
    // back to tasks-only mode.
    let search = pipeline
        .embedder()
        .map(|e| brain_lib::search_service::SearchService {
            store: brain_persistence::store::StoreReader::from_store(pipeline.store()),
            embedder: Arc::clone(e),
        });

    let mcp_context = McpContext::from_stores(stores, search, None, metrics);

    Ok(BrainInstance {
        name: name.to_string(),
        pipeline,
        work_queue: WorkQueue::default(),
        note_dirs,
        mcp_context,
    })
}
