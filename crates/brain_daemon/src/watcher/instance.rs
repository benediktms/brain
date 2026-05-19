//! Per-brain state held by the multi-brain watcher supervisor.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::prelude::{Embed, WorkQueue};
use brain_persistence::db::Db;
use brain_persistence::store::Store;
use tracing::warn;

/// Per-brain state held by the multi-brain event loop.
pub struct BrainInstance {
    pub name: String,
    pub brain_id: String,
    pub pipeline: IndexPipeline,
    pub work_queue: WorkQueue,
    pub note_dirs: Vec<PathBuf>,
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
    // Schema version is ensured by `bootstrap_and_run` before constructing
    // per-brain instances; the clone here inherits an already-migrated DB.
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

    Ok(BrainInstance {
        name: name.to_string(),
        brain_id: brain_id.to_string(),
        pipeline,
        work_queue: WorkQueue::default(),
        note_dirs,
    })
}
