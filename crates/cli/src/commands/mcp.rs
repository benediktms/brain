use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use brain_lib::pipeline::IndexPipeline;

/// Start the MCP stdio server.
///
/// Opens SQLite and tries to load the embedder via IndexPipeline. If the
/// embedding model is missing, the server still starts — task tools work,
/// but memory/search tools return an error asking the user to set up the model.
pub async fn run(model_dir: PathBuf, lance_db: PathBuf, sqlite_db: PathBuf) -> Result<()> {
    info!("starting MCP server");

    let tasks_dir = sqlite_db
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("tasks");

    match IndexPipeline::new(&model_dir, &lance_db, &sqlite_db).await {
        Ok(pipeline) => {
            let db = pipeline.db().clone();
            let embedder = Arc::clone(pipeline.embedder());
            let store_reader = brain_lib::store::StoreReader::from_store(pipeline.store());
            let tasks_db = pipeline.db().clone();
            let tasks = brain_lib::tasks::TaskStore::new(&tasks_dir, tasks_db)?;
            tasks.rebuild_projections()?;
            let metrics = Arc::clone(pipeline.metrics());
            let _pipeline = pipeline;

            let ctx = Arc::new(brain_lib::mcp::McpContext {
                db,
                store: Some(store_reader),
                embedder: Some(embedder),
                tasks,
                metrics,
            });
            brain_lib::mcp::run_server(ctx).await?;
        }
        Err(e) => {
            info!("embedding model unavailable ({e}), starting in tasks-only mode");
            let db = brain_lib::db::Db::open(&sqlite_db)?;
            let tasks_db = brain_lib::db::Db::open(&sqlite_db)?;
            let tasks = brain_lib::tasks::TaskStore::new(&tasks_dir, tasks_db)?;
            tasks.rebuild_projections()?;

            let ctx = Arc::new(brain_lib::mcp::McpContext {
                db,
                store: None,
                embedder: None,
                tasks,
                metrics: Arc::new(brain_lib::metrics::Metrics::new()),
            });
            brain_lib::mcp::run_server(ctx).await?;
        }
    }

    Ok(())
}
