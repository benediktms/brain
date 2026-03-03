use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use brain_lib::pipeline::IndexPipeline;

/// Start the MCP stdio server.
///
/// Opens SQLite, LanceDB, and loads the embedder via IndexPipeline, then
/// hands off to the MCP JSON-RPC server which reads from stdin and writes
/// to stdout.
pub async fn run(model_dir: PathBuf, lance_db: PathBuf, sqlite_db: PathBuf) -> Result<()> {
    info!("starting MCP server");

    let pipeline = IndexPipeline::new(&model_dir, &lance_db, &sqlite_db).await?;

    // Reuse Db and Embedder from the pipeline (cheap Arc clones).
    // Store doesn't implement Clone, so we open a second handle; SQLite WAL
    // allows concurrent readers and LanceDB is fine with parallel connections.
    let db = pipeline.db().clone();
    let embedder = Arc::clone(pipeline.embedder());
    let store = brain_lib::store::Store::open_or_create(&lance_db).await?;

    // Task store: derive tasks_dir from sqlite_db parent (e.g. .brain/tasks/)
    let tasks_dir = sqlite_db
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("tasks");
    let tasks_db = pipeline.db().clone();
    let tasks = brain_lib::tasks::TaskStore::new(&tasks_dir, tasks_db)?;

    // Keep the pipeline alive so its internal LanceDB table handles remain valid.
    let _pipeline = pipeline;

    let ctx = Arc::new(brain_lib::mcp::McpContext {
        db,
        store,
        embedder,
        tasks,
    });

    brain_lib::mcp::run_server(ctx).await?;

    Ok(())
}
