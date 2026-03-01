use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use brain_lib::pipeline::IndexPipeline;

/// Start the MCP stdio server.
///
/// Opens SQLite, LanceDB, and loads the embedder, then hands off to
/// the MCP JSON-RPC server which reads from stdin and writes to stdout.
pub async fn run(model_dir: PathBuf, lance_db: PathBuf, sqlite_db: PathBuf) -> Result<()> {
    info!("starting MCP server");

    let pipeline = IndexPipeline::new(&model_dir, &lance_db, &sqlite_db).await?;

    // Extract components for the MCP context.
    // IndexPipeline owns Db/Store/Embedder — we need a way to share them.
    // For now, open a second set of handles (SQLite WAL allows concurrent readers).
    let db = brain_lib::db::Db::open(&sqlite_db)?;
    let store = brain_lib::store::Store::open_or_create(&lance_db).await?;
    let embedder: Arc<dyn brain_lib::embedder::Embed> =
        Arc::new(brain_lib::embedder::Embedder::load(&model_dir)?);

    // Keep the pipeline alive so LanceDB isn't dropped
    let _pipeline = pipeline;

    let ctx = Arc::new(brain_lib::mcp::McpContext {
        db,
        store,
        embedder,
    });

    brain_lib::mcp::run_server(ctx).await?;

    Ok(())
}
