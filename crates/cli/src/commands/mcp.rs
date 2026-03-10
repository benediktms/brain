use std::path::PathBuf;

use anyhow::Result;
use tracing::info;

/// Start the MCP stdio server.
///
/// Uses layered bootstrap: always opens SQLite and creates a TaskStore first,
/// then optionally loads LanceDB and the embedder. If the embedding model is
/// missing or LanceDB fails to open, the server still starts in tasks-only mode
/// — task tools work, but memory/search tools return an error asking the user
/// to set up the model.
pub async fn run(model_dir: PathBuf, lance_db: PathBuf, sqlite_db: PathBuf) -> Result<()> {
    info!("starting MCP server");

    let ctx = brain_lib::mcp::McpContext::bootstrap(&model_dir, &lance_db, &sqlite_db).await?;
    brain_lib::mcp::run_server(ctx).await?;

    Ok(())
}
