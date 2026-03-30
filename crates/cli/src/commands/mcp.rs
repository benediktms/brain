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
pub async fn run(
    mut model_dir: PathBuf,
    mut lance_db: PathBuf,
    mut sqlite_db: PathBuf,
) -> Result<()> {
    info!("starting MCP server");

    // If resolve_defaults couldn't find a brain from cwd (paths still at
    // relative defaults), fall back to the global config registry.
    if !sqlite_db.is_absolute()
        && let Some((name, resolved)) = resolve_from_registry()
    {
        info!(brain = %name, "resolved brain from global config (cwd fallback)");
        model_dir = resolved.model_dir;
        lance_db = resolved.lance_db;
        sqlite_db = resolved.sqlite_db;
    }

    let ctx = brain_lib::mcp::McpContext::bootstrap(&model_dir, &lance_db, &sqlite_db).await?;
    brain_lib::mcp::run_server(ctx).await?;

    Ok(())
}

/// Try to resolve brain paths from the DB (source of truth).
///
/// First checks if cwd falls under any registered brain's roots.
/// Falls back to the first brain alphabetically if no root matches.
fn resolve_from_registry() -> Option<(String, brain_lib::config::ResolvedPaths)> {
    let home = brain_lib::config::brain_home().ok()?;
    let db = brain_persistence::db::Db::open(&home.join("brain.db")).ok()?;
    let brain_rows = db.list_brains(true).ok()?;
    let cwd = std::env::current_dir().ok();

    // Try to find a brain whose root contains the cwd.
    if let Some(ref cwd) = cwd {
        for row in &brain_rows {
            let roots: Vec<std::path::PathBuf> = row
                .roots_json
                .as_deref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();
            if roots.iter().any(|r| cwd.starts_with(r))
                && let Ok(resolved) = brain_lib::config::resolve_paths_for_brain(&row.name)
            {
                return Some((row.name.clone(), resolved));
            }
        }
    }

    // Fall back to first brain alphabetically.
    let name = &brain_rows.first()?.name;
    let resolved = brain_lib::config::resolve_paths_for_brain(name).ok()?;
    Some((name.clone(), resolved))
}
