use anyhow::{Context, Result};
use brain_lib::config::load_brain_toml;

/// Regenerate AGENTS.md and bridge CLAUDE.md from the current brain config.
pub fn run() -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let brain_toml = load_brain_toml(&cwd.join(".brain"))
        .context("no brain initialized in this directory (run `brain init` first)")?;

    super::init::upsert_agent_docs(&cwd, &brain_toml.name)
}
