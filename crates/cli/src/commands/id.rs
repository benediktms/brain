use anyhow::{Context, Result};
use brain_lib::config::get_or_generate_brain_id;

/// Show (or generate) the brain ID for the current project.
pub fn run() -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let brain_dir = cwd.join(".brain");
    let id = get_or_generate_brain_id(&brain_dir)
        .context("no brain initialized in this directory (run `brain init` first)")?;
    println!("{id}");
    Ok(())
}
