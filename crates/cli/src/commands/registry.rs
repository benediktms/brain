use anyhow::Result;
use brain_lib::config::{brain_home, load_global_config, save_global_config};

/// List all registered brains.
pub fn run_list() -> Result<()> {
    let global = load_global_config()?;

    if global.brains.is_empty() {
        println!("No brains registered. Run `brain init` in a project directory.");
        return Ok(());
    }

    for (name, entry) in &global.brains {
        let notes: Vec<String> = entry
            .notes
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        println!("{name}");
        println!("  root:  {}", entry.root.display());
        println!("  notes: {}", notes.join(", "));
    }

    Ok(())
}

/// Remove a registered brain from the global config.
pub fn run_remove(name: &str, purge: bool) -> Result<()> {
    let mut global = load_global_config()?;

    if global.brains.remove(name).is_none() {
        anyhow::bail!("brain \"{name}\" is not registered");
    }

    save_global_config(&global)?;
    println!("Removed brain \"{name}\" from registry.");

    // Signal daemon to reload registry (best-effort)
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    if purge {
        let brains_dir = brain_home()?.join("brains").join(name);
        if brains_dir.exists() {
            std::fs::remove_dir_all(&brains_dir)?;
            println!("Purged derived data at {}", brains_dir.display());
        }
    }

    Ok(())
}
