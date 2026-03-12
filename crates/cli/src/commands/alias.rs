use anyhow::{Result, bail};
use brain_lib::config::{load_global_config, save_global_config};

/// Add an alias to a brain entry.
pub fn run_add(brain: &str, alias: &str) -> Result<()> {
    let mut global = load_global_config()?;

    // Validate: alias must not collide with any existing brain name.
    if global.brains.contains_key(alias) {
        bail!("alias \"{alias}\" collides with an existing brain name");
    }

    // Validate: alias must not collide with any existing alias on any brain.
    for (name, entry) in &global.brains {
        if entry.aliases.iter().any(|a| a == alias) {
            bail!("alias \"{alias}\" already exists on brain \"{name}\"");
        }
    }

    let entry = global
        .brains
        .get_mut(brain)
        .ok_or_else(|| anyhow::anyhow!("brain \"{brain}\" is not registered"))?;

    if entry.aliases.iter().any(|a| a == alias) {
        bail!("alias \"{alias}\" already exists on brain \"{brain}\"");
    }

    entry.aliases.push(alias.to_string());
    save_global_config(&global)?;
    println!("Added alias \"{alias}\" to brain \"{brain}\".");

    // Signal daemon to reload registry (best-effort).
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    Ok(())
}

/// Remove an alias from a brain entry.
pub fn run_remove(brain: &str, alias: &str) -> Result<()> {
    let mut global = load_global_config()?;

    let entry = global
        .brains
        .get_mut(brain)
        .ok_or_else(|| anyhow::anyhow!("brain \"{brain}\" is not registered"))?;

    let pos = entry
        .aliases
        .iter()
        .position(|a| a == alias)
        .ok_or_else(|| anyhow::anyhow!("alias \"{alias}\" not found on brain \"{brain}\""))?;

    entry.aliases.remove(pos);
    save_global_config(&global)?;
    println!("Removed alias \"{alias}\" from brain \"{brain}\".");

    // Signal daemon to reload registry (best-effort).
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    Ok(())
}

/// List aliases for a specific brain, or all aliases across all brains.
pub fn run_list(brain: Option<&str>) -> Result<()> {
    let global = load_global_config()?;

    if let Some(brain_name) = brain {
        let entry = global
            .brains
            .get(brain_name)
            .ok_or_else(|| anyhow::anyhow!("brain \"{brain_name}\" is not registered"))?;

        if entry.aliases.is_empty() {
            println!("{brain_name}: (no aliases)");
        } else {
            println!("{brain_name}: {}", entry.aliases.join(", "));
        }
    } else {
        let mut entries: Vec<(&String, &brain_lib::config::BrainEntry)> =
            global.brains.iter().collect();
        entries.sort_by_key(|(name, _)| *name);

        let mut found = false;
        for (name, entry) in entries {
            if !entry.aliases.is_empty() {
                for alias in &entry.aliases {
                    println!("{alias} -> {name}");
                }
                found = true;
            }
        }

        if !found {
            println!("No aliases registered.");
        }
    }

    Ok(())
}
