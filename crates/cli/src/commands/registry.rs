use anyhow::Result;
use brain_lib::config::{
    brain_home, load_global_config, open_remote_task_store, save_global_config,
};

/// List all registered brains.
pub fn run_list(json: bool, all: bool, archived_only: bool) -> Result<()> {
    let global = load_global_config()?;

    if global.brains.is_empty() {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "brains": [],
                    "count": 0
                }))?
            );
        } else {
            println!("No brains registered. Run `brain init` in a project directory.");
        }
        return Ok(());
    }

    let mut entries: Vec<(String, brain_lib::config::BrainEntry, Option<String>)> = global
        .brains
        .into_iter()
        .filter(|(_, entry)| {
            if archived_only {
                entry.archived
            } else if all {
                true
            } else {
                !entry.archived
            }
        })
        .map(|(name, entry)| {
            let prefix = open_remote_task_store(&name, &entry)
                .ok()
                .and_then(|store| store.get_project_prefix().ok());
            (name, entry, prefix)
        })
        .collect();

    // Sort by name for deterministic output.
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    if json {
        let brains: Vec<serde_json::Value> = entries
            .iter()
            .map(|(name, entry, prefix)| {
                let extra_roots: Vec<String> = entry
                    .roots
                    .iter()
                    .skip(1)
                    .map(|p| p.display().to_string())
                    .collect();
                serde_json::json!({
                    "name": name,
                    "id": entry.id,
                    "root": entry.primary_root().display().to_string(),
                    "aliases": entry.aliases,
                    "extra_roots": extra_roots,
                    "prefix": prefix,
                    "archived": entry.archived,
                })
            })
            .collect();
        let count = brains.len();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "brains": brains,
                "count": count
            }))?
        );
        return Ok(());
    }

    for (name, entry, prefix) in &entries {
        let archived_tag = if entry.archived { " [archived]" } else { "" };
        if let Some(ref id) = entry.id {
            println!("{name} [{id}]{archived_tag}");
        } else {
            println!("{name}{archived_tag}");
        }
        if !entry.aliases.is_empty() {
            println!("  aka:    {}", entry.aliases.join(", "));
        }
        println!("  root:   {}", entry.primary_root().display());
        for extra in entry.roots.iter().skip(1) {
            println!("          {}", extra.display());
        }
        if let Some(p) = prefix {
            println!("  prefix: {p}");
        }
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
