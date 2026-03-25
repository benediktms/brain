use anyhow::{Result, bail};
use brain_lib::config::{brain_home, load_global_config, save_global_config};
use brain_lib::db::Db;
use brain_lib::db::schema::BrainUpsert;

/// Add an alias to a brain entry.
pub fn run_add(brain: &str, alias: &str) -> Result<()> {
    let home = brain_home().map_err(|e| anyhow::anyhow!("{e}"))?;
    let db = Db::open(&home.join("brain.db"))?;

    // Resolve the target brain from DB.
    let (brain_id, brain_name) = db
        .resolve_brain(brain)
        .map_err(|_| anyhow::anyhow!("brain \"{brain}\" is not registered"))?;

    // Validate: alias must not collide with any existing brain name.
    if db
        .get_brain_by_name(alias)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .is_some()
    {
        bail!("alias \"{alias}\" collides with an existing brain name");
    }

    // Validate: alias must not already exist on any brain.
    let all_brains = db.list_brains(false)?;
    for row in &all_brains {
        if let Some(ref aj) = row.aliases_json
            && let Ok(aliases) = serde_json::from_str::<Vec<String>>(aj)
            && aliases.iter().any(|a| a == alias)
        {
            bail!("alias \"{alias}\" already exists on brain \"{}\"", row.name);
        }
    }

    // Read current aliases, add new one, write back.
    let brain_row = db
        .get_brain(&brain_id)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("brain not found"))?;

    let mut aliases: Vec<String> = brain_row
        .aliases_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    if aliases.iter().any(|a| a == alias) {
        bail!("alias \"{alias}\" already exists on brain \"{brain_name}\"");
    }

    aliases.push(alias.to_string());
    let aliases_json = serde_json::to_string(&aliases)?;

    db.upsert_brain(&BrainUpsert {
        brain_id: &brain_id,
        name: &brain_name,
        prefix: brain_row.prefix.as_deref().unwrap_or("BRN"),
        roots_json: brain_row.roots_json.as_deref().unwrap_or("[]"),
        notes_json: brain_row.notes_json.as_deref().unwrap_or("[]"),
        aliases_json: &aliases_json,
        archived: brain_row.archived,
    })?;

    // Project to config.toml.
    if let Ok(mut global) = load_global_config() {
        if let Some(entry) = global.brains.get_mut(&brain_name) {
            entry.aliases = aliases;
        }
        let _ = save_global_config(&global);
    }

    println!("Added alias \"{alias}\" to brain \"{brain_name}\".");

    // Signal daemon to reload registry (best-effort).
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    Ok(())
}

/// Remove an alias from a brain entry.
pub fn run_remove(brain: &str, alias: &str) -> Result<()> {
    let home = brain_home().map_err(|e| anyhow::anyhow!("{e}"))?;
    let db = Db::open(&home.join("brain.db"))?;

    let (brain_id, brain_name) = db
        .resolve_brain(brain)
        .map_err(|_| anyhow::anyhow!("brain \"{brain}\" is not registered"))?;

    let brain_row = db
        .get_brain(&brain_id)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("brain not found"))?;

    let mut aliases: Vec<String> = brain_row
        .aliases_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    let pos = aliases
        .iter()
        .position(|a| a == alias)
        .ok_or_else(|| anyhow::anyhow!("alias \"{alias}\" not found on brain \"{brain_name}\""))?;

    aliases.remove(pos);
    let aliases_json = serde_json::to_string(&aliases)?;

    db.upsert_brain(&BrainUpsert {
        brain_id: &brain_id,
        name: &brain_name,
        prefix: brain_row.prefix.as_deref().unwrap_or("BRN"),
        roots_json: brain_row.roots_json.as_deref().unwrap_or("[]"),
        notes_json: brain_row.notes_json.as_deref().unwrap_or("[]"),
        aliases_json: &aliases_json,
        archived: brain_row.archived,
    })?;

    // Project to config.toml.
    if let Ok(mut global) = load_global_config() {
        if let Some(entry) = global.brains.get_mut(&brain_name) {
            entry.aliases = aliases;
        }
        let _ = save_global_config(&global);
    }

    println!("Removed alias \"{alias}\" from brain \"{brain_name}\".");

    // Signal daemon to reload registry (best-effort).
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    Ok(())
}

/// List aliases for a specific brain, or all aliases across all brains.
pub fn run_list(brain: Option<&str>) -> Result<()> {
    let home = brain_home().map_err(|e| anyhow::anyhow!("{e}"))?;
    let db = Db::open(&home.join("brain.db"))?;

    if let Some(brain_name) = brain {
        let (brain_id, resolved_name) = db
            .resolve_brain(brain_name)
            .map_err(|_| anyhow::anyhow!("brain \"{brain_name}\" is not registered"))?;

        let brain_row = db
            .get_brain(&brain_id)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("brain not found"))?;

        let aliases: Vec<String> = brain_row
            .aliases_json
            .as_deref()
            .and_then(|j| serde_json::from_str(j).ok())
            .unwrap_or_default();

        if aliases.is_empty() {
            println!("{resolved_name}: (no aliases)");
        } else {
            println!("{resolved_name}: {}", aliases.join(", "));
        }
    } else {
        let all_brains = db.list_brains(false)?;
        let mut found = false;
        for row in &all_brains {
            let aliases: Vec<String> = row
                .aliases_json
                .as_deref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();
            if !aliases.is_empty() {
                for alias in &aliases {
                    println!("{alias} -> {}", row.name);
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
