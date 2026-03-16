use anyhow::{Context, Result};
use brain_lib::config::{
    BrainToml, load_brain_toml, load_global_config, resolve_brain_entry_from_config,
    resolve_paths_for_brain, save_brain_toml, save_global_config,
};
use brain_lib::db::Db;
use std::fs;

/// Link the current directory as an additional root for an existing brain.
pub fn run(name: &str) -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("cannot canonicalize {}", cwd.display()))?;

    let mut config = load_global_config()?;

    let (brain_name, entry) = resolve_brain_entry_from_config(name, &config)
        .map_err(|_| anyhow::anyhow!("Error: brain '{}' not found in registry", name))?;

    // Error if cwd is already a root of this brain.
    if entry.roots.contains(&cwd) {
        anyhow::bail!(
            "'{}' is already a root of brain '{}'",
            cwd.display(),
            brain_name
        );
    }

    let brain_dir = cwd.join(".brain");
    let marker_path = brain_dir.join("brain.toml");

    // If .brain/brain.toml exists, verify it belongs to the same brain.
    if marker_path.exists() {
        let local_toml =
            load_brain_toml(&brain_dir).context("failed to read existing .brain/brain.toml")?;
        if let Some(ref local_id) = local_toml.id {
            let entry_id = entry.id.as_deref().unwrap_or("");
            if !entry_id.is_empty() && local_id != entry_id {
                // Find the name of the other brain this directory belongs to.
                let other_name = config
                    .brains
                    .iter()
                    .find(|(_, e)| e.id.as_deref() == Some(local_id.as_str()))
                    .map(|(n, _)| n.clone())
                    .unwrap_or_else(|| local_toml.name.clone());
                anyhow::bail!(
                    "'{}' already belongs to brain '{}' ({})",
                    cwd.display(),
                    other_name,
                    local_id
                );
            }
        }
    }

    // Create .brain/ directory and write marker files.
    fs::create_dir_all(&brain_dir)?;

    let brain_id = entry.id.clone().unwrap_or_default();
    let brain_toml = BrainToml {
        name: brain_name.clone(),
        notes: vec![],
        id: if brain_id.is_empty() {
            None
        } else {
            Some(brain_id.clone())
        },
    };
    save_brain_toml(&brain_dir, &brain_toml)?;

    let gitignore_path = brain_dir.join(".gitignore");
    if !gitignore_path.exists() {
        fs::write(
            &gitignore_path,
            "# Derived data — do not commit\nbrain.db*\nlancedb/\nmodels/\n",
        )?;
    }

    // Append cwd to this brain's roots in the global config.
    let was_archived = config.brains.get(&brain_name).unwrap().archived;
    config
        .brains
        .get_mut(&brain_name)
        .unwrap()
        .roots
        .push(cwd.clone());
    if was_archived {
        config.brains.get_mut(&brain_name).unwrap().archived = false;
    }
    save_global_config(&config)?;

    // If the brain was archived, clear the archived flag in the DB as well.
    if was_archived && !brain_id.is_empty() {
        let paths = resolve_paths_for_brain(&brain_name)
            .context("failed to resolve brain paths for unarchive")?;
        if paths.sqlite_db.exists() {
            let db = Db::open(&paths.sqlite_db).context("failed to open brain DB for unarchive")?;
            db.with_write_conn(|conn| {
                conn.execute(
                    "UPDATE brains SET archived = 0 WHERE brain_id = ?1",
                    rusqlite::params![brain_id],
                )?;
                Ok(())
            })?;
        }
        println!("Brain '{}' has been unarchived.", brain_name);
    }

    // Signal daemon to reload registry (best-effort).
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    println!("Linked {} → {}", cwd.display(), brain_name);

    Ok(())
}
