use anyhow::{Context, Result};
use brain_lib::config::{
    BrainToml, brain_home, load_brain_toml, load_global_config, save_brain_toml, save_global_config,
};
use brain_lib::db::Db;
use brain_lib::db::schema::BrainUpsert;
use std::fs;
use std::path::PathBuf;

/// Link the current directory as an additional root for an existing brain.
pub fn run(name: &str) -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("cannot canonicalize {}", cwd.display()))?;

    // Resolve brain from DB (source of truth).
    let home = brain_home().map_err(|e| anyhow::anyhow!("{e}"))?;
    let db_path = home.join("brain.db");
    let db = Db::open(&db_path).context("failed to open brain DB")?;

    let (brain_id, brain_name) = db
        .resolve_brain(name)
        .map_err(|_| anyhow::anyhow!("Error: brain '{}' not found in registry", name))?;

    let brain_row = db
        .get_brain(&brain_id)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("brain '{}' not found in DB", brain_id))?;

    // Parse existing roots from DB.
    let mut roots: Vec<PathBuf> = brain_row
        .roots_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    // Error if cwd is already a root of this brain.
    if roots.contains(&cwd) {
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
        if let Some(ref local_id) = local_toml.id
            && !brain_id.is_empty()
            && local_id != &brain_id
        {
            anyhow::bail!(
                "'{}' already belongs to a different brain ({})",
                cwd.display(),
                local_id
            );
        }
    }

    // Create .brain/ directory and write marker files.
    fs::create_dir_all(&brain_dir)?;

    let brain_toml = BrainToml {
        name: brain_name.clone(),
        notes: vec![],
        id: if brain_id.is_empty() {
            None
        } else {
            Some(brain_id.clone())
        },
        prefix: brain_row.prefix.clone(),
    };
    save_brain_toml(&brain_dir, &brain_toml)?;

    let gitignore_path = brain_dir.join(".gitignore");
    if !gitignore_path.exists() {
        fs::write(
            &gitignore_path,
            "# Derived data — do not commit\nbrain.db*\nlancedb/\nmodels/\n",
        )?;
    }

    // Append cwd to roots and update DB (source of truth).
    roots.push(cwd.clone());
    let was_archived = brain_row.archived;
    let roots_json = serde_json::to_string(&roots)?;
    let notes_json = brain_row.notes_json.as_deref().unwrap_or("[]");
    let aliases_json = brain_row.aliases_json.as_deref().unwrap_or("[]");
    let prefix = brain_row
        .prefix
        .as_deref()
        .unwrap_or_else(|| brain_lib::db::meta::generate_prefix(&brain_name).leak());
    db.upsert_brain(&BrainUpsert {
        brain_id: &brain_id,
        name: &brain_name,
        prefix,
        roots_json: &roots_json,
        notes_json,
        aliases_json,
        archived: false, // unarchive if linking
    })?;

    // Project to config.toml (read-only projection).
    if let Ok(mut config) = load_global_config() {
        if let Some(entry) = config.brains.get_mut(&brain_name) {
            entry.roots.push(cwd.clone());
            if was_archived {
                entry.archived = false;
            }
        }
        let _ = save_global_config(&config);
    }

    if was_archived {
        println!("Brain '{}' has been unarchived.", brain_name);
    }

    // Signal daemon to reload registry (best-effort).
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    println!("Linked {} → {}", cwd.display(), brain_name);

    Ok(())
}
