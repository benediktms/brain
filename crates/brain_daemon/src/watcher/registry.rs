//! Brain-registry housekeeping: brain-ID sync, SIGHUP reload, root
//! validation, DB <-> state_projection.toml projection.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::config::{
    GlobalConfig, get_or_generate_brain_id, load_global_config, resolve_brain_id,
    save_global_config,
};
use brain_lib::prelude::{BrainWatcher, Embed};
use brain_persistence::db::Db;
use brain_persistence::db::meta::generate_prefix;
use brain_persistence::db::schema::BrainProjection;
use brain_persistence::store::Store;
use tracing::{debug, info, warn};

use super::instance::{BrainInstance, init_brain_instance};

/// Ensure every registered brain has a stable ID in both its local
/// `brain.toml` and the global registry. Generates missing IDs on the fly.
pub fn sync_brain_ids(global_cfg: &GlobalConfig) {
    for (name, entry) in &global_cfg.brains {
        let Some(root) = entry.primary_root() else {
            continue;
        };
        let brain_dir = root.join(".brain");
        match get_or_generate_brain_id(&brain_dir) {
            Ok(id) => {
                if entry.id.as_deref() != Some(&id) {
                    info!(brain = %name, id = %id, "synced brain ID to registry");
                }
            }
            Err(e) => {
                warn!(brain = %name, error = %e, "failed to sync brain ID");
            }
        }
    }
}

/// Reload the brain registry and re-project all brains into the SQL brains table.
///
/// Combines `reload_brains()` with a fresh projection so that SQL-based brain
/// resolution stays in sync after any config change or SIGHUP. Called by both
/// the state_projection.toml watcher and the SIGHUP handler.
pub async fn reload_and_project(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    embedder: Arc<dyn Embed>,
    shared_db: &Db,
    shared_store: &Store,
) -> Result<()> {
    reload_brains(
        brains,
        watcher,
        Arc::clone(&embedder),
        shared_db,
        shared_store,
    )
    .await?;

    // Load the freshest config after reload to build projections.
    let cfg = load_global_config()?;

    // All brains share the unified SQLite DB via `shared_db`.
    let db = shared_db;

    // Read existing prefixes from DB to preserve manual overrides.
    let existing_prefixes: std::collections::HashMap<String, String> = db
        .list_brains(false)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| row.prefix.map(|p| (row.brain_id, p)))
        .collect();

    let projections: Vec<BrainProjection> = brains
        .iter()
        .filter_map(|(name, inst)| {
            let bid = inst.mcp_context.brain_id().to_string();
            cfg.brains.get(name).map(|entry| {
                let prefix = existing_prefixes
                    .get(&bid)
                    .cloned()
                    .unwrap_or_else(|| generate_prefix(name));
                BrainProjection {
                    brain_id: bid,
                    name: name.clone(),
                    prefix,
                    roots_json: serde_json::to_string(&entry.roots)
                        .unwrap_or_else(|_| "[]".to_string()),
                    notes_json: serde_json::to_string(&entry.notes)
                        .unwrap_or_else(|_| "[]".to_string()),
                    aliases_json: serde_json::to_string(&entry.aliases)
                        .unwrap_or_else(|_| "[]".to_string()),
                    archived: entry.archived,
                }
            })
        })
        .collect();

    if let Err(e) = db.project_config_to_brains(&projections) {
        warn!(error = %e, "failed to re-sync brains into DB");
    }

    // Sync prefixes from DB back to state_projection.toml.
    sync_prefixes_to_config(db, brains);

    Ok(())
}

/// Reload the brain registry from disk, diffing against the current state.
///
/// - New brains: initialise and watch
/// - Removed brains: unwatch and drop
/// - Updated brains (notes dirs changed): unwatch old, watch new
pub async fn reload_brains(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    embedder: Arc<dyn Embed>,
    shared_db: &Db,
    shared_store: &Store,
) -> Result<()> {
    let new_cfg = load_global_config()?;

    // Collect owned name sets before mutating `brains`
    let new_names: std::collections::HashSet<String> = new_cfg.brains.keys().cloned().collect();
    let old_names: std::collections::HashSet<String> = brains.keys().cloned().collect();

    // Removed brains: unwatch dirs and remove from map
    let removed: Vec<String> = old_names.difference(&new_names).cloned().collect();

    for name in &removed {
        if let Some(instance) = brains.remove(name) {
            for dir in &instance.note_dirs {
                if let Err(e) = watcher.unwatch_path(dir) {
                    warn!(brain = %name, dir = %dir.display(), error = %e, "failed to unwatch directory");
                }
            }
            info!(brain = %name, "brain removed from registry");
        }
    }

    // New brains: initialise and watch
    let added: Vec<String> = new_names.difference(&old_names).cloned().collect();

    for name in &added {
        if let Some(entry) = new_cfg.brains.get(name) {
            let brain_id = match resolve_brain_id(entry, name) {
                Ok(id) => id,
                Err(e) => {
                    warn!(brain = %name, error = %e, "failed to resolve brain ID for new brain, skipping");
                    continue;
                }
            };
            match init_brain_instance(
                name,
                entry.notes.clone(),
                Arc::clone(&embedder),
                &brain_id,
                shared_db.clone(),
                shared_store.clone(),
            )
            .await
            {
                Ok(instance) => {
                    for dir in &instance.note_dirs {
                        if let Err(e) = watcher.watch_path(dir) {
                            warn!(brain = %name, dir = %dir.display(), error = %e, "failed to watch new brain directory");
                        }
                    }
                    info!(brain = %name, "new brain added to registry");
                    brains.insert(name.clone(), instance);
                }
                Err(e) => {
                    warn!(brain = %name, error = %e, "failed to initialise new brain, skipping");
                }
            }
        }
    }

    // Updated brains: check if note dirs changed
    let updated: Vec<String> = old_names.intersection(&new_names).cloned().collect();
    for name in updated {
        let new_entry = match new_cfg.brains.get(&name) {
            Some(e) => e,
            None => continue,
        };
        let instance = match brains.get_mut(&name) {
            Some(i) => i,
            None => continue,
        };

        let old_dirs: std::collections::HashSet<&PathBuf> = instance.note_dirs.iter().collect();
        let new_dirs_raw: std::collections::HashSet<&PathBuf> = new_entry.notes.iter().collect();

        if old_dirs == new_dirs_raw {
            continue; // No change
        }

        // Unwatch removed dirs
        for dir in old_dirs.difference(&new_dirs_raw) {
            if let Err(e) = watcher.unwatch_path(dir) {
                warn!(brain = %name, dir = %dir.display(), error = %e, "failed to unwatch old directory");
            }
        }

        // Keep unchanged dirs, watch only truly new ones
        let unchanged: Vec<PathBuf> = old_dirs
            .intersection(&new_dirs_raw)
            .map(|d| (*d).clone())
            .collect();
        let mut new_note_dirs = unchanged;

        for dir in new_dirs_raw.difference(&old_dirs) {
            if dir.exists() {
                if let Err(e) = watcher.watch_path(dir) {
                    warn!(brain = %name, dir = %dir.display(), error = %e, "failed to watch new directory");
                } else {
                    new_note_dirs.push((*dir).clone());
                }
            } else {
                warn!(brain = %name, dir = %dir.display(), "new note directory does not exist, skipping");
            }
        }

        instance.note_dirs = new_note_dirs;
        info!(brain = %name, "brain note directories updated");
    }

    Ok(())
}

/// Validate that all registered brain roots still exist on disk.
///
/// For each brain in the active `brains` map:
/// - Loads the current global config from disk.
/// - Removes roots that no longer exist from the config entry.
/// - If a brain has no roots remaining, marks it archived in config and DB,
///   unwatches all its note directories, and removes it from the map.
/// - If a brain retains some roots, unwatches note dirs that fall under
///   removed roots and updates `instance.note_dirs`.
/// - Projects the updated DB state to state_projection.toml.
///
/// The DB is the source of truth. Config.toml is a projection.
///
/// Returns `true` if the prefix map needs rebuilding (any brain changed).
pub fn validate_roots(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    db: &Db,
) -> bool {
    // Read active brains from DB (source of truth).
    let db_brains = match db.list_brains(true) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "root validation: failed to read brains from DB, skipping");
            return false;
        }
    };

    let mut db_changed = false;
    let mut prefix_map_dirty = false;

    for row in &db_brains {
        let roots: Vec<std::path::PathBuf> = row
            .roots_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
            .unwrap_or_default()
            .into_iter()
            .map(std::path::PathBuf::from)
            .collect();

        let stale_roots: Vec<&std::path::PathBuf> = roots.iter().filter(|r| !r.exists()).collect();

        if stale_roots.is_empty() {
            continue;
        }

        for root in &stale_roots {
            info!(brain = %row.name, root = %root.display(), "removing stale root (DB source of truth)");
        }

        let remaining_roots: Vec<&std::path::PathBuf> =
            roots.iter().filter(|r| !stale_roots.contains(r)).collect();

        if remaining_roots.is_empty() {
            // All roots gone — atomically archive + clear roots in DB.
            info!(brain = %row.name, "all roots gone; archiving brain in DB");
            if let Err(e) = db.archive_and_clear_roots(&row.brain_id) {
                warn!(brain = %row.name, error = %e, "failed to archive brain in DB");
                continue;
            }
            db_changed = true;

            // Remove from in-memory map and unwatch.
            if let Some(instance) = brains.remove(&row.name) {
                for dir in &instance.note_dirs {
                    if let Err(e) = watcher.unwatch_path(dir) {
                        warn!(brain = %row.name, dir = %dir.display(), error = %e, "failed to unwatch dir during archival");
                    }
                }
            }
            prefix_map_dirty = true;
        } else {
            // Some roots remain — update DB with surviving roots.
            let remaining_strs: Vec<&str> =
                remaining_roots.iter().filter_map(|r| r.to_str()).collect();
            let new_roots_json =
                serde_json::to_string(&remaining_strs).unwrap_or_else(|_| "[]".to_string());

            if let Err(e) = db.update_brain_roots(&row.brain_id, &new_roots_json) {
                warn!(brain = %row.name, error = %e, "failed to update roots in DB");
                continue;
            }
            db_changed = true;

            // Prune note dirs under stale roots from in-memory map.
            if let Some(instance) = brains.get_mut(&row.name) {
                let removed_note_dirs: Vec<std::path::PathBuf> = instance
                    .note_dirs
                    .iter()
                    .filter(|dir| stale_roots.iter().any(|root| dir.starts_with(root)))
                    .cloned()
                    .collect();

                for dir in &removed_note_dirs {
                    info!(brain = %row.name, dir = %dir.display(), "unwatching note dir under stale root");
                    if let Err(e) = watcher.unwatch_path(dir) {
                        warn!(brain = %row.name, dir = %dir.display(), error = %e, "failed to unwatch dir");
                    }
                }

                if !removed_note_dirs.is_empty() {
                    instance
                        .note_dirs
                        .retain(|d| !removed_note_dirs.contains(d));
                    prefix_map_dirty = true;
                }
            }
        }
    }

    // Project DB state → state_projection.toml (config is a read-only projection).
    if db_changed {
        project_db_to_config(db);
    }

    prefix_map_dirty
}

/// Project DB brain state to state_projection.toml.
///
/// Reads all brains from the DB and overwrites state_projection.toml roots, notes,
/// aliases, and archived status. Config.toml is a projection, not a source.
pub fn project_db_to_config(db: &Db) {
    let mut cfg = match load_global_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!(error = %e, "project_db_to_config: failed to load config");
            return;
        }
    };

    let db_brains = match db.list_brains(false) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "project_db_to_config: failed to read brains from DB");
            return;
        }
    };

    let mut changed = false;
    for row in &db_brains {
        let entry = match cfg.brains.get_mut(&row.name) {
            Some(e) => e,
            None => continue,
        };

        // Project roots from DB.
        let db_roots: Vec<std::path::PathBuf> = row
            .roots_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
            .unwrap_or_default()
            .into_iter()
            .map(std::path::PathBuf::from)
            .collect();

        if entry.roots != db_roots {
            entry.roots = db_roots;
            changed = true;
        }

        if entry.archived != row.archived {
            entry.archived = row.archived;
            changed = true;
        }
    }

    if changed {
        cfg.last_projected_at = Some(chrono::Utc::now().to_rfc3339());
        if let Err(e) = save_global_config(&cfg) {
            warn!(error = %e, "project_db_to_config: failed to save config");
        }
    }
}

/// Sync prefixes from the DB (source of truth) back to state_projection.toml
/// (projection).
///
/// Only writes if any prefix actually changed, to avoid triggering the config
/// watcher unnecessarily.
pub fn sync_prefixes_to_config(db: &Db, brains: &HashMap<String, BrainInstance>) {
    let Ok(mut cfg) = load_global_config() else {
        return;
    };
    let mut changed = false;
    for (name, inst) in brains {
        let brain_id = inst.mcp_context.brain_id().to_string();
        if let Ok(Some(db_prefix)) = db.get_brain_prefix(&brain_id)
            && let Some(entry) = cfg.brains.get_mut(name)
            && entry.prefix.as_deref() != Some(&db_prefix)
        {
            entry.prefix = Some(db_prefix);
            changed = true;
        }
    }
    if changed {
        if let Err(e) = save_global_config(&cfg) {
            warn!(error = %e, "failed to sync prefix to state_projection.toml");
        } else {
            debug!("synced prefixes from DB to state_projection.toml");
        }
    }
}
