use std::path::Path;

use anyhow::Result;
use brain_lib::config::{brain_home, load_global_config, save_global_config};
use brain_persistence::db::Db;

/// List all registered brains.
pub fn run_list(db_path: &Path, json: bool, all: bool, archived_only: bool) -> Result<()> {
    let db = Db::open(db_path)?;

    // active_only=true filters to projected=1 AND archived=0.
    // For `all` or `archived_only` we need every row, then filter client-side.
    let active_only = !all && !archived_only;
    let rows = db.list_brains(active_only)?;

    // When showing archived-only, further filter to archived rows.
    let rows: Vec<_> = if archived_only {
        rows.into_iter().filter(|r| r.archived).collect()
    } else {
        rows
    };

    // Synthetic "(unscoped)" row — a virtual brain that holds tasks/records
    // not tied to any registered project. Tasks land here by default when
    // `brain tasks create` is invoked outside any registered brain root and
    // no `--brain` flag is passed. Surfaced under the `unscoped` key in
    // JSON output (NOT inside `brains`) so consumers counting registered
    // brains via `brains.len()` see the same number as before.
    let unscoped_json = serde_json::json!({
        "name": "(unscoped)",
        "id": "",
        "root": null,
        "aliases": [],
        "extra_roots": [],
        "prefix": "NSX",
        "archived": false,
        "synthetic": true,
    });
    let show_unscoped = !archived_only;

    if rows.is_empty() {
        if json {
            let mut out = serde_json::json!({
                "brains": [],
                "count": 0,
            });
            if show_unscoped {
                out.as_object_mut()
                    .unwrap()
                    .insert("unscoped".to_string(), unscoped_json);
            }
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No brains registered. Run `brain init` in a project directory.");
            if show_unscoped {
                println!();
                println!("(unscoped) []");
                println!("  prefix: NSX");
                println!("  note:   synthetic — holds tasks not tied to a registered brain");
            }
        }
        return Ok(());
    }

    // Helper: parse a JSON array string into Vec<String>.
    let parse_json_array = |opt: &Option<String>| -> Vec<String> {
        opt.as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default()
    };

    if json {
        let brains: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let roots = parse_json_array(&row.roots_json);
                let root = roots.first().cloned().unwrap_or_default();
                let extra_roots: Vec<String> = roots.into_iter().skip(1).collect();
                let aliases = parse_json_array(&row.aliases_json);
                serde_json::json!({
                    "name": row.name,
                    "id": row.brain_id,
                    "root": root,
                    "aliases": aliases,
                    "extra_roots": extra_roots,
                    "prefix": row.prefix,
                    "archived": row.archived,
                })
            })
            .collect();
        let count = brains.len();
        let mut out = serde_json::json!({
            "brains": brains,
            "count": count,
        });
        if show_unscoped {
            out.as_object_mut()
                .unwrap()
                .insert("unscoped".to_string(), unscoped_json);
        }
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    for row in &rows {
        let archived_tag = if row.archived { " [archived]" } else { "" };
        println!("{} [{}]{archived_tag}", row.name, row.brain_id);
        let aliases = parse_json_array(&row.aliases_json);
        if !aliases.is_empty() {
            println!("  aka:    {}", aliases.join(", "));
        }
        let roots = parse_json_array(&row.roots_json);
        if let Some(primary) = roots.first() {
            println!("  root:   {primary}");
        }
        for extra in roots.iter().skip(1) {
            println!("          {extra}");
        }
        if let Some(ref p) = row.prefix {
            println!("  prefix: {p}");
        }
    }

    // Append synthetic unscoped brain after registered rows.
    if show_unscoped {
        println!("(unscoped) []");
        println!("  prefix: NSX");
        println!("  note:   synthetic — holds tasks not tied to a registered brain");
    }

    Ok(())
}

/// Remove a registered brain from the DB (source of truth) and state_projection.toml (projection).
pub fn run_remove(name: &str, purge: bool) -> Result<()> {
    // Delete from DB (source of truth).
    let home = brain_home()?;
    let db = brain_persistence::db::Db::open(&home.join("brain.db"))?;
    let deleted = db.delete_brain(name).map_err(|e| anyhow::anyhow!("{e}"))?;
    if !deleted {
        anyhow::bail!("brain \"{name}\" is not registered");
    }

    // Project removal to state_projection.toml.
    if let Ok(mut global) = load_global_config() {
        global.brains.remove(name);
        let _ = save_global_config(&global);
    }

    println!("Removed brain \"{name}\" from registry.");

    // Signal daemon to reload registry (best-effort)
    super::daemon::Daemon::new()
        .and_then(|d| d.signal_reload())
        .ok();

    if purge {
        let brains_dir = home.join("brains").join(name);
        if brains_dir.exists() {
            std::fs::remove_dir_all(&brains_dir)?;
            println!("Purged derived data at {}", brains_dir.display());
        }
    }

    Ok(())
}
