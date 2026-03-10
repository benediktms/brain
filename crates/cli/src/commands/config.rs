use std::path::Path;

use anyhow::Result;

/// Get a configuration value by key and print it to stdout.
pub fn run_config_get(sqlite_db: &Path, key: &str) -> Result<()> {
    let db = brain_lib::db::Db::open(sqlite_db)?;
    db.with_write_conn(|conn| match key {
        "prefix" => {
            let brain_dir = sqlite_db.parent().unwrap_or(Path::new("."));
            let prefix = brain_lib::db::meta::get_or_init_project_prefix(conn, brain_dir)?;
            println!("{prefix}");
            Ok(())
        }
        other => Err(brain_lib::error::BrainCoreError::Config(format!(
            "unknown config key: {other}. Known keys: prefix"
        ))),
    })?;
    Ok(())
}

/// Set a configuration value by key.
///
/// Passing `value = None` auto-derives the value (supported for `prefix`).
pub fn run_config_set(sqlite_db: &Path, key: &str, value: Option<String>) -> Result<()> {
    match key {
        "prefix" => {
            let brain_dir = sqlite_db.parent().unwrap_or(Path::new("."));

            let db = brain_lib::db::Db::open(sqlite_db)?;
            let (old_prefix, new_prefix) = db.with_write_conn(|conn| {
                let old = brain_lib::db::meta::get_or_init_project_prefix(conn, brain_dir)?;

                let new = match value {
                    Some(ref v) => {
                        let upper = v.to_ascii_uppercase();
                        if upper.len() != 3 || !upper.chars().all(|c| c.is_ascii_uppercase()) {
                            return Err(brain_lib::error::BrainCoreError::Config(format!(
                                "prefix must be exactly 3 uppercase ASCII letters, got: {v}"
                            )));
                        }
                        upper
                    }
                    None => {
                        let name = brain_dir
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("BRN");
                        brain_lib::db::meta::generate_prefix(name)
                    }
                };

                brain_lib::db::meta::set_meta(conn, "project_prefix", &new)?;
                Ok((old, new))
            })?;
            drop(db);

            if old_prefix == new_prefix {
                println!("Prefix is already {new_prefix}");
            } else {
                let tasks_dir = brain_dir.join("tasks");
                let db2 = brain_lib::db::Db::open(sqlite_db)?;
                let store = brain_lib::tasks::TaskStore::new(&tasks_dir, db2)?;
                let count = store.rewrite_prefix(&old_prefix, &new_prefix)?;
                println!("Rewrote {count} events: {old_prefix} → {new_prefix}");
            }
            Ok(())
        }
        other => Err(anyhow::anyhow!(
            "unknown config key: {other}. Known keys: prefix"
        )),
    }
}
