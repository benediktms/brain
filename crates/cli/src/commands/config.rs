use std::path::Path;

use anyhow::Result;

/// Derive the brain name from the sqlite_db path.
///
/// Typical path: `~/.brain/brains/<name>/brain.db` → `<name>`.
/// Fallback: `~/.brain/brain.db` → directory name or "brain".
fn brain_name_from_path(sqlite_db: &Path) -> String {
    let brain_dir = sqlite_db.parent().unwrap_or(Path::new("."));
    brain_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("brain")
        .to_string()
}

/// Try to read the prefix from `brains.prefix` for the brain matching the
/// directory-derived name. Returns `None` if no match or column is NULL.
fn read_brain_prefix(
    conn: &rusqlite::Connection,
    brain_name: &str,
) -> brain_lib::error::Result<Option<String>> {
    let prefix: Option<String> = conn
        .query_row(
            "SELECT prefix FROM brains WHERE name = ?1",
            [brain_name],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten();
    Ok(prefix)
}

/// Get a configuration value by key and print it to stdout.
pub fn run_config_get(sqlite_db: &Path, key: &str) -> Result<()> {
    let db = brain_lib::db::Db::open(sqlite_db)?;
    db.with_write_conn(|conn| match key {
        "prefix" => {
            // Try brains.prefix first
            let brain_name = brain_name_from_path(sqlite_db);
            if let Some(prefix) = read_brain_prefix(conn, &brain_name)?
                .filter(|p| p.len() == 3 && p.chars().all(|c| c.is_ascii_uppercase()))
            {
                println!("{prefix}");
                return Ok(());
            }
            // Fallback: legacy brain_meta
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
            let brain_name = brain_name_from_path(sqlite_db);

            let db = brain_lib::db::Db::open(sqlite_db)?;
            let (old_prefix, new_prefix) = db.with_write_conn(|conn| {
                // Read current prefix: brains.prefix → brain_meta fallback
                let old = read_brain_prefix(conn, &brain_name)?
                    .filter(|p| p.len() == 3 && p.chars().all(|c| c.is_ascii_uppercase()))
                    .map(Ok)
                    .unwrap_or_else(|| {
                        brain_lib::db::meta::get_or_init_project_prefix(conn, brain_dir)
                    })?;

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

                // Write to brains.prefix
                conn.execute(
                    "UPDATE brains SET prefix = ?1 WHERE name = ?2",
                    rusqlite::params![new, brain_name],
                )?;

                // Also write to brain_meta for backward compatibility
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
