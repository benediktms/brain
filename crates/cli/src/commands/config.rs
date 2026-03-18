use std::path::Path;

use anyhow::Result;

/// Try to read the prefix from `brains.prefix` for the named brain.
/// Returns `None` if no match or column is NULL.
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
///
/// `brain_name` must be provided explicitly — it can no longer be derived
/// from `sqlite_db` now that the DB is unified (`~/.brain/brain.db`).
pub fn run_config_get(sqlite_db: &Path, brain_name: &str, key: &str) -> Result<()> {
    let db = brain_lib::db::Db::open(sqlite_db)?;
    db.with_write_conn(|conn| match key {
        "prefix" => {
            if let Some(prefix) = read_brain_prefix(conn, brain_name)?
                .filter(|p| p.len() == 3 && p.chars().all(|c| c.is_ascii_uppercase()))
            {
                println!("{prefix}");
                return Ok(());
            }
            // Fallback for legacy brains initialized before brains.prefix existed.
            // Use the per-brain data dir as the legacy brain_dir.
            let brain_home = sqlite_db.parent().unwrap_or(Path::new("."));
            let brain_dir = brain_home.join("brains").join(brain_name);
            let fallback = brain_lib::db::meta::get_or_init_project_prefix(conn, &brain_dir)?;
            println!("{fallback}");
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
///
/// `brain_name` must be provided explicitly — it can no longer be derived
/// from `sqlite_db` now that the DB is unified (`~/.brain/brain.db`).
pub fn run_config_set(sqlite_db: &Path, brain_name: &str, key: &str, value: Option<String>) -> Result<()> {
    match key {
        "prefix" => {
            // Per-brain data dir for tasks JSONL and prefix generation.
            let brain_home = sqlite_db.parent().unwrap_or(Path::new("."));
            let brain_dir = brain_home.join("brains").join(brain_name);

            let db = brain_lib::db::Db::open(sqlite_db)?;
            let (old_prefix, new_prefix) = db.with_write_conn(|conn| {
                let old = read_brain_prefix(conn, brain_name)?
                    .filter(|p| p.len() == 3 && p.chars().all(|c| c.is_ascii_uppercase()))
                    .unwrap_or_else(|| "BRN".to_string());

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
                    None => brain_lib::db::meta::generate_prefix(brain_name),
                };

                // Write to brains.prefix
                conn.execute(
                    "UPDATE brains SET prefix = ?1 WHERE name = ?2",
                    rusqlite::params![new, brain_name],
                )?;

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
