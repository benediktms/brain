use rusqlite::Connection;

use super::migrations::{
    migrate_v0_to_v1, migrate_v1_to_v2, migrate_v2_to_v3, migrate_v3_to_v4, migrate_v4_to_v5,
    migrate_v5_to_v6, migrate_v6_to_v7, migrate_v7_to_v8, migrate_v8_to_v9, migrate_v9_to_v10,
    migrate_v10_to_v11, migrate_v11_to_v12, migrate_v12_to_v13, migrate_v13_to_v14,
    migrate_v14_to_v15, migrate_v15_to_v16, migrate_v16_to_v17, migrate_v17_to_v18,
    migrate_v18_to_v19, migrate_v19_to_v20, migrate_v20_to_v21, migrate_v21_to_v22,
    migrate_v22_to_v23, migrate_v23_to_v24, migrate_v24_to_v25, migrate_v25_to_v26,
    migrate_v26_to_v27, migrate_v27_to_v28, migrate_v28_to_v29, migrate_v29_to_v30,
    migrate_v30_to_v31, migrate_v31_to_v32, migrate_v32_to_v33, migrate_v33_to_v34,
    migrate_v34_to_v35, migrate_v35_to_v36, migrate_v36_to_v37, migrate_v37_to_v38,
};
use crate::error::{BrainCoreError, Result};

/// Bump this when the schema changes after release.
/// Each bump requires a corresponding `migrate_vN_to_vN+1` function.
pub const SCHEMA_VERSION: i32 = 38;

/// Initialize the database schema: WAL mode, foreign keys, and all tables.
///
/// Uses a version-aware migration dispatch loop so that each migration
/// stamps its own version inside a transaction. This prevents the bug
/// where bumping `SCHEMA_VERSION` would silently stamp a new version
/// without running any migration DDL.
pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;

    let current: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;

    if current > SCHEMA_VERSION {
        return Err(BrainCoreError::SchemaVersion(format!(
            "database schema version {current} is newer than supported version {SCHEMA_VERSION}"
        )));
    }

    if current < SCHEMA_VERSION {
        run_migrations(conn, current)?;
    }

    // Always ensure FTS5 + triggers exist (idempotent, handles partial init)
    ensure_fts5(conn)?;

    Ok(())
}

/// Run migrations sequentially from `from_version` up to `SCHEMA_VERSION`.
fn run_migrations(conn: &Connection, from_version: i32) -> Result<()> {
    let mut version = from_version;
    while version < SCHEMA_VERSION {
        match version {
            0 => migrate_v0_to_v1(conn)?,
            1 => migrate_v1_to_v2(conn)?,
            2 => migrate_v2_to_v3(conn)?,
            3 => migrate_v3_to_v4(conn)?,
            4 => migrate_v4_to_v5(conn)?,
            5 => migrate_v5_to_v6(conn)?,
            6 => migrate_v6_to_v7(conn)?,
            7 => migrate_v7_to_v8(conn)?,
            8 => migrate_v8_to_v9(conn)?,
            9 => migrate_v9_to_v10(conn)?,
            10 => migrate_v10_to_v11(conn)?,
            11 => migrate_v11_to_v12(conn)?,
            12 => migrate_v12_to_v13(conn)?,
            13 => migrate_v13_to_v14(conn)?,
            14 => migrate_v14_to_v15(conn)?,
            15 => migrate_v15_to_v16(conn)?,
            16 => migrate_v16_to_v17(conn)?,
            17 => migrate_v17_to_v18(conn)?,
            18 => migrate_v18_to_v19(conn)?,
            19 => migrate_v19_to_v20(conn)?,
            20 => migrate_v20_to_v21(conn)?,
            21 => migrate_v21_to_v22(conn)?,
            22 => migrate_v22_to_v23(conn)?,
            23 => migrate_v23_to_v24(conn)?,
            24 => migrate_v24_to_v25(conn)?,
            25 => migrate_v25_to_v26(conn)?,
            26 => migrate_v26_to_v27(conn)?,
            27 => migrate_v27_to_v28(conn)?,
            28 => migrate_v28_to_v29(conn)?,
            29 => migrate_v29_to_v30(conn)?,
            30 => migrate_v30_to_v31(conn)?,
            31 => migrate_v31_to_v32(conn)?,
            32 => migrate_v32_to_v33(conn)?,
            33 => migrate_v33_to_v34(conn)?,
            34 => migrate_v34_to_v35(conn)?,
            35 => migrate_v35_to_v36(conn)?,
            36 => migrate_v36_to_v37(conn)?,
            37 => migrate_v37_to_v38(conn)?,
            other => {
                return Err(BrainCoreError::SchemaVersion(format!(
                    "no migration defined from version {other} to {}",
                    other + 1
                )));
            }
        }
        version += 1;
    }
    Ok(())
}

/// Ensure the current brain is registered in the `brains` table.
///
/// Called once during bootstrap, before any writes. This replaces the old
/// `backfill_brain_id()` self-healing approach — with FK constraints on
/// `brain_id`, every brain must be registered upfront.
pub fn ensure_brain_registered(conn: &Connection, brain_id: &str, brain_name: &str) -> Result<()> {
    use super::meta::generate_prefix;

    // Derive prefix solely from brain_name — never read brain_meta.project_prefix
    // (that table is unscoped and causes cross-brain prefix poisoning).
    let prefix = generate_prefix(brain_name);
    conn.execute(
        "INSERT INTO brains (brain_id, name, prefix, created_at) VALUES (?1, ?2, ?3, strftime('%s', 'now'))
         ON CONFLICT(brain_id) DO UPDATE SET prefix = COALESCE(brains.prefix, excluded.prefix)",
        rusqlite::params![brain_id, brain_name, prefix],
    )?;
    // Backfill brain_id on summaries rows that pre-date v25 migration.
    conn.execute(
        "UPDATE summaries SET brain_id = ?1 WHERE brain_id = ''",
        rusqlite::params![brain_id],
    )?;
    Ok(())
}

/// DTO for projecting state_projection.toml brain entries into the brains table.
pub struct BrainProjection {
    pub brain_id: String,
    pub name: String,
    pub prefix: String,
    pub roots_json: String,
    pub notes_json: String,
    pub aliases_json: String,
    pub archived: bool,
}

/// Full brain row returned by `list_brains()`.
#[derive(Debug, Clone)]
pub struct BrainRow {
    pub brain_id: String,
    pub name: String,
    pub prefix: Option<String>,
    pub roots_json: Option<String>,
    pub notes_json: Option<String>,
    pub aliases_json: Option<String>,
    pub archived: bool,
    pub projected: bool,
}

/// Input for `upsert_brain()`.
pub struct BrainUpsert<'a> {
    pub brain_id: &'a str,
    pub name: &'a str,
    pub prefix: &'a str,
    pub roots_json: &'a str,
    pub notes_json: &'a str,
    pub aliases_json: &'a str,
    pub archived: bool,
}

/// Upsert a brain entry into the `brains` table.
///
/// Preserves existing prefix via COALESCE — only uses the provided prefix
/// for new brains (when no row exists). Sets `projected = 1`.
pub fn upsert_brain(conn: &Connection, input: &BrainUpsert<'_>) -> Result<()> {
    conn.execute(
        "INSERT INTO brains (brain_id, name, prefix, roots, notes, aliases, archived, projected, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, strftime('%s','now'))
         ON CONFLICT(brain_id) DO UPDATE SET
           name = excluded.name,
           prefix = COALESCE(brains.prefix, excluded.prefix),
           roots = excluded.roots,
           notes = excluded.notes,
           aliases = excluded.aliases,
           archived = excluded.archived,
           projected = 1",
        rusqlite::params![
            input.brain_id,
            input.name,
            input.prefix,
            input.roots_json,
            input.notes_json,
            input.aliases_json,
            input.archived as i32
        ],
    )?;
    Ok(())
}

/// Check whether a brain has been archived.
///
/// Returns `false` when no matching row exists (brain not yet registered).
pub fn is_brain_archived(conn: &Connection, brain_id: &str) -> Result<bool> {
    use rusqlite::OptionalExtension;
    let archived: Option<i64> = conn
        .query_row(
            "SELECT archived FROM brains WHERE brain_id = ?1",
            [brain_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(archived.unwrap_or(0) == 1)
}

/// List all brain rows, optionally filtered.
///
/// When `active_only` is true, returns only projected=1, non-archived brains.
/// Always excludes the `(unscoped)` sentinel row (brain_id='') from migration v21→v22.
pub fn list_brains(conn: &Connection, active_only: bool) -> Result<Vec<BrainRow>> {
    let sql = if active_only {
        "SELECT brain_id, name, prefix, roots, notes, aliases, archived, projected FROM brains WHERE projected = 1 AND archived = 0 AND brain_id != '' ORDER BY name"
    } else {
        "SELECT brain_id, name, prefix, roots, notes, aliases, archived, projected FROM brains WHERE brain_id != '' ORDER BY name"
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(BrainRow {
            brain_id: row.get(0)?,
            name: row.get(1)?,
            prefix: row.get(2)?,
            roots_json: row.get(3)?,
            notes_json: row.get(4)?,
            aliases_json: row.get(5)?,
            archived: row.get::<_, i32>(6)? != 0,
            projected: row.get::<_, i32>(7)? != 0,
        })
    })?;
    crate::db::collect_rows(rows)
}

/// Read the prefix for a brain by brain_id.
pub fn get_brain_prefix(conn: &Connection, brain_id: &str) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    let prefix: Option<String> = conn
        .query_row(
            "SELECT prefix FROM brains WHERE brain_id = ?1",
            [brain_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    Ok(prefix)
}

/// Read a full brain row by brain_id.
pub fn get_brain(conn: &Connection, brain_id: &str) -> Result<Option<BrainRow>> {
    use rusqlite::OptionalExtension;
    let row = conn
        .query_row(
            "SELECT brain_id, name, prefix, roots, notes, aliases, archived, projected FROM brains WHERE brain_id = ?1",
            [brain_id],
            |row| {
                Ok(BrainRow {
                    brain_id: row.get(0)?,
                    name: row.get(1)?,
                    prefix: row.get(2)?,
                    roots_json: row.get(3)?,
                    notes_json: row.get(4)?,
                    aliases_json: row.get(5)?,
                    archived: row.get::<_, i32>(6)? != 0,
                    projected: row.get::<_, i32>(7)? != 0,
                })
            },
        )
        .optional()?;
    Ok(row)
}

/// Read a full brain row by name.
pub fn get_brain_by_name(conn: &Connection, name: &str) -> Result<Option<BrainRow>> {
    use rusqlite::OptionalExtension;
    let row = conn
        .query_row(
            "SELECT brain_id, name, prefix, roots, notes, aliases, archived, projected FROM brains WHERE name = ?1",
            [name],
            |row| {
                Ok(BrainRow {
                    brain_id: row.get(0)?,
                    name: row.get(1)?,
                    prefix: row.get(2)?,
                    roots_json: row.get(3)?,
                    notes_json: row.get(4)?,
                    aliases_json: row.get(5)?,
                    archived: row.get::<_, i32>(6)? != 0,
                    projected: row.get::<_, i32>(7)? != 0,
                })
            },
        )
        .optional()?;
    Ok(row)
}

/// Update the roots JSON for a brain by brain_id.
pub fn update_brain_roots(conn: &Connection, brain_id: &str, roots_json: &str) -> Result<()> {
    conn.execute(
        "UPDATE brains SET roots = ?2 WHERE brain_id = ?1",
        rusqlite::params![brain_id, roots_json],
    )?;
    Ok(())
}

/// Mark a brain as archived in the DB.
pub fn archive_brain(conn: &Connection, brain_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE brains SET archived = 1 WHERE brain_id = ?1",
        rusqlite::params![brain_id],
    )?;
    Ok(())
}

/// Atomically archive a brain and clear its roots in a single transaction.
pub fn archive_and_clear_roots(conn: &Connection, brain_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE brains SET archived = 1, roots = '[]' WHERE brain_id = ?1",
        rusqlite::params![brain_id],
    )?;
    Ok(())
}

/// Delete a brain by name from the `brains` table.
pub fn delete_brain(conn: &Connection, name: &str) -> Result<bool> {
    let rows = conn.execute("DELETE FROM brains WHERE name = ?1", [name])?;
    Ok(rows > 0)
}

/// Sync state_projection.toml brain entries into the brains table.
///
/// Uses UPSERT (not DELETE+INSERT) to preserve existing data, especially
/// manually-set prefixes. Existing prefix is preserved via COALESCE —
/// the provided prefix is only used for new brains without an existing row.
///
/// Brains no longer in config are soft-cleared to projected=0 but their
/// rows (and prefixes) are preserved for historical access.
pub fn project_config_to_brains(conn: &Connection, brains: &[BrainProjection]) -> Result<()> {
    // Alias uniqueness check — first-seen wins, duplicates are logged and skipped.
    let mut alias_owners: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // Build deduplicated alias JSON per brain (we rebuild the JSON after dedup).
    let deduplicated: Vec<(&BrainProjection, String)> = brains
        .iter()
        .map(|brain| {
            let aliases: Vec<String> =
                serde_json::from_str(&brain.aliases_json).unwrap_or_default();
            let clean: Vec<String> = aliases
                .into_iter()
                .filter(|alias| {
                    if let Some(owner) = alias_owners.get(alias) {
                        tracing::warn!(
                            alias = %alias,
                            brain = %brain.name,
                            owner = %owner,
                            "duplicate alias across brains — skipping for this brain"
                        );
                        false
                    } else {
                        alias_owners.insert(alias.clone(), brain.name.clone());
                        true
                    }
                })
                .collect();
            let clean_json = serde_json::to_string(&clean).unwrap_or_else(|_| "[]".to_string());
            (brain, clean_json)
        })
        .collect();

    // Step 1: Soft-clear all projected rows. Rows in the new config will be
    // re-marked projected=1 below. Rows NOT in the new config stay as
    // projected=0 — preserving their data for historical access.
    conn.execute("UPDATE brains SET projected = 0 WHERE projected = 1", [])?;

    // Step 2: Upsert each brain. COALESCE preserves existing prefix because
    // the row still exists (we didn't delete it).
    let mut stmt = conn.prepare_cached(
        "INSERT INTO brains (brain_id, name, prefix, roots, notes, aliases, archived, projected, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, strftime('%s','now'))
         ON CONFLICT(brain_id) DO UPDATE SET
           name = excluded.name,
           roots = excluded.roots,
           notes = excluded.notes,
           aliases = excluded.aliases,
           archived = excluded.archived,
           projected = 1,
           prefix = COALESCE(brains.prefix, excluded.prefix)",
    )?;

    for (brain, clean_aliases_json) in &deduplicated {
        stmt.execute(rusqlite::params![
            brain.brain_id,
            brain.name,
            brain.prefix,
            brain.roots_json,
            brain.notes_json,
            clean_aliases_json,
            brain.archived as i32,
        ])?;
    }

    Ok(())
}

/// Resolve a brain by any identifier: name, brain_id, alias, or root path.
///
/// Resolution order: exact name → exact ID → alias (projected only) → root path (projected only).
///
/// Removed brains (projected=0) remain resolvable by name/ID for historical access
/// (e.g. cross-brain task queries), but are excluded from alias/root resolution.
pub fn resolve_brain(conn: &Connection, input: &str) -> Result<(String, String)> {
    // 1. Exact name match (all rows — includes removed brains for historical access)
    if let Ok((id, name)) = conn.query_row(
        "SELECT brain_id, name FROM brains WHERE name = ?1",
        [input],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    ) {
        return Ok((id, name));
    }

    // 2. Exact brain_id match (all rows)
    if let Ok((id, name)) = conn.query_row(
        "SELECT brain_id, name FROM brains WHERE brain_id = ?1",
        [input],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    ) {
        return Ok((id, name));
    }

    // 3. Alias match (projected rows only)
    {
        let mut stmt = conn.prepare(
            "SELECT brain_id, name, aliases FROM brains WHERE projected = 1 AND aliases IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (id, name, aliases_json) = row?;
            if let Ok(aliases) = serde_json::from_str::<Vec<String>>(&aliases_json)
                && aliases.iter().any(|a| a == input)
            {
                return Ok((id, name));
            }
        }
    }

    // 4. Root path match (projected rows only, longest prefix wins)
    {
        let mut stmt = conn.prepare(
            "SELECT brain_id, name, roots FROM brains WHERE projected = 1 AND roots IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut best: Option<(String, String, usize)> = None;
        for row in rows {
            let (id, name, roots_json) = row?;
            if let Ok(roots) = serde_json::from_str::<Vec<String>>(&roots_json) {
                for root in &roots {
                    if input.starts_with(root.as_str())
                        && root.len() > best.as_ref().map_or(0, |b| b.2)
                    {
                        best = Some((id.clone(), name.clone(), root.len()));
                    }
                }
            }
        }
        if let Some((id, name, _)) = best {
            return Ok((id, name));
        }
    }

    Err(crate::error::BrainCoreError::Database(format!(
        "brain not found: {input}"
    )))
}

/// Ensure FTS5 virtual table and sync triggers exist (idempotent).
///
/// Called on every `init_schema` open, outside the migration transaction,
/// because FTS5 DDL has SQLite transaction limitations.
pub fn ensure_fts5(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_chunks USING fts5(
            content,
            content=chunks,
            content_rowid=rowid
        )",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS chunks_fts_insert AFTER INSERT ON chunks BEGIN
            INSERT INTO fts_chunks(rowid, content) VALUES (new.rowid, new.content);
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS chunks_fts_delete AFTER DELETE ON chunks BEGIN
            INSERT INTO fts_chunks(fts_chunks, rowid, content) VALUES('delete', old.rowid, old.content);
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS chunks_fts_update AFTER UPDATE OF content ON chunks BEGIN
            INSERT INTO fts_chunks(fts_chunks, rowid, content) VALUES('delete', old.rowid, old.content);
            INSERT INTO fts_chunks(rowid, content) VALUES (new.rowid, new.content);
        END",
        [],
    )?;

    // ── FTS5 for tasks (title + description) ────────────────────
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_tasks USING fts5(
            title, description,
            content=tasks,
            content_rowid=rowid
        )",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS tasks_fts_insert AFTER INSERT ON tasks BEGIN
            INSERT INTO fts_tasks(rowid, title, description)
            VALUES (new.rowid, new.title, COALESCE(new.description, ''));
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS tasks_fts_delete AFTER DELETE ON tasks BEGIN
            INSERT INTO fts_tasks(fts_tasks, rowid, title, description)
            VALUES ('delete', old.rowid, old.title, COALESCE(old.description, ''));
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS tasks_fts_update AFTER UPDATE OF title, description ON tasks BEGIN
            INSERT INTO fts_tasks(fts_tasks, rowid, title, description)
            VALUES ('delete', old.rowid, old.title, COALESCE(old.description, ''));
            INSERT INTO fts_tasks(rowid, title, description)
            VALUES (new.rowid, new.title, COALESCE(new.description, ''));
        END",
        [],
    )?;

    // ── FTS5 for summaries (title + content, porter stemming for prose) ──
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_summaries USING fts5(
            title, content,
            content=summaries,
            content_rowid=rowid,
            tokenize='porter unicode61'
        )",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS summaries_fts_insert AFTER INSERT ON summaries BEGIN
            INSERT INTO fts_summaries(rowid, title, content)
            VALUES (new.rowid, COALESCE(new.title, ''), new.content);
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS summaries_fts_delete AFTER DELETE ON summaries BEGIN
            INSERT INTO fts_summaries(fts_summaries, rowid, title, content)
            VALUES ('delete', old.rowid, COALESCE(old.title, ''), old.content);
        END",
        [],
    )?;

    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS summaries_fts_update AFTER UPDATE OF title, content ON summaries BEGIN
            INSERT INTO fts_summaries(fts_summaries, rowid, title, content)
            VALUES ('delete', old.rowid, COALESCE(old.title, ''), old.content);
            INSERT INTO fts_summaries(rowid, title, content)
            VALUES (new.rowid, COALESCE(new.title, ''), new.content);
        END",
        [],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_init_schema_creates_tables() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Verify WAL mode
        let journal_mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert!(
            journal_mode == "wal" || journal_mode == "memory",
            "expected wal or memory, got {journal_mode}"
        );

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"files".to_string()));
        assert!(tables.contains(&"chunks".to_string()));
        assert!(tables.contains(&"links".to_string()));
        assert!(tables.contains(&"summaries".to_string()));
        assert!(tables.contains(&"reflection_sources".to_string()));
    }

    #[test]
    fn test_init_schema_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        init_schema(&conn).unwrap(); // second call should not fail
    }

    /// Full migration harness: run all migrations from scratch and verify
    /// the final schema has all expected tables, columns, and version.
    #[test]
    fn test_full_migration_harness() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Verify final version
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // Verify all expected tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        for expected in &[
            "files",
            "chunks",
            "links",
            "summaries",
            "reflection_sources",
            "derived_summaries",
            "jobs",
            "providers",
            "summary_sources",
            "brains",
            "records",
        ] {
            assert!(
                tables.contains(&expected.to_string()),
                "missing table: {expected}"
            );
        }

        // v35: summary_sources has expected columns
        let ss_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('summary_sources')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        for col in &["summary_id", "source_id", "source_type", "created_at"] {
            assert!(
                ss_cols.contains(&col.to_string()),
                "summary_sources missing column: {col}"
            );
        }

        // v35: summaries.consolidated_by column exists
        let sum_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('summaries')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            sum_cols.contains(&"consolidated_by".to_string()),
            "summaries missing column: consolidated_by"
        );
        assert!(
            sum_cols.contains(&"embedded_at".to_string()),
            "summaries missing column: embedded_at"
        );

        // v35: derived_summaries.source_content_hash column exists
        let ds_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('derived_summaries')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            ds_cols.contains(&"source_content_hash".to_string()),
            "derived_summaries missing column: source_content_hash"
        );

        // v35: reverse-lookup index on summary_sources.source_id
        let indexes: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='summary_sources'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            indexes.iter().any(|i| i.contains("summary_sources_source")),
            "summary_sources missing reverse-lookup index"
        );
    }

    #[test]
    fn test_fts5_table_exists() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_chunks'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count > 0, "fts_chunks table should exist");
    }

    #[test]
    fn test_fts5_triggers_exist() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let triggers: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'chunks_fts_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(
            triggers,
            vec![
                "chunks_fts_delete",
                "chunks_fts_insert",
                "chunks_fts_update"
            ]
        );
    }

    #[test]
    fn test_fts5_sync_with_chunks() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/test.md', 'idle')",
            [],
        )
        .unwrap();

        // Insert a chunk with content — trigger should populate FTS5
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:0', 'f1', 0, 'hash0', 'hello world full text search')",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Delete the chunk — FTS5 should be cleaned up
        conn.execute("DELETE FROM chunks WHERE chunk_id = 'f1:0'", [])
            .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_summaries_table_constraints() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Valid episode without file_id
        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s1', 'episode', 'test content', 1000, 1000)",
            [],
        )
        .unwrap();

        // Invalid kind should fail
        let result = conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s2', 'invalid', 'content', 1000, 1000)",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_fresh_db_migrates_from_v0() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn test_already_current_is_noop() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Count objects before second init
        let count_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();

        init_schema(&conn).unwrap();

        let count_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count_before, count_after);

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn test_future_version_rejected() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();

        let result = init_schema(&conn);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("newer"),
            "error should mention 'newer', got: {err_msg}"
        );
    }

    #[test]
    fn test_version_not_stamped_without_migration() {
        let conn = Connection::open_in_memory().unwrap();

        // Bootstrap a real v1 database first
        init_schema(&conn).unwrap();

        // Simulate a hypothetical SCHEMA_VERSION bump by setting a future
        // version that no migration handles. If init_schema unconditionally
        // stamped the version, it would silently overwrite this.
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 99)
            .unwrap();

        // Re-opening should reject the future version, NOT silently stamp it
        let result = init_schema(&conn);
        assert!(result.is_err());

        // Version must remain untouched
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION + 99);
    }
}

#[cfg(test)]
mod projection_tests {
    use super::*;
    use rusqlite::Connection;

    fn setup(conn: &Connection) {
        init_schema(conn).unwrap();
    }

    fn make_projection(brain_id: &str, name: &str) -> BrainProjection {
        BrainProjection {
            brain_id: brain_id.to_string(),
            name: name.to_string(),
            prefix: name[..3.min(name.len())].to_uppercase(),
            roots_json: "[\"/home/user/code\"]".to_string(),
            notes_json: "[\"/home/user/notes\"]".to_string(),
            aliases_json: "[\"alias1\"]".to_string(),
            archived: false,
        }
    }

    #[test]
    fn test_project_insert_and_read_back() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let brain = BrainProjection {
            brain_id: "brain-abc".to_string(),
            name: "myproject".to_string(),
            prefix: "MYP".to_string(),
            roots_json: "[\"/home/user/myproject\"]".to_string(),
            notes_json: "[\"/home/user/notes\"]".to_string(),
            aliases_json: "[\"mp\",\"proj\"]".to_string(),
            archived: false,
        };

        project_config_to_brains(&conn, &[brain]).unwrap();

        let (roots, aliases, notes, projected, prefix): (String, String, String, i32, String) = conn
            .query_row(
                "SELECT roots, aliases, notes, projected, prefix FROM brains WHERE brain_id = 'brain-abc'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();

        assert_eq!(roots, "[\"/home/user/myproject\"]");
        assert_eq!(aliases, "[\"mp\",\"proj\"]");
        assert_eq!(notes, "[\"/home/user/notes\"]");
        assert_eq!(projected, 1);
        assert_eq!(prefix, "MYP");
    }

    #[test]
    fn test_re_project_with_removed_brain_sets_projected_zero() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        // Project two brains
        let b1 = make_projection("b1", "alpha");
        let b2 = make_projection("b2", "beta");
        project_config_to_brains(&conn, &[b1, b2]).unwrap();

        // Re-project with only b1 — b2 should become projected=0
        let b1_again = make_projection("b1", "alpha");
        project_config_to_brains(&conn, &[b1_again]).unwrap();

        let b1_projected: i32 = conn
            .query_row(
                "SELECT projected FROM brains WHERE brain_id = 'b1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(b1_projected, 1, "b1 should remain projected=1");

        // b2 should still exist but with projected=0 (soft-cleared, not deleted)
        let b2_projected: i32 = conn
            .query_row(
                "SELECT projected FROM brains WHERE brain_id = 'b2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(b2_projected, 0, "b2 should be soft-cleared to projected=0");
    }

    #[test]
    fn test_ensure_brain_registered_then_project_preserves_prefix() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        // ensure_brain_registered sets the prefix first
        ensure_brain_registered(&conn, "brain-xyz", "zetabrain").unwrap();

        // Verify prefix was set
        let prefix_before: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'brain-xyz'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !prefix_before.is_empty(),
            "prefix should be set by ensure_brain_registered"
        );

        // project_config_to_brains with a different prefix value — COALESCE should preserve the original
        let brain = BrainProjection {
            brain_id: "brain-xyz".to_string(),
            name: "zetabrain".to_string(),
            prefix: "NEWPREFIX".to_string(),
            roots_json: "[]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain]).unwrap();

        // With UPSERT, COALESCE(brains.prefix, excluded.prefix) preserves the existing prefix.
        let prefix_after: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'brain-xyz'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            prefix_after, prefix_before,
            "UPSERT should preserve the original prefix via COALESCE"
        );
    }

    #[test]
    fn test_empty_roots_and_aliases_arrays() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let brain = BrainProjection {
            brain_id: "brain-empty".to_string(),
            name: "emptyarrays".to_string(),
            prefix: "EMP".to_string(),
            roots_json: "[]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };

        project_config_to_brains(&conn, &[brain]).unwrap();

        let (roots, aliases): (String, String) = conn
            .query_row(
                "SELECT roots, aliases FROM brains WHERE brain_id = 'brain-empty'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(roots, "[]");
        assert_eq!(aliases, "[]");
    }

    #[test]
    fn test_prefix_survives_multiple_projection_cycles() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let brain = BrainProjection {
            brain_id: "brain-multi".to_string(),
            name: "multiproject".to_string(),
            prefix: "ABC".to_string(),
            roots_json: "[\"/first\"]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain]).unwrap();

        // Second projection with a DIFFERENT prefix — COALESCE preserves "ABC"
        let brain2 = BrainProjection {
            brain_id: "brain-multi".to_string(),
            name: "multiproject".to_string(),
            prefix: "XYZ".to_string(),
            roots_json: "[\"/second\"]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain2]).unwrap();

        let prefix: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'brain-multi'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            prefix, "ABC",
            "prefix should survive second projection cycle"
        );

        // Third projection — still preserved
        let brain3 = BrainProjection {
            brain_id: "brain-multi".to_string(),
            name: "multiproject".to_string(),
            prefix: "QQQ".to_string(),
            roots_json: "[\"/third\"]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain3]).unwrap();

        let prefix: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'brain-multi'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            prefix, "ABC",
            "prefix should survive third projection cycle"
        );
    }

    #[test]
    fn test_upsert_brain_preserves_prefix_via_coalesce() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        upsert_brain(
            &conn,
            &BrainUpsert {
                brain_id: "brain-ups",
                name: "upsertbrain",
                prefix: "UPS",
                roots_json: "[\"/root\"]",
                notes_json: "[]",
                aliases_json: "[]",
                archived: false,
            },
        )
        .unwrap();

        let prefix: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'brain-ups'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prefix, "UPS");

        // Second upsert with different prefix — COALESCE preserves original
        upsert_brain(
            &conn,
            &BrainUpsert {
                brain_id: "brain-ups",
                name: "upsertbrain",
                prefix: "NEW",
                roots_json: "[\"/root2\"]",
                notes_json: "[]",
                aliases_json: "[\"al\"]",
                archived: false,
            },
        )
        .unwrap();

        let prefix: String = conn
            .query_row(
                "SELECT prefix FROM brains WHERE brain_id = 'brain-ups'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            prefix, "UPS",
            "upsert_brain COALESCE should preserve the original prefix"
        );

        // But other fields should update
        let (roots, aliases): (String, String) = conn
            .query_row(
                "SELECT roots, aliases FROM brains WHERE brain_id = 'brain-ups'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(roots, "[\"/root2\"]");
        assert_eq!(aliases, "[\"al\"]");
    }

    #[test]
    fn test_list_brains_returns_correct_data() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let b1 = BrainProjection {
            brain_id: "b-list-1".to_string(),
            name: "alpha".to_string(),
            prefix: "ALP".to_string(),
            roots_json: "[\"/alpha\"]".to_string(),
            notes_json: "[\"/alpha/notes\"]".to_string(),
            aliases_json: "[\"a\"]".to_string(),
            archived: false,
        };
        let b2 = BrainProjection {
            brain_id: "b-list-2".to_string(),
            name: "beta".to_string(),
            prefix: "BET".to_string(),
            roots_json: "[\"/beta\"]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: true,
        };
        project_config_to_brains(&conn, &[b1, b2]).unwrap();

        // list all
        let all = list_brains(&conn, false).unwrap();
        let test_brains: Vec<_> = all
            .iter()
            .filter(|b| b.brain_id.starts_with("b-list-"))
            .collect();
        assert_eq!(test_brains.len(), 2);

        let a = test_brains.iter().find(|b| b.name == "alpha").unwrap();
        assert_eq!(a.prefix.as_deref(), Some("ALP"));
        assert_eq!(a.roots_json.as_deref(), Some("[\"/alpha\"]"));
        assert!(!a.archived);
        assert!(a.projected);

        // list active only — archived beta excluded
        let active = list_brains(&conn, true).unwrap();
        let active_test: Vec<_> = active
            .iter()
            .filter(|b| b.brain_id.starts_with("b-list-"))
            .collect();
        assert_eq!(active_test.len(), 1);
        assert_eq!(active_test[0].name, "alpha");
    }

    #[test]
    fn test_get_brain_and_get_brain_by_name() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let brain = BrainProjection {
            brain_id: "brain-get-test".to_string(),
            name: "gettest".to_string(),
            prefix: "GET".to_string(),
            roots_json: "[\"/get\"]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[\"gt\"]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain]).unwrap();

        let row = get_brain(&conn, "brain-get-test").unwrap().unwrap();
        assert_eq!(row.name, "gettest");
        assert_eq!(row.prefix.as_deref(), Some("GET"));
        assert!(row.projected);

        let row2 = get_brain_by_name(&conn, "gettest").unwrap().unwrap();
        assert_eq!(row2.brain_id, "brain-get-test");

        assert!(get_brain(&conn, "nonexistent").unwrap().is_none());
        assert!(get_brain_by_name(&conn, "nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_delete_brain() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let brain = BrainProjection {
            brain_id: "brain-del".to_string(),
            name: "deleteme".to_string(),
            prefix: "DEL".to_string(),
            roots_json: "[]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain]).unwrap();

        let deleted = delete_brain(&conn, "deleteme").unwrap();
        assert!(deleted);
        assert!(get_brain(&conn, "brain-del").unwrap().is_none());

        let deleted_again = delete_brain(&conn, "deleteme").unwrap();
        assert!(!deleted_again);
    }

    #[test]
    fn test_duplicate_alias_across_brains_first_seen_wins() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let b1 = BrainProjection {
            brain_id: "b1".to_string(),
            name: "first".to_string(),
            prefix: "FIR".to_string(),
            roots_json: "[]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[\"shared-alias\"]".to_string(),
            archived: false,
        };
        let b2 = BrainProjection {
            brain_id: "b2".to_string(),
            name: "second".to_string(),
            prefix: "SEC".to_string(),
            roots_json: "[]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[\"shared-alias\",\"unique-alias\"]".to_string(),
            archived: false,
        };

        project_config_to_brains(&conn, &[b1, b2]).unwrap();

        // b1 keeps shared-alias
        let b1_aliases: String = conn
            .query_row(
                "SELECT aliases FROM brains WHERE brain_id = 'b1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            b1_aliases.contains("shared-alias"),
            "b1 should keep shared-alias"
        );

        // b2 loses shared-alias but keeps unique-alias
        let b2_aliases: String = conn
            .query_row(
                "SELECT aliases FROM brains WHERE brain_id = 'b2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !b2_aliases.contains("shared-alias"),
            "b2 should not have shared-alias"
        );
        assert!(
            b2_aliases.contains("unique-alias"),
            "b2 should keep unique-alias"
        );
    }
}

#[cfg(test)]
mod resolver_tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_with_brains(conn: &Connection) {
        init_schema(conn).unwrap();

        let brains = vec![
            BrainProjection {
                brain_id: "brain-001".to_string(),
                name: "gateway".to_string(),
                prefix: "GAT".to_string(),
                roots_json: "[\"/home/user/projects/gateway\"]".to_string(),
                notes_json: "[]".to_string(),
                aliases_json: "[\"gw\",\"gate\"]".to_string(),
                archived: false,
            },
            BrainProjection {
                brain_id: "brain-002".to_string(),
                name: "analytics".to_string(),
                prefix: "ANA".to_string(),
                roots_json: "[\"/home/user/projects\",\"/home/user/projects/analytics\"]"
                    .to_string(),
                notes_json: "[]".to_string(),
                aliases_json: "[\"an\"]".to_string(),
                archived: false,
            },
        ];

        project_config_to_brains(conn, &brains).unwrap();
    }

    #[test]
    fn test_resolve_by_name() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_brains(&conn);

        let (id, name) = resolve_brain(&conn, "gateway").unwrap();
        assert_eq!(id, "brain-001");
        assert_eq!(name, "gateway");
    }

    #[test]
    fn test_resolve_by_brain_id() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_brains(&conn);

        let (id, name) = resolve_brain(&conn, "brain-002").unwrap();
        assert_eq!(id, "brain-002");
        assert_eq!(name, "analytics");
    }

    #[test]
    fn test_resolve_by_alias() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_brains(&conn);

        let (id, name) = resolve_brain(&conn, "gw").unwrap();
        assert_eq!(id, "brain-001");
        assert_eq!(name, "gateway");

        let (id2, name2) = resolve_brain(&conn, "an").unwrap();
        assert_eq!(id2, "brain-002");
        assert_eq!(name2, "analytics");
    }

    #[test]
    fn test_resolve_by_root_path() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_brains(&conn);

        // Matches gateway's root exactly
        let (id, name) = resolve_brain(&conn, "/home/user/projects/gateway").unwrap();
        assert_eq!(id, "brain-001");
        assert_eq!(name, "gateway");
    }

    #[test]
    fn test_resolve_root_path_longer_prefix_wins() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_brains(&conn);

        // analytics has both "/home/user/projects" and "/home/user/projects/analytics"
        // A path under /home/user/projects/analytics should match analytics (longer)
        let (id, name) = resolve_brain(&conn, "/home/user/projects/analytics/src/main.rs").unwrap();
        assert_eq!(id, "brain-002", "longer root prefix should win");
        assert_eq!(name, "analytics");

        // A path only under /home/user/projects (not analytics subfolder) matches analytics too
        // (analytics has the /home/user/projects root — but gateway doesn't)
        let (id2, _) = resolve_brain(&conn, "/home/user/projects/other/file.rs").unwrap();
        assert_eq!(
            id2, "brain-002",
            "should match shorter prefix when longer doesn't apply"
        );
    }

    #[test]
    fn test_name_shadows_alias() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // brain-001 is named "gateway"; brain-002 has "gateway" as an alias
        let brains = vec![
            BrainProjection {
                brain_id: "brain-001".to_string(),
                name: "gateway".to_string(),
                prefix: "GAT".to_string(),
                roots_json: "[]".to_string(),
                notes_json: "[]".to_string(),
                aliases_json: "[]".to_string(),
                archived: false,
            },
            BrainProjection {
                brain_id: "brain-002".to_string(),
                name: "other".to_string(),
                prefix: "OTH".to_string(),
                roots_json: "[]".to_string(),
                notes_json: "[]".to_string(),
                aliases_json: "[\"gateway\"]".to_string(),
                archived: false,
            },
        ];
        project_config_to_brains(&conn, &brains).unwrap();

        // Name match on "gateway" should return brain-001, NOT the alias in brain-002
        let (id, _) = resolve_brain(&conn, "gateway").unwrap();
        assert_eq!(id, "brain-001", "name should shadow alias");
    }

    #[test]
    fn test_resolve_nonexistent_returns_error() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_brains(&conn);

        let result = resolve_brain(&conn, "nonexistent-brain-xyz");
        assert!(result.is_err(), "nonexistent brain should return error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("brain not found"),
            "error should mention brain not found, got: {err}"
        );
    }

    #[test]
    fn test_removed_brain_resolvable_by_name_and_id_not_alias_or_root() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        // Project with brain that has alias and root
        let brain = BrainProjection {
            brain_id: "brain-removed".to_string(),
            name: "removedproject".to_string(),
            prefix: "REM".to_string(),
            roots_json: "[\"/home/user/removed\"]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[\"rem\"]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain]).unwrap();

        // Now re-project with an empty set — the brain gets projected=0 (via UPDATE SET projected=0)
        // and then deleted (via DELETE). To test the "removed but not deleted" case, we manually
        // set projected=0 without deleting the row.
        conn.execute(
            "UPDATE brains SET projected = 0 WHERE brain_id = 'brain-removed'",
            [],
        )
        .unwrap();

        // Resolvable by name — projected=0 rows are still accessible by name
        let (id, _) = resolve_brain(&conn, "removedproject").unwrap();
        assert_eq!(id, "brain-removed");

        // Resolvable by brain_id
        let (id2, _) = resolve_brain(&conn, "brain-removed").unwrap();
        assert_eq!(id2, "brain-removed");

        // NOT resolvable by alias (alias resolution is projected=1 only)
        let alias_result = resolve_brain(&conn, "rem");
        assert!(
            alias_result.is_err(),
            "removed brain alias should not resolve"
        );

        // NOT resolvable by root path (root resolution is projected=1 only)
        let root_result = resolve_brain(&conn, "/home/user/removed/file.rs");
        assert!(
            root_result.is_err(),
            "removed brain root should not resolve"
        );
    }

    #[test]
    fn test_empty_aliases_array_does_not_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let brain = BrainProjection {
            brain_id: "brain-noalias".to_string(),
            name: "noalias".to_string(),
            prefix: "NOA".to_string(),
            roots_json: "[]".to_string(),
            notes_json: "[]".to_string(),
            aliases_json: "[]".to_string(),
            archived: false,
        };
        project_config_to_brains(&conn, &[brain]).unwrap();

        let result = resolve_brain(&conn, "somealiasvalue");
        assert!(result.is_err(), "empty aliases should not match any input");
    }
}
