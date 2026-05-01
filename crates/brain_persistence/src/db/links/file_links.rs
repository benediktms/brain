//! Legacy file/wiki link helpers.
//!
//! Backing SQL table: `links` (note linking, used by pagerank). Distinct from
//! the polymorphic `entity_links` table managed by `entity_graph`.

use rusqlite::Connection;
use ulid::Ulid;

use crate::db::collect_rows;
use crate::error::Result;
use crate::links::Link;

/// Resolve a link's `target_path` to a `file_id` using Obsidian-style disambiguation.
///
/// Resolution order (for wiki/markdown links):
/// 1. Exact `path` match (handles absolute paths stored as-is)
/// 2. Path ends with `/<target>.md` (wiki bare stems, e.g. "headings" → .../headings.md)
/// 3. Path ends with `/<target>` (markdown links that already carry an extension)
///
/// When multiple files match the same rule, the shortest path wins (nearest-match
/// semantics, mimicking Obsidian). Returns `None` for external links or no match.
pub(crate) fn resolve_target_file_id(
    conn: &Connection,
    target_path: &str,
    link_type: &str,
) -> Option<String> {
    if link_type == "external" {
        return None;
    }

    // Collect all candidate (file_id, path) rows that match any of the three strategies.
    let suffix_with_md = format!("/{}.md", target_path);
    let suffix_bare = format!("/{}", target_path);

    let mut stmt = conn
        .prepare_cached(
            "SELECT file_id, path FROM files
              WHERE path = ?1
                 OR path LIKE '%' || ?2
                 OR path LIKE '%' || ?3",
        )
        .ok()?;

    let candidates: Vec<(String, String)> = stmt
        .query_map(
            rusqlite::params![target_path, suffix_with_md, suffix_bare],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    // Pick the candidate with the shortest path (nearest-match).
    candidates
        .into_iter()
        .min_by_key(|(_, path)| path.len())
        .map(|(file_id, _)| file_id)
}

/// Returns true if the `links.target_file_id` column exists (i.e. v15+ schema).
fn has_target_file_id_column(conn: &Connection) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('links') WHERE name = 'target_file_id'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|n| n > 0)
    .unwrap_or(false)
}

/// Atomically replace all links for a file.
///
/// Deletes existing links for the `source_file_id`, then inserts the new set.
/// When the schema is at v15+, resolves `target_file_id` for wiki/markdown links
/// at insert time. Falls back to the v14 INSERT when the column is absent.
pub fn replace_links(conn: &Connection, source_file_id: &str, links: &[Link]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute(
        "DELETE FROM links WHERE source_file_id = ?1",
        [source_file_id],
    )?;

    let with_target_file_id = has_target_file_id_column(&tx);

    if with_target_file_id {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO links (link_id, source_file_id, target_path, link_text, link_type, target_file_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for link in links {
            let target_file_id = resolve_target_file_id(&tx, &link.target, link.link_type.as_str());
            stmt.execute(rusqlite::params![
                Ulid::new().to_string(),
                source_file_id,
                link.target,
                link.link_text,
                link.link_type.as_str(),
                target_file_id,
            ])?;
        }
    } else {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO links (link_id, source_file_id, target_path, link_text, link_type)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for link in links {
            stmt.execute(rusqlite::params![
                Ulid::new().to_string(),
                source_file_id,
                link.target,
                link.link_text,
                link.link_type.as_str(),
            ])?;
        }
    }

    tx.commit()?;
    Ok(())
}

/// Get all files that link to the given target path.
///
/// Returns `(source_file_id, link_text)` pairs.
pub fn get_backlinks(conn: &Connection, target_path: &str) -> Result<Vec<(String, String)>> {
    let mut stmt =
        conn.prepare("SELECT source_file_id, link_text FROM links WHERE target_path = ?1")?;
    let rows = stmt.query_map([target_path], |row| Ok((row.get(0)?, row.get(1)?)))?;

    collect_rows(rows)
}

/// Get all file_ids that `source_file_id` links to (outgoing 1-hop neighbours).
///
/// Returns resolved `target_file_id` values. When `target_file_id` is already
/// set on the link row, it is used directly. For wiki/markdown links where
/// `target_file_id` is NULL (e.g. because the target file was indexed after
/// the source file), the `target_path` is resolved against the `files` table
/// using Obsidian-style nearest-match logic.
///
/// External links and links whose target cannot be resolved are excluded.
pub fn get_outlinks(conn: &Connection, source_file_id: &str) -> Result<Vec<String>> {
    // Collect all outgoing links for this source file. We first gather all rows
    // into memory, then resolve target_file_id for unresolved entries afterwards.
    // This avoids re-entering the connection while a cursor is still open.
    let link_rows: Vec<(Option<String>, String, String)> = {
        let mut stmt = conn.prepare_cached(
            "SELECT l.target_file_id, l.target_path, l.link_type
             FROM links l
             WHERE l.source_file_id = ?1 AND l.link_type != 'external'",
        )?;
        let rows = stmt.query_map([source_file_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        collect_rows(rows)?
    };

    let mut result: Vec<String> = Vec::new();
    for (target_file_id, target_path, link_type) in link_rows {
        let resolved_fid = if let Some(fid) = target_file_id {
            Some(fid)
        } else {
            // Attempt runtime resolution via the files table.
            // Safe: the cursor from the SELECT above is fully consumed before this.
            resolve_target_file_id(conn, &target_path, &link_type)
        };
        if let Some(fid) = resolved_fid
            && !result.contains(&fid)
        {
            result.push(fid);
        }
    }
    Ok(result)
}

/// Count backlinks for a given target path.
pub fn count_backlinks(conn: &Connection, target_path: &str) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM links WHERE target_path = ?1",
        [target_path],
        |row| row.get(0),
    )?;
    Ok(count as usize)
}
