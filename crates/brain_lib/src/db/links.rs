use rusqlite::Connection;
use ulid::Ulid;

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

    super::collect_rows(rows)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::files::get_or_create_file_id;
    use crate::db::migrations::migrate_v14_to_v15;
    use crate::db::schema::init_schema;
    use crate::links::{Link, LinkType};

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        // Apply v14→v15 to add target_file_id to links and pagerank_score to files.
        // init_schema currently stamps v14; this will be removed once schema.rs is bumped to v15.
        migrate_v14_to_v15(&conn).unwrap();
        conn
    }

    #[test]
    fn test_replace_links_and_backlinks() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/graph.md").unwrap();

        let links = vec![
            Link {
                target: "headings".to_string(),
                link_text: "headings".to_string(),
                link_type: LinkType::Wiki,
            },
            Link {
                target: "simple.md".to_string(),
                link_text: "the simple note".to_string(),
                link_type: LinkType::Markdown,
            },
        ];

        replace_links(&conn, &file_id, &links).unwrap();

        // Verify links stored
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM links WHERE source_file_id = ?1",
                [&file_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Query backlinks
        let backlinks = get_backlinks(&conn, "headings").unwrap();
        assert_eq!(backlinks.len(), 1);
        assert_eq!(backlinks[0].0, file_id);
        assert_eq!(backlinks[0].1, "headings");

        assert_eq!(count_backlinks(&conn, "headings").unwrap(), 1);
        assert_eq!(count_backlinks(&conn, "nonexistent").unwrap(), 0);
    }

    #[test]
    fn test_replace_links_is_atomic() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();

        let links_v1 = vec![Link {
            target: "old".to_string(),
            link_text: "old".to_string(),
            link_type: LinkType::Wiki,
        }];
        replace_links(&conn, &file_id, &links_v1).unwrap();

        let links_v2 = vec![Link {
            target: "new".to_string(),
            link_text: "new".to_string(),
            link_type: LinkType::Wiki,
        }];
        replace_links(&conn, &file_id, &links_v2).unwrap();

        // Old link gone
        assert_eq!(count_backlinks(&conn, "old").unwrap(), 0);
        // New link present
        assert_eq!(count_backlinks(&conn, "new").unwrap(), 1);
    }

    #[test]
    fn test_replace_does_not_affect_other_files() {
        let conn = setup();
        let (file_a, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();
        let (file_b, _) = get_or_create_file_id(&conn, "/notes/b.md").unwrap();

        replace_links(
            &conn,
            &file_a,
            &[Link {
                target: "shared".to_string(),
                link_text: "shared".to_string(),
                link_type: LinkType::Wiki,
            }],
        )
        .unwrap();
        replace_links(
            &conn,
            &file_b,
            &[Link {
                target: "shared".to_string(),
                link_text: "shared".to_string(),
                link_type: LinkType::Wiki,
            }],
        )
        .unwrap();

        // Both link to "shared"
        assert_eq!(count_backlinks(&conn, "shared").unwrap(), 2);

        // Replace file_a links with empty
        replace_links(&conn, &file_a, &[]).unwrap();

        // file_b's link still intact
        assert_eq!(count_backlinks(&conn, "shared").unwrap(), 1);
    }
}
