use rusqlite::Connection;

use crate::error::Result;

/// v14 → v15: Add `target_file_id` to `links` and `pagerank_score` to `files`.
///
/// - `links.target_file_id`: nullable FK to `files(file_id)`, resolved at index
///   time for wiki/markdown links. External links remain NULL.
/// - `files.pagerank_score`: nullable REAL, populated by the optimize cycle.
pub fn migrate_v14_to_v15(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        ALTER TABLE links ADD COLUMN target_file_id TEXT REFERENCES files(file_id) ON DELETE SET NULL;
        CREATE INDEX idx_links_target_file ON links(target_file_id);

        UPDATE links SET target_file_id = (
            SELECT file_id FROM files
            WHERE files.path = links.target_path
               OR files.path LIKE '%/' || links.target_path || '.md'
               OR files.path LIKE '%/' || links.target_path
        ) WHERE link_type IN ('wiki', 'markdown');

        ALTER TABLE files ADD COLUMN pagerank_score REAL;

        PRAGMA user_version = 15;

        COMMIT;
    ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal v14 schema: files + links tables only (no other dependencies needed).
    fn setup_v14(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;

             CREATE TABLE files (
                 file_id         TEXT PRIMARY KEY,
                 path            TEXT UNIQUE NOT NULL,
                 content_hash    TEXT,
                 mtime           INTEGER,
                 size            INTEGER,
                 last_indexed_at INTEGER,
                 deleted_at      INTEGER,
                 indexing_state  TEXT NOT NULL DEFAULT 'idle'
             );

             CREATE TABLE links (
                 link_id        TEXT PRIMARY KEY,
                 source_file_id TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
                 target_path    TEXT NOT NULL,
                 link_text      TEXT,
                 link_type      TEXT NOT NULL CHECK(link_type IN ('wiki', 'markdown', 'external'))
             );
             CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_file_id);
             CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_path);

             PRAGMA user_version = 14;",
        )
        .unwrap();
    }

    #[test]
    fn test_schema_version_is_15_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);

        migrate_v14_to_v15(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 15);
    }

    #[test]
    fn test_target_file_id_column_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        // Insert a file and a link — target_file_id column must exist
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f1', '/notes/foo.md')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO links (link_id, source_file_id, target_path, link_type, target_file_id)
             VALUES ('l1', 'f1', 'foo.md', 'markdown', NULL)",
            [],
        )
        .unwrap();

        let val: Option<String> = conn
            .query_row(
                "SELECT target_file_id FROM links WHERE link_id = 'l1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn test_pagerank_score_column_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f1', '/notes/bar.md')",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE files SET pagerank_score = 0.42 WHERE file_id = 'f1'",
            [],
        )
        .unwrap();

        let score: Option<f64> = conn
            .query_row(
                "SELECT pagerank_score FROM files WHERE file_id = 'f1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(score, Some(0.42));
    }

    #[test]
    fn test_backfill_resolves_wiki_and_markdown_links() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        setup_v14(&conn);

        // Seed files
        conn.execute_batch(
            "INSERT INTO files (file_id, path) VALUES
                 ('f1', '/notes/source.md'),
                 ('f2', '/notes/headings.md'),
                 ('f3', '/notes/simple.md');",
        )
        .unwrap();

        // Seed links (pre-migration, no target_file_id column yet)
        conn.execute_batch(
            "INSERT INTO links (link_id, source_file_id, target_path, link_type) VALUES
                 ('l1', 'f1', 'headings', 'wiki'),
                 ('l2', 'f1', 'simple.md', 'markdown'),
                 ('l3', 'f1', 'https://example.com', 'external');",
        )
        .unwrap();

        migrate_v14_to_v15(&conn).unwrap();

        // Wiki link "headings" should resolve to f2 (/notes/headings.md via LIKE '%/headings.md')
        let wiki_target: Option<String> = conn
            .query_row(
                "SELECT target_file_id FROM links WHERE link_id = 'l1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(wiki_target, Some("f2".to_string()), "wiki link should resolve to f2");

        // Markdown link "simple.md" should resolve to f3 (/notes/simple.md via path = target_path)
        let md_target: Option<String> = conn
            .query_row(
                "SELECT target_file_id FROM links WHERE link_id = 'l2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(md_target, Some("f3".to_string()), "markdown link should resolve to f3");

        // External link should remain NULL
        let ext_target: Option<String> = conn
            .query_row(
                "SELECT target_file_id FROM links WHERE link_id = 'l3'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ext_target.is_none(), "external link should have NULL target_file_id");
    }

    #[test]
    fn test_target_file_index_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);
        migrate_v14_to_v15(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                  WHERE type = 'index' AND name = 'idx_links_target_file'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_links_target_file index should exist");
    }

    #[test]
    fn test_unresolvable_wiki_link_stays_null() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v14(&conn);

        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f1', '/notes/source.md')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO links (link_id, source_file_id, target_path, link_type)
             VALUES ('l1', 'f1', 'nonexistent', 'wiki')",
            [],
        )
        .unwrap();

        migrate_v14_to_v15(&conn).unwrap();

        let target: Option<String> = conn
            .query_row(
                "SELECT target_file_id FROM links WHERE link_id = 'l1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(target.is_none(), "unresolvable wiki link should remain NULL");
    }
}
