use rusqlite::Connection;

use crate::error::Result;

/// Bump this when the schema changes after release.
/// During pre-release development, stay at 1 and wipe the DB for breaking changes.
const SCHEMA_VERSION: i32 = 1;

/// Initialize the database schema: WAL mode, foreign keys, and all tables.
pub fn init_schema(conn: &Connection) -> Result<()> {
    // Enable WAL mode for concurrent reads during writes
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    let current: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(crate::error::BrainCoreError::SchemaVersion(format!(
            "database schema version {current} is newer than supported version {SCHEMA_VERSION}"
        )));
    }

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS files (
            file_id         TEXT PRIMARY KEY,
            path            TEXT UNIQUE NOT NULL,
            content_hash    TEXT,
            mtime           INTEGER,
            size            INTEGER,
            last_indexed_at INTEGER,
            deleted_at      INTEGER,
            indexing_state  TEXT NOT NULL DEFAULT 'idle'
        );

        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id        TEXT PRIMARY KEY,
            file_id         TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
            chunk_ord       INTEGER NOT NULL,
            chunk_hash      TEXT NOT NULL,
            content         TEXT NOT NULL DEFAULT '',
            chunker_version INTEGER NOT NULL DEFAULT 1,
            heading_path    TEXT,
            byte_start      INTEGER,
            byte_end        INTEGER,
            token_estimate  INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON chunks(file_id);

        CREATE TABLE IF NOT EXISTS links (
            link_id        TEXT PRIMARY KEY,
            source_file_id TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
            target_path    TEXT NOT NULL,
            link_text      TEXT,
            link_type      TEXT NOT NULL CHECK(link_type IN ('wiki', 'markdown', 'external'))
        );
        CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_file_id);
        CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_path);

        CREATE TABLE IF NOT EXISTS summaries (
            summary_id  TEXT PRIMARY KEY,
            file_id     TEXT REFERENCES files(file_id) ON DELETE SET NULL,
            kind        TEXT NOT NULL CHECK(kind IN ('episode', 'reflection', 'summary')),
            title       TEXT,
            content     TEXT NOT NULL,
            tags        TEXT NOT NULL DEFAULT '[]',
            importance  REAL NOT NULL DEFAULT 1.0,
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL,
            valid_from  INTEGER,
            valid_to    INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind);

        CREATE TABLE IF NOT EXISTS reflection_sources (
            reflection_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
            source_id     TEXT NOT NULL,
            PRIMARY KEY (reflection_id, source_id)
        );
        ",
    )?;

    // FTS5 and triggers use semicolons in bodies — create them individually.
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

    conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;

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
}
