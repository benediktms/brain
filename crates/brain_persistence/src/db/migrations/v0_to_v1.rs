use rusqlite::Connection;

use crate::error::Result;

/// Fresh database: create all tables and stamp version 1.
pub fn migrate_v0_to_v1(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

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

        PRAGMA user_version = 1;

        COMMIT;
        ",
    )?;
    Ok(())
}
