use rusqlite::Connection;

use crate::error::Result;

/// v18 → v19: Schema hardening — FK constraints, dead-column drops, CHECK and NOT NULL additions.
///
/// Changes applied:
///
/// 1. `files`         — DROP COLUMN mtime, DROP COLUMN size; recreate with
///                      CHECK (indexing_state IN ('idle', 'indexing_started', 'indexed'))
/// 2. `chunks`        — fill NULLs in heading_path/byte_start/byte_end/token_estimate,
///                      recreate with NOT NULL DEFAULT on those 4 columns
/// 3. `task_events`   — recreate with FK task_id → tasks(task_id) ON DELETE CASCADE
/// 4. `record_events` — recreate with FK record_id → records(record_id) ON DELETE CASCADE
/// 5. `task_note_links` — recreate with FK chunk_id → chunks(chunk_id) ON DELETE CASCADE
/// 6. `reflection_sources` — recreate with FK source_id → summaries(summary_id) ON DELETE CASCADE
/// 7. `record_links`  — recreate with composite UNIQUE (record_id, task_id, chunk_id)
///
/// FTS5 triggers on `chunks` (chunks_fts_insert / chunks_fts_delete / chunks_fts_update) are
/// dropped before rebuilding `chunks`. `ensure_fts5()` recreates them idempotently after migration.
pub fn migrate_v18_to_v19(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;

        BEGIN;

        -- ── 1. files: drop dead columns, add CHECK constraint ─────────────────────

        CREATE TABLE files_new (
            file_id         TEXT PRIMARY KEY,
            path            TEXT UNIQUE NOT NULL,
            content_hash    TEXT,
            last_indexed_at INTEGER,
            deleted_at      INTEGER,
            indexing_state  TEXT NOT NULL DEFAULT 'idle'
                            CHECK (indexing_state IN ('idle', 'indexing_started', 'indexed')),
            chunker_version INTEGER,
            pagerank_score  REAL
        );

        INSERT INTO files_new
            SELECT file_id, path, content_hash, last_indexed_at, deleted_at,
                   indexing_state, chunker_version, pagerank_score
            FROM files;

        DROP TABLE files;
        ALTER TABLE files_new RENAME TO files;

        CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_file_id);
        CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_path);
        CREATE INDEX IF NOT EXISTS idx_links_target_file ON links(target_file_id);

        -- ── 2. chunks: fill NULLs, drop FTS triggers, recreate with NOT NULL ──────

        UPDATE chunks
            SET heading_path   = COALESCE(heading_path, ''),
                byte_start     = COALESCE(byte_start, 0),
                byte_end       = COALESCE(byte_end, 0),
                token_estimate = COALESCE(token_estimate, 0);

        DROP TRIGGER IF EXISTS chunks_fts_insert;
        DROP TRIGGER IF EXISTS chunks_fts_delete;
        DROP TRIGGER IF EXISTS chunks_fts_update;
        DROP TABLE IF EXISTS fts_chunks;

        CREATE TABLE chunks_new (
            chunk_id        TEXT PRIMARY KEY,
            file_id         TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
            chunk_ord       INTEGER NOT NULL,
            chunk_hash      TEXT NOT NULL,
            content         TEXT NOT NULL DEFAULT '',
            chunker_version INTEGER NOT NULL DEFAULT 1,
            heading_path    TEXT NOT NULL DEFAULT '',
            byte_start      INTEGER NOT NULL DEFAULT 0,
            byte_end        INTEGER NOT NULL DEFAULT 0,
            token_estimate  INTEGER NOT NULL DEFAULT 0
        );

        INSERT INTO chunks_new
            SELECT chunk_id, file_id, chunk_ord, chunk_hash, content,
                   chunker_version, heading_path, byte_start, byte_end, token_estimate
            FROM chunks;

        DROP TABLE chunks;
        ALTER TABLE chunks_new RENAME TO chunks;

        CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON chunks(file_id);

        -- ── 3. task_events: add FK, normalise columns ──────────────────────────────

        CREATE TABLE task_events_new (
            event_id   TEXT PRIMARY KEY,
            task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            timestamp  INTEGER NOT NULL,
            actor      TEXT NOT NULL,
            payload    TEXT NOT NULL DEFAULT '{}'
        );

        INSERT INTO task_events_new (event_id, task_id, event_type, timestamp, actor, payload)
            SELECT event_id, task_id, event_type, timestamp, actor, payload
            FROM task_events;

        DROP TABLE task_events;
        ALTER TABLE task_events_new RENAME TO task_events;

        CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

        -- ── 4. record_events: add FK on record_id ─────────────────────────────────

        CREATE TABLE record_events_new (
            event_id   TEXT PRIMARY KEY,
            record_id  TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            timestamp  INTEGER NOT NULL,
            actor      TEXT NOT NULL,
            payload    TEXT NOT NULL DEFAULT '{}',
            brain_id   TEXT NOT NULL DEFAULT ''
        );

        INSERT INTO record_events_new (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
            SELECT event_id, record_id, event_type, timestamp, actor, payload, brain_id
            FROM record_events;

        DROP TABLE record_events;
        ALTER TABLE record_events_new RENAME TO record_events;

        CREATE INDEX IF NOT EXISTS record_events_record_id ON record_events(record_id);

        -- ── 5. task_note_links: add FK on chunk_id ─────────────────────────────────

        CREATE TABLE task_note_links_new (
            task_id  TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
            chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
            PRIMARY KEY (task_id, chunk_id)
        );

        INSERT INTO task_note_links_new SELECT task_id, chunk_id FROM task_note_links;

        DROP TABLE task_note_links;
        ALTER TABLE task_note_links_new RENAME TO task_note_links;

        -- ── 6. reflection_sources: add FK on source_id ─────────────────────────────

        CREATE TABLE reflection_sources_new (
            reflection_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
            source_id     TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
            PRIMARY KEY (reflection_id, source_id)
        );

        INSERT INTO reflection_sources_new SELECT reflection_id, source_id FROM reflection_sources;

        DROP TABLE reflection_sources;
        ALTER TABLE reflection_sources_new RENAME TO reflection_sources;

        -- ── 7. record_links: add composite UNIQUE ──────────────────────────────────

        CREATE TABLE record_links_new (
            record_id  TEXT NOT NULL REFERENCES records(record_id),
            task_id    TEXT,
            chunk_id   TEXT,
            created_at INTEGER NOT NULL,
            CHECK (task_id IS NOT NULL OR chunk_id IS NOT NULL)
        );

        INSERT INTO record_links_new SELECT record_id, task_id, chunk_id, created_at FROM record_links
            GROUP BY record_id, COALESCE(task_id, ''), COALESCE(chunk_id, '');

        DROP TABLE record_links;
        ALTER TABLE record_links_new RENAME TO record_links;

        CREATE INDEX IF NOT EXISTS record_links_record_id ON record_links(record_id);
        CREATE INDEX IF NOT EXISTS record_links_task_id   ON record_links(task_id)  WHERE task_id  IS NOT NULL;
        CREATE INDEX IF NOT EXISTS record_links_chunk_id  ON record_links(chunk_id) WHERE chunk_id IS NOT NULL;
        -- Partial unique indexes: SQLite treats NULLs as distinct in UNIQUE, so we
        -- need separate indexes for each nullable column combination.
        CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_task
            ON record_links(record_id, task_id) WHERE chunk_id IS NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_chunk
            ON record_links(record_id, chunk_id) WHERE task_id IS NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_both
            ON record_links(record_id, task_id, chunk_id) WHERE task_id IS NOT NULL AND chunk_id IS NOT NULL;

        PRAGMA user_version = 19;

        COMMIT;

        PRAGMA foreign_keys = ON;
    ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v18 schema covering all tables touched by this migration.
    fn setup_v18(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE files (
                 file_id         TEXT PRIMARY KEY,
                 path            TEXT UNIQUE NOT NULL,
                 content_hash    TEXT,
                 mtime           INTEGER,
                 size            INTEGER,
                 last_indexed_at INTEGER,
                 deleted_at      INTEGER,
                 indexing_state  TEXT NOT NULL DEFAULT 'idle',
                 chunker_version INTEGER,
                 pagerank_score  REAL
             );

             CREATE TABLE chunks (
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

             CREATE TABLE links (
                 link_id        TEXT PRIMARY KEY,
                 source_file_id TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
                 target_path    TEXT NOT NULL,
                 link_text      TEXT,
                 link_type      TEXT NOT NULL CHECK(link_type IN ('wiki', 'markdown', 'external')),
                 target_file_id TEXT REFERENCES files(file_id) ON DELETE SET NULL
             );
             CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_file_id);
             CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_path);
             CREATE INDEX IF NOT EXISTS idx_links_target_file ON links(target_file_id);

             CREATE TABLE summaries (
                 summary_id TEXT PRIMARY KEY,
                 file_id    TEXT REFERENCES files(file_id) ON DELETE SET NULL,
                 kind       TEXT NOT NULL CHECK(kind IN ('episode', 'reflection', 'summary')),
                 title      TEXT,
                 content    TEXT NOT NULL,
                 tags       TEXT NOT NULL DEFAULT '[]',
                 importance REAL NOT NULL DEFAULT 1.0,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 valid_from INTEGER,
                 valid_to   INTEGER
             );

             CREATE TABLE reflection_sources (
                 reflection_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
                 source_id     TEXT NOT NULL,
                 PRIMARY KEY (reflection_id, source_id)
             );

             CREATE TABLE tasks (
                 task_id        TEXT PRIMARY KEY,
                 title          TEXT NOT NULL,
                 description    TEXT,
                 status         TEXT NOT NULL DEFAULT 'open',
                 priority       INTEGER NOT NULL DEFAULT 4,
                 blocked_reason TEXT,
                 due_ts         INTEGER,
                 task_type      TEXT NOT NULL DEFAULT 'task',
                 assignee       TEXT,
                 defer_until    INTEGER,
                 parent_task_id TEXT,
                 child_seq      INTEGER,
                 created_at     INTEGER NOT NULL,
                 updated_at     INTEGER NOT NULL,
                 brain_id       TEXT NOT NULL DEFAULT ''
             );

             CREATE TABLE task_events (
                 event_id   TEXT PRIMARY KEY,
                 task_id    TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}'
             );
             CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

             CREATE TABLE task_note_links (
                 task_id  TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                 chunk_id TEXT NOT NULL,
                 PRIMARY KEY (task_id, chunk_id)
             );

             CREATE TABLE records (
                 record_id        TEXT PRIMARY KEY,
                 title            TEXT NOT NULL,
                 kind             TEXT NOT NULL,
                 status           TEXT NOT NULL DEFAULT 'active',
                 description      TEXT,
                 content_hash     TEXT NOT NULL,
                 content_size     INTEGER NOT NULL,
                 media_type       TEXT,
                 task_id          TEXT,
                 actor            TEXT NOT NULL,
                 created_at       INTEGER NOT NULL,
                 updated_at       INTEGER NOT NULL,
                 retention_class  TEXT,
                 pinned           INTEGER NOT NULL DEFAULT 0,
                 payload_available INTEGER NOT NULL DEFAULT 1,
                 content_encoding TEXT NOT NULL DEFAULT 'identity',
                 original_size    INTEGER,
                 brain_id         TEXT NOT NULL DEFAULT ''
             );

             CREATE TABLE record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL,
                 brain_id   TEXT NOT NULL DEFAULT ''
             );
             CREATE INDEX IF NOT EXISTS record_events_record_id ON record_events(record_id);

             CREATE TABLE record_tags (
                 record_id TEXT NOT NULL REFERENCES records(record_id),
                 tag       TEXT NOT NULL,
                 PRIMARY KEY (record_id, tag)
             );

             CREATE TABLE record_links (
                 record_id  TEXT NOT NULL REFERENCES records(record_id),
                 task_id    TEXT,
                 chunk_id   TEXT,
                 created_at INTEGER NOT NULL,
                 CHECK (task_id IS NOT NULL OR chunk_id IS NOT NULL)
             );
             CREATE INDEX IF NOT EXISTS record_links_record_id ON record_links(record_id);
             CREATE INDEX IF NOT EXISTS record_links_task_id ON record_links(task_id) WHERE task_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS record_links_chunk_id ON record_links(chunk_id) WHERE chunk_id IS NOT NULL;

             PRAGMA user_version = 18;",
        )
        .unwrap();
    }

    #[test]
    fn test_schema_version_is_19_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 19);
    }

    #[test]
    fn test_files_dead_columns_dropped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();

        // mtime and size should not exist — INSERT with them should fail
        let result = conn.execute(
            "INSERT INTO files (file_id, path, mtime) VALUES ('f1', '/a.md', 0)",
            [],
        );
        assert!(
            result.is_err(),
            "mtime column should not exist after migration"
        );

        let result2 = conn.execute(
            "INSERT INTO files (file_id, path, size) VALUES ('f2', '/b.md', 0)",
            [],
        );
        assert!(
            result2.is_err(),
            "size column should not exist after migration"
        );
    }

    #[test]
    fn test_files_indexing_state_check_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        // Valid states
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/a.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f2', '/b.md', 'indexing_started')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f3', '/c.md', 'indexed')",
            [],
        )
        .unwrap();

        // Invalid state — should fail
        let result = conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f4', '/d.md', 'bad_state')",
            [],
        );
        assert!(result.is_err(), "invalid indexing_state should be rejected");
    }

    #[test]
    fn test_chunks_not_null_defaults() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f1', '/a.md')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash) VALUES ('c1', 'f1', 0, 'h0')",
            [],
        )
        .unwrap();

        let (heading_path, byte_start, byte_end, token_estimate): (String, i64, i64, i64) = conn
            .query_row(
                "SELECT heading_path, byte_start, byte_end, token_estimate FROM chunks WHERE chunk_id = 'c1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert_eq!(heading_path, "");
        assert_eq!(byte_start, 0);
        assert_eq!(byte_end, 0);
        assert_eq!(token_estimate, 0);
    }

    #[test]
    fn test_chunks_null_values_backfilled() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);

        // Insert a row with NULLs before migration
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES ('f1', '/a.md')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, heading_path, byte_start, byte_end, token_estimate)
             VALUES ('c1', 'f1', 0, 'h0', NULL, NULL, NULL, NULL)",
            [],
        )
        .unwrap();

        migrate_v18_to_v19(&conn).unwrap();

        let (heading_path, byte_start, byte_end, token_estimate): (String, i64, i64, i64) = conn
            .query_row(
                "SELECT heading_path, byte_start, byte_end, token_estimate FROM chunks WHERE chunk_id = 'c1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert_eq!(heading_path, "");
        assert_eq!(byte_start, 0);
        assert_eq!(byte_end, 0);
        assert_eq!(token_estimate, 0);
    }

    #[test]
    fn test_task_events_fk_preserves_columns() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);

        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('e1', 't1', 'task_created', 1000, 'agent', '{}')",
            [],
        )
        .unwrap();

        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        // timestamp and actor preserved
        let (ts, actor): (i64, String) = conn
            .query_row(
                "SELECT timestamp, actor FROM task_events WHERE event_id = 'e1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 1000);
        assert_eq!(actor, "agent");

        // FK should reject orphan insert
        let result = conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('e2', 'no_such_task', 'task_created', 2000, 'agent', '{}')",
            [],
        );
        assert!(result.is_err(), "orphan task_id should be rejected by FK");
    }

    #[test]
    fn test_record_events_fk_brain_id_preserved() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'T', 'snapshot', 'abc', 0, 'agent', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
             VALUES ('ev1', 'r1', 'created', 2000, 'agent', '{}', 'b1')",
            [],
        )
        .unwrap();

        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        // brain_id preserved
        let brain_id: String = conn
            .query_row(
                "SELECT brain_id FROM record_events WHERE event_id = 'ev1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(brain_id, "b1");

        // timestamp and actor preserved
        let (ts, actor): (i64, String) = conn
            .query_row(
                "SELECT timestamp, actor FROM record_events WHERE event_id = 'ev1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 2000);
        assert_eq!(actor, "agent");

        // FK should reject orphan
        let result = conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload)
             VALUES ('ev3', 'no_such_record', 'created', 3000, 'agent', '{}')",
            [],
        );
        assert!(result.is_err(), "orphan record_id should be rejected by FK");
    }

    #[test]
    fn test_task_note_links_chunk_fk() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T', 0, 0)",
            [],
        )
        .unwrap();

        // Orphan chunk_id should be rejected
        let result = conn.execute(
            "INSERT INTO task_note_links (task_id, chunk_id) VALUES ('t1', 'no_such_chunk')",
            [],
        );
        assert!(result.is_err(), "orphan chunk_id should be rejected by FK");
    }

    #[test]
    fn test_reflection_sources_source_id_fk() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s1', 'reflection', 'c', 0, 0)",
            [],
        )
        .unwrap();

        // Orphan source_id should be rejected
        let result = conn.execute(
            "INSERT INTO reflection_sources (reflection_id, source_id) VALUES ('s1', 'no_such_summary')",
            [],
        );
        assert!(result.is_err(), "orphan source_id should be rejected by FK");
    }

    #[test]
    fn test_record_links_unique_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        setup_v18(&conn);
        migrate_v18_to_v19(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'T', 'snapshot', 'abc', 0, 'agent', 0, 0)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO record_links (record_id, task_id, chunk_id, created_at)
             VALUES ('r1', 't1', NULL, 0)",
            [],
        )
        .unwrap();

        // Duplicate (record_id, task_id, chunk_id) should fail
        let result = conn.execute(
            "INSERT INTO record_links (record_id, task_id, chunk_id, created_at)
             VALUES ('r1', 't1', NULL, 1)",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate record_links entry should be rejected"
        );
    }

    #[test]
    fn test_data_preserved_across_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v18(&conn);

        // Seed data across all tables
        conn.execute(
            "INSERT INTO files (file_id, path, mtime, size, indexing_state)
             VALUES ('f1', '/doc.md', 999, 512, 'indexed')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('c1', 'f1', 0, 'hh', 'hello world')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'Do it', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('ev1', 't1', 'task_created', 42, 'agent', '{}')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'R', 'snapshot', 'x', 0, 'agent', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
             VALUES ('re1', 'r1', 'created', 77, 'agent', '{}', 'b1')",
            [],
        )
        .unwrap();

        migrate_v18_to_v19(&conn).unwrap();

        // Files row preserved
        let path: String = conn
            .query_row("SELECT path FROM files WHERE file_id = 'f1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(path, "/doc.md");

        // Chunk row preserved
        let content: String = conn
            .query_row(
                "SELECT content FROM chunks WHERE chunk_id = 'c1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(content, "hello world");

        // task_event row preserved with timestamp and actor
        let (ts, actor): (i64, String) = conn
            .query_row(
                "SELECT timestamp, actor FROM task_events WHERE event_id = 'ev1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 42);
        assert_eq!(actor, "agent");

        // record_event row preserved (including brain_id)
        let (re_ts, re_actor): (i64, String) = conn
            .query_row(
                "SELECT timestamp, actor FROM record_events WHERE event_id = 're1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(re_ts, 77);
        assert_eq!(re_actor, "agent");
    }
}
