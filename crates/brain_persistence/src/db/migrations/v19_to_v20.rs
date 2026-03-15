use rusqlite::Connection;

use crate::error::Result;

/// v19 → v20: Self-healing migration for corrupted task_events/record_events schema.
///
/// Two possible v19 states exist:
///
/// **Corrupted** (produced by original buggy v18→v19, commit 9ada3ad):
/// - `task_events` has `created_at TEXT NOT NULL` instead of `timestamp INTEGER NOT NULL`,
///   missing `actor` column
/// - `record_events` has `created_at TEXT NOT NULL` instead of `timestamp INTEGER NOT NULL`,
///   missing `actor` column, possibly missing `brain_id` column
///
/// **Correct** (produced by fixed v18→v19, commit c794004):
/// - `task_events` has `timestamp INTEGER NOT NULL`, `actor TEXT NOT NULL`
/// - `record_events` has `timestamp INTEGER NOT NULL`, `actor TEXT NOT NULL`,
///   `brain_id TEXT NOT NULL DEFAULT ''`
///
/// Detection: query `PRAGMA table_info(task_events)` and check for a `created_at` column.
/// If found → corrupted path; recreate both tables with correct schema, casting timestamps.
/// If not found → clean path; no DDL changes.
///
/// Both paths stamp `PRAGMA user_version = 20` inside the transaction.
pub fn migrate_v19_to_v20(conn: &Connection) -> Result<()> {
    // Detect corruption: check if task_events has a `created_at` column.
    let is_corrupted: bool = {
        let mut stmt = conn.prepare("PRAGMA table_info(task_events)")?;
        let col_names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .filter_map(|r| r.ok())
            .collect();
        col_names.iter().any(|c| c == "created_at")
    };

    if is_corrupted {
        // Also check if record_events has brain_id (it may not in the corrupted schema).
        let record_events_has_brain_id: bool = {
            let mut stmt = conn.prepare("PRAGMA table_info(record_events)")?;
            let col_names: Vec<String> = stmt
                .query_map([], |row| row.get::<_, String>(1))?
                .filter_map(|r| r.ok())
                .collect();
            col_names.iter().any(|c| c == "brain_id")
        };

        let insert_record_events = if record_events_has_brain_id {
            "INSERT INTO record_events_new (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
                SELECT event_id, record_id, event_type,
                       CAST(created_at AS INTEGER),
                       'unknown',
                       payload,
                       brain_id
                FROM record_events;"
        } else {
            "INSERT INTO record_events_new (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
                SELECT event_id, record_id, event_type,
                       CAST(created_at AS INTEGER),
                       'unknown',
                       payload,
                       ''
                FROM record_events;"
        };

        let sql = format!(
            "PRAGMA foreign_keys = OFF;

            BEGIN;

            -- ── task_events: recreate with correct schema ───────────────────────────────

            CREATE TABLE task_events_new (
                event_id   TEXT PRIMARY KEY,
                task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                timestamp  INTEGER NOT NULL,
                actor      TEXT NOT NULL,
                payload    TEXT NOT NULL DEFAULT '{{}}'
            );

            INSERT INTO task_events_new (event_id, task_id, event_type, timestamp, actor, payload)
                SELECT event_id, task_id, event_type,
                       CAST(created_at AS INTEGER),
                       'unknown',
                       payload
                FROM task_events;

            DROP TABLE task_events;
            ALTER TABLE task_events_new RENAME TO task_events;

            CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

            -- ── record_events: recreate with correct schema ─────────────────────────────

            CREATE TABLE record_events_new (
                event_id   TEXT PRIMARY KEY,
                record_id  TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                timestamp  INTEGER NOT NULL,
                actor      TEXT NOT NULL,
                payload    TEXT NOT NULL DEFAULT '{{}}',
                brain_id   TEXT NOT NULL DEFAULT ''
            );

            {insert_record_events}

            DROP TABLE record_events;
            ALTER TABLE record_events_new RENAME TO record_events;

            CREATE INDEX IF NOT EXISTS record_events_record_id ON record_events(record_id);

            PRAGMA user_version = 20;

            COMMIT;

            PRAGMA foreign_keys = ON;
        "
        );

        conn.execute_batch(&sql)?;
    } else {
        // Clean path: schema is already correct. Just stamp the version.
        conn.execute_batch(
            "
            PRAGMA foreign_keys = OFF;

            BEGIN;

            PRAGMA user_version = 20;

            COMMIT;

            PRAGMA foreign_keys = ON;
        ",
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v19 schema in the **corrupted** state:
    /// - task_events: created_at TEXT, no actor
    /// - record_events: created_at TEXT, no actor, no brain_id
    fn setup_corrupted_v19(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE files (
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

             CREATE TABLE chunks (
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
                 source_id     TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
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

             -- Corrupted schema: created_at TEXT instead of timestamp INTEGER, no actor
             CREATE TABLE task_events (
                 event_id   TEXT PRIMARY KEY,
                 task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                 event_type TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}'
             );
             CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

             CREATE TABLE task_note_links (
                 task_id  TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                 chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
                 PRIMARY KEY (task_id, chunk_id)
             );

             CREATE TABLE records (
                 record_id         TEXT PRIMARY KEY,
                 title             TEXT NOT NULL,
                 kind              TEXT NOT NULL,
                 status            TEXT NOT NULL DEFAULT 'active',
                 description       TEXT,
                 content_hash      TEXT NOT NULL,
                 content_size      INTEGER NOT NULL,
                 media_type        TEXT,
                 task_id           TEXT,
                 actor             TEXT NOT NULL,
                 created_at        INTEGER NOT NULL,
                 updated_at        INTEGER NOT NULL,
                 retention_class   TEXT,
                 pinned            INTEGER NOT NULL DEFAULT 0,
                 payload_available INTEGER NOT NULL DEFAULT 1,
                 content_encoding  TEXT NOT NULL DEFAULT 'identity',
                 original_size     INTEGER,
                 brain_id          TEXT NOT NULL DEFAULT ''
             );

             -- Corrupted schema: created_at TEXT, no actor, no brain_id
             CREATE TABLE record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
                 event_type TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}'
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
             CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_task
                 ON record_links(record_id, task_id) WHERE chunk_id IS NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_chunk
                 ON record_links(record_id, chunk_id) WHERE task_id IS NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_both
                 ON record_links(record_id, task_id, chunk_id) WHERE task_id IS NOT NULL AND chunk_id IS NOT NULL;

             PRAGMA user_version = 19;",
        )
        .unwrap();
    }

    /// Build a v19 schema in the **correct** state:
    /// - task_events: timestamp INTEGER, actor TEXT
    /// - record_events: timestamp INTEGER, actor TEXT, brain_id TEXT
    fn setup_correct_v19(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE files (
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

             CREATE TABLE chunks (
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
                 source_id     TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
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

             -- Correct schema: timestamp INTEGER, actor TEXT
             CREATE TABLE task_events (
                 event_id   TEXT PRIMARY KEY,
                 task_id    TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}'
             );
             CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

             CREATE TABLE task_note_links (
                 task_id  TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                 chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
                 PRIMARY KEY (task_id, chunk_id)
             );

             CREATE TABLE records (
                 record_id         TEXT PRIMARY KEY,
                 title             TEXT NOT NULL,
                 kind              TEXT NOT NULL,
                 status            TEXT NOT NULL DEFAULT 'active',
                 description       TEXT,
                 content_hash      TEXT NOT NULL,
                 content_size      INTEGER NOT NULL,
                 media_type        TEXT,
                 task_id           TEXT,
                 actor             TEXT NOT NULL,
                 created_at        INTEGER NOT NULL,
                 updated_at        INTEGER NOT NULL,
                 retention_class   TEXT,
                 pinned            INTEGER NOT NULL DEFAULT 0,
                 payload_available INTEGER NOT NULL DEFAULT 1,
                 content_encoding  TEXT NOT NULL DEFAULT 'identity',
                 original_size     INTEGER,
                 brain_id          TEXT NOT NULL DEFAULT ''
             );

             -- Correct schema: timestamp INTEGER, actor TEXT, brain_id TEXT
             CREATE TABLE record_events (
                 event_id   TEXT PRIMARY KEY,
                 record_id  TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
                 event_type TEXT NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 actor      TEXT NOT NULL,
                 payload    TEXT NOT NULL DEFAULT '{}',
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
             CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_task
                 ON record_links(record_id, task_id) WHERE chunk_id IS NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_chunk
                 ON record_links(record_id, chunk_id) WHERE task_id IS NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS record_links_uniq_both
                 ON record_links(record_id, task_id, chunk_id) WHERE task_id IS NOT NULL AND chunk_id IS NOT NULL;

             PRAGMA user_version = 19;",
        )
        .unwrap();
    }

    #[test]
    fn test_corrupted_schema_repaired() {
        let conn = Connection::open_in_memory().unwrap();
        setup_corrupted_v19(&conn);

        // Seed data in corrupted schema
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, created_at, payload)
             VALUES ('e1', 't1', 'task_created', '1234', '{}')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'R', 'snapshot', 'abc', 0, 'agent', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, created_at, payload)
             VALUES ('re1', 'r1', 'created', '5678', '{}')",
            [],
        )
        .unwrap();

        migrate_v19_to_v20(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        // Version stamped
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 20);

        // task_events: timestamp cast correctly, actor backfilled
        let (ts, actor): (i64, String) = conn
            .query_row(
                "SELECT timestamp, actor FROM task_events WHERE event_id = 'e1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 1234);
        assert_eq!(actor, "unknown");

        // record_events: timestamp cast correctly, actor backfilled, brain_id defaulted
        let (re_ts, re_actor, brain_id): (i64, String, String) = conn
            .query_row(
                "SELECT timestamp, actor, brain_id FROM record_events WHERE event_id = 're1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(re_ts, 5678);
        assert_eq!(re_actor, "unknown");
        assert_eq!(brain_id, "");

        // FK constraints active: orphan insert must fail
        let result = conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor)
             VALUES ('e2', 'no_such_task', 'task_created', 9999, 'x')",
            [],
        );
        assert!(result.is_err(), "orphan task_id should be rejected by FK");

        let result2 = conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor)
             VALUES ('re2', 'no_such_record', 'created', 9999, 'x')",
            [],
        );
        assert!(
            result2.is_err(),
            "orphan record_id should be rejected by FK"
        );
    }

    #[test]
    fn test_correct_schema_noop() {
        let conn = Connection::open_in_memory().unwrap();
        setup_correct_v19(&conn);

        // Seed data in correct schema
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('e1', 't1', 'task_created', 1234, 'agent', '{}')",
            [],
        )
        .unwrap();

        migrate_v19_to_v20(&conn).unwrap();

        // Version stamped
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 20);

        // Data unchanged
        let (ts, actor): (i64, String) = conn
            .query_row(
                "SELECT timestamp, actor FROM task_events WHERE event_id = 'e1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 1234);
        assert_eq!(actor, "agent");
    }

    #[test]
    fn test_corrupted_schema_data_preserved() {
        let conn = Connection::open_in_memory().unwrap();
        setup_corrupted_v19(&conn);

        // Seed multiple rows in both tables
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t1', 'T1', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, title, created_at, updated_at) VALUES ('t2', 'T2', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, created_at, payload)
             VALUES ('e1', 't1', 'task_created', '100', '{\"a\":1}')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, created_at, payload)
             VALUES ('e2', 't2', 'status_changed', '200', '{\"b\":2}')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, created_at, payload)
             VALUES ('e3', 't1', 'comment_added', '300', '{\"c\":3}')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r1', 'R1', 'snapshot', 'abc', 0, 'agent', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
             VALUES ('r2', 'R2', 'artifact', 'def', 0, 'agent', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, created_at, payload)
             VALUES ('re1', 'r1', 'created', '400', '{}')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, created_at, payload)
             VALUES ('re2', 'r2', 'updated', '500', '{}')",
            [],
        )
        .unwrap();

        migrate_v19_to_v20(&conn).unwrap();

        // Row counts preserved
        let task_event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM task_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(task_event_count, 3);

        let record_event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM record_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(record_event_count, 2);

        // Timestamps cast correctly
        let ts_e1: i64 = conn
            .query_row(
                "SELECT timestamp FROM task_events WHERE event_id = 'e1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ts_e1, 100);

        let ts_re2: i64 = conn
            .query_row(
                "SELECT timestamp FROM record_events WHERE event_id = 're2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ts_re2, 500);

        // Payload preserved
        let payload: String = conn
            .query_row(
                "SELECT payload FROM task_events WHERE event_id = 'e2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(payload, "{\"b\":2}");
    }
}
