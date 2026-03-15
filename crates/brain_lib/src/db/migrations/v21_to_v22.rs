use rusqlite::Connection;

use crate::error::Result;

/// v21 → v22: Add FK constraint `brain_id → brains(brain_id)` on tasks, records, record_events.
///
/// SQLite cannot add FK constraints via `ALTER TABLE` — requires table recreation.
///
/// Pre-migration steps (in Rust, before the DDL batch):
/// 1. Insert orphan brain_ids (present in data tables but missing from `brains`) into `brains`.
/// 2. Backfill empty `brain_id` rows: if exactly one brain exists, stamp them; otherwise error.
///
/// FTS5 triggers on `tasks` are dropped before the table recreation — `ensure_fts5()` recreates
/// them idempotently after the migration completes.
pub fn migrate_v21_to_v22(conn: &Connection) -> Result<()> {
    // ── Phase 1: data healing (Rust logic) ──────────────────────────────────

    // Register a sentinel brain for unscoped rows (brain_id = '').
    // The FK constraint requires every brain_id to reference brains(brain_id).
    // Rows with brain_id = '' are legacy or unscoped — the sentinel satisfies
    // the FK without requiring a backfill.
    conn.execute(
        "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES ('', '(unscoped)', strftime('%s', 'now'))",
        [],
    )?;

    // Insert orphan brain_ids (non-empty, present in data but not in brains).
    conn.execute(
        "INSERT OR IGNORE INTO brains (brain_id, name, created_at)
         SELECT DISTINCT brain_id, brain_id, strftime('%s', 'now')
         FROM tasks
         WHERE brain_id != '' AND brain_id NOT IN (SELECT brain_id FROM brains)",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO brains (brain_id, name, created_at)
         SELECT DISTINCT brain_id, brain_id, strftime('%s', 'now')
         FROM records
         WHERE brain_id != '' AND brain_id NOT IN (SELECT brain_id FROM brains)",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO brains (brain_id, name, created_at)
         SELECT DISTINCT brain_id, brain_id, strftime('%s', 'now')
         FROM record_events
         WHERE brain_id != '' AND brain_id NOT IN (SELECT brain_id FROM brains)",
        [],
    )?;

    // ── Phase 2: table recreation with FK constraints ───────────────────────

    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;

        BEGIN;

        -- Drop FTS5 triggers and virtual table on tasks (ensure_fts5 recreates them).
        DROP TRIGGER IF EXISTS tasks_fts_insert;
        DROP TRIGGER IF EXISTS tasks_fts_delete;
        DROP TRIGGER IF EXISTS tasks_fts_update;
        DROP TABLE IF EXISTS fts_tasks;

        -- ── tasks: add FK brain_id → brains(brain_id) ──────────────────────────

        CREATE TABLE tasks_new (
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
            brain_id       TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id),
            embedded_at    INTEGER
        );

        INSERT INTO tasks_new (task_id, title, description, status, priority,
            blocked_reason, due_ts, task_type, assignee, defer_until,
            parent_task_id, child_seq, created_at, updated_at, brain_id, embedded_at)
        SELECT task_id, title, description, status, priority,
            blocked_reason, due_ts, task_type, assignee, defer_until,
            parent_task_id, child_seq, created_at, updated_at, brain_id, embedded_at
        FROM tasks;

        DROP TABLE tasks;
        ALTER TABLE tasks_new RENAME TO tasks;

        CREATE INDEX idx_tasks_status ON tasks(status);
        CREATE INDEX idx_tasks_defer_until ON tasks(defer_until);
        CREATE INDEX idx_tasks_status_priority ON tasks(status, priority);
        CREATE INDEX idx_tasks_brain_status ON tasks(brain_id, status);
        CREATE INDEX idx_tasks_brain_priority ON tasks(brain_id, priority);
        CREATE INDEX idx_tasks_parent ON tasks(parent_task_id);

        -- ── records: add FK brain_id → brains(brain_id) ────────────────────────

        CREATE TABLE records_new (
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
            brain_id          TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id)
        );

        INSERT INTO records_new (record_id, title, kind, status, description,
            content_hash, content_size, media_type, task_id, actor,
            created_at, updated_at, retention_class, pinned,
            payload_available, content_encoding, original_size, brain_id)
        SELECT record_id, title, kind, status, description,
            content_hash, content_size, media_type, task_id, actor,
            created_at, updated_at, retention_class, pinned,
            payload_available, content_encoding, original_size, brain_id
        FROM records;

        DROP TABLE records;
        ALTER TABLE records_new RENAME TO records;

        CREATE INDEX idx_records_brain ON records(brain_id);
        CREATE INDEX idx_records_brain_status ON records(brain_id, status);

        -- ── record_events: add FK brain_id → brains(brain_id) ──────────────────

        CREATE TABLE record_events_new (
            event_id   TEXT PRIMARY KEY,
            record_id  TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            timestamp  INTEGER NOT NULL,
            actor      TEXT NOT NULL,
            payload    TEXT NOT NULL DEFAULT '{}',
            brain_id   TEXT NOT NULL DEFAULT '' REFERENCES brains(brain_id)
        );

        INSERT INTO record_events_new (event_id, record_id, event_type, timestamp,
            actor, payload, brain_id)
        SELECT event_id, record_id, event_type, timestamp,
            actor, payload, brain_id
        FROM record_events;

        DROP TABLE record_events;
        ALTER TABLE record_events_new RENAME TO record_events;

        CREATE INDEX record_events_record_id ON record_events(record_id);

        PRAGMA user_version = 22;

        COMMIT;

        PRAGMA foreign_keys = ON;
    ",
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v21 schema covering tables touched by this migration.
    fn setup_v21(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL
             );

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
                 token_estimate  INTEGER NOT NULL DEFAULT 0,
                 embedded_at     INTEGER
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
                 brain_id       TEXT NOT NULL DEFAULT '',
                 embedded_at    INTEGER
             );
             CREATE INDEX IF NOT EXISTS idx_tasks_brain_status ON tasks(brain_id, status);
             CREATE INDEX IF NOT EXISTS idx_tasks_brain_priority ON tasks(brain_id, priority);
             CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id);

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
             CREATE INDEX IF NOT EXISTS idx_records_brain ON records(brain_id);
             CREATE INDEX IF NOT EXISTS idx_records_brain_status ON records(brain_id, status);

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

             PRAGMA user_version = 21;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        // Register a brain so FK constraints are satisfiable.
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 22);
    }

    #[test]
    fn test_fk_rejects_invalid_brain_id_on_tasks() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        // Valid brain_id — should succeed.
        conn.execute(
            "INSERT INTO tasks (task_id, title, brain_id, created_at, updated_at)
             VALUES ('t1', 'T', 'b1', 0, 0)",
            [],
        )
        .unwrap();

        // Invalid brain_id — FK should reject.
        let result = conn.execute(
            "INSERT INTO tasks (task_id, title, brain_id, created_at, updated_at)
             VALUES ('t2', 'T', 'nonexistent', 0, 0)",
            [],
        );
        assert!(result.is_err(), "invalid brain_id should be rejected by FK");
    }

    #[test]
    fn test_fk_rejects_invalid_brain_id_on_records() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        let result = conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, brain_id, created_at, updated_at)
             VALUES ('r1', 'R', 'snapshot', 'abc', 0, 'agent', 'nonexistent', 0, 0)",
            [],
        );
        assert!(
            result.is_err(),
            "invalid brain_id should be rejected by FK on records"
        );
    }

    #[test]
    fn test_fk_rejects_invalid_brain_id_on_record_events() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();

        // Need a valid record first.
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, brain_id, created_at, updated_at)
             VALUES ('r1', 'R', 'snapshot', 'abc', 0, 'agent', 'b1', 0, 0)",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, brain_id)
             VALUES ('e1', 'r1', 'created', 0, 'agent', 'nonexistent')",
            [],
        );
        assert!(
            result.is_err(),
            "invalid brain_id should be rejected by FK on record_events"
        );
    }

    #[test]
    fn test_sentinel_brain_registered() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        migrate_v21_to_v22(&conn).unwrap();

        // Sentinel brain (brain_id='') should exist.
        let name: String = conn
            .query_row("SELECT name FROM brains WHERE brain_id = ''", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(name, "(unscoped)");
    }

    #[test]
    fn test_empty_brain_id_preserved_via_sentinel() {
        // Empty brain_ids reference the sentinel brain — no backfill needed.
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        conn.execute(
            "INSERT INTO tasks (task_id, title, brain_id, created_at, updated_at)
             VALUES ('t1', 'T', '', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, brain_id, created_at, updated_at)
             VALUES ('r1', 'R', 'snapshot', 'abc', 0, 'agent', '', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, brain_id)
             VALUES ('e1', 'r1', 'created', 0, 'agent', '')",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();

        // brain_id stays '' — sentinel satisfies FK
        let task_bid: String = conn
            .query_row(
                "SELECT brain_id FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_bid, "");

        let rec_bid: String = conn
            .query_row(
                "SELECT brain_id FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rec_bid, "");
    }

    #[test]
    fn test_empty_brain_id_with_multiple_brains_succeeds() {
        // With the sentinel brain, empty brain_ids are always valid
        // regardless of how many real brains exist.
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'one', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b2', 'two', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, title, brain_id, created_at, updated_at)
             VALUES ('t1', 'T', '', 0, 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();

        // brain_id stays '' — sentinel handles it
        let bid: String = conn
            .query_row(
                "SELECT brain_id FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bid, "");
    }

    #[test]
    fn test_orphan_brain_ids_inserted_into_brains() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();
        // Task with a brain_id not in brains table.
        conn.execute(
            "INSERT INTO tasks (task_id, title, brain_id, created_at, updated_at)
             VALUES ('t1', 'T', 'orphan-brain', 0, 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();

        // orphan-brain should now exist in brains.
        let name: String = conn
            .query_row(
                "SELECT name FROM brains WHERE brain_id = 'orphan-brain'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "orphan-brain");
    }

    #[test]
    fn test_data_preserved_across_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, title, description, status, priority, brain_id,
                task_type, assignee, created_at, updated_at, embedded_at)
             VALUES ('t1', 'My Task', 'desc', 'in_progress', 2, 'b1', 'epic', 'Queen', 100, 200, 300)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO records (record_id, title, kind, status, description, content_hash,
                content_size, media_type, task_id, actor, created_at, updated_at,
                retention_class, pinned, payload_available, content_encoding, original_size, brain_id)
             VALUES ('r1', 'Rec', 'artifact', 'active', 'rdesc', 'hash1',
                1024, 'text/plain', 't1', 'drone', 400, 500,
                'ephemeral', 1, 1, 'zstd', 2048, 'b1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
             VALUES ('e1', 'r1', 'created', 600, 'drone', '{\"key\":\"val\"}', 'b1')",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();

        // tasks data preserved
        let (title, desc, status, priority, task_type, assignee, embedded_at): (
            String,
            String,
            String,
            i64,
            String,
            String,
            i64,
        ) = conn
            .query_row(
                "SELECT title, description, status, priority, task_type, assignee, embedded_at
                 FROM tasks WHERE task_id = 't1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(title, "My Task");
        assert_eq!(desc, "desc");
        assert_eq!(status, "in_progress");
        assert_eq!(priority, 2);
        assert_eq!(task_type, "epic");
        assert_eq!(assignee, "Queen");
        assert_eq!(embedded_at, 300);

        // records data preserved (including all columns)
        let (kind, content_encoding, original_size, pinned): (String, String, i64, i64) = conn
            .query_row(
                "SELECT kind, content_encoding, original_size, pinned FROM records WHERE record_id = 'r1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(kind, "artifact");
        assert_eq!(content_encoding, "zstd");
        assert_eq!(original_size, 2048);
        assert_eq!(pinned, 1);

        // record_events data preserved
        let (evt_type, payload, brain_id): (String, String, String) = conn
            .query_row(
                "SELECT event_type, payload, brain_id FROM record_events WHERE event_id = 'e1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(evt_type, "created");
        assert_eq!(payload, "{\"key\":\"val\"}");
        assert_eq!(brain_id, "b1");
    }

    #[test]
    fn test_empty_tables_migrate_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        // No brains, no data — should succeed (no empty brain_ids to resolve).
        migrate_v21_to_v22(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 22);
    }

    #[test]
    fn test_indexes_recreated() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        conn.execute(
            "INSERT INTO brains (brain_id, name, created_at) VALUES ('b1', 'test', 0)",
            [],
        )
        .unwrap();

        migrate_v21_to_v22(&conn).unwrap();

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(indexes.contains(&"idx_tasks_brain_status".to_string()));
        assert!(indexes.contains(&"idx_tasks_brain_priority".to_string()));
        assert!(indexes.contains(&"idx_tasks_parent".to_string()));
        assert!(indexes.contains(&"idx_records_brain".to_string()));
        assert!(indexes.contains(&"idx_records_brain_status".to_string()));
        assert!(indexes.contains(&"record_events_record_id".to_string()));
    }
}
