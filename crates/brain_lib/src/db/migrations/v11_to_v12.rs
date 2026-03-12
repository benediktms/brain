use rusqlite::Connection;

use crate::error::Result;

/// v11 → v12: Add records domain projection tables.
///
/// Creates four tables for the records domain:
/// - `records` — primary metadata for artifacts and snapshots
/// - `record_tags` — many-to-one tag set per record
/// - `record_links` — cross-reference links to tasks or note chunks
/// - `record_events` — full event audit log for queryable replay
///
/// The projection is fully rebuildable from `records/events.jsonl`.
pub fn migrate_v11_to_v12(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE records (
            record_id     TEXT PRIMARY KEY,
            title         TEXT NOT NULL,
            kind          TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active', 'archived')),
            description   TEXT,
            content_hash  TEXT NOT NULL,
            content_size  INTEGER NOT NULL,
            media_type    TEXT,
            -- Intentional denormalization: fast-path for listing records by originating task
            -- without joining record_links. record_links is the normalized form and supports
            -- many-to-many record<->task associations. Both must be kept in sync by the
            -- application layer.
            task_id       TEXT,
            actor         TEXT NOT NULL,
            created_at    INTEGER NOT NULL,
            updated_at    INTEGER NOT NULL
        );

        CREATE INDEX records_kind_status ON records(kind, status);
        CREATE INDEX records_task_id ON records(task_id) WHERE task_id IS NOT NULL;
        CREATE INDEX records_created_at ON records(created_at);

        CREATE TABLE record_tags (
            record_id  TEXT NOT NULL REFERENCES records(record_id),
            tag        TEXT NOT NULL,
            PRIMARY KEY (record_id, tag)
        );

        CREATE INDEX record_tags_tag ON record_tags(tag);

        CREATE TABLE record_links (
            record_id  TEXT NOT NULL REFERENCES records(record_id),
            task_id    TEXT,
            chunk_id   TEXT,
            created_at INTEGER NOT NULL,
            CHECK (task_id IS NOT NULL OR chunk_id IS NOT NULL)
        );

        CREATE INDEX record_links_record_id ON record_links(record_id);
        CREATE INDEX record_links_task_id ON record_links(task_id) WHERE task_id IS NOT NULL;
        CREATE INDEX record_links_chunk_id ON record_links(chunk_id) WHERE chunk_id IS NOT NULL;

        CREATE TABLE record_events (
            event_id    TEXT PRIMARY KEY,
            record_id   TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            timestamp   INTEGER NOT NULL,
            actor       TEXT NOT NULL,
            payload     TEXT NOT NULL
        );

        CREATE INDEX record_events_record_id ON record_events(record_id);

        PRAGMA user_version = 12;

        COMMIT;
    ",
    )?;
    Ok(())
}
