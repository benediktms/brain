use rusqlite::Connection;

use crate::error::Result;

/// v30 → v31: Add `jobs` table for the async LLM job queue.
///
/// New table:
/// - `jobs` — tracks async summarization and consolidation jobs processed by
///   background workers. Jobs move through a strict lifecycle:
///   `pending` → `running` → `done` | `failed`
///
/// Schema:
/// ```sql
/// CREATE TABLE jobs (
///     id            TEXT    PRIMARY KEY,
///     job_type      TEXT    NOT NULL,
///     status        TEXT    NOT NULL,
///     priority      INTEGER NOT NULL DEFAULT 4,
///     chunk_id      TEXT,
///     brain_id      TEXT    NOT NULL,
///     payload       TEXT,
///     result        TEXT,
///     error         TEXT,
///     created_at    TEXT    NOT NULL,
///     updated_at    TEXT    NOT NULL,
///     started_at    TEXT,
///     completed_at  TEXT,
///     worker_id     TEXT
/// );
///
/// CREATE INDEX idx_jobs_status_priority ON jobs(status, priority DESC, created_at ASC);
/// CREATE INDEX idx_jobs_brain_id ON jobs(brain_id, created_at DESC);
/// CREATE INDEX idx_jobs_chunk_id ON jobs(chunk_id) WHERE chunk_id IS NOT NULL;
/// ```
///
/// ## Job types
///
/// - **`summarize`**: async L0/L1/L2 chunk summarization. `chunk_id` is set.
///   `payload` = `{"text": "...", "level": "L0"|"L1"|"L2"}`.
/// - **`consolidate`**: hierarchy consolidation. `chunk_id` is NULL. `payload` is NULL.
///
/// ## Status lifecycle
///
/// `pending` → `running` → `done` | `failed`
pub fn migrate_v30_to_v31(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS jobs (
             id           TEXT    PRIMARY KEY,
             job_type     TEXT    NOT NULL,
             status       TEXT    NOT NULL,
             priority     INTEGER NOT NULL DEFAULT 4,
             chunk_id     TEXT,
             brain_id     TEXT    NOT NULL,
             payload      TEXT,
             result       TEXT,
             error        TEXT,
             created_at   TEXT    NOT NULL,
             updated_at   TEXT    NOT NULL,
             started_at   TEXT,
             completed_at TEXT,
             worker_id    TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_jobs_status_priority
             ON jobs(status, priority DESC, created_at ASC);
         CREATE INDEX IF NOT EXISTS idx_jobs_brain_id
             ON jobs(brain_id, created_at DESC);
         CREATE INDEX IF NOT EXISTS idx_jobs_chunk_id
             ON jobs(chunk_id) WHERE chunk_id IS NOT NULL;
         PRAGMA user_version = 31;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v30(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;

             CREATE TABLE brains (
                 brain_id   TEXT PRIMARY KEY,
                 name       TEXT NOT NULL UNIQUE,
                 prefix     TEXT,
                 created_at INTEGER NOT NULL,
                 archived   INTEGER NOT NULL DEFAULT 0,
                 roots      TEXT,
                 aliases    TEXT,
                 notes      TEXT,
                 projected  INTEGER NOT NULL DEFAULT 0
             );

             PRAGMA user_version = 30;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 31);
    }

    #[test]
    fn test_jobs_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='jobs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "jobs table should exist");
    }

    #[test]
    fn test_status_priority_index_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_jobs_status_priority'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_jobs_status_priority should exist");
    }

    #[test]
    fn test_brain_id_index_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_jobs_brain_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_jobs_brain_id should exist");
    }

    #[test]
    fn test_chunk_id_partial_index_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_jobs_chunk_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "idx_jobs_chunk_id partial index should exist");
    }

    #[test]
    fn test_insert_summarize_job() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        conn.execute(
            "INSERT INTO jobs (id, job_type, status, priority, chunk_id, brain_id, payload, created_at, updated_at)
             VALUES ('job-001', 'summarize', 'pending', 2, 'chunk-abc', 'brain-xyz',
                     '{\"text\": \"hello world\", \"level\": \"L0\"}',
                     '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        let (job_type, status, chunk_id): (String, String, Option<String>) = conn
            .query_row(
                "SELECT job_type, status, chunk_id FROM jobs WHERE id = 'job-001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(job_type, "summarize");
        assert_eq!(status, "pending");
        assert_eq!(chunk_id, Some("chunk-abc".to_string()));
    }

    #[test]
    fn test_insert_consolidate_job() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        conn.execute(
            "INSERT INTO jobs (id, job_type, status, priority, brain_id, created_at, updated_at)
             VALUES ('job-002', 'consolidate', 'pending', 4, 'brain-xyz',
                     '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        let (chunk_id, payload): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT chunk_id, payload FROM jobs WHERE id = 'job-002'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(chunk_id, None, "consolidate jobs have NULL chunk_id");
        assert_eq!(payload, None, "consolidate jobs have NULL payload");
    }

    #[test]
    fn test_status_lifecycle() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();

        conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, created_at, updated_at)
             VALUES ('job-003', 'summarize', 'pending', 'brain-xyz',
                     '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        // pending → running
        conn.execute(
            "UPDATE jobs SET status = 'running', started_at = '2026-01-01T00:01:00Z',
             worker_id = 'worker-1', updated_at = '2026-01-01T00:01:00Z'
             WHERE id = 'job-003'",
            [],
        )
        .unwrap();

        let status: String = conn
            .query_row("SELECT status FROM jobs WHERE id = 'job-003'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "running");

        // running → done
        conn.execute(
            "UPDATE jobs SET status = 'done', result = '{\"summary\": \"short\"}',
             completed_at = '2026-01-01T00:02:00Z', updated_at = '2026-01-01T00:02:00Z'
             WHERE id = 'job-003'",
            [],
        )
        .unwrap();

        let (status, result): (String, Option<String>) = conn
            .query_row(
                "SELECT status, result FROM jobs WHERE id = 'job-003'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "done");
        assert!(result.is_some());
    }

    #[test]
    fn test_migration_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v30(&conn);
        migrate_v30_to_v31(&conn).unwrap();
        // IF NOT EXISTS clauses make this safe to re-run
        migrate_v30_to_v31(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 31);
    }
}
