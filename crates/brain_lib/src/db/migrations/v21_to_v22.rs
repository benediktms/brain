use rusqlite::Connection;

use crate::error::Result;

/// v21 → v22: Add generic `jobs` table for background work scheduling.
///
/// Replaces the implicit "poll for `embedded_at IS NULL`" queue with an
/// explicit, priority-aware, observable job queue.  Supports deduplication
/// via a partial unique index on `(kind, ref_id)` for active jobs.
pub fn migrate_v21_to_v22(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;

        BEGIN;

        CREATE TABLE jobs (
            job_id        TEXT PRIMARY KEY,
            kind          TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead')),
            brain_id      TEXT NOT NULL DEFAULT '',
            ref_id        TEXT,
            ref_kind      TEXT,
            priority      INTEGER NOT NULL DEFAULT 100,
            payload       TEXT NOT NULL DEFAULT '{}',
            attempts      INTEGER NOT NULL DEFAULT 0,
            max_attempts  INTEGER NOT NULL DEFAULT 3,
            last_error    TEXT,
            created_at    INTEGER NOT NULL,
            scheduled_at  INTEGER NOT NULL,
            started_at    INTEGER,
            completed_at  INTEGER,
            updated_at    INTEGER NOT NULL
        );

        CREATE INDEX idx_jobs_poll ON jobs(status, priority, scheduled_at);
        CREATE INDEX idx_jobs_brain_status ON jobs(brain_id, status);
        CREATE UNIQUE INDEX idx_jobs_dedup ON jobs(kind, ref_id)
            WHERE status IN ('pending', 'running');

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

    /// Minimal v21 schema: just enough to validate the migration.
    fn setup_v21(conn: &Connection) {
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
                 token_estimate  INTEGER NOT NULL DEFAULT 0,
                 embedded_at     INTEGER
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

             PRAGMA user_version = 21;",
        )
        .unwrap();
    }

    #[test]
    fn test_jobs_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);

        migrate_v21_to_v22(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 22);

        // Verify jobs table exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='jobs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_jobs_indexes_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        migrate_v21_to_v22(&conn).unwrap();

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_jobs%' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(indexes.contains(&"idx_jobs_poll".to_string()));
        assert!(indexes.contains(&"idx_jobs_brain_status".to_string()));
        assert!(indexes.contains(&"idx_jobs_dedup".to_string()));
    }

    #[test]
    fn test_jobs_status_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        migrate_v21_to_v22(&conn).unwrap();

        // Valid status
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, created_at, scheduled_at, updated_at)
             VALUES ('j1', 'embed_task', 'pending', '', 1000, 1000, 1000)",
            [],
        )
        .unwrap();

        // Invalid status should fail
        let result = conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, created_at, scheduled_at, updated_at)
             VALUES ('j2', 'embed_task', 'invalid', '', 1000, 1000, 1000)",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_jobs_dedup_index() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        migrate_v21_to_v22(&conn).unwrap();

        // First pending job
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, created_at, scheduled_at, updated_at)
             VALUES ('j1', 'embed_task', 'pending', '', 't1', 'task', 1000, 1000, 1000)",
            [],
        )
        .unwrap();

        // Duplicate (same kind + ref_id, also pending) should fail
        let result = conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, created_at, scheduled_at, updated_at)
             VALUES ('j2', 'embed_task', 'pending', '', 't1', 'task', 1001, 1001, 1001)",
            [],
        );
        assert!(result.is_err());

        // But a completed job for the same (kind, ref_id) should succeed
        conn.execute(
            "UPDATE jobs SET status = 'completed', completed_at = 1002 WHERE job_id = 'j1'",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, brain_id, ref_id, ref_kind, created_at, scheduled_at, updated_at)
             VALUES ('j3', 'embed_task', 'pending', '', 't1', 'task', 1003, 1003, 1003)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn test_jobs_defaults() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v21(&conn);
        migrate_v21_to_v22(&conn).unwrap();

        conn.execute(
            "INSERT INTO jobs (job_id, kind, created_at, scheduled_at, updated_at)
             VALUES ('j1', 'embed_task', 1000, 1000, 1000)",
            [],
        )
        .unwrap();

        let (status, brain_id, priority, payload, attempts, max_attempts): (
            String,
            String,
            i32,
            String,
            i32,
            i32,
        ) = conn
            .query_row(
                "SELECT status, brain_id, priority, payload, attempts, max_attempts FROM jobs WHERE job_id = 'j1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )
            .unwrap();

        assert_eq!(status, "pending");
        assert_eq!(brain_id, "");
        assert_eq!(priority, 100);
        assert_eq!(payload, "{}");
        assert_eq!(attempts, 0);
        assert_eq!(max_attempts, 3);
    }
}
