use rusqlite::Connection;

use crate::error::Result;

/// v32 → v33: Create `jobs` table for async LLM job queue.
///
/// The jobs table stores pending, running, completed, and failed jobs for
/// async operations like summarization. Key design:
///
/// - **Dedup index**: partial unique on `(kind, ref_id)` WHERE status IN
///   ('pending', 'running') prevents duplicate active jobs for the same target.
/// - **Poll index**: `(status, priority, scheduled_at)` for efficient claim queries.
/// - **Backoff**: `scheduled_at` is pushed forward on retryable failures.
/// - **Crash recovery**: reaper finds stuck `running` jobs by `started_at` age.
pub fn migrate_v32_to_v33(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE jobs (
            job_id        TEXT PRIMARY KEY,
            kind          TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead')),
            brain_id      TEXT NOT NULL DEFAULT '',
            ref_id        TEXT,
            ref_kind      TEXT,
            priority      INTEGER NOT NULL DEFAULT 100,
            payload       TEXT NOT NULL DEFAULT '{}',
            result        TEXT,
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

        PRAGMA user_version = 33;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v32(conn: &Connection) {
        // Minimal v32 schema — just enough to run migration.
        // The jobs table doesn't depend on other tables.
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             PRAGMA user_version = 32;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 33);
    }

    #[test]
    fn test_table_exists() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

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
    fn test_indices_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        let indices: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='jobs' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(indices.contains(&"idx_jobs_poll".to_string()));
        assert!(indices.contains(&"idx_jobs_brain_status".to_string()));
        assert!(indices.contains(&"idx_jobs_dedup".to_string()));
    }

    #[test]
    fn test_insert_succeeds() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        conn.execute(
            "INSERT INTO jobs (job_id, kind, brain_id, priority, payload, created_at, scheduled_at, updated_at)
             VALUES ('J1', 'summarize_scope', 'brain-1', 100, '{\"scope\":\"test\"}', 1000, 1000, 1000)",
            [],
        )
        .unwrap();

        let status: String = conn
            .query_row("SELECT status FROM jobs WHERE job_id = 'J1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "pending");
    }

    #[test]
    fn test_check_constraint_rejects_invalid_status() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        let result = conn.execute(
            "INSERT INTO jobs (job_id, kind, status, created_at, scheduled_at, updated_at)
             VALUES ('J2', 'test', 'invalid_status', 1000, 1000, 1000)",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_dedup_index_prevents_duplicate_active_jobs() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        // First insert succeeds
        conn.execute(
            "INSERT INTO jobs (job_id, kind, ref_id, status, created_at, scheduled_at, updated_at)
             VALUES ('J3', 'summarize', 'ref-1', 'pending', 1000, 1000, 1000)",
            [],
        )
        .unwrap();

        // Duplicate (kind, ref_id) with active status fails
        let result = conn.execute(
            "INSERT INTO jobs (job_id, kind, ref_id, status, created_at, scheduled_at, updated_at)
             VALUES ('J4', 'summarize', 'ref-1', 'pending', 1001, 1001, 1001)",
            [],
        );
        assert!(result.is_err());

        // Same (kind, ref_id) with completed status succeeds (dedup only covers active)
        conn.execute(
            "UPDATE jobs SET status = 'completed', completed_at = 1002 WHERE job_id = 'J3'",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO jobs (job_id, kind, ref_id, status, created_at, scheduled_at, updated_at)
             VALUES ('J5', 'summarize', 'ref-1', 'pending', 1003, 1003, 1003)",
            [],
        )
        .unwrap();
    }
}
