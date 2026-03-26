use rusqlite::Connection;

use crate::error::Result;

/// v32 → v33: Add `jobs` table for async job queue.
///
/// The job queue is a central service (shared by all brains in a ~/.brain/
/// install). It manages async operations: LLM summarization, consolidation,
/// and future background work.
///
/// Schema:
/// - `kind` TEXT — job variant discriminant (e.g. "summarize_scope")
/// - `status` — ready → pending → in_progress → done/failed
/// - `payload` TEXT (JSON) — typed per-kind payload
/// - `retry_config` TEXT (JSON) — NoRetry/Fixed/Infinite
/// - No `brain_id` (jobs are global)
/// - No `ref_id` (dedup handled at application layer)
/// - No `is_recurring`/`period_secs` (scheduling policy lives in the daemon)
pub fn migrate_v32_to_v33(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;

        BEGIN;

        CREATE TABLE jobs (
            job_id              TEXT PRIMARY KEY,
            kind                TEXT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'ready'
                                CHECK (status IN ('ready', 'pending', 'in_progress', 'done', 'failed')),
            priority            INTEGER NOT NULL DEFAULT 100,
            payload             TEXT NOT NULL DEFAULT '{}',
            retry_config        TEXT NOT NULL DEFAULT '{\"type\":\"no_retry\"}',
            stuck_threshold_secs INTEGER NOT NULL DEFAULT 300,
            result              TEXT,
            attempts            INTEGER NOT NULL DEFAULT 0,
            last_error          TEXT,
            metadata            TEXT NOT NULL DEFAULT '{}',
            created_at          INTEGER NOT NULL,
            scheduled_at        INTEGER NOT NULL,
            started_at          INTEGER,
            processed_at        INTEGER,
            updated_at          INTEGER NOT NULL
        );

        CREATE INDEX idx_jobs_poll ON jobs(status, priority, scheduled_at);
        CREATE INDEX idx_jobs_kind ON jobs(kind, status);

        COMMIT;

        PRAGMA foreign_keys = ON;

        PRAGMA user_version = 33;
        ",
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v32(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             PRAGMA foreign_keys = ON;
             PRAGMA user_version = 32;",
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
    fn test_jobs_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "jobs table should exist (empty)");
    }

    #[test]
    fn test_status_default_ready() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        conn.execute(
            "INSERT INTO jobs (job_id, kind, created_at, scheduled_at, updated_at)
             VALUES ('J1', 'summarize_scope', 1000, 1000, 1000)",
            [],
        )
        .unwrap();

        let status: String = conn
            .query_row("SELECT status FROM jobs WHERE job_id = 'J1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "ready");
    }

    #[test]
    fn test_columns_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(jobs)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        // Present
        for expected in [
            "job_id",
            "kind",
            "status",
            "priority",
            "payload",
            "retry_config",
            "stuck_threshold_secs",
            "result",
            "attempts",
            "last_error",
            "metadata",
            "created_at",
            "scheduled_at",
            "started_at",
            "processed_at",
            "updated_at",
        ] {
            assert!(
                cols.contains(&expected.to_string()),
                "missing column: {expected}"
            );
        }

        // Absent
        assert!(!cols.contains(&"brain_id".to_string()));
        assert!(!cols.contains(&"ref_id".to_string()));
        assert!(!cols.contains(&"ref_kind".to_string()));
        assert!(!cols.contains(&"is_recurring".to_string()));
        assert!(!cols.contains(&"period_secs".to_string()));
    }

    #[test]
    fn test_indexes_created() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v32(&conn);
        migrate_v32_to_v33(&conn).unwrap();

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='jobs'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(indexes.iter().any(|i| i == "idx_jobs_poll"));
        assert!(indexes.iter().any(|i| i == "idx_jobs_kind"));
    }
}
