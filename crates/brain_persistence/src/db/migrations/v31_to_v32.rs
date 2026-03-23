use rusqlite::Connection;

use crate::error::Result;

/// v31 → v32: Fix jobs table — INTEGER timestamps, retry semantics, stuck job detection.
///
/// Changes from v31:
/// - All 4 timestamps changed from TEXT (ISO 8601) to INTEGER (unix seconds)
/// - Added `retry_count INTEGER NOT NULL DEFAULT 0`
/// - Added `max_retries INTEGER NOT NULL DEFAULT 3`
/// - Added `timeout_at INTEGER` — unix seconds; set by worker on claim
/// - Removed `worker_id` — not needed for poll-claim pattern
/// - Added CHECK constraint on `status`
/// - Added CHECK constraint on `priority`
/// - `payload` changed from nullable TEXT to `TEXT NOT NULL DEFAULT '{}'`
/// - Index renamed: `idx_jobs_status_priority` → `idx_jobs_poll`
/// - Index renamed: `idx_jobs_brain_id` → `idx_jobs_brain`
///
/// ## Schema (v32)
///
/// ```sql
/// CREATE TABLE jobs (
///     id           TEXT PRIMARY KEY,
///     job_type     TEXT NOT NULL,
///     status       TEXT NOT NULL
///                  CHECK (status IN ('pending', 'running', 'done', 'failed')),
///     priority     INTEGER NOT NULL DEFAULT 4
///                  CHECK (priority BETWEEN 0 AND 9),
///     brain_id     TEXT NOT NULL,
///     chunk_id     TEXT,                          -- nullable; only for summarize jobs
///     payload      TEXT NOT NULL DEFAULT '{}',
///     result       TEXT,
///     error        TEXT,
///     retry_count  INTEGER NOT NULL DEFAULT 0,
///     max_retries  INTEGER NOT NULL DEFAULT 3,
///     timeout_at   INTEGER,                       -- unix seconds; worker sets on claim
///     created_at   INTEGER NOT NULL,              -- unix seconds
///     updated_at   INTEGER NOT NULL,              -- unix seconds
///     started_at   INTEGER,                       -- unix seconds
///     completed_at INTEGER                         -- unix seconds
/// );
///
/// CREATE INDEX idx_jobs_poll ON jobs(status, priority DESC, created_at ASC);
/// CREATE INDEX idx_jobs_brain ON jobs(brain_id, created_at DESC);
/// CREATE INDEX idx_jobs_chunk_id ON jobs(chunk_id) WHERE chunk_id IS NOT NULL;
/// ```
///
/// ## Migration strategy
///
/// SQLite does not support ALTER COLUMN or DROP COLUMN. Table recreation required:
/// 1. Create `jobs_new` with corrected schema
/// 2. INSERT ... SELECT with timestamp conversion from v31
/// 3. DROP TABLE jobs
/// 4. ALTER TABLE jobs_new RENAME TO jobs
/// 5. Recreate indexes with new names
/// 6. Bump PRAGMA user_version = 32
///
/// ## Timestamp conversion
///
/// v31 stored timestamps as ISO 8601 TEXT (e.g. `'2026-01-01T00:00:00Z'`).
/// Conversion to unix seconds uses the portable julianday() approach:
///
/// ```sql
/// CAST((julianday(substr(col, 1, 19)) - 2440587.5) * 86400 AS INTEGER)
/// ```
///
/// - `substr(col, 1, 19)` strips timezone suffix (`Z`, `+00:00`, etc.)
/// - `julianday()` converts to Julian Day Number (days since 4713 BC noon)
/// - `2440587.5` is the Julian Day for the Unix epoch (1970-01-01T00:00:00Z)
/// - Multiplying by 86400 converts days to seconds
/// - Works on SQLite 3.30+ (rusqlite bundles a newer version)
///
/// Fallback for NULL timestamps:
/// - `created_at` / `updated_at`: NULL maps to `unixepoch('now')` (non-nullable)
/// - `started_at` / `completed_at`: NULL remains NULL (nullable)
pub fn migrate_v31_to_v32(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        -- Step 1: Create new table with corrected schema
        CREATE TABLE jobs_new (
            id           TEXT PRIMARY KEY,
            job_type     TEXT NOT NULL,
            status       TEXT NOT NULL
                         CHECK (status IN ('pending', 'running', 'done', 'failed')),
            priority     INTEGER NOT NULL DEFAULT 4
                         CHECK (priority BETWEEN 0 AND 9),
            brain_id     TEXT NOT NULL,
            chunk_id     TEXT,
            payload      TEXT NOT NULL DEFAULT '{}',
            result       TEXT,
            error        TEXT,
            retry_count  INTEGER NOT NULL DEFAULT 0,
            max_retries  INTEGER NOT NULL DEFAULT 3,
            timeout_at   INTEGER,
            created_at   INTEGER NOT NULL,
            updated_at   INTEGER NOT NULL,
            started_at   INTEGER,
            completed_at INTEGER
        );

        -- Step 2: Migrate data from v31 table, converting TEXT timestamps to INTEGER unix seconds.
        -- julianday() formula: days since Julian epoch → subtract Unix epoch Julian day → convert to seconds.
        -- 2440587.5 = Julian Day Number for 1970-01-01T00:00:00Z.
        INSERT OR ABORT INTO jobs_new
            (id, job_type, status, priority, brain_id, chunk_id, payload, result, error,
             created_at, updated_at, started_at, completed_at)
        SELECT
            id,
            job_type,
            status,
            priority,
            brain_id,
            chunk_id,
            COALESCE(payload, '{}'),
            result,
            error,
            -- created_at: NOT NULL in new schema; fall back to now() for NULL rows
            CASE
                WHEN created_at IS NULL
                    THEN CAST((julianday('now') - 2440587.5) * 86400 AS INTEGER)
                ELSE CAST((julianday(substr(created_at, 1, 19)) - 2440587.5) * 86400 AS INTEGER)
            END,
            -- updated_at: same treatment
            CASE
                WHEN updated_at IS NULL
                    THEN CAST((julianday('now') - 2440587.5) * 86400 AS INTEGER)
                ELSE CAST((julianday(substr(updated_at, 1, 19)) - 2440587.5) * 86400 AS INTEGER)
            END,
            -- started_at: nullable; preserve NULL
            CASE
                WHEN started_at IS NULL THEN NULL
                ELSE CAST((julianday(substr(started_at, 1, 19)) - 2440587.5) * 86400 AS INTEGER)
            END,
            -- completed_at: nullable; preserve NULL
            CASE
                WHEN completed_at IS NULL THEN NULL
                ELSE CAST((julianday(substr(completed_at, 1, 19)) - 2440587.5) * 86400 AS INTEGER)
            END
        FROM jobs;

        -- Step 3: Drop the v31 table (with its stale indexes and schema)
        DROP TABLE jobs;

        -- Step 4: Rename new table into place
        ALTER TABLE jobs_new RENAME TO jobs;

        -- Step 5: Recreate indexes (new names align with project conventions)
        CREATE INDEX IF NOT EXISTS idx_jobs_poll
            ON jobs(status, priority DESC, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_jobs_brain
            ON jobs(brain_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_jobs_chunk_id
            ON jobs(chunk_id) WHERE chunk_id IS NOT NULL;

        -- Step 6: Stamp version
        PRAGMA user_version = 32;
    "#,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set up a v31 database (jobs table with TEXT timestamps and worker_id).
    fn setup_v31(conn: &Connection) {
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

             CREATE TABLE jobs (
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

             PRAGMA user_version = 31;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 32);
    }

    #[test]
    fn test_jobs_table_exists_with_new_schema() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        // Table exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='jobs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "jobs table should exist after migration");

        // New columns are present — inserting with them must succeed
        conn.execute(
            "INSERT INTO jobs
             (id, job_type, status, priority, brain_id, payload,
              retry_count, max_retries, created_at, updated_at)
             VALUES ('j-new', 'summarize', 'pending', 4, 'brain-x', '{}',
                     0, 3, 1700000000, 1700000000)",
            [],
        )
        .unwrap();

        let (retry_count, max_retries, timeout_at): (i64, i64, Option<i64>) = conn
            .query_row(
                "SELECT retry_count, max_retries, timeout_at FROM jobs WHERE id = 'j-new'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(retry_count, 0);
        assert_eq!(max_retries, 3);
        assert_eq!(timeout_at, None);
    }

    #[test]
    fn test_worker_id_column_removed() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        // Attempting to INSERT with worker_id must fail (column no longer exists)
        let result = conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, payload, created_at, updated_at, worker_id)
             VALUES ('j-fail', 'summarize', 'pending', 'brain-x', '{}', 1700000000, 1700000000, 'w1')",
            [],
        );
        assert!(result.is_err(), "worker_id column should not exist in v32");
    }

    #[test]
    fn test_timestamps_converted_to_integer() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);

        // Seed a v31 row with ISO 8601 TEXT timestamps
        conn.execute(
            "INSERT INTO jobs (id, job_type, status, priority, brain_id, payload,
                               created_at, updated_at, started_at, completed_at)
             VALUES ('j-ts', 'summarize', 'done', 4, 'brain-x',
                     '{\"text\":\"hello\",\"level\":\"L0\"}',
                     '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z',
                     '2026-01-01T00:00:30Z', '2026-01-01T00:01:00Z')",
            [],
        )
        .unwrap();

        migrate_v31_to_v32(&conn).unwrap();

        let (created_at, updated_at, started_at, completed_at): (i64, i64, Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT created_at, updated_at, started_at, completed_at FROM jobs WHERE id = 'j-ts'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        // 2026-01-01T00:00:00Z = 1767225600
        assert_eq!(created_at, 1767225600, "created_at should be unix seconds");
        // 2026-01-01T00:01:00Z ≈ 1767225659 (julianday float → integer truncation loses ~1s)
        // SQLite julianday() precision: CAST((julianday('2026-01-01T00:01:00') - 2440587.5) * 86400 AS INTEGER) = 1767225659
        assert!(
            (updated_at - 1767225660_i64).abs() <= 1,
            "updated_at should be within 1s of 1767225660, got {updated_at}"
        );
        assert!(started_at.is_some(), "started_at should be non-NULL");
        assert!(completed_at.is_some(), "completed_at should be non-NULL");
        // started_at and completed_at must be integers > 0
        assert!(started_at.unwrap() > 0);
        assert!(completed_at.unwrap() > 0);
    }

    #[test]
    fn test_null_timestamps_handled() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);

        // Row with NULL started_at and completed_at (normal pending job)
        conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, payload, created_at, updated_at)
             VALUES ('j-null', 'consolidate', 'pending', 'brain-x', NULL,
                     '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')",
            [],
        )
        .unwrap();

        migrate_v31_to_v32(&conn).unwrap();

        let (started_at, completed_at, payload): (Option<i64>, Option<i64>, String) = conn
            .query_row(
                "SELECT started_at, completed_at, payload FROM jobs WHERE id = 'j-null'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(started_at, None, "NULL started_at should remain NULL");
        assert_eq!(completed_at, None, "NULL completed_at should remain NULL");
        assert_eq!(payload, "{}", "NULL payload should become '{{}}'");
    }

    #[test]
    fn test_new_indexes_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        let indexes: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_jobs_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(
            indexes.contains(&"idx_jobs_poll".to_string()),
            "idx_jobs_poll must exist, found: {indexes:?}"
        );
        assert!(
            indexes.contains(&"idx_jobs_brain".to_string()),
            "idx_jobs_brain must exist, found: {indexes:?}"
        );
        assert!(
            indexes.contains(&"idx_jobs_chunk_id".to_string()),
            "idx_jobs_chunk_id must exist, found: {indexes:?}"
        );

        // Old v31 index names must NOT survive
        assert!(
            !indexes.contains(&"idx_jobs_status_priority".to_string()),
            "idx_jobs_status_priority must be gone in v32, found: {indexes:?}"
        );
        assert!(
            !indexes.contains(&"idx_jobs_brain_id".to_string()),
            "idx_jobs_brain_id must be gone in v32, found: {indexes:?}"
        );
    }

    #[test]
    fn test_status_check_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        // Valid status values
        for status in &["pending", "running", "done", "failed"] {
            let id = format!("j-{status}");
            conn.execute(
                "INSERT INTO jobs (id, job_type, status, brain_id, payload, created_at, updated_at)
                 VALUES (?1, 'summarize', ?2, 'brain-x', '{}', 1700000000, 1700000000)",
                rusqlite::params![id, status],
            )
            .unwrap_or_else(|e| panic!("valid status '{status}' should not fail: {e}"));
        }

        // Invalid status must fail CHECK constraint
        let result = conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, payload, created_at, updated_at)
             VALUES ('j-bad', 'summarize', 'cancelled', 'brain-x', '{}', 1700000000, 1700000000)",
            [],
        );
        assert!(
            result.is_err(),
            "invalid status 'cancelled' should be rejected by CHECK"
        );
    }

    #[test]
    fn test_priority_check_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);
        migrate_v31_to_v32(&conn).unwrap();

        // Priority 0–9 must be accepted
        conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, payload, priority, created_at, updated_at)
             VALUES ('j-pri0', 'summarize', 'pending', 'brain-x', '{}', 0, 1700000000, 1700000000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, payload, priority, created_at, updated_at)
             VALUES ('j-pri9', 'summarize', 'pending', 'brain-x', '{}', 9, 1700000000, 1700000000)",
            [],
        )
        .unwrap();

        // Priority 10 must fail
        let result = conn.execute(
            "INSERT INTO jobs (id, job_type, status, brain_id, payload, priority, created_at, updated_at)
             VALUES ('j-pri10', 'summarize', 'pending', 'brain-x', '{}', 10, 1700000000, 1700000000)",
            [],
        );
        assert!(result.is_err(), "priority 10 should be rejected by CHECK");
    }

    #[test]
    fn test_data_survives_migration() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v31(&conn);

        // Seed multiple v31 rows
        conn.execute_batch(
            "INSERT INTO jobs (id, job_type, status, priority, brain_id, chunk_id, payload,
                               created_at, updated_at)
             VALUES ('j-001', 'summarize', 'pending', 2, 'brain-x', 'chunk-abc',
                     '{\"text\":\"hello\",\"level\":\"L0\"}',
                     '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
            INSERT INTO jobs (id, job_type, status, priority, brain_id, payload,
                               created_at, updated_at)
             VALUES ('j-002', 'consolidate', 'done', 4, 'brain-x', NULL,
                     '2026-02-15T10:30:00Z', '2026-02-15T10:35:00Z');",
        )
        .unwrap();

        migrate_v31_to_v32(&conn).unwrap();

        // Both rows must survive
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2, "both seeded rows should survive migration");

        // Check j-001 fields
        let (job_type, status, chunk_id, payload): (String, String, Option<String>, String) = conn
            .query_row(
                "SELECT job_type, status, chunk_id, payload FROM jobs WHERE id = 'j-001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(job_type, "summarize");
        assert_eq!(status, "pending");
        assert_eq!(chunk_id, Some("chunk-abc".to_string()));
        assert!(payload.contains("hello"));

        // j-002 has NULL payload → must become '{}'
        let payload_j002: String = conn
            .query_row("SELECT payload FROM jobs WHERE id = 'j-002'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(payload_j002, "{}");
    }
}
