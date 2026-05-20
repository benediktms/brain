//! Writers for the `task_external_ids` table.

use crate::error::BrainCoreError;
use crate::sql::{SqlError, SqlResult};
use rusqlite::{Connection, OptionalExtension};

// Keys in the TaskTransferred JSON payload.
const PK_FROM_BRAIN_ID: &str = "from_brain_id";
const PK_FROM_DISPLAY_ID: &str = "from_display_id";

/// INSERT OR IGNORE into task_external_ids with default `blocking=0`.
pub fn add_external_id(
    conn: &Connection,
    task_id: &str,
    source: &str,
    external_id: &str,
    external_url: Option<&str>,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "INSERT OR IGNORE INTO task_external_ids (task_id, source, external_id, external_url, imported_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![task_id, source, external_id, external_url, ts],
    )?;
    Ok(())
}

/// DELETE from task_external_ids.
pub fn remove_external_id(
    conn: &Connection,
    task_id: &str,
    source: &str,
    external_id: &str,
) -> SqlResult<()> {
    conn.execute(
        "DELETE FROM task_external_ids WHERE task_id = ?1 AND source = ?2 AND external_id = ?3",
        rusqlite::params![task_id, source, external_id],
    )?;
    Ok(())
}

/// UPSERT into task_external_ids with `blocking=1, resolved_at=NULL`.
///
/// Idempotent: re-applying this event for an existing row clears any prior
/// `resolved_at` and promotes the row to a real blocker.
pub fn add_external_blocker(
    conn: &Connection,
    task_id: &str,
    source: &str,
    external_id: &str,
    external_url: Option<&str>,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO task_external_ids
             (task_id, source, external_id, external_url, imported_at, blocking, resolved_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL)
         ON CONFLICT(task_id, source, external_id) DO UPDATE SET
             external_url = COALESCE(excluded.external_url, task_external_ids.external_url),
             blocking     = 1,
             resolved_at  = NULL",
        rusqlite::params![task_id, source, external_id, external_url, ts],
    )?;
    Ok(())
}

/// Outcome of an `ExternalBlockerResolved` projection attempt.
pub enum ExternalBlockerResolveOutcome {
    /// No matching row in `task_external_ids` — likely a caller bug.
    NoMatchingRow,
    /// Row exists but `blocking=0` (metadata only) — wrong event sequence.
    MetadataOnly,
    /// Blocker was already resolved — idempotent re-stamp. `prior` carries the
    /// previously-recorded `resolved_at` so the caller can log both old and
    /// new timestamps.
    AlreadyResolved { prior: i64 },
    /// Fresh resolution — the happy path.
    FreshResolve,
}

/// Stamp `resolved_at` on the matching blocker row and return the outcome.
///
/// The caller is responsible for emitting any tracing log calls based on the
/// returned outcome (preserves exact log structure at the call site).
pub fn resolve_external_blocker(
    conn: &Connection,
    task_id: &str,
    source: &str,
    external_id: &str,
    resolved_at: i64,
) -> SqlResult<ExternalBlockerResolveOutcome> {
    let existing: Option<(i64, Option<i64>)> = conn
        .query_row(
            "SELECT blocking, resolved_at FROM task_external_ids
             WHERE task_id = ?1 AND source = ?2 AND external_id = ?3",
            rusqlite::params![task_id, source, external_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    match existing {
        None => Ok(ExternalBlockerResolveOutcome::NoMatchingRow),
        Some((0, _)) => Ok(ExternalBlockerResolveOutcome::MetadataOnly),
        Some((_, Some(prior))) => {
            conn.execute(
                "UPDATE task_external_ids
                 SET resolved_at = ?1
                 WHERE task_id = ?2 AND source = ?3 AND external_id = ?4
                   AND blocking = 1",
                rusqlite::params![resolved_at, task_id, source, external_id],
            )?;
            Ok(ExternalBlockerResolveOutcome::AlreadyResolved { prior })
        }
        Some((_, None)) => {
            conn.execute(
                "UPDATE task_external_ids
                 SET resolved_at = ?1
                 WHERE task_id = ?2 AND source = ?3 AND external_id = ?4
                   AND blocking = 1",
                rusqlite::params![resolved_at, task_id, source, external_id],
            )?;
            Ok(ExternalBlockerResolveOutcome::FreshResolve)
        }
    }
}

/// Backfill `task_external_ids` from `task_events` for all pre-existing transfers.
///
/// Walks every `TaskTransferred` event ever emitted and inserts an alias row with
/// `source='previous'` mapping the task to its old `brain_id/display_id` combination.
///
/// Idempotent: uses `INSERT OR IGNORE` so re-runs are safe. Skips rows where the
/// alias already exists (e.g. tasks transferred after the Phase 3 fix was deployed).
///
/// Returns the number of alias rows inserted.
pub fn backfill_task_aliases(conn: &Connection) -> SqlResult<usize> {
    let mut inserted = 0;

    let mut stmt = conn.prepare(
        "SELECT task_id, payload FROM task_events
         WHERE event_type = 'TaskTransferred'
         ORDER BY timestamp ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (task_id, payload): (String, String) = row?;

        // Parse JSON without a full struct — extract just the two fields we need.
        // JSON structure: {"from_brain_id":"...","from_display_id":"...","to_brain_id":"...","to_display_id":"..."}
        let from_brain_id = extract_json_string(&payload, PK_FROM_BRAIN_ID)?;
        let from_display_id = extract_json_string(&payload, PK_FROM_DISPLAY_ID)?;
        let alias = format!("{from_brain_id}/{from_display_id}");

        let rows_affected = conn.execute(
            "INSERT OR IGNORE INTO task_external_ids
             (task_id, source, external_id, external_url, imported_at)
             VALUES (?1, 'previous', ?2, NULL, ?3)",
            rusqlite::params![task_id, alias, crate::utils::now_ts()],
        )?;
        inserted += rows_affected;
    }

    Ok(inserted)
}

/// Extract a string value from a minimal JSON snippet using basic text parsing.
///
/// Used by `backfill_task_aliases` to pull two known fields from a `TaskTransferred`
/// payload without pulling in a full JSON parser. Handles the payload structure:
/// `{"from_brain_id":"...","from_display_id":"...",...}`
fn extract_json_string(json: &str, key: &str) -> SqlResult<String> {
    // JSON structure: {"from_brain_id":"brain-aaa","from_display_id":"abc",...}
    // Find `"key":"` pattern (note: no space after colon, matching serde_json output)
    let search = format!("\"{key}\":\"");
    let start = json.find(&search).ok_or_else(|| {
        SqlError::Domain(BrainCoreError::TaskEvent(format!(
            "key '{key}' not found in TaskTransferred payload: {json}"
        )))
    })?;
    let val_start = start + search.len();
    let val_end = json[val_start..]
        .find('"')
        .map(|i| val_start + i)
        .ok_or_else(|| {
            SqlError::Domain(BrainCoreError::TaskEvent(format!(
                "unterminated string for '{key}' in: {json}"
            )))
        })?;
    Ok(json[val_start..val_end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_task_aliases_inserts_alias_for_transfer_event() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::schema::init_schema(&conn).unwrap();

        // FK is ON; disable during fixture setup then re-enable.
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();

        // Insert a task (needed for FK)
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES ('TASK-1', 'brain-aaa', 'Test', 'open', 2, 1000, 1000, 'abc')",
            [],
        )
        .unwrap();

        // Insert a TaskTransferred event
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('evt-1', 'TASK-1', 'TaskTransferred', 2000, 'test',
                     '{\"from_brain_id\":\"brain-aaa\",\"from_display_id\":\"abc\",\"to_brain_id\":\"brain-bbb\",\"to_display_id\":\"xyz\"}')",
            [],
        )
        .unwrap();

        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        let count = backfill_task_aliases(&conn).unwrap();
        assert_eq!(count, 1);

        // Verify alias row
        let alias: String = conn
            .query_row(
                "SELECT external_id FROM task_external_ids WHERE source = 'previous'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(alias, "brain-aaa/abc");
    }

    #[test]
    fn backfill_task_aliases_is_idempotent() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::schema::init_schema(&conn).unwrap();

        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES ('TASK-1', 'brain-aaa', 'Test', 'open', 2, 1000, 1000, 'abc')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('evt-1', 'TASK-1', 'TaskTransferred', 2000, 'test',
                     '{\"from_brain_id\":\"brain-aaa\",\"from_display_id\":\"abc\",\"to_brain_id\":\"brain-bbb\",\"to_display_id\":\"xyz\"}')",
            [],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        // Run twice
        let first = backfill_task_aliases(&conn).unwrap();
        let second = backfill_task_aliases(&conn).unwrap();

        assert_eq!(first, 1);
        assert_eq!(second, 0); // Nothing new inserted
    }

    #[test]
    fn backfill_task_aliases_skips_non_transfer_events() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::schema::init_schema(&conn).unwrap();

        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES ('TASK-1', 'brain-aaa', 'Test', 'open', 2, 1000, 1000, 'abc')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
             VALUES ('evt-1', 'TASK-1', 'TaskCreated', 2000, 'test', '{}')",
            [],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        let count = backfill_task_aliases(&conn).unwrap();
        assert_eq!(count, 0);
    }
}
