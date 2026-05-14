//! Writers for the `task_external_ids` table.

use rusqlite::{Connection, OptionalExtension};

use crate::sql::SqlResult;

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
