use rusqlite::{Connection, OptionalExtension, params};

use crate::db::sagas::events::{SagaEvent, SagaEventType, SagaTaskCascadedPayload};
use crate::db::tasks::events::{StatusChangedPayload, TaskEvent, TaskStatus};
use crate::db::tasks::projections::apply_event_inner;
use crate::error::{BrainCoreError, Result};
use crate::utils::now_ts;

/// A saga event row for insertion.
pub struct SagaEventInsert<'a> {
    pub event_id: &'a str,
    pub saga_id: &'a str,
    pub event_type: &'a str,
    pub timestamp: i64,
    pub actor: &'a str,
    pub payload: &'a str,
}

/// A fully-projected saga row from the `sagas` table.
#[derive(Debug, Clone)]
pub struct SagaRow {
    pub saga_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub closed_at: Option<i64>,
}

/// A lightweight task stub for saga membership rendering. Includes `brain_id`
/// because saga members are cross-brain by design — callers need to know which
/// brain each member task belongs to without an extra round-trip.
///
/// Orphans (saga_tasks rows whose task is missing) are dropped by the LEFT-JOIN-style
/// query; only resolvable members are returned.
#[derive(Debug, Clone)]
pub struct SagaMemberStub {
    pub task_id: String,
    pub brain_id: String,
    pub title: String,
    pub status: String,
    pub task_type: String,
}

/// Return saga member task stubs in `added_at` order, joined with `tasks`.
///
/// Single query — no N+1. Orphaned memberships (task deleted in another brain)
/// are silently dropped via the INNER JOIN: `saga_tasks` has no FK to `tasks`
/// by design, so this is the only place where orphans get filtered.
pub fn list_saga_member_stubs(conn: &Connection, saga_id: &str) -> Result<Vec<SagaMemberStub>> {
    let mut stmt = conn.prepare(
        "SELECT t.task_id, COALESCE(t.brain_id, ''), t.title, t.status, t.task_type \
         FROM saga_tasks st \
         INNER JOIN tasks t ON t.task_id = st.task_id \
         WHERE st.saga_id = ?1 \
         ORDER BY st.added_at, st.task_id",
    )?;
    let rows = stmt
        .query_map([saga_id], |row| {
            Ok(SagaMemberStub {
                task_id: row.get(0)?,
                brain_id: row.get(1)?,
                title: row.get(2)?,
                status: row.get(3)?,
                task_type: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SagaRow> {
    Ok(SagaRow {
        saga_id: row.get(0)?,
        title: row.get(1)?,
        description: row.get(2)?,
        status: row.get(3)?,
        created_at: row.get(4)?,
        updated_at: row.get(5)?,
        closed_at: row.get(6)?,
    })
}

/// Insert a new saga row.
pub fn insert_saga(
    conn: &Connection,
    saga_id: &str,
    title: &str,
    description: Option<&str>,
) -> Result<SagaRow> {
    let ts = now_ts();
    conn.execute(
        "INSERT INTO sagas (saga_id, title, description, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)",
        params![saga_id, title, description, ts],
    )?;
    get_saga(conn, saga_id)?.ok_or_else(|| {
        crate::error::BrainCoreError::Internal("saga disappeared after insert".into())
    })
}

/// Insert a saga event row into `saga_events`.
pub fn insert_saga_event(conn: &Connection, ev: &SagaEventInsert<'_>) -> Result<()> {
    conn.execute(
        "INSERT INTO saga_events (event_id, saga_id, event_type, timestamp, actor, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            ev.event_id,
            ev.saga_id,
            ev.event_type,
            ev.timestamp,
            ev.actor,
            ev.payload
        ],
    )?;
    Ok(())
}

/// Filters for listing sagas.
#[derive(Debug, Default)]
pub struct SagaListFilter {
    pub include_closed: bool,
    pub include_cancelled: bool,
    /// Only return sagas that have at least one member-task in this brain.
    pub containing_brain: Option<String>,
}

/// List sagas with optional filters. Single query, no N+1.
pub fn list_sagas(conn: &Connection, filter: &SagaListFilter) -> Result<Vec<SagaRow>> {
    // Build status exclusion clause.
    let mut where_clauses: Vec<&str> = Vec::new();
    if !filter.include_closed {
        where_clauses.push("s.status != 'closed'");
    }
    if !filter.include_cancelled {
        where_clauses.push("s.status != 'cancelled'");
    }
    if filter.containing_brain.is_some() {
        where_clauses.push(
            "EXISTS (SELECT 1 FROM saga_tasks st \
             JOIN tasks t ON t.task_id = st.task_id \
             WHERE st.saga_id = s.saga_id AND t.brain_id = :brain_id)",
        );
    }

    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    let sql = format!(
        "SELECT s.saga_id, s.title, s.description, s.status, \
                s.created_at, s.updated_at, s.closed_at \
         FROM sagas s \
         {where_sql} \
         ORDER BY s.created_at DESC"
    );

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<(&str, &dyn rusqlite::ToSql)> = match &filter.containing_brain {
        Some(b) => vec![(":brain_id", b)],
        None => vec![],
    };
    let rows = stmt
        .query_map(params.as_slice(), map_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(rows)
}

/// Update title and/or description. Bumps `updated_at`. At least one field must be Some.
pub fn update_saga(
    conn: &Connection,
    saga_id: &str,
    title: Option<&str>,
    description: Option<Option<&str>>,
) -> Result<SagaRow> {
    let ts = now_ts();
    match (title, description) {
        (Some(t), Some(d)) => {
            conn.execute(
                "UPDATE sagas SET title = ?1, description = ?2, updated_at = ?3 WHERE saga_id = ?4",
                rusqlite::params![t, d, ts, saga_id],
            )?;
        }
        (Some(t), None) => {
            conn.execute(
                "UPDATE sagas SET title = ?1, updated_at = ?2 WHERE saga_id = ?3",
                rusqlite::params![t, ts, saga_id],
            )?;
        }
        (None, Some(d)) => {
            conn.execute(
                "UPDATE sagas SET description = ?1, updated_at = ?2 WHERE saga_id = ?3",
                rusqlite::params![d, ts, saga_id],
            )?;
        }
        (None, None) => {
            return Err(crate::error::BrainCoreError::InvalidOperation(
                "update_saga: at least one field must be provided".into(),
            ));
        }
    }
    get_saga(conn, saga_id)?
        .ok_or_else(|| crate::error::BrainCoreError::SagaNotFound(saga_id.to_string()))
}

/// Close a saga: set status = 'closed', closed_at = now, bump updated_at.
/// Returns an error if the saga is not in 'planning' or 'open' status.
pub fn close_saga(conn: &Connection, saga_id: &str) -> Result<SagaRow> {
    let ts = now_ts();
    let rows_changed = conn.execute(
        "UPDATE sagas SET status = 'closed', closed_at = ?1, updated_at = ?1
         WHERE saga_id = ?2 AND status IN ('planning','open')",
        rusqlite::params![ts, saga_id],
    )?;
    if rows_changed == 0 {
        let existing = get_saga(conn, saga_id)?;
        return Err(match existing {
            None => crate::error::BrainCoreError::SagaNotFound(saga_id.to_string()),
            Some(row) => crate::error::BrainCoreError::Parse(format!(
                "saga cannot be closed from status '{}'; only 'planning' or 'open' sagas can be closed",
                row.status
            )),
        });
    }
    get_saga(conn, saga_id)?
        .ok_or_else(|| crate::error::BrainCoreError::SagaNotFound(saga_id.to_string()))
}

/// Fetch a saga row by ID.
pub fn get_saga(conn: &Connection, saga_id: &str) -> Result<Option<SagaRow>> {
    let row = conn
        .query_row(
            "SELECT saga_id, title, description, status, created_at, updated_at, closed_at
             FROM sagas WHERE saga_id = ?1",
            [saga_id],
            map_row,
        )
        .optional()?;
    Ok(row)
}

/// Insert a batch of (saga_id, task_id) rows into `saga_tasks`.
///
/// Caller is responsible for transaction boundaries and pre-validation.
/// Returns the number of rows inserted.
pub fn insert_saga_tasks(conn: &Connection, saga_id: &str, task_ids: &[String]) -> Result<usize> {
    let ts = now_ts();
    let mut stmt = conn.prepare_cached(
        "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, ?3)",
    )?;
    let mut count = 0usize;
    for task_id in task_ids {
        stmt.execute(params![saga_id, task_id, ts])?;
        count += 1;
    }
    Ok(count)
}

/// Check whether a task is already a member of a saga.
pub fn saga_has_task(conn: &Connection, saga_id: &str, task_id: &str) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1 AND task_id = ?2",
        params![saga_id, task_id],
        |row| row.get(0),
    )?;
    Ok(n > 0)
}

/// Summary of a brain that has member tasks in a saga.
#[derive(Debug, Clone)]
pub struct BrainSummary {
    pub brain_id: String,
    pub name: String,
    pub prefix: Option<String>,
}

/// Return the distinct set of brains that have member tasks in a saga.
///
/// Derived at read time via saga_tasks → tasks → brains JOIN.
/// Returns an empty vec when the saga has no members.
pub fn brains_for_saga(conn: &Connection, saga_id: &str) -> Result<Vec<BrainSummary>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT b.brain_id, b.name, b.prefix
         FROM saga_tasks st
         JOIN tasks t ON t.task_id = st.task_id
         JOIN brains b ON b.brain_id = t.brain_id
         WHERE st.saga_id = ?1
         ORDER BY b.brain_id",
    )?;
    let rows = stmt
        .query_map([saga_id], |row| {
            Ok(BrainSummary {
                brain_id: row.get(0)?,
                name: row.get(1)?,
                prefix: row.get(2)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// List all task_ids belonging to a saga, ordered by `added_at` ascending.
///
/// Warning: the returned IDs are read directly from `saga_tasks` and may
/// include orphans — task IDs whose underlying `tasks` row has been deleted.
/// Use `list_saga_member_stubs` if you need only live (joinable) members.
pub fn list_saga_task_ids(conn: &Connection, saga_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY added_at")?;
    let ids = stmt
        .query_map([saga_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(ids)
}

/// Remove tasks from a saga. Returns the number of rows actually deleted.
///
/// Missing memberships are silently skipped (idempotent). Runs inside the
/// caller's transaction — does NOT commit.
pub fn remove_saga_tasks(conn: &Connection, saga_id: &str, task_ids: &[String]) -> Result<usize> {
    if task_ids.is_empty() {
        return Ok(0);
    }
    let placeholders = task_ids
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 2))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM saga_tasks WHERE saga_id = ?1 AND task_id IN ({placeholders})");
    let mut stmt = conn.prepare(&sql)?;
    let mut params: Vec<&dyn rusqlite::ToSql> = vec![&saga_id as &dyn rusqlite::ToSql];
    for tid in task_ids {
        params.push(tid);
    }
    let changed = stmt.execute(params.as_slice())?;
    Ok(changed)
}

/// Reopen a saga: set status = 'open', closed_at = NULL, updated_at = now.
/// Returns the updated row. Caller is responsible for validating the prior
/// status before calling this.
pub fn reopen_saga(conn: &Connection, saga_id: &str) -> Result<SagaRow> {
    let ts = now_ts();
    conn.execute(
        "UPDATE sagas SET status = 'open', closed_at = NULL, updated_at = ?1
         WHERE saga_id = ?2",
        params![ts, saga_id],
    )?;
    get_saga(conn, saga_id)?.ok_or_else(|| {
        crate::error::BrainCoreError::Internal("saga disappeared after reopen".into())
    })
}

/// Stats for a saga's member tasks.
///
/// `total` counts only live (non-orphan) members; `orphan` is the count of
/// memberships whose underlying task has been deleted.
#[derive(Debug, Clone)]
pub struct SagaStatsRow {
    /// Count of live (JOIN-resolved) member tasks.
    pub total: i64,
    pub open: i64,
    pub in_progress: i64,
    pub blocked: i64,
    pub done: i64,
    pub cancelled: i64,
    /// Count of saga_tasks rows whose underlying `tasks` row has been deleted.
    pub orphan: i64,
    /// done / (total - cancelled); None if denominator is 0
    pub completion_pct: Option<f64>,
}

/// Compute aggregate stats for a saga in a single SQL query.
///
/// `total` counts only live (non-orphan) members; `orphan` is the count of
/// memberships whose underlying task has been deleted.
pub fn saga_stats(conn: &Connection, saga_id: &str) -> Result<SagaStatsRow> {
    conn.query_row(
        "SELECT
             COUNT(t.task_id) AS total,
             COUNT(CASE WHEN t.status = 'open'        THEN 1 END) AS open,
             COUNT(CASE WHEN t.status = 'in_progress' THEN 1 END) AS in_progress,
             COUNT(CASE WHEN t.status = 'blocked'     THEN 1 END) AS blocked,
             COUNT(CASE WHEN t.status = 'done'        THEN 1 END) AS done,
             COUNT(CASE WHEN t.status = 'cancelled'   THEN 1 END) AS cancelled,
             (SELECT COUNT(*) FROM saga_tasks st
                WHERE st.saga_id = ?1
                  AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.task_id = st.task_id)
             ) AS orphan
         FROM saga_tasks st
         LEFT JOIN tasks t ON t.task_id = st.task_id
         WHERE st.saga_id = ?1",
        [saga_id],
        |row| {
            let total: i64 = row.get(0)?;
            let open: i64 = row.get(1)?;
            let in_progress: i64 = row.get(2)?;
            let blocked: i64 = row.get(3)?;
            let done: i64 = row.get(4)?;
            let cancelled: i64 = row.get(5)?;
            let orphan: i64 = row.get(6)?;
            let denominator = total - cancelled;
            let completion_pct = if denominator > 0 {
                Some(done as f64 / denominator as f64 * 100.0)
            } else {
                None
            };
            Ok(SagaStatsRow {
                total,
                open,
                in_progress,
                blocked,
                done,
                cancelled,
                orphan,
                completion_pct,
            })
        },
    )
    .map_err(Into::into)
}

/// A label with its occurrence count across saga member tasks.
#[derive(Debug, Clone)]
pub struct LabelCount {
    pub label: String,
    pub count: i64,
}

/// Compute label histogram across all member tasks of a saga.
pub fn saga_label_histogram(conn: &Connection, saga_id: &str) -> Result<Vec<LabelCount>> {
    let mut stmt = conn.prepare(
        "SELECT tl.label, COUNT(*) AS cnt
         FROM saga_tasks st
         JOIN task_labels tl ON tl.task_id = st.task_id
         WHERE st.saga_id = ?1
         GROUP BY tl.label
         ORDER BY cnt DESC, tl.label ASC",
    )?;
    let rows = stmt
        .query_map([saga_id], |row| {
            Ok(LabelCount {
                label: row.get(0)?,
                count: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Project saga status to `cancelled` and set `closed_at`.
///
/// Returns `SagaNotFound` if the saga does not exist, or `Parse` if the saga
/// is already in a terminal status (`closed` or `cancelled`). Sagas in
/// `planning` or `open` status transition to `cancelled` successfully.
pub fn cancel_saga(conn: &Connection, saga_id: &str) -> Result<()> {
    let ts = now_ts();
    let rows_changed = conn.execute(
        "UPDATE sagas SET status = 'cancelled', closed_at = ?2, updated_at = ?2
         WHERE saga_id = ?1 AND status IN ('planning','open')",
        params![saga_id, ts],
    )?;
    if rows_changed == 0 {
        let existing = get_saga(conn, saga_id)?;
        return Err(match existing {
            None => crate::error::BrainCoreError::SagaNotFound(saga_id.to_string()),
            Some(row) => crate::error::BrainCoreError::Parse(format!(
                "saga already in terminal status '{}'",
                row.status
            )),
        });
    }
    Ok(())
}

/// Per-task outcome of a `close --cascade` or `cancel --cascade`.
#[derive(Debug, Clone)]
pub struct CascadeResult {
    pub task_id: String,
    pub outcome: CascadeOutcome,
}

#[derive(Debug, Clone)]
pub enum CascadeOutcome {
    /// Task transitioned to Done (close-cascade success).
    Closed,
    /// Task transitioned to Cancelled (cancel-cascade success).
    Cancelled,
    /// Task was already terminal — left untouched.
    Skipped { reason: String },
    /// Task event append failed; saga's own state still committed.
    Failed { error: String },
}

impl CascadeResult {
    pub fn is_failure(&self) -> bool {
        matches!(self.outcome, CascadeOutcome::Failed { .. })
    }
}

/// Cascade member tasks of a saga to a target terminal status (Done for
/// close-cascade, Cancelled for cancel-cascade). Returns one `CascadeResult`
/// per member task; the caller commits or rolls back the enclosing
/// transaction.
///
/// Invariants:
/// - Already-terminal tasks (`done`, `cancelled`) are recorded as `Skipped`,
///   not retransitioned.
/// - Tasks with NULL/empty `brain_id` are recorded as `Failed` with an
///   "orphan" reason rather than silently calling `apply_event("")`.
/// - Each cascade attempt also emits a `SagaTaskCascaded` saga event so
///   replay can reconstruct cascade results from the saga log alone.
/// - The cascade does NOT roll back the outer transaction on per-task
///   failures; failures are reported to the caller via `CascadeOutcome`.
pub fn cascade_member_tasks(
    conn: &Connection,
    saga_id: &str,
    actor: &str,
    target_status: TaskStatus,
) -> Result<Vec<CascadeResult>> {
    let target_str: &'static str = match target_status {
        TaskStatus::Done => "done",
        TaskStatus::Cancelled => "cancelled",
        // Other statuses don't make sense as a cascade target; the call
        // sites only pass Done or Cancelled.
        _ => {
            return Err(BrainCoreError::Parse(format!(
                "cascade_member_tasks: unsupported target status {target_status:?}"
            )));
        }
    };
    let task_ids = list_saga_task_ids(conn, saga_id)?;
    let mut results = Vec::with_capacity(task_ids.len());
    let mut event_stmt = conn.prepare_cached(
        "INSERT INTO saga_events (event_id, saga_id, event_type, timestamp, actor, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for task_id in task_ids {
        let outcome = cascade_one_task_inner(conn, actor, &task_id, target_status.clone());

        // Always emit a SagaTaskCascaded event regardless of outcome — the saga
        // log is the single source of truth for what the cascade did.
        let outcome_str: &'static str = match &outcome {
            CascadeOutcome::Closed => "closed",
            CascadeOutcome::Cancelled => "cancelled",
            CascadeOutcome::Skipped { .. } => "skipped",
            CascadeOutcome::Failed { .. } => "failed",
        };
        let payload = SagaTaskCascadedPayload {
            task_id: task_id.clone(),
            new_status: target_str.to_string(),
            outcome: outcome_str.to_string(),
        };
        let event = SagaEvent::new(saga_id, actor, SagaEventType::SagaTaskCascaded, &payload);
        let payload_json = serde_json::to_string(&event.payload)?;
        event_stmt.execute(params![
            event.event_id,
            event.saga_id,
            event.event_type.as_column_str(),
            event.timestamp,
            event.actor,
            payload_json,
        ])?;

        results.push(CascadeResult { task_id, outcome });
    }
    Ok(results)
}

fn cascade_one_task_inner(
    conn: &Connection,
    actor: &str,
    task_id: &str,
    target_status: TaskStatus,
) -> CascadeOutcome {
    // Fetch current task status. Orphan saga_tasks rows (task deleted) are
    // recorded as Failed rather than panicking.
    let row: rusqlite::Result<(String, Option<String>)> = conn.query_row(
        "SELECT status, brain_id FROM tasks WHERE task_id = ?1",
        [task_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
    );
    let (current_status, brain_id_opt) = match row {
        Ok(v) => v,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            return CascadeOutcome::Failed {
                error: "task not found (orphaned saga membership)".into(),
            };
        }
        Err(e) => {
            return CascadeOutcome::Failed {
                error: format!("failed to read task: {e}"),
            };
        }
    };

    if current_status == "done" || current_status == "cancelled" {
        return CascadeOutcome::Skipped {
            reason: current_status,
        };
    }

    let brain_id = match brain_id_opt {
        Some(b) if !b.is_empty() => b,
        _ => {
            return CascadeOutcome::Failed {
                error: "task has no brain_id (orphan)".into(),
            };
        }
    };

    let success_outcome = match &target_status {
        TaskStatus::Done => CascadeOutcome::Closed,
        TaskStatus::Cancelled => CascadeOutcome::Cancelled,
        _ => {
            return CascadeOutcome::Failed {
                error: "unexpected target status".into(),
            };
        }
    };
    let ev = TaskEvent::from_payload(
        task_id,
        actor,
        StatusChangedPayload {
            new_status: target_status,
        },
    );
    // Use `apply_event_inner` (no internal tx) because we already operate
    // inside the saga's outer transaction; calling `apply_event` here would
    // attempt a nested BEGIN and SQLite would reject it.
    match apply_event_inner(conn, &ev, &brain_id) {
        Ok(_) => success_outcome,
        Err(e) => CascadeOutcome::Failed {
            error: format!("{e}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::error::BrainCoreError;

    /// Open an in-memory DB and register one brain so tasks have a valid FK target.
    fn setup_db() -> Db {
        let db = Db::open_in_memory().expect("open in-memory db");
        db.ensure_brain_registered("brain-x", "xeno")
            .expect("register brain");
        db
    }

    /// Insert a task row directly via SQL. Tests don't depend on task projection.
    fn insert_task(conn: &Connection, task_id: &str, status: &str) {
        conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, status, priority, created_at, updated_at, display_id)
             VALUES (?1, 'brain-x', 'T', ?2, 2, strftime('%s','now'), strftime('%s','now'), ?1)",
            params![task_id, status],
        )
        .unwrap();
    }

    fn insert_saga_row(conn: &Connection, saga_id: &str, status: &str) {
        conn.execute(
            "INSERT INTO sagas (saga_id, title, status, created_at, updated_at)
             VALUES (?1, 'My saga', ?2, 1000, 1000)",
            params![saga_id, status],
        )
        .unwrap();
    }

    #[test]
    fn empty_saga_stats_returns_zeros_and_no_completion_pct() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_empty", "open");
            let stats = saga_stats(conn, "s_empty")?;
            assert_eq!(stats.total, 0);
            assert_eq!(stats.orphan, 0);
            assert_eq!(stats.done, 0);
            assert_eq!(stats.cancelled, 0);
            assert!(stats.completion_pct.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn all_cancelled_stats_completion_pct_is_none() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_can", "open");
            insert_task(conn, "t_a", "cancelled");
            insert_task(conn, "t_b", "cancelled");
            insert_saga_tasks(conn, "s_can", &["t_a".into(), "t_b".into()])?;
            let stats = saga_stats(conn, "s_can")?;
            assert_eq!(stats.total, 2);
            assert_eq!(stats.cancelled, 2);
            assert_eq!(stats.orphan, 0);
            // denominator = total - cancelled = 0 → None
            assert!(
                stats.completion_pct.is_none(),
                "expected None when every member is cancelled"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn orphan_member_excluded_from_stats_total_counted_in_orphan() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_orph", "open");
            insert_task(conn, "t_live", "open");
            insert_task(conn, "t_doomed", "open");
            insert_saga_tasks(conn, "s_orph", &["t_live".into(), "t_doomed".into()])?;
            // Delete the underlying task → its saga_tasks row becomes an orphan.
            conn.execute("DELETE FROM tasks WHERE task_id = 't_doomed'", [])?;
            let stats = saga_stats(conn, "s_orph")?;
            assert_eq!(stats.total, 1, "orphan must not be counted in total");
            assert_eq!(stats.orphan, 1, "orphan column counts dangling membership");
            assert_eq!(stats.open, 1);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn containing_brain_filter_only_matches_brain_id_not_name() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_brain", "open");
            insert_task(conn, "t_a", "open");
            insert_saga_tasks(conn, "s_brain", &["t_a".into()])?;

            // brain_id matches → returned
            let by_id = list_sagas(
                conn,
                &SagaListFilter {
                    include_closed: false,
                    include_cancelled: false,
                    containing_brain: Some("brain-x".into()),
                },
            )?;
            assert_eq!(by_id.len(), 1);

            // brain name does NOT match — filter is brain_id only
            let by_name = list_sagas(
                conn,
                &SagaListFilter {
                    include_closed: false,
                    include_cancelled: false,
                    containing_brain: Some("xeno".into()),
                },
            )?;
            assert!(
                by_name.is_empty(),
                "containing_brain filter must compare brain_id, not name"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn close_saga_from_planning_succeeds() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_plan", "planning");
            let row = close_saga(conn, "s_plan")?;
            assert_eq!(row.status, "closed");
            assert!(row.closed_at.is_some());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn cancel_saga_missing_returns_not_found() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let err = cancel_saga(conn, "no_such_saga").unwrap_err();
            match err {
                BrainCoreError::SagaNotFound(id) => assert_eq!(id, "no_such_saga"),
                other => panic!("expected SagaNotFound, got {other:?}"),
            }
            Ok::<_, BrainCoreError>(())
        })
        .unwrap();
    }

    #[test]
    fn cancel_saga_already_cancelled_returns_error() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_done", "cancelled");
            let err = cancel_saga(conn, "s_done").unwrap_err();
            match err {
                BrainCoreError::Parse(msg) => assert!(
                    msg.contains("terminal status 'cancelled'"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Parse, got {other:?}"),
            }
            Ok::<_, BrainCoreError>(())
        })
        .unwrap();
    }

    #[test]
    fn dedup_list_saga_task_ids_returns_added_at_order() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_ord", "open");
            insert_task(conn, "t1", "open");
            insert_task(conn, "t2", "open");
            insert_task(conn, "t3", "open");
            // Insert with explicit added_at so order is deterministic regardless of
            // strftime resolution.
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES ('s_ord','t2',100)",
                [],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES ('s_ord','t1',200)",
                [],
            )?;
            conn.execute(
                "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES ('s_ord','t3',300)",
                [],
            )?;
            let ids = list_saga_task_ids(conn, "s_ord")?;
            assert_eq!(ids, vec!["t2".to_string(), "t1".into(), "t3".into()]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn check_constraint_rejects_invalid_status() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let err = conn
                .execute(
                    "INSERT INTO sagas (saga_id, title, status, created_at, updated_at)
                     VALUES ('s_bad', 'T', 'nope', 1, 1)",
                    [],
                )
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.to_lowercase().contains("check") || msg.to_lowercase().contains("constraint"),
                "expected CHECK constraint failure, got: {msg}"
            );
            Ok::<_, BrainCoreError>(())
        })
        .unwrap();
    }

    #[test]
    fn check_constraint_rejects_invalid_event_type() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            insert_saga_row(conn, "s_ev", "open");
            let err = conn
                .execute(
                    "INSERT INTO saga_events (event_id, saga_id, event_type, timestamp, actor, payload)
                     VALUES ('e1', 's_ev', 'bogus_event', 1, 'me', '{}')",
                    [],
                )
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.to_lowercase().contains("check") || msg.to_lowercase().contains("constraint"),
                "expected CHECK constraint failure, got: {msg}"
            );
            Ok::<_, BrainCoreError>(())
        })
        .unwrap();
    }
}
