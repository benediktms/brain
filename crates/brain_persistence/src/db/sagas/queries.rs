use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Result;
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
        crate::error::BrainCoreError::Database("saga disappeared after insert".into())
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
            return Err(crate::error::BrainCoreError::Parse(
                "update_saga: at least one field must be provided".into(),
            ));
        }
    }
    get_saga(conn, saga_id)?
        .ok_or_else(|| crate::error::BrainCoreError::SagaNotFound(saga_id.to_string()))
}

/// Close a saga: set status = 'closed', closed_at = now, bump updated_at.
/// Returns an error if the saga is not in 'open' status.
pub fn close_saga(conn: &Connection, saga_id: &str) -> Result<SagaRow> {
    let ts = now_ts();
    let rows_changed = conn.execute(
        "UPDATE sagas SET status = 'closed', closed_at = ?1, updated_at = ?1
         WHERE saga_id = ?2 AND status = 'open'",
        rusqlite::params![ts, saga_id],
    )?;
    if rows_changed == 0 {
        let existing = get_saga(conn, saga_id)?;
        return Err(match existing {
            None => crate::error::BrainCoreError::Database(format!("saga not found: {saga_id}")),
            Some(row) => crate::error::BrainCoreError::Database(format!(
                "saga cannot be closed from status '{}'; only 'open' sagas can be closed",
                row.status
            )),
        });
    }
    get_saga(conn, saga_id)?
        .ok_or_else(|| crate::error::BrainCoreError::Database(format!("saga not found: {saga_id}")))
}

/// List all member task IDs for a saga.
pub fn list_saga_member_task_ids(conn: &Connection, saga_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY added_at ASC")?;
    let ids = stmt
        .query_map([saga_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<String>, _>>()?;
    Ok(ids)
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
    let mut count = 0usize;
    for task_id in task_ids {
        conn.execute(
            "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, ?3)",
            params![saga_id, task_id, ts],
        )?;
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

/// List all task_ids belonging to a saga.
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
        crate::error::BrainCoreError::Database("saga disappeared after reopen".into())
    })
}

/// Stats for a saga's member tasks.
#[derive(Debug, Clone)]
pub struct SagaStatsRow {
    pub total: i64,
    pub open: i64,
    pub in_progress: i64,
    pub blocked: i64,
    pub done: i64,
    pub cancelled: i64,
    /// done / (total - cancelled); None if denominator is 0
    pub completion_pct: Option<f64>,
}

/// Compute aggregate stats for a saga in a single SQL query.
pub fn saga_stats(conn: &Connection, saga_id: &str) -> Result<SagaStatsRow> {
    conn.query_row(
        "SELECT
             COUNT(*) AS total,
             COUNT(CASE WHEN t.status = 'open'        THEN 1 END) AS open,
             COUNT(CASE WHEN t.status = 'in_progress' THEN 1 END) AS in_progress,
             COUNT(CASE WHEN t.status = 'blocked'     THEN 1 END) AS blocked,
             COUNT(CASE WHEN t.status = 'done'        THEN 1 END) AS done,
             COUNT(CASE WHEN t.status = 'cancelled'   THEN 1 END) AS cancelled
         FROM saga_tasks st
         JOIN tasks t ON t.task_id = st.task_id
         WHERE st.saga_id = ?1",
        [saga_id],
        |row| {
            let total: i64 = row.get(0)?;
            let open: i64 = row.get(1)?;
            let in_progress: i64 = row.get(2)?;
            let blocked: i64 = row.get(3)?;
            let done: i64 = row.get(4)?;
            let cancelled: i64 = row.get(5)?;
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
pub fn cancel_saga(conn: &Connection, saga_id: &str) -> Result<()> {
    let ts = now_ts();
    conn.execute(
        "UPDATE sagas SET status = 'cancelled', closed_at = ?2, updated_at = ?2
         WHERE saga_id = ?1",
        params![saga_id, ts],
    )?;
    Ok(())
}
