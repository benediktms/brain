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
