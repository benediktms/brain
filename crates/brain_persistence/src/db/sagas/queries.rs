use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Result;
use crate::utils::now_ts;

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
        "INSERT INTO sagas (saga_id, title, description, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'planning', ?4, ?4)",
        params![saga_id, title, description, ts],
    )?;
    get_saga(conn, saga_id)?.ok_or_else(|| {
        crate::error::BrainCoreError::Database("saga disappeared after insert".into())
    })
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
