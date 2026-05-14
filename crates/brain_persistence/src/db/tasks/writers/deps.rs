//! Writers for the `task_deps` table.

use rusqlite::Connection;

use crate::db::links::projections::{LinkEvent, apply_link_event};
use crate::db::links::{EdgeKind, EntityRef, LinkCreatedPayload, LinkRemovedPayload};
use crate::sql::SqlResult;

/// INSERT OR IGNORE into task_deps + dual-write LinkCreated blocks.
pub fn add_dependency(conn: &Connection, task_id: &str, depends_on: &str) -> SqlResult<()> {
    conn.execute(
        "INSERT OR IGNORE INTO task_deps (task_id, depends_on) VALUES (?1, ?2)",
        rusqlite::params![task_id, depends_on],
    )?;
    apply_link_event(
        conn,
        &LinkEvent::Created(LinkCreatedPayload {
            from: EntityRef {
                kind: crate::db::links::EntityType::Task,
                id: task_id.to_string(),
            },
            to: EntityRef {
                kind: crate::db::links::EntityType::Task,
                id: depends_on.to_string(),
            },
            edge_kind: EdgeKind::Blocks,
        }),
    )?;
    Ok(())
}

/// DELETE from task_deps + dual-write LinkRemoved blocks.
pub fn remove_dependency(conn: &Connection, task_id: &str, depends_on: &str) -> SqlResult<()> {
    conn.execute(
        "DELETE FROM task_deps WHERE task_id = ?1 AND depends_on = ?2",
        rusqlite::params![task_id, depends_on],
    )?;
    apply_link_event(
        conn,
        &LinkEvent::Removed(LinkRemovedPayload {
            from: EntityRef {
                kind: crate::db::links::EntityType::Task,
                id: task_id.to_string(),
            },
            to: EntityRef {
                kind: crate::db::links::EntityType::Task,
                id: depends_on.to_string(),
            },
            edge_kind: EdgeKind::Blocks,
        }),
    )?;
    Ok(())
}
