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

/// Test helper: insert a TASK→TASK 'blocks' edge directly into `entity_links`
/// with foreign-key enforcement temporarily disabled. Lets tests construct
/// corrupt-state fixtures (e.g. a `to_id` that has no corresponding row in
/// `tasks`) so that defensive code paths in the cycle-detection BFS can be
/// exercised. Production callers must use `add_dependency` instead.
pub fn add_orphan_blocks_edge(conn: &Connection, from_id: &str, to_id: &str) -> SqlResult<()> {
    conn.execute_batch("PRAGMA foreign_keys = OFF")?;
    conn.execute(
        "INSERT OR IGNORE INTO entity_links \
         (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope) \
         VALUES \
         (lower(hex(randomblob(16))), 'TASK', ?1, 'TASK', ?2, 'blocks', \
          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL)",
        rusqlite::params![from_id, to_id],
    )?;
    conn.execute_batch("PRAGMA foreign_keys = ON")?;
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
