//! Projection layer for the polymorphic edge graph.
//!
//! Handles `LinkCreated` and `LinkRemoved` events, applying them to the
//! `entity_links` table. The SQL table is named `entity_links` (v48→v49
//! migration) to avoid collision with the legacy `links` table.
//!
//! `brain_scope` is always `NULL` on writes from this projection — cross-brain
//! symmetry. A future task will scope per-brain when callers emit it.

use rusqlite::Connection;
use ulid::Ulid;

use crate::db::links::{EdgeKind, EntityType, LinkCreatedPayload, LinkRemovedPayload};
use crate::error::Result;

// ── Event enum ────────────────────────────────────────────────────────────────

/// Unified projection event for the polymorphic edge graph.
pub enum LinkEvent {
    Created(LinkCreatedPayload),
    Removed(LinkRemovedPayload),
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn entity_type_str(t: EntityType) -> &'static str {
    match t {
        EntityType::Task => "TASK",
        EntityType::Record => "RECORD",
        EntityType::Episode => "EPISODE",
        EntityType::Procedure => "PROCEDURE",
        EntityType::Chunk => "CHUNK",
        EntityType::Note => "NOTE",
    }
}

fn edge_kind_str(k: EdgeKind) -> &'static str {
    match k {
        EdgeKind::ParentOf => "parent_of",
        EdgeKind::Blocks => "blocks",
        EdgeKind::Covers => "covers",
        EdgeKind::RelatesTo => "relates_to",
        EdgeKind::SeeAlso => "see_also",
        EdgeKind::Supersedes => "supersedes",
        EdgeKind::Contradicts => "contradicts",
    }
}

// ── Projection ────────────────────────────────────────────────────────────────

/// Apply a single polymorphic link event to the `entity_links` table.
///
/// - `LinkCreated` → `INSERT INTO entity_links(...)` with an auto-generated
///   ULID as the primary key. UNIQUE-tuple conflicts are detected by extended
///   error code and swallowed (idempotent on the composite key). Other
///   constraint violations — notably the self-loop CHECK — propagate as `Err`.
/// - `LinkRemoved` → `DELETE FROM entity_links WHERE (from_type, from_id,
///   to_type, to_id, edge_kind) = (?, ?, ?, ?, ?)`. No-op when no row
///   matches (idempotent).
///
/// `brain_scope` is always written as `NULL` — cross-brain edge symmetry.
pub fn apply_link_event(conn: &Connection, event: &LinkEvent) -> Result<()> {
    match event {
        LinkEvent::Created(p) => {
            let id = Ulid::new().to_string();
            let from_type = entity_type_str(p.from.kind);
            let from_id = &p.from.id;
            let to_type = entity_type_str(p.to.kind);
            let to_id = &p.to.id;
            let edge_kind = edge_kind_str(p.edge_kind);
            let created_at =
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

            // Use a plain INSERT (ABORT on conflict) so that the CHECK constraint
            // on self-loops surfaces as an error. UNIQUE conflicts are idempotent
            // — we detect and swallow them here; all other errors propagate.
            match conn.execute(
                "INSERT INTO entity_links
                     (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
                rusqlite::params![
                    id, from_type, from_id, to_type, to_id, edge_kind, created_at
                ],
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::SqliteFailure(ffi_err, _))
                    if ffi_err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE =>
                {
                    // Duplicate tuple — silent no-op (idempotent semantics).
                }
                Err(e) => return Err(e.into()),
            }
        }

        LinkEvent::Removed(p) => {
            let from_type = entity_type_str(p.from.kind);
            let from_id = &p.from.id;
            let to_type = entity_type_str(p.to.kind);
            let to_id = &p.to.id;
            let edge_kind = edge_kind_str(p.edge_kind);

            conn.execute(
                "DELETE FROM entity_links
                 WHERE from_type = ?1
                   AND from_id   = ?2
                   AND to_type   = ?3
                   AND to_id     = ?4
                   AND edge_kind = ?5",
                rusqlite::params![from_type, from_id, to_type, to_id, edge_kind],
            )?;
        }
    }

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::db::links::{
        EdgeKind, EntityRef, EntityType, LinkCreatedPayload, LinkRemovedPayload,
    };

    fn open_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        crate::db::schema::run_migrations(&conn, 0).unwrap();
        conn
    }

    fn task_ref(id: &str) -> EntityRef {
        EntityRef {
            kind: EntityType::Task,
            id: id.to_string(),
        }
    }

    fn record_ref(id: &str) -> EntityRef {
        EntityRef {
            kind: EntityType::Record,
            id: id.to_string(),
        }
    }

    fn count_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM entity_links", [], |r| r.get(0))
            .unwrap()
    }

    // ── 1. apply_create_inserts_row ───────────────────────────────────────────

    #[test]
    fn apply_create_inserts_row() {
        let conn = open_db();

        let payload = LinkCreatedPayload {
            from: task_ref("task-a"),
            to: record_ref("rec-b"),
            edge_kind: EdgeKind::Covers,
        };
        apply_link_event(&conn, &LinkEvent::Created(payload)).unwrap();

        let (from_type, from_id, to_type, to_id, edge_kind, brain_scope): (
            String,
            String,
            String,
            String,
            String,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT from_type, from_id, to_type, to_id, edge_kind, brain_scope
                 FROM entity_links LIMIT 1",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(from_type, "TASK");
        assert_eq!(from_id, "task-a");
        assert_eq!(to_type, "RECORD");
        assert_eq!(to_id, "rec-b");
        assert_eq!(edge_kind, "covers");
        assert!(brain_scope.is_none(), "brain_scope must be NULL");
    }

    // ── 2. apply_create_idempotent ────────────────────────────────────────────

    #[test]
    fn apply_create_idempotent() {
        let conn = open_db();

        let payload = LinkCreatedPayload {
            from: task_ref("task-a"),
            to: task_ref("task-b"),
            edge_kind: EdgeKind::Blocks,
        };

        apply_link_event(&conn, &LinkEvent::Created(payload.clone())).unwrap();
        apply_link_event(&conn, &LinkEvent::Created(payload)).unwrap();

        assert_eq!(count_rows(&conn), 1, "duplicate must be silently skipped");
    }

    // ── 3. apply_remove_deletes_row ───────────────────────────────────────────

    #[test]
    fn apply_remove_deletes_row() {
        let conn = open_db();

        let created = LinkCreatedPayload {
            from: task_ref("task-x"),
            to: task_ref("task-y"),
            edge_kind: EdgeKind::ParentOf,
        };
        apply_link_event(&conn, &LinkEvent::Created(created)).unwrap();
        assert_eq!(count_rows(&conn), 1);

        let removed = LinkRemovedPayload {
            from: task_ref("task-x"),
            to: task_ref("task-y"),
            edge_kind: EdgeKind::ParentOf,
        };
        apply_link_event(&conn, &LinkEvent::Removed(removed)).unwrap();
        assert_eq!(count_rows(&conn), 0, "row must be deleted");
    }

    // ── 4. apply_remove_idempotent ────────────────────────────────────────────

    #[test]
    fn apply_remove_idempotent() {
        let conn = open_db();

        let removed = LinkRemovedPayload {
            from: task_ref("ghost-1"),
            to: task_ref("ghost-2"),
            edge_kind: EdgeKind::RelatesTo,
        };
        // No row present — must succeed without error.
        apply_link_event(&conn, &LinkEvent::Removed(removed)).unwrap();
        assert_eq!(count_rows(&conn), 0);
    }

    // ── 5. apply_create_self_loop_returns_error ───────────────────────────────

    #[test]
    fn apply_create_self_loop_returns_error() {
        let conn = open_db();

        let payload = LinkCreatedPayload {
            from: task_ref("task-loop"),
            to: task_ref("task-loop"),
            edge_kind: EdgeKind::Blocks,
        };
        let result = apply_link_event(&conn, &LinkEvent::Created(payload));
        assert!(
            result.is_err(),
            "CHECK constraint on self-loop must surface as Err"
        );
    }

    // ── 6. apply_create_brain_scope_null_round_trips ──────────────────────────

    #[test]
    fn apply_create_brain_scope_null_round_trips() {
        let conn = open_db();

        let payload = LinkCreatedPayload {
            from: record_ref("rec-1"),
            to: record_ref("rec-2"),
            edge_kind: EdgeKind::SeeAlso,
        };
        apply_link_event(&conn, &LinkEvent::Created(payload)).unwrap();

        let brain_scope: Option<String> = conn
            .query_row("SELECT brain_scope FROM entity_links LIMIT 1", [], |r| {
                r.get(0)
            })
            .unwrap();

        assert!(
            brain_scope.is_none(),
            "brain_scope must be NULL — cross-brain edge symmetry"
        );
    }
}
