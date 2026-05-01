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
///
/// # Wire shape
///
/// `LinkEvent` serialises with **externally-tagged** enum encoding (Rust/serde default):
///
/// ```json
/// { "Created": { "from": {...}, "to": {...}, "edge_kind": "covers" } }
/// { "Removed": { "from": {...}, "to": {...}, "edge_kind": "blocks" } }
/// ```
///
/// # Difference from `TaskEvent` / `RecordEvent`
///
/// Those types use a **flat envelope** with explicit fields:
/// `event_type`, `payload`, `event_id`, `timestamp`, `actor`, `event_version`.
/// `LinkEvent` deliberately omits that envelope — it is the raw in-memory
/// projection-input type consumed by [`apply_link_event`], not the durable
/// event-log shape.
///
/// # Deferred reconciliation (Wave 2)
///
/// When `add_link_checked` wires `LinkEvent` into the unified event log (Wave 2
/// work), a wrapper translation layer will map the bare `LinkEvent` to the
/// canonical flat-envelope format before persistence. No changes to this type
/// are expected at that point.
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

// Re-exported under distinct names so the DRY-mirror tests in `mod.rs` can
// reference them without leaking the private helpers into the public API.
#[cfg(test)]
pub(crate) fn entity_type_str_for_test(t: EntityType) -> &'static str {
    entity_type_str(t)
}

#[cfg(test)]
pub(crate) fn edge_kind_str_for_test(k: EdgeKind) -> &'static str {
    edge_kind_str(k)
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
            let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

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

    // ── 4b. apply_remove_unrelated_rows_survive ───────────────────────────────
    //
    // Removing a non-existent edge must not disturb pre-existing rows.
    // This catches a future regression where the DELETE WHERE clause widens.

    #[test]
    fn apply_remove_unrelated_rows_survive() {
        let conn = open_db();

        // Insert two distinct edges.
        apply_link_event(
            &conn,
            &LinkEvent::Created(LinkCreatedPayload {
                from: task_ref("task-a"),
                to: task_ref("task-b"),
                edge_kind: EdgeKind::Blocks,
            }),
        )
        .unwrap();
        apply_link_event(
            &conn,
            &LinkEvent::Created(LinkCreatedPayload {
                from: record_ref("rec-1"),
                to: task_ref("task-a"),
                edge_kind: EdgeKind::Covers,
            }),
        )
        .unwrap();
        assert_eq!(count_rows(&conn), 2, "two edges inserted");

        // Remove a non-existent third edge.
        apply_link_event(
            &conn,
            &LinkEvent::Removed(LinkRemovedPayload {
                from: task_ref("nobody"),
                to: task_ref("nowhere"),
                edge_kind: EdgeKind::RelatesTo,
            }),
        )
        .unwrap();

        // Original two rows must survive intact.
        assert_eq!(count_rows(&conn), 2, "unrelated rows must survive DELETE");
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

        // `apply_link_event` converts rusqlite errors via `BrainCoreError::Database(msg)`,
        // so we cannot pattern-match the raw `SqliteFailure` after the fact.
        // We verify (a) the call returns Err, and (b) the raw SQLite layer raises
        // SQLITE_CONSTRAINT_CHECK (extended code 275) by re-running the same INSERT
        // directly — this mirrors what Drone Alpha applies in the fixture test.
        assert!(
            result.is_err(),
            "CHECK constraint on self-loop must surface as Err"
        );

        // Secondary: confirm the raw constraint kind via a direct rusqlite call.
        let raw_result = conn.execute(
            "INSERT INTO entity_links (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope)
             VALUES ('test-self-loop', 'TASK', 'task-loop', 'TASK', 'task-loop', 'blocks', '2024-01-01T00:00:00Z', NULL)",
            [],
        );
        match raw_result {
            Err(rusqlite::Error::SqliteFailure(ffi_err, _)) => {
                assert_eq!(
                    ffi_err.extended_code,
                    rusqlite::ffi::SQLITE_CONSTRAINT_CHECK,
                    "expected SQLITE_CONSTRAINT_CHECK (275), got extended_code={}",
                    ffi_err.extended_code
                );
            }
            Err(other) => panic!("expected SqliteFailure with CHECK constraint, got {other:?}"),
            Ok(_) => panic!("self-loop INSERT must fail, schema CHECK constraint not active"),
        }
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

    // ── 7. apply_concurrent_unique_collision ──────────────────────────────────
    //
    // Two threads race to insert the same composite tuple. Both must return
    // `Ok(())` (idempotent UNIQUE handling). Final row count == 1.
    //
    // `Connection::open_in_memory()` is not thread-safe across connections, so
    // we use a file-backed temp DB and open a separate connection per thread.

    #[test]
    fn apply_concurrent_unique_collision() {
        use std::sync::{Arc, Barrier};

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Initialise the schema on the temp DB.
        {
            let setup_conn = Connection::open(&path).unwrap();
            setup_conn
                .pragma_update(None, "journal_mode", "WAL")
                .unwrap();
            setup_conn
                .pragma_update(None, "foreign_keys", "ON")
                .unwrap();
            crate::db::schema::run_migrations(&setup_conn, 0).unwrap();
        }

        let barrier = Arc::new(Barrier::new(2));

        let payload = LinkCreatedPayload {
            from: task_ref("task-concurrent-a"),
            to: task_ref("task-concurrent-b"),
            edge_kind: EdgeKind::Blocks,
        };

        let make_thread =
            |barrier: Arc<Barrier>, path: std::path::PathBuf, payload: LinkCreatedPayload| {
                std::thread::spawn(move || {
                    let conn = Connection::open(&path).unwrap();
                    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
                    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
                    barrier.wait(); // maximise contention
                    apply_link_event(&conn, &LinkEvent::Created(payload))
                })
            };

        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);
        let p1 = payload.clone();
        let p2 = payload;
        let path1 = path.clone();
        let path2 = path;

        let t1 = make_thread(b1, path1, p1);
        let t2 = make_thread(b2, path2, p2);

        let r1 = t1.join().expect("thread 1 panicked");
        let r2 = t2.join().expect("thread 2 panicked");

        assert!(r1.is_ok(), "thread 1 must return Ok: {r1:?}");
        assert!(r2.is_ok(), "thread 2 must return Ok: {r2:?}");

        // Exactly one row must exist.
        let final_conn = Connection::open(tmp.path()).unwrap();
        let row_count: i64 = final_conn
            .query_row("SELECT COUNT(*) FROM entity_links", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            row_count, 1,
            "concurrent inserts of same tuple must yield exactly 1 row"
        );
    }

    // ── 8. all_entity_types_round_trip ────────────────────────────────────────
    //
    // Smoke-test: a `LinkCreated` for every `EntityType` variant inserts without
    // error. Catches gaps where `entity_type_str` lacks a variant.

    #[test]
    fn all_entity_types_round_trip() {
        let conn = open_db();

        let variants: &[(EntityType, &str)] = &[
            (EntityType::Task, "et-task"),
            (EntityType::Record, "et-rec"),
            (EntityType::Episode, "et-ep"),
            (EntityType::Procedure, "et-proc"),
            (EntityType::Chunk, "et-chunk"),
            (EntityType::Note, "et-note"),
        ];

        for (entity_type, id) in variants {
            let payload = LinkCreatedPayload {
                from: EntityRef {
                    kind: EntityType::Task,
                    id: "anchor".to_string(),
                },
                to: EntityRef {
                    kind: *entity_type,
                    id: id.to_string(),
                },
                edge_kind: EdgeKind::RelatesTo,
            };
            apply_link_event(&conn, &LinkEvent::Created(payload))
                .unwrap_or_else(|e| panic!("insert failed for {entity_type:?}: {e}"));
        }

        let row_count = count_rows(&conn);
        assert_eq!(
            row_count,
            variants.len() as i64,
            "each EntityType variant must produce exactly one row"
        );
    }
}
