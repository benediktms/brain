//! Application-layer write API for the polymorphic edge graph.
//!
//! The three entry points are:
//! - [`add_link_checked`] — insert an edge, enforcing DAG constraint where required.
//! - [`remove_link`] — delete an edge, emitting a `LinkRemoved` event.
//! - [`for_entity`] — query all edges (outgoing + incoming) for an entity.

use rusqlite::Connection;
use thiserror::Error;

use crate::db::links::{
    EdgeKind, EntityRef, EntityType, LinkCreatedPayload, LinkRemovedPayload,
    projections::{LinkEvent, apply_link_event},
};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LinkError {
    #[error("adding this edge would create a cycle in the {0:?} graph")]
    Cycle(EdgeKind),

    #[error("database error: {0}")]
    Database(String),
}

impl From<rusqlite::Error> for LinkError {
    fn from(e: rusqlite::Error) -> Self {
        LinkError::Database(e.to_string())
    }
}

impl From<crate::error::BrainCoreError> for LinkError {
    fn from(e: crate::error::BrainCoreError) -> Self {
        LinkError::Database(e.to_string())
    }
}

// ── Output types ──────────────────────────────────────────────────────────────

/// A single edge as returned by [`for_entity`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityLink {
    pub from: EntityRef,
    pub to: EntityRef,
    pub edge_kind: EdgeKind,
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn entity_type_from_str(s: &str) -> Option<EntityType> {
    match s {
        "TASK" => Some(EntityType::Task),
        "RECORD" => Some(EntityType::Record),
        "EPISODE" => Some(EntityType::Episode),
        "PROCEDURE" => Some(EntityType::Procedure),
        "CHUNK" => Some(EntityType::Chunk),
        "NOTE" => Some(EntityType::Note),
        _ => None,
    }
}

fn edge_kind_from_str(s: &str) -> Option<EdgeKind> {
    match s {
        "parent_of" => Some(EdgeKind::ParentOf),
        "blocks" => Some(EdgeKind::Blocks),
        "covers" => Some(EdgeKind::Covers),
        "relates_to" => Some(EdgeKind::RelatesTo),
        "see_also" => Some(EdgeKind::SeeAlso),
        "supersedes" => Some(EdgeKind::Supersedes),
        "contradicts" => Some(EdgeKind::Contradicts),
        _ => None,
    }
}

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

// ── Cycle detection ───────────────────────────────────────────────────────────

/// Returns `true` if `from` is reachable from `to` via outgoing `edge_kind`
/// edges — i.e., inserting `from → to` would create a cycle.
///
/// Uses a recursive CTE that traverses the `entity_links` graph. The traversal
/// is bounded by the acyclicity property: in a valid DAG, the CTE terminates
/// at leaf nodes. Violation detection is O(V+E) in the worst case.
fn would_create_cycle(
    conn: &Connection,
    from: &EntityRef,
    to: &EntityRef,
    edge_kind: EdgeKind,
) -> Result<bool, LinkError> {
    // Walk all nodes reachable from `to` via outgoing same-kind edges.
    // If `from.id` appears in the reachable set, inserting from→to creates a cycle.
    //
    // NOTE: The CTE constrains `from_type` to the same type as `from` because
    // the graph is typed — a Task→Task edge and a Record→Task edge are distinct.
    // Cross-type DAGs (e.g. ParentOf between different entity types) are not
    // currently expected, but the constraint prevents cross-type false positives.
    let reachable: bool = conn
        .query_row(
            "WITH RECURSIVE reachable(id) AS (
                 SELECT to_id FROM entity_links
                     WHERE from_type = ?1 AND from_id = ?2 AND edge_kind = ?3
                 UNION
                 SELECT entity_links.to_id FROM entity_links
                     JOIN reachable ON entity_links.from_id = reachable.id
                     WHERE entity_links.from_type = ?1 AND entity_links.edge_kind = ?3
             )
             SELECT 1 FROM reachable WHERE id = ?4 LIMIT 1",
            rusqlite::params![
                entity_type_str(from.kind),
                to.id,
                edge_kind_str(edge_kind),
                from.id,
            ],
            |_| Ok(true),
        )
        .unwrap_or(false);

    Ok(reachable)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Insert a directed edge `from → to` of `edge_kind`.
///
/// When `edge_kind.requires_dag()`, the function first checks (within the same
/// transaction) whether inserting the edge would create a cycle. If so, returns
/// `LinkError::Cycle`. Otherwise emits a `LinkCreated` event through the Wave 1
/// projection, which writes to `entity_links`.
///
/// SQLite serializes writers on a single connection, so the read-then-write
/// sequence is race-free without additional locking.
pub fn add_link_checked(
    conn: &Connection,
    from: EntityRef,
    to: EntityRef,
    edge_kind: EdgeKind,
) -> Result<(), LinkError> {
    let tx = conn.unchecked_transaction()?;

    if edge_kind.requires_dag() && would_create_cycle(&tx, &from, &to, edge_kind)? {
        return Err(LinkError::Cycle(edge_kind));
    }

    apply_link_event(
        &tx,
        &LinkEvent::Created(LinkCreatedPayload {
            from,
            to,
            edge_kind,
        }),
    )?;

    tx.commit()?;
    Ok(())
}

/// Delete the directed edge `from → to` of `edge_kind`.
///
/// Emits a `LinkRemoved` event through the Wave 1 projection. If no matching
/// edge exists the operation is a no-op (idempotent).
pub fn remove_link(
    conn: &Connection,
    from: EntityRef,
    to: EntityRef,
    edge_kind: EdgeKind,
) -> Result<(), LinkError> {
    let tx = conn.unchecked_transaction()?;

    apply_link_event(
        &tx,
        &LinkEvent::Removed(LinkRemovedPayload {
            from,
            to,
            edge_kind,
        }),
    )?;

    tx.commit()?;
    Ok(())
}

/// Return all edges where `entity` is either the source or the target.
///
/// Both outgoing (`from = entity`) and incoming (`to = entity`) edges are
/// included. Results are ordered by `created_at` ascending.
pub fn for_entity(conn: &Connection, entity: EntityRef) -> Result<Vec<EntityLink>, LinkError> {
    let entity_type = entity_type_str(entity.kind);

    let mut stmt = conn.prepare_cached(
        "SELECT from_type, from_id, to_type, to_id, edge_kind
         FROM entity_links
         WHERE (from_type = ?1 AND from_id = ?2)
            OR (to_type   = ?1 AND to_id   = ?2)
         ORDER BY created_at ASC",
    )?;

    let rows: Vec<EntityLink> = stmt
        .query_map(rusqlite::params![entity_type, entity.id], |row| {
            let from_type_str: String = row.get(0)?;
            let from_id: String = row.get(1)?;
            let to_type_str: String = row.get(2)?;
            let to_id: String = row.get(3)?;
            let edge_kind_s: String = row.get(4)?;
            Ok((from_type_str, from_id, to_type_str, to_id, edge_kind_s))
        })?
        .filter_map(|r| r.ok())
        .filter_map(|(ft, fi, tt, ti, ek)| {
            let from_kind = entity_type_from_str(&ft)?;
            let to_kind = entity_type_from_str(&tt)?;
            let edge_kind = edge_kind_from_str(&ek)?;
            Some(EntityLink {
                from: EntityRef {
                    kind: from_kind,
                    id: fi,
                },
                to: EntityRef {
                    kind: to_kind,
                    id: ti,
                },
                edge_kind,
            })
        })
        .collect();

    Ok(rows)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::db::links::{EdgeKind, EntityRef, EntityType};

    fn open_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        crate::db::schema::run_migrations(&conn, 0).unwrap();
        conn
    }

    fn task(id: &str) -> EntityRef {
        EntityRef {
            kind: EntityType::Task,
            id: id.to_string(),
        }
    }

    fn count_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM entity_links", [], |r| r.get(0))
            .unwrap()
    }

    // ── Linear chain cycle detection ──────────────────────────────────────────

    #[test]
    fn linear_chain_blocks_back_edge() {
        let conn = open_db();

        // A → B → C (all Blocks edges)
        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("B"), task("C"), EdgeKind::Blocks).unwrap();

        // C → A would close the cycle
        let result = add_link_checked(&conn, task("C"), task("A"), EdgeKind::Blocks);
        assert_eq!(result, Err(LinkError::Cycle(EdgeKind::Blocks)));

        // Two edges inserted, third rejected
        assert_eq!(count_rows(&conn), 2);
    }

    #[test]
    fn direct_back_edge_blocked() {
        let conn = open_db();

        add_link_checked(&conn, task("A"), task("B"), EdgeKind::ParentOf).unwrap();

        let result = add_link_checked(&conn, task("B"), task("A"), EdgeKind::ParentOf);
        assert_eq!(result, Err(LinkError::Cycle(EdgeKind::ParentOf)));
    }

    // ── Diamond — no cycle ────────────────────────────────────────────────────

    #[test]
    fn diamond_shape_accepted() {
        let conn = open_db();

        // A → B, A → C, B → D, C → D — classic diamond, no cycle
        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("A"), task("C"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("B"), task("D"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("C"), task("D"), EdgeKind::Blocks).unwrap();

        assert_eq!(count_rows(&conn), 4);
    }

    // ── relates_to accepts cycles ─────────────────────────────────────────────

    #[test]
    fn relates_to_accepts_cycle() {
        let conn = open_db();

        // RelatesTo does not require DAG — cycles are semantically valid
        add_link_checked(&conn, task("X"), task("Y"), EdgeKind::RelatesTo).unwrap();
        add_link_checked(&conn, task("Y"), task("X"), EdgeKind::RelatesTo).unwrap();

        assert_eq!(count_rows(&conn), 2);
    }

    #[test]
    fn see_also_accepts_cycle() {
        let conn = open_db();

        add_link_checked(&conn, task("P"), task("Q"), EdgeKind::SeeAlso).unwrap();
        add_link_checked(&conn, task("Q"), task("P"), EdgeKind::SeeAlso).unwrap();

        assert_eq!(count_rows(&conn), 2);
    }

    #[test]
    fn contradicts_accepts_cycle() {
        let conn = open_db();

        add_link_checked(&conn, task("M"), task("N"), EdgeKind::Contradicts).unwrap();
        add_link_checked(&conn, task("N"), task("M"), EdgeKind::Contradicts).unwrap();

        assert_eq!(count_rows(&conn), 2);
    }

    // ── supersedes requires DAG ────────────────────────────────────────────────

    #[test]
    fn supersedes_blocks_cycle() {
        let conn = open_db();

        add_link_checked(&conn, task("V1"), task("V2"), EdgeKind::Supersedes).unwrap();

        let result = add_link_checked(&conn, task("V2"), task("V1"), EdgeKind::Supersedes);
        assert_eq!(result, Err(LinkError::Cycle(EdgeKind::Supersedes)));
    }

    // ── remove_link ───────────────────────────────────────────────────────────

    #[test]
    fn remove_link_deletes_edge() {
        let conn = open_db();

        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();
        assert_eq!(count_rows(&conn), 1);

        remove_link(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();
        assert_eq!(count_rows(&conn), 0);
    }

    #[test]
    fn remove_link_idempotent() {
        let conn = open_db();

        // Removing a non-existent edge must succeed without error
        remove_link(&conn, task("ghost"), task("nowhere"), EdgeKind::Blocks).unwrap();
        assert_eq!(count_rows(&conn), 0);
    }

    #[test]
    fn remove_link_unrelated_rows_survive() {
        let conn = open_db();

        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("C"), task("D"), EdgeKind::RelatesTo).unwrap();

        remove_link(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();

        assert_eq!(count_rows(&conn), 1, "unrelated edge must survive");
    }

    #[test]
    fn remove_link_after_cycle_check_passes() {
        let conn = open_db();

        // A → B (Blocks), then remove, then C → A allowed (no residual cycle)
        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();
        remove_link(&conn, task("A"), task("B"), EdgeKind::Blocks).unwrap();

        // After removal, B → A is no longer blocked by a cycle
        add_link_checked(&conn, task("B"), task("A"), EdgeKind::Blocks).unwrap();
        assert_eq!(count_rows(&conn), 1);
    }

    // ── for_entity ────────────────────────────────────────────────────────────

    #[test]
    fn for_entity_returns_outgoing_and_incoming() {
        let conn = open_db();

        // B → A (incoming for A), A → C (outgoing for A)
        add_link_checked(&conn, task("B"), task("A"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("A"), task("C"), EdgeKind::RelatesTo).unwrap();

        let links = for_entity(&conn, task("A")).unwrap();
        assert_eq!(links.len(), 2);

        let has_incoming = links.iter().any(|l| l.from.id == "B" && l.to.id == "A");
        let has_outgoing = links.iter().any(|l| l.from.id == "A" && l.to.id == "C");
        assert!(has_incoming, "incoming edge B→A must appear");
        assert!(has_outgoing, "outgoing edge A→C must appear");
    }

    #[test]
    fn for_entity_empty_when_no_edges() {
        let conn = open_db();

        let links = for_entity(&conn, task("orphan")).unwrap();
        assert!(links.is_empty());
    }

    #[test]
    fn for_entity_excludes_unrelated_edges() {
        let conn = open_db();

        add_link_checked(&conn, task("X"), task("Y"), EdgeKind::Blocks).unwrap();
        add_link_checked(&conn, task("P"), task("Q"), EdgeKind::RelatesTo).unwrap();

        let links = for_entity(&conn, task("X")).unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].from.id, "X");
        assert_eq!(links[0].to.id, "Y");
    }

    // ── add_link_checked is idempotent (duplicate edge) ───────────────────────

    #[test]
    fn add_link_checked_idempotent_on_duplicate() {
        let conn = open_db();

        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Covers).unwrap();
        add_link_checked(&conn, task("A"), task("B"), EdgeKind::Covers).unwrap();

        assert_eq!(count_rows(&conn), 1, "duplicate must be silently skipped");
    }

    // ── DAG edge kinds are checked; non-DAG kinds are not ────────────────────

    #[test]
    fn all_dag_kinds_enforce_cycle_check() {
        for dag_kind in [EdgeKind::ParentOf, EdgeKind::Blocks, EdgeKind::Supersedes] {
            let conn = open_db();
            add_link_checked(&conn, task("A"), task("B"), dag_kind).unwrap();
            let result = add_link_checked(&conn, task("B"), task("A"), dag_kind);
            assert_eq!(
                result,
                Err(LinkError::Cycle(dag_kind)),
                "{dag_kind:?} must enforce DAG"
            );
        }
    }

    #[test]
    fn all_non_dag_kinds_allow_cycles() {
        for free_kind in [
            EdgeKind::Covers,
            EdgeKind::RelatesTo,
            EdgeKind::SeeAlso,
            EdgeKind::Contradicts,
        ] {
            let conn = open_db();
            add_link_checked(&conn, task("A"), task("B"), free_kind).unwrap();
            add_link_checked(&conn, task("B"), task("A"), free_kind)
                .unwrap_or_else(|e| panic!("{free_kind:?} must allow cycles but got error: {e}"));
        }
    }
}
