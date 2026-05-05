//! Link-graph traversal for episode neighbourhoods.
//!
//! BFS over the polymorphic `entity_links` graph, restricted to
//! episode-to-episode edges with kinds that imply semantic relatedness
//! (`relates_to`, `see_also`). Used by link-aware consolidation and (later)
//! by retrieve-time graph expansion.

use std::collections::{HashSet, VecDeque};

use rusqlite::Connection;
use tracing::warn;

use crate::db::links::{EdgeKind, EntityLink, EntityRef, EntityType, for_entity};
use crate::db::summaries::get_summaries_by_ids;
use crate::error::Result;

/// Edge kinds traversed by [`collect_linked_episode_set`].
///
/// Only non-DAG semantic-relatedness edges are followed. DAG kinds
/// (`parent_of`, `blocks`, `supersedes`) and `contradicts` carry different
/// intent and are excluded — consolidating across "A supersedes B" or "A
/// contradicts B" would silently merge episodes whose authors signalled
/// distinction.
const TRAVERSAL_EDGE_KINDS: [EdgeKind; 2] = [EdgeKind::RelatesTo, EdgeKind::SeeAlso];

/// Upper bound on visited-set size. Caps both writer-mutex hold time when the
/// caller invokes via the write connection AND prevents `get_summaries_by_ids`
/// from exceeding `SQLITE_MAX_VARIABLE_NUMBER` (32766 on modern bundled
/// SQLite, 999 on legacy builds — 1024 stays safely below either).
const MAX_VISITED: usize = 1024;

/// BFS over `entity_links` from a seed episode, returning all reachable
/// episode IDs (including the seed) in deterministic order.
///
/// # Invocation contract
/// This is a **read-only** traversal. Callers MUST invoke it via
/// `Db::with_read_conn`, never `with_write_conn` — holding the writer mutex
/// across the BFS serializes every concurrent writer in the process for the
/// duration of the call.
///
/// # Behaviour
/// - Traverses only Episode↔Episode edges of kind `RelatesTo` or `SeeAlso`.
/// - Both edge directions (incoming and outgoing) are followed.
/// - Cycle-safe: a `HashSet` tracks visited IDs.
/// - `max_depth` bounds expansion. Depth `0` returns only the seed; depth `1`
///   returns the seed plus direct neighbours; depth `n` returns all
///   BFS-reachable nodes within `n` hops.
/// - Bounded by [`MAX_VISITED`]. When the cap is reached, traversal halts
///   early with a `warn!` — useful for pathological neighbourhoods and for
///   keeping `get_summaries_by_ids`'s `IN (...)` placeholder list under
///   SQLite's parameter limit.
/// - Episodes referenced by edges but no longer present in the `summaries`
///   table are silently dropped, with a `warn!` log entry — consistent with
///   the stale-row tolerance applied in `for_entity`.
///
/// # Concurrency
/// Snapshot semantics are best-effort. Edges concurrently inserted by another
/// connection mid-traversal may or may not be reflected within a single
/// call. The visited-set guarantees we never re-enqueue, so duplicate
/// observations of an edge are harmless.
///
/// # Sort order
/// Returned IDs are sorted by episode `created_at` ASC; ties are broken by
/// ID ASC. The seed always appears in the result (provided it exists in
/// `summaries`); a missing seed yields an empty `Vec` plus a `warn!`.
pub fn collect_linked_episode_set(
    conn: &Connection,
    seed_episode_id: &str,
    max_depth: u32,
) -> Result<Vec<String>> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, u32)> = VecDeque::new();

    visited.insert(seed_episode_id.to_string());
    queue.push_back((seed_episode_id.to_string(), 0));

    'bfs: while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        let entity = EntityRef {
            kind: EntityType::Episode,
            id: current.clone(),
        };

        let edges = for_entity(conn, entity)?;

        for link in edges {
            extend_with_neighbour(&link, &current, depth, &mut visited, &mut queue);
            if visited.len() >= MAX_VISITED {
                warn!(
                    seed = %seed_episode_id,
                    cap = MAX_VISITED,
                    "collect_linked_episode_set: visited cap reached; halting BFS early"
                );
                break 'bfs;
            }
        }
    }

    let ids: Vec<String> = visited.into_iter().collect();
    let mut rows = get_summaries_by_ids(conn, &ids)?;

    rows.retain(|r| r.kind == "episode");

    if rows.len() < ids.len() {
        warn!(
            seed = %seed_episode_id,
            traversed = ids.len(),
            resolved = rows.len(),
            "collect_linked_episode_set: dropped IDs without matching episode rows"
        );
    }

    rows.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.summary_id.cmp(&b.summary_id))
    });

    Ok(rows.into_iter().map(|r| r.summary_id).collect())
}

/// Inspect an edge incident to `current` and, if it qualifies as a traversal
/// step, enqueue the other end at `depth + 1`.
///
/// An edge qualifies when its kind is in [`TRAVERSAL_EDGE_KINDS`] and both
/// endpoints are episode-typed. The "other end" is the endpoint that is not
/// `current`; rows that fail to identify a current-side endpoint are skipped
/// defensively (they should not occur given `for_entity`'s SQL filter, but a
/// direct SQL writer could create them).
fn extend_with_neighbour(
    link: &EntityLink,
    current: &str,
    depth: u32,
    visited: &mut HashSet<String>,
    queue: &mut VecDeque<(String, u32)>,
) {
    if !TRAVERSAL_EDGE_KINDS.contains(&link.edge_kind) {
        return;
    }

    let other = if link.from.id == current && link.from.kind == EntityType::Episode {
        &link.to
    } else if link.to.id == current && link.to.kind == EntityType::Episode {
        &link.from
    } else {
        return;
    };

    if other.kind != EntityType::Episode {
        return;
    }

    if visited.insert(other.id.clone()) {
        queue.push_back((other.id.clone(), depth + 1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rusqlite::Connection;

    use crate::db::Db;
    use crate::db::links::{EdgeKind, EntityRef, EntityType, add_link_checked};
    use crate::db::summaries::{Episode, store_episode};

    const TEST_BRAIN_ID: &str = "brain-test";
    const TEST_BRAIN_NAME: &str = "test";

    fn setup_db() -> Db {
        let db = Db::open_in_memory().expect("open in-memory db");
        db.ensure_brain_registered(TEST_BRAIN_ID, TEST_BRAIN_NAME)
            .expect("register brain");
        db
    }

    /// Insert an episode with a controlled `created_at` so tests can assert
    /// deterministic sort order. The default `store_episode` stamps the row
    /// with wall-clock now() at second resolution, which collapses
    /// quick-succession inserts into ties.
    fn insert_episode_at(conn: &Connection, label: &str, created_at: i64) -> String {
        let ep = Episode {
            brain_id: TEST_BRAIN_ID.into(),
            goal: format!("goal-{label}"),
            actions: format!("actions-{label}"),
            outcome: format!("outcome-{label}"),
            tags: vec![],
            importance: 0.5,
        };
        let id = store_episode(conn, &ep).expect("store episode");
        conn.execute(
            "UPDATE summaries SET created_at = ?1, updated_at = ?1 WHERE summary_id = ?2",
            rusqlite::params![created_at, id],
        )
        .expect("override created_at");
        id
    }

    fn ep(id: &str) -> EntityRef {
        EntityRef {
            kind: EntityType::Episode,
            id: id.to_string(),
        }
    }

    fn link(conn: &Connection, from: &str, to: &str, kind: EdgeKind) {
        add_link_checked(conn, ep(from), ep(to), kind).expect("add link");
    }

    // ── Isolated seed ────────────────────────────────────────────────────────

    #[test]
    fn isolated_seed_returns_only_self() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let result = collect_linked_episode_set(conn, &a, 5).unwrap();
            assert_eq!(result, vec![a]);
            Ok(())
        })
        .unwrap();
    }

    // ── Linear chain ─────────────────────────────────────────────────────────

    #[test]
    fn chain_at_depth_2_finds_three_nodes() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);
            let d = insert_episode_at(conn, "D", 400);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &b, &c, EdgeKind::RelatesTo);
            link(conn, &c, &d, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 2).unwrap();
            assert_eq!(result, vec![a, b, c]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn chain_at_depth_0_returns_only_seed() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            link(conn, &a, &b, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 0).unwrap();
            assert_eq!(result, vec![a]);
            Ok(())
        })
        .unwrap();
    }

    // ── Fork ─────────────────────────────────────────────────────────────────

    #[test]
    fn fork_at_depth_1_finds_all_neighbours() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);
            let d = insert_episode_at(conn, "D", 400);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &a, &c, EdgeKind::SeeAlso);
            link(conn, &a, &d, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 1).unwrap();
            assert_eq!(result, vec![a, b, c, d]);
            Ok(())
        })
        .unwrap();
    }

    // ── Cycle terminates ─────────────────────────────────────────────────────

    #[test]
    fn cycle_terminates_at_visited_set() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &b, &c, EdgeKind::RelatesTo);
            link(conn, &c, &a, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 100).unwrap();
            assert_eq!(result, vec![a, b, c]);
            Ok(())
        })
        .unwrap();
    }

    // ── Diamond dedupes ──────────────────────────────────────────────────────

    #[test]
    fn diamond_dedupes_via_multiple_paths() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);
            let d = insert_episode_at(conn, "D", 400);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &a, &c, EdgeKind::RelatesTo);
            link(conn, &b, &d, EdgeKind::RelatesTo);
            link(conn, &c, &d, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 2).unwrap();
            assert_eq!(result, vec![a, b, c, d]);
            Ok(())
        })
        .unwrap();
    }

    // ── Both edge directions are traversed ───────────────────────────────────

    #[test]
    fn incoming_edges_are_traversed() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);

            // Edge points B → A; from A's perspective this is incoming.
            link(conn, &b, &a, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 1).unwrap();
            assert_eq!(result, vec![a, b]);
            Ok(())
        })
        .unwrap();
    }

    // ── Non-traversal kinds are skipped ──────────────────────────────────────

    #[test]
    fn non_traversal_edge_kinds_are_skipped() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);

            link(conn, &a, &b, EdgeKind::ParentOf);
            link(conn, &a, &c, EdgeKind::Supersedes);

            let result = collect_linked_episode_set(conn, &a, 5).unwrap();
            assert_eq!(result, vec![a]);
            Ok(())
        })
        .unwrap();
    }

    // ── Mixed entity types — only Episode endpoints are followed ─────────────

    #[test]
    fn mixed_entity_types_only_episode_returned() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);

            link(conn, &a, &b, EdgeKind::RelatesTo);

            // Episode → Task edge — must not follow into Task space.
            add_link_checked(
                conn,
                ep(&a),
                EntityRef {
                    kind: EntityType::Task,
                    id: "task-foreign".into(),
                },
                EdgeKind::RelatesTo,
            )
            .unwrap();

            let result = collect_linked_episode_set(conn, &a, 5).unwrap();
            assert_eq!(result, vec![a, b]);
            Ok(())
        })
        .unwrap();
    }

    // ── Sort order ───────────────────────────────────────────────────────────

    #[test]
    fn results_sorted_by_created_at_then_id() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let later = insert_episode_at(conn, "later", 300);
            let earlier = insert_episode_at(conn, "earlier", 100);
            let middle = insert_episode_at(conn, "middle", 200);

            link(conn, &earlier, &middle, EdgeKind::RelatesTo);
            link(conn, &middle, &later, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &later, 5).unwrap();
            assert_eq!(result, vec![earlier, middle, later]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn ties_on_created_at_break_by_id() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 100);
            let c = insert_episode_at(conn, "C", 100);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &b, &c, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 5).unwrap();
            let mut expected = vec![a, b, c];
            expected.sort();
            assert_eq!(result, expected);
            Ok(())
        })
        .unwrap();
    }

    // ── Stale edge: edge points to a deleted episode — silently dropped ──────

    #[test]
    fn missing_episode_silently_dropped() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            link(conn, &a, &b, EdgeKind::RelatesTo);

            conn.execute(
                "DELETE FROM summaries WHERE summary_id = ?1",
                rusqlite::params![b],
            )
            .unwrap();

            let result = collect_linked_episode_set(conn, &a, 5).unwrap();
            assert_eq!(result, vec![a], "missing episode must be dropped");
            Ok(())
        })
        .unwrap();
    }

    // ── Stale seed: caller passes an ID whose summary row is gone ───────────

    #[test]
    fn seed_missing_returns_empty() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            // Drop the seed row from summaries before traversal. Doc contract:
            // "a missing seed yields an empty Vec plus a warn!".
            conn.execute(
                "DELETE FROM summaries WHERE summary_id = ?1",
                rusqlite::params![a],
            )
            .unwrap();

            let result = collect_linked_episode_set(conn, &a, 5).unwrap();
            assert!(
                result.is_empty(),
                "missing seed must produce empty result, got {result:?}"
            );
            Ok(())
        })
        .unwrap();
    }
}
