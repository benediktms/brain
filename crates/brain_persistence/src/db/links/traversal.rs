//! Link-graph traversal for episode neighbourhoods.
//!
//! BFS over the polymorphic `entity_links` graph, restricted to
//! episode-to-episode edges that imply same-cohort membership for
//! consolidation: `relates_to`, `see_also`, and `continues`. Used by
//! link-aware consolidation and (later) by retrieve-time graph expansion.

use std::collections::{HashSet, VecDeque};

use rusqlite::Connection;
use tracing::warn;

use crate::db::links::{EdgeKind, EntityLink, EntityRef, EntityType, for_entity};
use crate::db::summaries::{SummaryRow, get_summaries_by_ids};
use crate::error::Result;

/// Edge kinds traversed by [`collect_linked_episode_set`] (consolidation cohort).
///
/// Followed:
/// - `relates_to`, `see_also` — symmetric semantic-relatedness edges.
/// - `continues` — agent-declared thread continuation. Even though `continues`
///   is a DAG kind, it represents the consolidation cohort by construction —
///   a thread IS what we want to summarize together.
///
/// Excluded:
/// - `parent_of`, `blocks`, `supersedes` — DAG kinds with hierarchical or
///   ordering semantics that do not imply same-cohort membership.
/// - `contradicts` — consolidating episodes whose authors signalled
///   distinction would silently merge contradictory content.
const TRAVERSAL_EDGE_KINDS: [EdgeKind; 3] =
    [EdgeKind::RelatesTo, EdgeKind::SeeAlso, EdgeKind::Continues];

/// Edge kinds traversed by [`collect_thread_episodes`] (thread enumeration).
///
/// A thread is the chain of episodes connected via agent-declared
/// `continues` edges. This narrower set is what `memory.walk_thread`
/// walks — related episodes (via `relates_to`/`see_also`) are not part
/// of the thread proper, even though consolidation considers them in
/// the same cohort.
const THREAD_EDGE_KINDS: [EdgeKind; 1] = [EdgeKind::Continues];

/// Upper bound on visited-set size. Caps both writer-mutex hold time when the
/// caller invokes via the write connection AND prevents `get_summaries_by_ids`
/// from exceeding `SQLITE_MAX_VARIABLE_NUMBER` (32766 on modern bundled
/// SQLite, 999 on legacy builds — 1024 stays safely below either).
pub(crate) const MAX_VISITED: usize = 1024;

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
/// - Traverses only Episode↔Episode edges of kind `RelatesTo`, `SeeAlso`, or
///   `Continues`.
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
    collect_episode_set_inner(conn, seed_episode_id, max_depth, &TRAVERSAL_EDGE_KINDS)
        .map(|rows| rows.into_iter().map(|r| r.summary_id).collect())
}

/// BFS from a seed episode along **only** `continues` edges, returning the
/// thread of episodes connected via agent-declared continuation. Companion
/// to [`collect_linked_episode_set`] for thread enumeration (`memory.walk_thread`).
///
/// Same invocation contract, sort order, and bounds as
/// [`collect_linked_episode_set`] — only the edge-kind filter differs.
/// Bidirectional: walks both `B → A` (predecessors of B) and `C → B`
/// (successors of B) for any node B.
pub fn collect_thread_episodes(
    conn: &Connection,
    seed_episode_id: &str,
    max_depth: u32,
) -> Result<Vec<String>> {
    collect_thread_episode_rows(conn, seed_episode_id, max_depth)
        .map(|rows| rows.into_iter().map(|r| r.summary_id).collect())
}

/// Same as [`collect_thread_episodes`] but returns full [`SummaryRow`]s
/// directly, avoiding a second `get_summaries_by_ids` round-trip when the
/// caller needs episode metadata (e.g. `memory.walk_thread` MCP tool).
///
/// The inner BFS already hydrates rows to filter by kind; this variant
/// exposes them rather than discarding to IDs and forcing the caller to
/// re-query.
pub fn collect_thread_episode_rows(
    conn: &Connection,
    seed_episode_id: &str,
    max_depth: u32,
) -> Result<Vec<SummaryRow>> {
    collect_episode_set_inner(conn, seed_episode_id, max_depth, &THREAD_EDGE_KINDS)
}

/// Shared BFS logic. Caller chooses which edge kinds qualify as traversal
/// steps. The endpoint-type check ensures only Episode↔Episode edges drive
/// expansion regardless of which kinds are passed.
///
/// Returns rows sorted by `(created_at ASC, summary_id ASC)`. The sort is
/// retained so that *any* caller — ID-returning or row-returning — observes
/// a deterministic order without re-sorting.
fn collect_episode_set_inner(
    conn: &Connection,
    seed_episode_id: &str,
    max_depth: u32,
    edge_kinds: &[EdgeKind],
) -> Result<Vec<SummaryRow>> {
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

        let edges = for_entity(conn, &entity, Some(edge_kinds))?;

        for link in edges {
            extend_with_neighbour(&link, &current, depth, &mut visited, &mut queue);
            if visited.len() >= MAX_VISITED {
                warn!(
                    seed = %seed_episode_id,
                    cap = MAX_VISITED,
                    "collect_episode_set_inner: visited cap reached; halting BFS early"
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
            "collect_episode_set_inner: dropped IDs without matching episode rows"
        );
    }

    rows.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.summary_id.cmp(&b.summary_id))
    });

    Ok(rows)
}

/// Inspect an edge incident to `current` and, if it qualifies as a traversal
/// step, enqueue the other end at `depth + 1`.
///
/// The edge-kind filter is applied in SQL by [`for_entity`] using the
/// caller-supplied `edge_kinds` slice (see [`collect_episode_set_inner`]),
/// so only edges of the configured kinds reach this function. The
/// endpoint-type check (both sides must be Episode-typed) is retained
/// defensively — a direct SQL writer could insert cross-type edges that
/// bypass the API.
///
/// The "other end" is the endpoint that is not `current`; rows that fail to
/// identify a current-side endpoint are skipped defensively.
fn extend_with_neighbour(
    link: &EntityLink,
    current: &str,
    depth: u32,
    visited: &mut HashSet<String>,
    queue: &mut VecDeque<(String, u32)>,
) {
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

    // ── collect_thread_episodes: follows ONLY Continues edges ───────────────

    #[test]
    fn thread_traversal_ignores_relates_to_and_see_also() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);
            let d = insert_episode_at(conn, "D", 400);

            // Thread: B continues A; C continues B.
            link(conn, &b, &a, EdgeKind::Continues);
            link(conn, &c, &b, EdgeKind::Continues);

            // Off-thread relations — must NOT be followed by collect_thread_episodes.
            link(conn, &b, &d, EdgeKind::RelatesTo);
            link(conn, &c, &d, EdgeKind::SeeAlso);

            let thread = collect_thread_episodes(conn, &b, 5).unwrap();
            assert_eq!(
                thread,
                vec![a, b, c],
                "thread must include only Continues neighbours"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn thread_traversal_handles_fork_predecessors() {
        // Fork shape: two separate threads converging — sibling B and C both
        // continue A. From B's perspective the thread reaches A but not C.
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);

            link(conn, &b, &a, EdgeKind::Continues);
            link(conn, &c, &a, EdgeKind::Continues);

            let thread_from_a = collect_thread_episodes(conn, &a, 5).unwrap();
            assert_eq!(
                thread_from_a,
                vec![a.clone(), b.clone(), c.clone()],
                "from A both successors are reachable"
            );

            let thread_from_b = collect_thread_episodes(conn, &b, 1).unwrap();
            assert_eq!(
                thread_from_b,
                vec![a.clone(), b.clone()],
                "from B at depth 1 only A is reachable, not sibling C"
            );
            Ok(())
        })
        .unwrap();
    }

    // ── Continues edges are followed (thread traversal) ─────────────────────

    #[test]
    fn continues_edges_form_traversable_thread() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);
            let c = insert_episode_at(conn, "C", 300);

            // B continues A; C continues B. Thread: A → B → C.
            link(conn, &b, &a, EdgeKind::Continues);
            link(conn, &c, &b, EdgeKind::Continues);

            // From any node in the thread we recover the whole thread.
            let from_head = collect_linked_episode_set(conn, &a, 5).unwrap();
            assert_eq!(from_head, vec![a.clone(), b.clone(), c.clone()]);

            let from_tail = collect_linked_episode_set(conn, &c, 5).unwrap();
            assert_eq!(from_tail, vec![a, b, c]);
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

    // ── Self-loop equivalent: visited-set blocks re-enqueue of the seed ─────

    /// The schema forbids DB-level self-loops (CHECK constraint). We instead
    /// verify the visited-set blocks re-enqueue via a 2-cycle A→B→A, where A
    /// is the seed. After depth-1 expansion B is visited; B's neighbour A is
    /// already in visited and must not be re-enqueued. Result: exactly [A, B].
    #[test]
    fn self_loop_a_relates_to_a_terminates() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &b, &a, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 10).unwrap();
            // Visited-set must block re-enqueue of A from B; no duplicates.
            assert_eq!(
                result.len(),
                2,
                "visited-set must block re-enqueue; got {result:?}"
            );
            assert!(result.contains(&a), "seed must be present");
            assert!(result.contains(&b), "neighbour must be present");
            Ok(())
        })
        .unwrap();
    }

    // ── max_depth = 0 with a cycle in the graph ──────────────────────────────

    #[test]
    fn max_depth_zero_with_cycle() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &b, &a, EdgeKind::RelatesTo);

            let result = collect_linked_episode_set(conn, &a, 0).unwrap();
            assert_eq!(result, vec![a.clone()], "depth 0 must return seed only");
            Ok(())
        })
        .unwrap();
    }

    // ── max_depth = u32::MAX terminates without overflow ─────────────────────

    #[test]
    fn max_depth_u32_max_terminates() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            // Small cycle: A → B → A. BFS terminates via visited-set, not depth.
            let a = insert_episode_at(conn, "A", 100);
            let b = insert_episode_at(conn, "B", 200);

            link(conn, &a, &b, EdgeKind::RelatesTo);
            link(conn, &b, &a, EdgeKind::RelatesTo);

            // Must not panic or overflow regardless of the high depth limit.
            let result = collect_linked_episode_set(conn, &a, u32::MAX).unwrap();
            let mut expected = vec![a.clone(), b.clone()];
            expected.sort();
            let mut got = result.clone();
            got.sort();
            assert_eq!(got, expected, "all reachable episodes must be returned");
            Ok(())
        })
        .unwrap();
    }

    // ── Empty seed string returns empty Vec ──────────────────────────────────

    #[test]
    fn empty_seed_returns_empty() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            // No summaries row for ""; BFS finds no edges; result is empty.
            let result = collect_linked_episode_set(conn, "", 5).unwrap();
            assert!(
                result.is_empty(),
                "empty seed must produce empty result, got {result:?}"
            );
            Ok(())
        })
        .unwrap();
    }

    // ── Non-episode seed: Task ID passed as seed returns empty ───────────────

    #[test]
    fn non_episode_seed_returns_empty() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            let a = insert_episode_at(conn, "A", 100);

            // Insert a Task→Episode link so "task-x" exists in entity_links.
            // The traversal seeds with Episode kind, so it queries for
            // (from_type='episode' AND from_id='task-x') — no match.
            // The summaries table has no row for "task-x", so result is empty.
            add_link_checked(
                conn,
                EntityRef {
                    kind: EntityType::Task,
                    id: "task-x".into(),
                },
                ep(&a),
                EdgeKind::RelatesTo,
            )
            .expect("add task→episode link");

            let result = collect_linked_episode_set(conn, "task-x", 5).unwrap();
            assert!(
                result.is_empty(),
                "non-episode seed must produce empty result, got {result:?}"
            );
            Ok(())
        })
        .unwrap();
    }

    // ── Visited cap halts BFS early ──────────────────────────────────────────

    /// Constructs a chain longer than `MAX_VISITED` and verifies BFS halts at
    /// the cap. Gated behind `#[ignore]` because inserting >1024 episodes is slow.
    ///
    /// Run explicitly with: `cargo test -p brain-persistence -- --ignored visited_cap`
    #[test]
    #[ignore]
    fn visited_cap_terminates_bfs_early() {
        let db = setup_db();
        db.with_write_conn(|conn| {
            // Build a linear chain of MAX_VISITED + 5 episodes.
            let chain_len = MAX_VISITED + 5;
            let mut ids: Vec<String> = Vec::with_capacity(chain_len);
            for i in 0..chain_len {
                let id = insert_episode_at(conn, &format!("ep-{i:04}"), i as i64 * 10);
                ids.push(id);
            }
            for window in ids.windows(2) {
                link(conn, &window[0], &window[1], EdgeKind::RelatesTo);
            }

            // Traverse from the first node with a depth large enough to reach all.
            let result = collect_linked_episode_set(conn, &ids[0], chain_len as u32 + 10).unwrap();

            assert_eq!(
                result.len(),
                MAX_VISITED,
                "BFS must halt at MAX_VISITED cap; got {} episodes",
                result.len()
            );
            Ok(())
        })
        .unwrap();
    }
}
