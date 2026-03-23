//! Integration tests for `memory.consolidate`.
//!
//! These tests verify the behaviour of [`brain_lib::consolidation::consolidate_episodes`]
//! (and, transitively, the `memory.consolidate` MCP tool) against an
//! in-memory SQLite database seeded with known episode timestamps.

use rusqlite::Connection;

use brain_lib::consolidation::{ConsolidateResult, consolidate_episodes};
use brain_persistence::db::summaries::list_episodes;

// ─── Schema helpers ──────────────────────────────────────────────────────────

/// Minimal in-memory schema that supports `kind='episode'`.
fn setup_schema() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;

         CREATE TABLE IF NOT EXISTS summaries (
             summary_id  TEXT    PRIMARY KEY,
             kind        TEXT    NOT NULL
                                 CHECK(kind IN ('episode','reflection','summary','procedure')),
             title       TEXT,
             content     TEXT    NOT NULL DEFAULT '',
             tags        TEXT    NOT NULL DEFAULT '[]',
             importance  REAL    NOT NULL DEFAULT 1.0,
             brain_id    TEXT    NOT NULL DEFAULT '',
             parent_id   TEXT    REFERENCES summaries(summary_id),
             source_hash TEXT,
             confidence  REAL    NOT NULL DEFAULT 1.0,
             valid_from  INTEGER,
             chunk_id    TEXT,
             summarizer  TEXT,
             created_at  INTEGER NOT NULL,
             updated_at  INTEGER NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind);",
    )
    .unwrap();
    conn
}

/// Insert an episode row with an explicit `created_at` timestamp (Unix seconds).
/// We bypass `store_episode` to control timestamps precisely.
fn insert_episode_at(conn: &Connection, title: &str, brain_id: &str, created_at: i64) -> String {
    let id = ulid::Ulid::new().to_string();
    conn.execute(
        "INSERT INTO summaries
             (summary_id, kind, title, content, tags, importance, brain_id,
              valid_from, created_at, updated_at)
         VALUES (?1, 'episode', ?2, ?3, '[]', 1.0, ?4, ?5, ?5, ?5)",
        rusqlite::params![
            id,
            title,
            format!("Goal: {title}\nActions: none\nOutcome: done"),
            brain_id,
            created_at,
        ],
    )
    .unwrap();
    id
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Verifies that `ConsolidateResult` and `consolidate_episodes` are reachable
/// and that an empty input produces empty clusters.
#[test]
fn test_stub_returns_empty_clusters() {
    let result: ConsolidateResult = consolidate_episodes(vec![], 3600);
    assert!(
        result.clusters.is_empty(),
        "empty input must return empty clusters"
    );
}

/// Verifies that an empty episode list produces an empty result regardless of
/// the gap_seconds parameter.
#[test]
fn test_empty_episodes_empty_result() {
    let result = consolidate_episodes(vec![], 300);
    assert_eq!(result.clusters.len(), 0);
}

/// Seed 5 episodes split into two temporal groups:
/// - Group A (recent):  t=1_000_000, t=1_000_060  (1 minute apart)
/// - Group B (older):   t=900_000,   t=900_120,   t=900_180  (2 min window)
///
/// Gap between groups: ~100_000 seconds >> gap_seconds=3600.
///
/// Asserts that the clustering algorithm produces exactly 2 clusters.
#[test]
fn test_two_temporal_groups_produce_two_clusters() {
    let conn = setup_schema();
    let brain = "test-brain";

    // Group A — recent
    insert_episode_at(&conn, "Episode A1", brain, 1_000_000);
    insert_episode_at(&conn, "Episode A2", brain, 1_000_060);

    // Group B — older
    insert_episode_at(&conn, "Episode B1", brain, 900_000);
    insert_episode_at(&conn, "Episode B2", brain, 900_120);
    insert_episode_at(&conn, "Episode B3", brain, 900_180);

    let episodes = list_episodes(&conn, 20, brain).unwrap();
    assert_eq!(episodes.len(), 5, "all 5 episodes must be in DB");

    let result = consolidate_episodes(episodes, 3600);
    assert_eq!(result.clusters.len(), 2, "expected 2 clusters");
}

/// Each cluster must expose the episode IDs of its members.
#[test]
fn test_clusters_contain_episode_ids() {
    let conn = setup_schema();
    let brain = "test-brain-ids";

    let id1 = insert_episode_at(&conn, "Ep 1", brain, 1_000_000);
    let id2 = insert_episode_at(&conn, "Ep 2", brain, 1_000_060);
    insert_episode_at(&conn, "Ep 3", brain, 900_000);

    let episodes = list_episodes(&conn, 20, brain).unwrap();
    let result = consolidate_episodes(episodes, 3600);

    // After implementation there should be 2 clusters.
    assert!(
        !result.clusters.is_empty(),
        "cluster 0 must contain episode IDs"
    );

    // The recent cluster must contain id1 and id2.
    let recent = result
        .clusters
        .iter()
        .find(|c| c.episode_ids.contains(&id1))
        .expect("must have a cluster containing id1");
    assert!(
        recent.episode_ids.contains(&id2),
        "recent cluster must also contain id2"
    );
}

/// Each cluster must carry the full `SummaryRow` objects so callers can
/// display episode content without additional DB lookups.
#[test]
fn test_clusters_contain_episode_rows() {
    let conn = setup_schema();
    let brain = "test-brain-rows";

    insert_episode_at(&conn, "Row Ep 1", brain, 1_000_000);
    insert_episode_at(&conn, "Row Ep 2", brain, 1_000_030);

    let episodes = list_episodes(&conn, 20, brain).unwrap();
    let result = consolidate_episodes(episodes, 3600);

    assert!(
        !result.clusters.is_empty(),
        "episodes vec must be non-empty"
    );
    let cluster = &result.clusters[0];
    assert!(
        !cluster.episodes.is_empty(),
        "episodes vec in cluster must not be empty"
    );
}

/// Each cluster must provide a non-empty `suggested_title`.
#[test]
fn test_clusters_have_suggested_title() {
    let conn = setup_schema();
    let brain = "test-brain-title";

    insert_episode_at(&conn, "Title Ep 1", brain, 1_000_000);
    insert_episode_at(&conn, "Title Ep 2", brain, 1_000_060);

    let episodes = list_episodes(&conn, 20, brain).unwrap();
    let result = consolidate_episodes(episodes, 3600);

    assert!(
        !result.clusters.is_empty(),
        "suggested_title must be non-empty"
    );
    for cluster in &result.clusters {
        assert!(
            !cluster.suggested_title.is_empty(),
            "suggested_title must be non-empty for every cluster"
        );
    }
}

/// Recent episodes (within `gap_seconds` of each other) must end up in the
/// same cluster, not split across multiple clusters.
#[test]
fn test_recent_episodes_colocated_in_same_cluster() {
    let conn = setup_schema();
    let brain = "test-brain-coloc";

    // Three episodes within a 5-minute window → must be one cluster.
    let id1 = insert_episode_at(&conn, "Recent 1", brain, 1_000_000);
    let id2 = insert_episode_at(&conn, "Recent 2", brain, 1_000_120);
    let id3 = insert_episode_at(&conn, "Recent 3", brain, 1_000_240);

    // One old episode far away.
    insert_episode_at(&conn, "Old 1", brain, 500_000);

    let episodes = list_episodes(&conn, 20, brain).unwrap();
    let result = consolidate_episodes(episodes, 3600);

    assert!(
        !result.clusters.is_empty(),
        "recent episodes must share one cluster"
    );

    let has_shared_cluster = result.clusters.iter().any(|c| {
        c.episode_ids.contains(&id1) && c.episode_ids.contains(&id2) && c.episode_ids.contains(&id3)
    });
    assert!(has_shared_cluster, "recent episodes must share one cluster");
}
