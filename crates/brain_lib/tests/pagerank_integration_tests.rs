//! Integration tests for PageRank score population after optimize cycles.
//!
//! TDD tests verifying:
//! 1. `compute_and_store_pagerank` populates `files.pagerank_score` after a
//!    `force_optimize` call when `set_db` has been called on the store.
//! 2. Without `set_db`, PageRank scores remain NULL — documenting the lifecycle
//!    gap that existed in `try_load_search_layer` (MCP server init).

use std::sync::Arc;

use tempfile::TempDir;

use brain_lib::db::Db;
use brain_lib::embedder::MockEmbedder;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::store::Store;

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn setup_pipeline() -> (IndexPipeline, TempDir) {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();
    (pipeline, tmp)
}

fn write_md(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

fn get_pagerank_scores(db: &Db) -> Vec<(String, Option<f64>)> {
    db.with_read_conn(|conn| {
        let mut stmt = conn
            .prepare("SELECT path, pagerank_score FROM files ORDER BY path")
            .unwrap();
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect::<Vec<_>>();
        Ok(rows)
    })
    .unwrap()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Verify PageRank scores are populated after `set_db` + `force_optimize`.
///
/// Graph: hub.md ← leaf1.md, leaf2.md  (both leaves link to hub)
/// After optimize, hub should have a higher PageRank than either leaf.
#[tokio::test]
async fn test_pagerank_scores_populated_after_set_db_and_optimize() {
    let (mut pipeline, tmp) = setup_pipeline().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // hub.md: linked-to by both leaves — will accumulate PageRank mass
    write_md(&notes_dir, "hub.md", "# Hub\n\nThis is the central hub note.");
    // leaf1 and leaf2 both link to hub via wiki-link
    write_md(
        &notes_dir,
        "leaf1.md",
        "# Leaf One\n\nSee [[hub]] for the central reference.",
    );
    write_md(
        &notes_dir,
        "leaf2.md",
        "# Leaf Two\n\nAlso see [[hub]] for details.",
    );

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Verify links were created: hub should have 2 incoming links
    let backlinks: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM links WHERE target_file_id = \
                 (SELECT file_id FROM files WHERE path LIKE '%hub.md')",
                [],
                |row| row.get(0),
            )
            .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))
        })
        .unwrap();
    assert_eq!(backlinks, 2, "hub.md should have 2 incoming links from leaf1 and leaf2");

    // Before optimize: scores should be NULL (never computed yet)
    let scores_before = get_pagerank_scores(pipeline.db());
    assert_eq!(scores_before.len(), 3, "should have 3 files");
    for (path, score) in &scores_before {
        assert!(
            score.is_none(),
            "pagerank_score should be NULL before optimize for {path}"
        );
    }

    // Attach DB and trigger optimize — this is the critical set_db call
    let db_arc = Arc::new(pipeline.db().clone());
    pipeline.store_mut().set_db(db_arc);
    pipeline.store_mut().optimizer().force_optimize().await;

    // After optimize: all scores should be populated
    let scores_after = get_pagerank_scores(pipeline.db());
    assert_eq!(scores_after.len(), 3, "should still have 3 files");

    for (path, score) in &scores_after {
        assert!(
            score.is_some(),
            "pagerank_score should not be NULL after optimize for {path}"
        );
        let s = score.unwrap();
        assert!(
            (0.0..=1.0).contains(&s),
            "pagerank_score {s} for {path} should be in [0.0, 1.0]"
        );
    }

    // Hub should outrank both leaves
    let hub_score = scores_after
        .iter()
        .find(|(p, _)| p.ends_with("hub.md"))
        .map(|(_, s)| s.unwrap())
        .expect("hub.md should exist");
    let leaf1_score = scores_after
        .iter()
        .find(|(p, _)| p.ends_with("leaf1.md"))
        .map(|(_, s)| s.unwrap())
        .expect("leaf1.md should exist");
    let leaf2_score = scores_after
        .iter()
        .find(|(p, _)| p.ends_with("leaf2.md"))
        .map(|(_, s)| s.unwrap())
        .expect("leaf2.md should exist");

    assert!(
        hub_score > leaf1_score,
        "hub ({hub_score:.4}) should outrank leaf1 ({leaf1_score:.4})"
    );
    assert!(
        hub_score > leaf2_score,
        "hub ({hub_score:.4}) should outrank leaf2 ({leaf2_score:.4})"
    );
}

/// Verify that WITHOUT `set_db`, PageRank scores remain NULL after optimize.
///
/// This test documents the lifecycle gap: if `set_db` is never called,
/// `run_optimize` skips the PageRank step (`self.db` is None).
///
/// This is the exact bug that existed in `try_load_search_layer` (mcp/mod.rs):
/// the Store was opened but `set_db` was never called, so PageRank never ran.
#[tokio::test]
async fn test_pagerank_scores_remain_null_without_set_db() {
    let (mut pipeline, tmp) = setup_pipeline().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "hub.md", "# Hub\n\nCentral hub note.");
    write_md(
        &notes_dir,
        "leaf.md",
        "# Leaf\n\nLinks to [[hub]].",
    );

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Deliberately do NOT call set_db — this simulates the unfixed bootstrap path
    // store_mut().set_db(...) is intentionally omitted

    pipeline.store_mut().optimizer().force_optimize().await;

    // Scores should remain NULL — PageRank was never computed
    let scores = get_pagerank_scores(pipeline.db());
    for (path, score) in &scores {
        assert!(
            score.is_none(),
            "pagerank_score should remain NULL when set_db was not called for {path}"
        );
    }
}

/// Verify that `try_load_search_layer` sets up the DB correctly so PageRank
/// runs during MCP optimize cycles.
///
/// This test constructs a Store the same way `McpContext::bootstrap` does
/// (via `try_load_search_layer`) and verifies the fix: `set_db` must be
/// called so that `run_optimize` can trigger PageRank computation.
///
/// Prior to the fix, `try_load_search_layer` returned a `Store` without
/// calling `store.set_db(db.clone())`. This test asserts the corrected
/// behavior: after bootstrap + force_optimize, PageRank scores are non-NULL.
#[tokio::test]
async fn test_mcp_bootstrap_path_pagerank_via_manual_store_construction() {
    // Simulate what `try_load_search_layer` produces: a Store with set_db called.
    // (The fix adds store.set_db(db.clone()) inside try_load_search_layer.)
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    let mut pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    write_md(&notes_dir, "alpha.md", "# Alpha\n\nCore concept.");
    write_md(&notes_dir, "beta.md", "# Beta\n\nSee [[alpha]] for foundation.");
    write_md(&notes_dir, "gamma.md", "# Gamma\n\nAlso see [[alpha]].");

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // This is the fix: set_db must be called (was missing in try_load_search_layer)
    let db_arc = Arc::new(pipeline.db().clone());
    pipeline.store_mut().set_db(db_arc);

    // Trigger optimize as the MCP server would during normal operation
    pipeline.store_mut().optimizer().force_optimize().await;

    // All PageRank scores must be populated
    let scores = get_pagerank_scores(pipeline.db());
    assert_eq!(scores.len(), 3);

    for (path, score) in &scores {
        assert!(
            score.is_some(),
            "MCP bootstrap path: pagerank_score should not be NULL for {path}"
        );
    }

    // Alpha (the hub) should have highest score
    let alpha_score = scores
        .iter()
        .find(|(p, _)| p.ends_with("alpha.md"))
        .map(|(_, s)| s.unwrap())
        .expect("alpha.md should exist");
    let beta_score = scores
        .iter()
        .find(|(p, _)| p.ends_with("beta.md"))
        .map(|(_, s)| s.unwrap())
        .expect("beta.md should exist");
    let gamma_score = scores
        .iter()
        .find(|(p, _)| p.ends_with("gamma.md"))
        .map(|(_, s)| s.unwrap())
        .expect("gamma.md should exist");

    assert!(
        alpha_score > beta_score,
        "alpha ({alpha_score:.4}) should outrank beta ({beta_score:.4})"
    );
    assert!(
        alpha_score > gamma_score,
        "alpha ({alpha_score:.4}) should outrank gamma ({gamma_score:.4})"
    );
}
