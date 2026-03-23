//! TDD integration test — 1-hop link expansion adds candidates to search results.
//!
//! RED PHASE: The graph expansion integration test asserts behaviour that does
//! not yet exist. The graph expansion logic (`graph_expand: true`) is not
//! implemented in `QueryPipeline::search_ranked`. The expansion-specific
//! assertions will fail until that feature is wired in.
//!
//! Scenario:
//!   A ("quantum computing") --links--> B ("database optimization")
//!   B ("database optimization") --links--> C ("network protocols")
//!   (no direct link A→C)
//!
//! Ranking rationale with MockEmbedder (hash-based, not semantic):
//!   - FTS for "quantum computing" matches A (bm25 > 0); B and C score 0.
//!   - Vector: all files get hash-based vectors; A ranks highest because
//!     query "quantum computing" hashes to a vector most similar to A's
//!     indexed content.
//!   - k=1 packs only the top-1 ranked result (A). Without expansion, B is
//!     not packed. With expansion, B must be included despite ranking below A.
//!
//! NOTE: Because MockEmbedder is hash-based the vector distance between the
//! query and B/C is non-deterministic relative to A. Tests that assert B's
//! ABSENCE rely on FTS being the dominant signal when k=1 (A wins via BM25).
//! The TDD red assertions (`graph_expand: true`) test expansion, not ranking.

use std::sync::Arc;

use tempfile::TempDir;

use brain_lib::db::Db;
use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::metrics::Metrics;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::query_pipeline::{QueryPipeline, SearchParams};
use brain_lib::store::{Store, StoreReader, VectorSearchMode};

// ─── Helpers ────────────────────────────────────────────────────────────────

async fn setup() -> (IndexPipeline, TempDir) {
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

// ─── Tests ──────────────────────────────────────────────────────────────────

/// Verify the graph_expand flag exists on SearchParams and defaults to false.
/// This test must always pass (green) — it validates the struct extension only.
#[test]
fn test_search_params_graph_expand_defaults_to_false() {
    let tags: Vec<String> = vec![];
    let params = SearchParams::new("test query", "lookup", 1000, 5, &tags);
    assert!(
        !params.graph_expand,
        "graph_expand must default to false to preserve existing behaviour"
    );
}

/// Verify the link infrastructure: indexing A (which contains [[b]] wiki-link)
/// stores a link record pointing to B in the SQLite links table.
/// This test must always pass (green) — it validates the link pipeline only.
#[tokio::test]
async fn test_link_a_to_b_is_stored_after_indexing() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "a.md",
        "# Quantum Computing\n\nQuantum computing uses qubits. See [[b]].",
    );
    write_md(
        &notes_dir,
        "b.md",
        "# Database Optimization\n\nDatabase optimization improves query performance.",
    );
    write_md(
        &notes_dir,
        "c.md",
        "# Network Protocols\n\nNetwork protocols define communication rules.",
    );

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // The wiki-link [[b]] in a.md should resolve to target_path = "b"
    let link_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM links WHERE target_path = 'b'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))
        })
        .unwrap();

    assert_eq!(link_count, 1, "a.md must store exactly one link to 'b'");

    // There must be NO direct link from A to C
    let a_to_c_count: i64 = pipeline
        .db()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM links WHERE target_path = 'c'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| brain_lib::error::BrainCoreError::Database(e.to_string()))
        })
        .unwrap();

    assert_eq!(
        a_to_c_count, 0,
        "there must be no link from a.md to c.md — C is 2 hops away"
    );
}

/// RED PHASE — 1-hop graph expansion integration test.
///
/// With `graph_expand: true`, a search for "quantum computing" should include
/// B ("database optimization") in its results because B is linked from A
/// (the direct match). B would NOT appear in a pure vector+FTS search because:
///   - FTS: "quantum computing" does not match B's content
///   - Vector: with k=2 budget and A ranking above B, expansion must supply B
///
/// This test FAILS until `QueryPipeline::search_ranked` implements 1-hop
/// graph expansion that follows outgoing links from top-K candidates and
/// injects the linked chunks into the candidate pool before ranking.
#[tokio::test]
async fn test_1hop_graph_expansion_adds_linked_neighbour_to_results() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // A links to B via wiki-link; B does NOT link to A or C (for this test)
    write_md(
        &notes_dir,
        "a.md",
        "# Quantum Computing\n\nQuantum computing leverages superposition and entanglement. See [[b]].",
    );
    write_md(
        &notes_dir,
        "b.md",
        "# Database Optimization\n\nB-tree indexes accelerate database query performance significantly.",
    );
    write_md(
        &notes_dir,
        "c.md",
        "# Network Protocols\n\nTCP/IP and UDP govern packet transmission across distributed systems.",
    );

    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let store_reader = StoreReader::from_store(pipeline.store());
    let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());
    let qp = QueryPipeline::new(pipeline.db(), &store_reader, &embedder, &metrics);

    // Search WITH graph expansion — this is the RED assertion.
    // Once expansion is implemented, B must appear because A→B link is followed.
    let mut params_expand = SearchParams::new(
        "quantum computing superposition entanglement",
        "lookup",
        5000,
        10,
        &[],
    );
    params_expand.mode = VectorSearchMode::Exact;
    params_expand.graph_expand = true;

    let result_expand = qp.search(&params_expand).await.unwrap();

    let expanded_paths: Vec<&str> = result_expand
        .results
        .iter()
        .map(|r| r.file_path.as_str())
        .collect();

    // A must appear — direct FTS/vector match
    assert!(
        expanded_paths.iter().any(|p| p.contains("a.md")),
        "A (quantum computing) must appear in expanded results: {expanded_paths:?}"
    );

    // B must appear — 1-hop expansion from A via A→B link
    // RED: fails until graph expansion is implemented
    assert!(
        expanded_paths.iter().any(|p| p.contains("b.md")),
        "B (database optimization) must appear via 1-hop expansion from A: {expanded_paths:?}"
    );
}

/// RED PHASE — graph expansion must be 1-hop only: C (2 hops from A via B) must
/// NOT be included when expanding A's links.
///
/// Setup: A links to B. B links to C. A does NOT directly link to C.
/// With `graph_expand: true` on a search for content matching A:
///   - B must appear (1 hop)
///   - C must NOT appear (2 hops — out of scope for 1-hop expansion)
///
/// This test FAILS until 1-hop graph expansion is implemented because currently
/// all three files appear in vector search results for small corpora.
#[tokio::test]
async fn test_1hop_expansion_does_not_follow_2hop_links() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // A links to B; B links to C; A does NOT link to C
    write_md(
        &notes_dir,
        "a.md",
        "# Quantum Computing\n\nQuantum computing leverages superposition and entanglement. See [[b]].",
    );
    write_md(
        &notes_dir,
        "b.md",
        "# Database Optimization\n\nB-tree indexes accelerate query performance. See [[c]].",
    );
    write_md(
        &notes_dir,
        "c.md",
        "# Network Protocols\n\nTCP/IP and UDP govern packet transmission.",
    );

    // Add 60 extra filler files to saturate the vector top-50, pushing C out
    // of the non-expansion candidate pool. This ensures C can only appear via
    // graph expansion (which should only follow 1 hop from A, not reach C).
    for i in 0..60 {
        write_md(
            &notes_dir,
            &format!("filler_{i:02}.md"),
            &format!(
                "# Filler Document {i}\n\nThis document {i} contains miscellaneous content about topic {i}.",
            ),
        );
    }

    pipeline
        .full_scan(std::slice::from_ref(&notes_dir))
        .await
        .unwrap();

    let store_reader = StoreReader::from_store(pipeline.store());
    let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());
    let qp = QueryPipeline::new(pipeline.db(), &store_reader, &embedder, &metrics);

    let mut params = SearchParams::new(
        "quantum computing superposition entanglement",
        "lookup",
        5000,
        10,
        &[],
    );
    params.mode = VectorSearchMode::Exact;
    params.graph_expand = true;

    let result = qp.search(&params).await.unwrap();

    let file_paths: Vec<&str> = result
        .results
        .iter()
        .map(|r| r.file_path.as_str())
        .collect();

    // A must appear (direct match)
    assert!(
        file_paths.iter().any(|p| p.contains("a.md")),
        "A must appear in results: {file_paths:?}"
    );

    // B must appear (1-hop from A)
    // RED: fails until graph expansion is implemented
    assert!(
        file_paths.iter().any(|p| p.contains("b.md")),
        "B must appear via 1-hop expansion from A: {file_paths:?}"
    );

    // C must NOT appear in the top-10 results —
    // C is only reachable via 2 hops (A→B→C), which 1-hop expansion must not follow.
    // With 60 filler files pushing C out of the top-50 vector candidates,
    // and 1-hop expansion only adding B (not C), C must not appear.
    assert!(
        !file_paths.iter().any(|p| p.contains("c.md")),
        "C must NOT appear — it is 2 hops from A and must not be included by 1-hop expansion: {file_paths:?}"
    );
}
