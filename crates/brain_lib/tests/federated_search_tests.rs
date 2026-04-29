//! Integration tests for `FederatedPipeline`.
//!
//! Uses `MockEmbedder` (deterministic hash-based vectors) and tempdir
//! databases. Tests validate search across multiple brain stores.

use std::sync::Arc;

use brain_lib::embedder::MockEmbedder;
use brain_lib::metrics::Metrics;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::query_pipeline::{FederatedPipeline, SearchParams};
use brain_persistence::store::{Store, StoreReader};
use tempfile::TempDir;

// ─── Helpers ────────────────────────────────────────────────────────────────

/// A fully set-up brain: IndexPipeline (for writing) + detached Db/StoreReader
/// (for querying via FederatedPipeline).
struct BrainFixture {
    /// Kept alive to prevent temp directory cleanup.
    _tmp: TempDir,
    db: brain_persistence::db::Db,
    store_reader: StoreReader,
}

async fn setup_brain(notes: &[(&str, &str)]) -> BrainFixture {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("lancedb");
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    for (name, content) in notes {
        std::fs::write(notes_dir.join(name), content).unwrap();
    }

    let db = brain_persistence::db::Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();

    pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Re-open separate handles for the query path
    let db2 = brain_persistence::db::Db::open(&sqlite_path).unwrap();
    let store2 = Store::open_or_create(&lance_path).await.unwrap();
    let store_reader = StoreReader::from_store(&store2);

    BrainFixture {
        _tmp: tmp,
        db: db2,
        store_reader,
    }
}

// ─── 1. Merges results from multiple brains ──────────────────────────────────

#[tokio::test]
async fn test_federated_search_merges_results_from_multiple_brains() {
    let brain_a = setup_brain(&[(
        "rust.md",
        "## Rust async programming\n\nRust async programming with tokio and futures.",
    )])
    .await;

    let brain_b = setup_brain(&[(
        "python.md",
        "## Python data science\n\nPython data science with pandas and numpy.",
    )])
    .await;

    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    // brain_a's Db is the shared Db (FTS covers brain_a content).
    // brain_b contributes via vector search only.
    let pipeline = FederatedPipeline {
        db: &brain_a.db,
        brains: vec![
            (
                "brain-a".to_string(),
                "brain-a-id".to_string(),
                Some(brain_a.store_reader),
            ),
            (
                "brain-b".to_string(),
                "brain-b-id".to_string(),
                Some(brain_b.store_reader),
            ),
        ],
        embedder: &embedder,
        metrics: &metrics,
    };

    // Use a query whose exact text exists in brain-a so MockEmbedder can find it
    let result = pipeline
        .search(
            &SearchParams::new(
                "Rust async programming with tokio and futures.",
                "lookup",
                4000,
                10,
                &[],
            ),
            false,
        )
        .await
        .unwrap();

    // Should have results from at least one brain
    assert!(
        result.num_results > 0,
        "federated search should return results"
    );

    // All results should have brain_name set
    for stub in &result.results {
        assert!(
            stub.brain_name.is_some(),
            "every stub should have brain_name set, got: {:?}",
            stub
        );
    }

    // Collect the brain names from results
    let brain_names: std::collections::HashSet<&str> = result
        .results
        .iter()
        .filter_map(|s| s.brain_name.as_deref())
        .collect();

    // At minimum the local brain should have contributed results
    assert!(
        brain_names.contains("brain-a"),
        "brain-a results expected; got names: {:?}",
        brain_names
    );
}

// ─── 2. Brain name attribution ───────────────────────────────────────────────

#[tokio::test]
async fn test_federated_search_brain_attribution() {
    let brain_a = setup_brain(&[(
        "systems.md",
        "## Systems programming\n\nSystems programming requires low-level control.",
    )])
    .await;

    let brain_b = setup_brain(&[(
        "web.md",
        "## Web development\n\nWeb development with HTML CSS and JavaScript.",
    )])
    .await;

    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    let pipeline = FederatedPipeline {
        db: &brain_a.db,
        brains: vec![
            (
                "systems-brain".to_string(),
                "systems-brain-id".to_string(),
                Some(brain_a.store_reader),
            ),
            (
                "web-brain".to_string(),
                "web-brain-id".to_string(),
                Some(brain_b.store_reader),
            ),
        ],
        embedder: &embedder,
        metrics: &metrics,
    };

    // Query using exact content from brain_a so it definitely hits
    let result = pipeline
        .search(
            &SearchParams::new(
                "Systems programming requires low-level control.",
                "lookup",
                4000,
                10,
                &[],
            ),
            false,
        )
        .await
        .unwrap();

    // Every result must have a brain_name
    for stub in &result.results {
        assert!(
            stub.brain_name.is_some(),
            "stub missing brain_name: {:?}",
            stub
        );
    }

    // brain_name values should only be from the two known brains
    let known_names = ["systems-brain", "web-brain"];
    for stub in &result.results {
        let name = stub.brain_name.as_deref().unwrap();
        assert!(
            known_names.contains(&name),
            "unexpected brain_name '{}' in result",
            name
        );
    }
}

// ─── 3. Single brain fallback (empty brains list with just local) ─────────────

#[tokio::test]
async fn test_federated_search_single_brain_fallback() {
    let brain = setup_brain(&[(
        "solo.md",
        "## Solo brain note\n\nThis content is only in the local brain.",
    )])
    .await;

    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    // Only the local brain — no remotes
    let pipeline = FederatedPipeline {
        db: &brain.db,
        brains: vec![(
            "solo-brain".to_string(),
            "solo-brain-id".to_string(),
            Some(brain.store_reader),
        )],
        embedder: &embedder,
        metrics: &metrics,
    };

    let result = pipeline
        .search(
            &SearchParams::new(
                "This content is only in the local brain.",
                "lookup",
                4000,
                10,
                &[],
            ),
            false,
        )
        .await
        .unwrap();

    assert!(
        result.num_results > 0,
        "single-brain fallback should return results"
    );

    // All results from single-brain should still have brain_name set
    for stub in &result.results {
        assert!(
            stub.brain_name.is_some(),
            "stub should have brain_name even in single-brain mode"
        );
        assert_eq!(
            stub.brain_name.as_deref(),
            Some("solo-brain"),
            "brain_name should match local brain name"
        );
    }
}

// ─── 4. Respects token budget ─────────────────────────────────────────────────

#[tokio::test]
async fn test_federated_search_respects_budget() {
    // Create a brain with many chunks
    let notes: Vec<(String, String)> = (0..10)
        .map(|i| {
            (
                format!("note{i}.md"),
                format!(
                    "## Note {i}\n\nThis is note number {i} with some content about topic {i}. \
                     It contains enough words to count as meaningful content for token budgeting."
                ),
            )
        })
        .collect();

    let note_refs: Vec<(&str, &str)> = notes
        .iter()
        .map(|(n, c)| (n.as_str(), c.as_str()))
        .collect();

    let brain_a = setup_brain(&note_refs).await;

    let brain_b_notes: Vec<(String, String)> = (0..10)
        .map(|i| {
            (
                format!("other{i}.md"),
                format!(
                    "## Other {i}\n\nThis is other note {i} with different content about subject {i}. \
                     This also has enough words to be significant for token budget testing."
                ),
            )
        })
        .collect();

    let brain_b_refs: Vec<(&str, &str)> = brain_b_notes
        .iter()
        .map(|(n, c)| (n.as_str(), c.as_str()))
        .collect();

    let brain_b = setup_brain(&brain_b_refs).await;

    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    let tight_budget = 200; // Very tight budget

    let pipeline = FederatedPipeline {
        db: &brain_a.db,
        brains: vec![
            (
                "brain-a".to_string(),
                "brain-a-id".to_string(),
                Some(brain_a.store_reader),
            ),
            (
                "brain-b".to_string(),
                "brain-b-id".to_string(),
                Some(brain_b.store_reader),
            ),
        ],
        embedder: &embedder,
        metrics: &metrics,
    };

    let result = pipeline
        .search(
            &SearchParams::new("note content topic", "lookup", tight_budget, 20, &[]),
            false,
        )
        .await
        .unwrap();

    assert!(
        result.used_tokens_est <= tight_budget,
        "federated search must not exceed token budget: {} > {}",
        result.used_tokens_est,
        tight_budget
    );
}

// ─── 5. Missing brain (store=None) is skipped gracefully ─────────────────────

#[tokio::test]
async fn test_federated_search_skips_brain_without_lancedb() {
    let brain_a = setup_brain(&[(
        "valid.md",
        "## Valid brain content\n\nThis brain has a proper LanceDB store.",
    )])
    .await;

    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    let pipeline = FederatedPipeline {
        db: &brain_a.db,
        brains: vec![
            (
                "valid-brain".to_string(),
                "valid-brain-id".to_string(),
                Some(brain_a.store_reader),
            ),
            (
                "no-store-brain".to_string(),
                "no-store-brain-id".to_string(),
                None,
            ), // LanceDB not initialised
        ],
        embedder: &embedder,
        metrics: &metrics,
    };

    // Should not panic or error — just skip the brain without a store
    let result = pipeline
        .search(
            &SearchParams::new(
                "This brain has a proper LanceDB store.",
                "lookup",
                4000,
                10,
                &[],
            ),
            false,
        )
        .await;

    assert!(
        result.is_ok(),
        "federated search should not fail when a remote brain has no store"
    );

    // Local brain should still contribute results
    let result = result.unwrap();
    assert!(
        result.num_results > 0,
        "valid local brain should still return results"
    );

    // No results should be attributed to the no-store brain
    let no_store_results: Vec<_> = result
        .results
        .iter()
        .filter(|s| s.brain_name.as_deref() == Some("no-store-brain"))
        .collect();
    assert!(
        no_store_results.is_empty(),
        "brain without store should contribute no results"
    );
}

// ─── 6. Ranks by hybrid_score across brains ──────────────────────────────────

#[tokio::test]
async fn test_federated_search_ranks_by_hybrid_score() {
    // brain-a has content that exactly matches the query (high relevance)
    let brain_a = setup_brain(&[(
        "exact.md",
        "## Rust memory safety\n\nRust memory safety is guaranteed by the borrow checker.",
    )])
    .await;

    // brain-b has completely unrelated content (low relevance)
    let brain_b = setup_brain(&[(
        "unrelated.md",
        "## Cooking recipes\n\nBoil water add pasta and cook for eight minutes.",
    )])
    .await;

    let embedder: Arc<dyn brain_lib::embedder::Embed> = Arc::new(MockEmbedder);
    let metrics = Arc::new(Metrics::new());

    let pipeline = FederatedPipeline {
        db: &brain_a.db,
        brains: vec![
            (
                "brain-a".to_string(),
                "brain-a-id".to_string(),
                Some(brain_a.store_reader),
            ),
            (
                "brain-b".to_string(),
                "brain-b-id".to_string(),
                Some(brain_b.store_reader),
            ),
        ],
        embedder: &embedder,
        metrics: &metrics,
    };

    // Query that exactly matches brain-a content
    let result = pipeline
        .search(
            &SearchParams::new(
                "Rust memory safety is guaranteed by the borrow checker.",
                "lookup",
                4000,
                10,
                &[],
            ),
            false,
        )
        .await
        .unwrap();

    assert!(result.num_results > 0, "should return at least one result");

    // Results should be sorted by hybrid_score descending
    let scores: Vec<f64> = result.results.iter().map(|s| s.hybrid_score).collect();
    for window in scores.windows(2) {
        assert!(
            window[0] >= window[1],
            "results should be sorted by hybrid_score descending: {:?}",
            scores
        );
    }
}
