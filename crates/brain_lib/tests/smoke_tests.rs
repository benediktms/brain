//! POC smoke tests: validate the core pipeline end-to-end.
//!
//! These tests use MockEmbedder (deterministic BLAKE3 hash-based 384-dim vectors)
//! so they run without model weights and are safe for CI.
//!
//! **Semantic relevance limitation**: MockEmbedder produces vectors based on
//! exact text hashes, not learned semantics. The round-trip tests prove the
//! pipeline plumbing works (scan → chunk → embed → store → query) via identity
//! matching (query with exact chunk text → distance ≈ 0). They do NOT validate
//! that semantically similar but differently-worded queries return relevant
//! results. True semantic relevance testing requires a golden database built
//! with real model weights — see brain-0de.13 for CI integration plans.

use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::embedder::MockEmbedder;
use brain_lib::prelude::*;
use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────

/// Create a pipeline with MockEmbedder in a fresh temp directory.
async fn setup() -> (IndexPipeline, TempDir) {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    let pipeline = IndexPipeline::with_embedder(db, store, embedder);
    (pipeline, tmp)
}

/// Write a markdown file and return its path.
fn write_md(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

// ─── 1. Embedder: output shape and unit norm ────────────────────

#[test]
fn test_mock_embedder_shape_and_norm() {
    let embedder = MockEmbedder;
    let vectors = embedder
        .embed_batch(&["The quick brown fox jumps over the lazy dog."])
        .unwrap();

    // Single input → single output vector
    assert_eq!(vectors.len(), 1);
    // Hidden size = 384
    assert_eq!(vectors[0].len(), embedder.hidden_size());
    assert_eq!(vectors[0].len(), 384);

    // L2 norm should be ≈ 1.0
    let norm: f32 = vectors[0].iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!(
        (1.0 - norm).abs() < 1e-5,
        "expected unit vector, got norm={norm}"
    );
}

#[test]
fn test_mock_embedder_batch_shape() {
    let embedder = MockEmbedder;
    let texts = &["first sentence", "second sentence", "third sentence"];
    let vectors = embedder.embed_batch(texts).unwrap();

    assert_eq!(vectors.len(), 3);
    for (i, v) in vectors.iter().enumerate() {
        assert_eq!(v.len(), 384, "vector {i} has wrong dimension");
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (1.0 - norm).abs() < 1e-5,
            "vector {i}: expected unit vector, got norm={norm}"
        );
    }
}

#[test]
fn test_mock_embedder_empty_batch() {
    let embedder = MockEmbedder;
    let vectors = embedder.embed_batch(&[]).unwrap();
    assert!(vectors.is_empty());
}

// ─── 2. Embedder: consistency across calls ──────────────────────

#[test]
fn test_mock_embedder_deterministic() {
    let embedder = MockEmbedder;
    let text = "Determinism is essential for reproducible tests.";

    let v1 = embedder.embed_batch(&[text]).unwrap();
    let v2 = embedder.embed_batch(&[text]).unwrap();

    assert_eq!(v1.len(), 1);
    assert_eq!(v2.len(), 1);
    assert_eq!(v1[0], v2[0], "same input must produce identical embeddings");
}

#[test]
fn test_mock_embedder_different_inputs_differ() {
    let embedder = MockEmbedder;
    let v1 = embedder.embed_batch(&["alpha"]).unwrap();
    let v2 = embedder.embed_batch(&["beta"]).unwrap();
    assert_ne!(
        v1[0], v2[0],
        "different inputs should produce different embeddings"
    );
}

// ─── 3. Chunker: known markdown → expected chunks ───────────────

#[test]
fn test_chunker_paragraph_splits() {
    let md = "First paragraph about local-first software.\n\n\
              Second paragraph about vector embeddings.\n\n\
              Third paragraph about semantic search.";

    let chunks = chunk_text(md);
    // Heading-aware chunker: no headings → single root section → one chunk
    // (all 3 paragraphs fit within the 400-token limit)
    assert_eq!(chunks.len(), 1, "no headings, small content → one chunk");
    assert_eq!(chunks[0].ord, 0);
    assert!(!chunks[0].content.is_empty());
}

#[test]
fn test_chunker_heading_splits() {
    let md = "# Chapter One\n\nIntro paragraph.\n\n\
              ## Section A\n\nSection A content.\n\n\
              ## Section B\n\nSection B content.";

    let chunks = chunk_text(md);
    // Heading-aware chunker: 3 heading sections → 3 chunks (one per section)
    assert_eq!(
        chunks.len(),
        3,
        "expected 3 chunks from heading-separated content, got {}",
        chunks.len()
    );
}

#[test]
fn test_chunker_empty_input() {
    let chunks = chunk_text("");
    assert!(chunks.is_empty(), "empty input should produce zero chunks");
}

#[test]
fn test_chunker_whitespace_only() {
    let chunks = chunk_text("   \n  \n\n  ");
    assert!(
        chunks.is_empty(),
        "whitespace-only input should produce zero chunks"
    );
}

#[test]
fn test_chunker_fixture_simple() {
    let content = include_str!("../../../tests/fixtures/simple.md");
    let chunks = chunk_text(content);

    // simple.md has no headings — heading-aware chunker produces a single root section.
    // All 3 paragraphs (~250 tokens) fit within the 400-token limit → 1 chunk.
    assert_eq!(
        chunks.len(),
        1,
        "simple.md (no headings) should produce 1 chunk, got {}",
        chunks.len()
    );

    for chunk in &chunks {
        assert!(!chunk.content.trim().is_empty(), "no chunk should be empty");
    }
}

#[test]
fn test_chunker_fixture_headings() {
    let content = include_str!("../../../tests/fixtures/headings.md");
    let chunks = chunk_text(content);

    // headings.md has 11 heading sections. The heading-aware chunker produces one
    // chunk per section with non-empty content. "Practical Considerations" has no
    // body text (only child headings) → skipped. Result: 10 chunks.
    assert_eq!(
        chunks.len(),
        10,
        "headings.md should produce 10 chunks, got {}",
        chunks.len()
    );
}

#[test]
fn test_chunker_fixture_large() {
    let content = include_str!("../../../tests/fixtures/large.md");
    let chunks = chunk_text(content);

    // large.md is a 31KB document — should produce many chunks
    assert!(
        chunks.len() > 50,
        "large.md should produce many chunks, got {}",
        chunks.len()
    );

    // Every chunk must respect MAX_CHUNK_CHARS (2000) and be non-empty
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(
            !chunk.content.trim().is_empty(),
            "chunk {i} should not be empty"
        );
        assert!(
            chunk.content.len() <= 2000,
            "chunk {i} exceeds MAX_CHUNK_CHARS: {} chars",
            chunk.content.len()
        );
    }
}

#[test]
fn test_chunker_fixture_empty() {
    let content = include_str!("../../../tests/fixtures/empty.md");
    let chunks = chunk_text(content);
    assert!(chunks.is_empty(), "empty.md should produce zero chunks");
}

// ─── 4. Round-trip: index → query ───────────────────────────────

#[tokio::test]
async fn test_roundtrip_index_then_query() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Three files with distinct topics
    write_md(
        &notes_dir,
        "cooking.md",
        "# Pasta Recipe\n\nBoil water, add salt, cook spaghetti for 8 minutes.",
    );
    write_md(
        &notes_dir,
        "embeddings.md",
        "# Vector Embeddings\n\nTransformer models encode text into dense numerical vectors.",
    );
    write_md(
        &notes_dir,
        "gardening.md",
        "# Growing Tomatoes\n\nPlant seedlings in spring and water regularly.",
    );

    // Index all three files
    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats.indexed, 3, "all 3 files should be indexed");
    assert_eq!(stats.errors, 0, "no errors during indexing");

    // Identity-match test: query with exact chunk text. MockEmbedder is hash-based
    // so identical text → identical vector → dot-product distance ≈ 0.0.
    // This validates pipeline plumbing, not semantic relevance (see module doc).
    let embedder = MockEmbedder;
    let query_text = "Boil water, add salt, cook spaghetti for 8 minutes.";
    let query_vec = embedder.embed_batch(&[query_text]).unwrap();

    // Open a fresh Store handle for querying (same LanceDB directory)
    let store = Store::open_or_create(&tmp.path().join("brain_lancedb"))
        .await
        .unwrap();
    let results = store.query(&query_vec[0], 5).await.unwrap();

    assert!(!results.is_empty(), "query should return results");

    // The top result should be the cooking file's chunk
    let top = &results[0];
    assert!(
        top.file_path.ends_with("cooking.md"),
        "expected top result from cooking.md, got: {}",
        top.file_path
    );
    assert!(
        top.content.contains("spaghetti"),
        "top result should contain the query text"
    );

    // With identical embedding, distance should be near zero
    assert!(top.score.is_some(), "expected distance score from LanceDB");
    let score = top.score.unwrap();
    assert!(
        score < 0.01,
        "exact-text query should have near-zero distance, got {score}"
    );
}

#[tokio::test]
async fn test_roundtrip_with_fixtures() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Copy fixture files into temp notes directory
    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");

    for entry in std::fs::read_dir(&fixtures_dir).unwrap() {
        let entry = entry.unwrap();
        let src = entry.path();
        if src.extension().is_some_and(|e| e == "md") {
            let dst = notes_dir.join(entry.file_name());
            std::fs::copy(&src, &dst).unwrap();
        }
    }

    // Index all fixture files
    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    // All 9 fixtures are indexed (empty.md produces no chunks but is still "indexed")
    assert_eq!(stats.indexed, 9, "all 9 fixtures should be indexed");
    assert_eq!(stats.errors, 0, "no errors during fixture indexing");

    // Identity-match test: query with exact chunk text (see module doc for
    // semantic relevance limitation). Use the body of "### Cosine Similarity"
    // from headings.md which becomes a single chunk under heading-aware splitting.
    let query_text = "Cosine similarity measures the angle between two vectors, ignoring their magnitude. It ranges from -1 (opposite) to 1 (identical direction). For normalized vectors (unit length), cosine similarity equals the dot product, which is cheaper to compute.";
    let embedder = MockEmbedder;
    let query_vec = embedder.embed_batch(&[query_text]).unwrap();

    let store = Store::open_or_create(&tmp.path().join("brain_lancedb"))
        .await
        .unwrap();
    let results = store.query(&query_vec[0], 10).await.unwrap();

    assert!(
        !results.is_empty(),
        "query over fixtures should return results"
    );

    // The top result should come from headings.md (exact chunk text → identical vector)
    let top = &results[0];
    assert!(
        top.file_path.ends_with("headings.md"),
        "expected top result from headings.md, got: {}",
        top.file_path
    );
    // Exact match → near-zero distance
    assert!(top.score.is_some(), "expected distance score from LanceDB");
    let score = top.score.unwrap();
    assert!(
        score < 0.01,
        "exact chunk text query should have near-zero distance, got {score}"
    );
}

// ─── 5. Optimize scheduler: force_optimize resets counter ────────

#[tokio::test]
async fn test_force_optimize_resets_counter() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(
        &notes_dir,
        "opt_test.md",
        "# Optimize Test\n\nSome content to create fragments in LanceDB.",
    );

    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats.indexed, 1);

    // After indexing, the optimizer should have pending mutations
    let pending = pipeline.store().optimizer().pending_count();
    assert!(
        pending > 0,
        "expected pending mutations after upsert, got 0"
    );

    // Force optimize should compact and reset the counter
    pipeline.store().optimizer().force_optimize().await;

    let after = pipeline.store().optimizer().pending_count();
    assert_eq!(
        after, 0,
        "expected 0 pending after force_optimize, got {after}"
    );
}

// ─── 6. Empty vault: no crash ───────────────────────────────────

#[tokio::test]
async fn test_empty_vault_no_crash() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("empty_vault");
    std::fs::create_dir_all(&notes_dir).unwrap();

    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    assert_eq!(stats.indexed, 0);
    assert_eq!(stats.skipped, 0);
    assert_eq!(stats.deleted, 0);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn test_empty_vault_query_returns_nothing() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("empty_vault");
    std::fs::create_dir_all(&notes_dir).unwrap();

    // Index empty vault
    let _ = pipeline.full_scan(&[notes_dir]).await.unwrap();

    // Query against empty index
    let embedder = MockEmbedder;
    let query_vec = embedder.embed_batch(&["anything"]).unwrap();

    let store = Store::open_or_create(&tmp.path().join("brain_lancedb"))
        .await
        .unwrap();
    let results = store.query(&query_vec[0], 5).await.unwrap();
    assert!(results.is_empty(), "empty index should return no results");
}

#[tokio::test]
async fn test_vault_with_only_empty_files() {
    let (pipeline, tmp) = setup().await;
    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).unwrap();

    write_md(&notes_dir, "blank1.md", "");
    write_md(&notes_dir, "blank2.md", "   \n\n   ");

    let stats = pipeline.full_scan(&[notes_dir]).await.unwrap();
    // Empty files are still "indexed" (state tracked) but produce no chunks
    assert_eq!(stats.indexed, 2);
    assert_eq!(stats.errors, 0);
}
