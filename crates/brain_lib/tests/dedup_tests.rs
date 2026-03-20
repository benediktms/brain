//! Integration tests for near-duplicate detection via vector similarity.
//!
//! # TDD status: RED
//!
//! [`brain_lib::dedup::check_duplicate`] is currently a stub that always
//! returns `None`.  The tests below assert `Some(..)` for the duplicate path
//! and `None` for the non-duplicate path.  They will:
//!
//! - **Fail** (`duplicate_detected` and `returns_correct_summary_id`) until
//!   the stub is replaced with real cosine-similarity logic.
//! - **Pass** (`no_duplicate_for_different_content`) immediately because the
//!   stub already returns `None`.
//!
//! # MockEmbedder behaviour
//!
//! `MockEmbedder` uses BLAKE3 hash-based deterministic embeddings.  Identical
//! strings produce identical unit vectors (cosine similarity = 1.0), while
//! strings with any difference produce very different vectors (near-zero
//! similarity) due to the avalanche effect of BLAKE3.  This makes it suitable
//! for TDD: use identical content to exercise the duplicate path, and
//! unrelated content for the non-duplicate path.

use brain_lib::dedup::{DEFAULT_DEDUP_THRESHOLD, DuplicateCandidate, check_duplicate};
use brain_lib::embedder::{Embed, MockEmbedder};

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Embed a single string with MockEmbedder and return the vector.
fn embed(text: &str) -> Vec<f32> {
    MockEmbedder
        .embed_batch(&[text])
        .expect("MockEmbedder must not fail")
        .into_iter()
        .next()
        .expect("embed_batch returned empty vec")
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// When the candidate content is identical to an existing entry, the dedup
/// check should detect it as a near-duplicate.
///
/// **STATUS: RED** — fails until `check_duplicate` is implemented.
#[test]
fn duplicate_detected_for_identical_content() {
    let content = "Rust ownership rules prevent data races";
    let embedder = MockEmbedder;

    // Simulate an existing summary stored with the same content.
    let existing_embedding = embed(content);
    let existing = vec![("sum-001".to_string(), existing_embedding)];

    let result = check_duplicate(content, &embedder, &existing, DEFAULT_DEDUP_THRESHOLD)
        .expect("check_duplicate must not error");

    // RED: stub returns None; implementation must return Some(..).
    assert!(
        result.is_some(),
        "expected a duplicate candidate for identical content, got None"
    );

    let candidate = result.unwrap();
    assert_eq!(
        candidate.summary_id, "sum-001",
        "duplicate should reference the stored summary_id"
    );
    assert!(
        candidate.similarity >= DEFAULT_DEDUP_THRESHOLD,
        "similarity {:.4} is below threshold {:.4}",
        candidate.similarity,
        DEFAULT_DEDUP_THRESHOLD
    );
}

/// Confirm the returned `DuplicateCandidate` carries the correct `summary_id`
/// when multiple existing entries are present.
///
/// **STATUS: RED** — fails until `check_duplicate` is implemented.
#[test]
fn returns_correct_summary_id_among_multiple_existing() {
    let content = "Rust ownership rules prevent data races";
    let embedder = MockEmbedder;

    // Three existing summaries; only "sum-exact" is identical to `content`.
    let existing = vec![
        (
            "sum-unrelated-a".to_string(),
            embed("A completely different topic about networking protocols"),
        ),
        ("sum-exact".to_string(), embed(content)),
        (
            "sum-unrelated-b".to_string(),
            embed("Database indexing and query optimisation strategies"),
        ),
    ];

    let result = check_duplicate(content, &embedder, &existing, DEFAULT_DEDUP_THRESHOLD)
        .expect("check_duplicate must not error");

    // RED: stub returns None; implementation must return Some(..).
    assert!(
        result.is_some(),
        "expected a duplicate candidate, got None"
    );
    assert_eq!(
        result.unwrap().summary_id,
        "sum-exact",
        "should identify the matching summary"
    );
}

/// When the candidate content is clearly different from all existing entries,
/// `check_duplicate` must return `None`.
///
/// **STATUS: GREEN** — the stub returns `None` unconditionally, so this test
/// already passes.  It will continue to pass once the implementation is in
/// place because BLAKE3-hashed vectors for different strings have very low
/// cosine similarity (well below the 0.95 threshold).
#[test]
fn no_duplicate_for_different_content() {
    let content = "Async runtimes handle I/O concurrency with an event loop";
    let embedder = MockEmbedder;

    // Existing entries contain unrelated content.
    let existing = vec![
        (
            "sum-001".to_string(),
            embed("Rust ownership rules prevent data races"),
        ),
        (
            "sum-002".to_string(),
            embed("The borrow checker enforces lifetime rules at compile time"),
        ),
    ];

    let result = check_duplicate(content, &embedder, &existing, DEFAULT_DEDUP_THRESHOLD)
        .expect("check_duplicate must not error");

    assert!(
        result.is_none(),
        "expected no duplicate for unrelated content, got {:?}",
        result
    );
}

/// Edge case: when the existing list is empty, no duplicate can be found.
#[test]
fn no_duplicate_when_existing_is_empty() {
    let content = "Rust ownership rules prevent data races";
    let embedder = MockEmbedder;

    let result = check_duplicate(content, &embedder, &[], DEFAULT_DEDUP_THRESHOLD)
        .expect("check_duplicate must not error");

    assert!(
        result.is_none(),
        "empty existing list must never yield a duplicate"
    );
}

/// Verify that `DuplicateCandidate` carries both required fields.
#[test]
fn duplicate_candidate_fields_accessible() {
    let candidate = DuplicateCandidate {
        summary_id: "sum-xyz".to_string(),
        similarity: 0.99,
    };
    assert_eq!(candidate.summary_id, "sum-xyz");
    assert!((candidate.similarity - 0.99).abs() < 1e-6);
}
