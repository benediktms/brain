//! Near-duplicate detection for reflections and consolidated summaries.
//!
//! Before committing a new reflection, callers can use [`check_duplicate`] to
//! detect whether a semantically identical entry already exists in the store.
//! The check is purely vector-based: it embeds the candidate content and
//! computes cosine similarity against a set of pre-computed embeddings.

use crate::embedder::Embed;
use crate::error::Result;

/// Default cosine similarity threshold above which content is considered a
/// near-duplicate.
pub const DEFAULT_DEDUP_THRESHOLD: f32 = 0.95;

/// A candidate duplicate returned by [`check_duplicate`].
#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateCandidate {
    /// The summary_id of the existing entry that is a near-duplicate.
    pub summary_id: String,
    /// Cosine similarity score in [0.0, 1.0].  Values above
    /// [`DEFAULT_DEDUP_THRESHOLD`] are flagged as duplicates.
    pub similarity: f32,
}

/// Check whether `content` is a near-duplicate of any existing summary.
///
/// # Arguments
///
/// * `content`   — The new content string to evaluate.
/// * `embedder`  — Embedder used to produce the candidate vector.
/// * `existing`  — Slice of `(summary_id, embedding)` pairs to compare against.
/// * `threshold` — Cosine similarity cutoff; use [`DEFAULT_DEDUP_THRESHOLD`].
///
/// # Returns
///
/// `Ok(Some(candidate))` if a near-duplicate is found, `Ok(None)` otherwise.
///
pub fn check_duplicate(
    content: &str,
    embedder: &dyn Embed,
    existing: &[(String, Vec<f32>)],
    threshold: f32,
) -> Result<Option<DuplicateCandidate>> {
    if existing.is_empty() {
        return Ok(None);
    }

    let vectors = embedder.embed_batch(&[content])?;
    let candidate_vec = &vectors[0];

    let mut best: Option<DuplicateCandidate> = None;
    for (summary_id, existing_vec) in existing {
        let sim = cosine_similarity(candidate_vec, existing_vec);
        if sim >= threshold {
            match &best {
                None => {
                    best = Some(DuplicateCandidate {
                        summary_id: summary_id.clone(),
                        similarity: sim,
                    });
                }
                Some(prev) if sim > prev.similarity => {
                    best = Some(DuplicateCandidate {
                        summary_id: summary_id.clone(),
                        similarity: sim,
                    });
                }
                _ => {}
            }
        }
    }

    Ok(best)
}

/// Compute cosine similarity between two L2-normalised vectors.
///
/// Both inputs are expected to be unit-length (as produced by the embedding
/// pipeline). The result is the dot product, clamped to `[-1.0, 1.0]`.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimension mismatch");
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    dot.clamp(-1.0, 1.0)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::embedder::MockEmbedder;

    #[test]
    fn cosine_similarity_identical_unit_vectors() {
        let v = vec![1.0_f32, 0.0, 0.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_orthogonal_vectors() {
        let a = vec![1.0_f32, 0.0, 0.0];
        let b = vec![0.0_f32, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 1e-6);
    }

    #[test]
    fn check_duplicate_identical_content_returns_some() {
        // Verifies the implemented check_duplicate detects identical content.
        let embedder = MockEmbedder;
        let content = "Rust ownership rules prevent data races";
        let vecs = embedder.embed_batch(&[content]).unwrap();
        let existing = vec![("sum-001".to_string(), vecs[0].clone())];

        let result =
            check_duplicate(content, &embedder, &existing, DEFAULT_DEDUP_THRESHOLD).unwrap();
        assert!(result.is_some(), "identical content must be detected as duplicate");
        assert_eq!(result.unwrap().summary_id, "sum-001");
    }
}
