//! LOD resolution for ranked search results.
//!
//! Maps a batch of [`RankedResult`] values to resolved content at the
//! requested [`LodLevel`], with fallback logic and background enqueue.

use tracing::warn;

use crate::lod::{LodChunkStore, LodLevel};
use crate::ranking::RankedResult;
use crate::retrieval::derive_kind;
use crate::uri::SynapseUri;
use brain_persistence::db::Db;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The resolved content and metadata for a single ranked result.
#[derive(Debug, Clone)]
pub struct LodResolution {
    /// Resolved content (may be L0, L1, or L2 passthrough depending on availability).
    pub content: String,
    /// The actual LOD level that was served.
    pub actual_lod: LodLevel,
    /// Whether the served chunk is fresh (source hash matches current content).
    pub lod_fresh: bool,
    /// ISO-8601 string when the LOD chunk was generated, if applicable.
    pub generated_at: Option<String>,
    /// Whether an L1 summarization job was enqueued during this resolution.
    pub enqueued: bool,
}

/// Aggregate diagnostic counts for a batch LOD resolution.
#[derive(Debug, Clone, Default)]
pub struct LodDiagnostics {
    pub lod_hits: usize,
    pub lod_misses: usize,
    pub lod_generation_enqueued: usize,
}

// ---------------------------------------------------------------------------
// build_object_uri helper
// ---------------------------------------------------------------------------

fn build_object_uri(ranked: &RankedResult, brain_name: &str) -> String {
    let kind = derive_kind(&ranked.chunk_id, ranked.summary_kind.as_deref());
    let uri = match kind.as_str() {
        "episode" => SynapseUri::for_episode(brain_name, &ranked.chunk_id),
        "reflection" => SynapseUri::for_reflection(brain_name, &ranked.chunk_id),
        "procedure" => SynapseUri::for_procedure(brain_name, &ranked.chunk_id),
        "record" => SynapseUri::for_record(brain_name, &ranked.chunk_id),
        "task" | "task-outcome" => SynapseUri::for_task(brain_name, &ranked.chunk_id),
        _ => SynapseUri::for_memory(brain_name, &ranked.chunk_id),
    };
    uri.to_string()
}

// ---------------------------------------------------------------------------
// Core resolution logic
// ---------------------------------------------------------------------------

/// Resolve LOD content for a batch of ranked results.
///
/// - `db` — implements [`LodChunkStore`] via the ports layer.
/// - `ranked` — ordered ranked results from the query pipeline.
/// - `requested_lod` — desired detail level (L0, L1, or L2).
/// - `brain_name` — used for URI construction.
/// - `brain_id` — used when enqueuing L1 jobs.
///
/// Returns a `Vec<LodResolution>` in the same order as `ranked`, plus
/// aggregate [`LodDiagnostics`].
pub fn resolve_lod_batch(
    db: &Db,
    ranked: &[RankedResult],
    requested_lod: LodLevel,
    brain_name: &str,
    brain_id: &str,
) -> (Vec<LodResolution>, LodDiagnostics) {
    let mut resolutions = Vec::with_capacity(ranked.len());
    let mut diag = LodDiagnostics::default();

    for result in ranked {
        let resolution = resolve_single(db, result, requested_lod, brain_name, brain_id, &mut diag);
        resolutions.push(resolution);
    }

    (resolutions, diag)
}

fn resolve_single(
    db: &Db,
    ranked: &RankedResult,
    requested_lod: LodLevel,
    brain_name: &str,
    brain_id: &str,
    diag: &mut LodDiagnostics,
) -> LodResolution {
    // Use the trait explicitly to avoid name collision with Db's own get_lod_chunk method.
    let store: &dyn LodChunkStore = db;

    match requested_lod {
        LodLevel::L2 => {
            // Passthrough — always a hit, no DB lookup needed.
            diag.lod_hits += 1;
            LodResolution {
                content: ranked.content.clone(),
                actual_lod: LodLevel::L2,
                lod_fresh: true,
                generated_at: None,
                enqueued: false,
            }
        }

        LodLevel::L0 => {
            let uri = build_object_uri(ranked, brain_name);
            let source_hash = crate::utils::content_hash(&ranked.content);

            match store.get_lod_chunk(&uri, LodLevel::L0) {
                Ok(Some(chunk)) => {
                    let fresh = store
                        .is_lod_fresh(&uri, LodLevel::L0, &source_hash)
                        .unwrap_or(false);
                    diag.lod_hits += 1;
                    LodResolution {
                        content: chunk.content,
                        actual_lod: LodLevel::L0,
                        lod_fresh: fresh,
                        generated_at: Some(chunk.created_at),
                        enqueued: false,
                    }
                }
                Ok(None) | Err(_) => {
                    // L0 miss — fall back to L2 passthrough.
                    diag.lod_misses += 1;
                    LodResolution {
                        content: ranked.content.clone(),
                        actual_lod: LodLevel::L2,
                        lod_fresh: true,
                        generated_at: None,
                        enqueued: false,
                    }
                }
            }
        }

        LodLevel::L1 => {
            let uri = build_object_uri(ranked, brain_name);
            let source_hash = crate::utils::content_hash(&ranked.content);

            match store.get_lod_chunk(&uri, LodLevel::L1) {
                Ok(Some(chunk)) => {
                    let fresh = store.is_l1_fresh(&uri, &source_hash).unwrap_or(false);
                    if fresh {
                        diag.lod_hits += 1;
                        LodResolution {
                            content: chunk.content,
                            actual_lod: LodLevel::L1,
                            lod_fresh: true,
                            generated_at: Some(chunk.created_at),
                            enqueued: false,
                        }
                    } else {
                        // Stale L1 — serve it but enqueue regeneration.
                        let enqueued =
                            try_enqueue_l1(db, &uri, brain_id, &ranked.content, &source_hash, diag);
                        LodResolution {
                            content: chunk.content,
                            actual_lod: LodLevel::L1,
                            lod_fresh: false,
                            generated_at: Some(chunk.created_at),
                            enqueued,
                        }
                    }
                }
                Ok(None) | Err(_) => {
                    // L1 miss — try L0 fallback, then L2, and enqueue L1 generation.
                    let enqueued =
                        try_enqueue_l1(db, &uri, brain_id, &ranked.content, &source_hash, diag);

                    match store.get_lod_chunk(&uri, LodLevel::L0) {
                        Ok(Some(l0_chunk)) => {
                            diag.lod_misses += 1;
                            LodResolution {
                                content: l0_chunk.content,
                                actual_lod: LodLevel::L0,
                                lod_fresh: false,
                                generated_at: Some(l0_chunk.created_at),
                                enqueued,
                            }
                        }
                        Ok(None) | Err(_) => {
                            diag.lod_misses += 1;
                            LodResolution {
                                content: ranked.content.clone(),
                                actual_lod: LodLevel::L2,
                                lod_fresh: true,
                                generated_at: None,
                                enqueued,
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Attempt to enqueue an L1 summarization job. Best-effort: logs on error.
/// Returns `true` if a job was actually enqueued.
fn try_enqueue_l1(
    db: &Db,
    object_uri: &str,
    brain_id: &str,
    source_content: &str,
    source_hash: &str,
    diag: &mut LodDiagnostics,
) -> bool {
    match crate::pipeline::job_worker::enqueue_l1_summarize(
        db,
        object_uri,
        brain_id,
        source_content,
        source_hash,
    ) {
        Ok(Some(_job_id)) => {
            diag.lod_generation_enqueued += 1;
            true
        }
        Ok(None) => false, // skipped (already queued or content too large)
        Err(e) => {
            warn!(object_uri = %object_uri, error = %e, "lod_resolver: failed to enqueue L1 job");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lod::{LodChunkStore, LodMethod, UpsertLodChunk};
    use brain_persistence::db::Db;

    /// Build a minimal RankedResult with just the fields we need.
    fn make_ranked(chunk_id: &str, content: &str) -> RankedResult {
        use crate::ranking::SignalScores;
        RankedResult {
            chunk_id: chunk_id.to_string(),
            hybrid_score: 1.0,
            scores: SignalScores {
                vector: 0.0,
                keyword: 0.0,
                recency: 0.0,
                links: 0.0,
                tag_match: 0.0,
                importance: 0.0,
            },
            file_path: String::new(),
            heading_path: String::new(),
            content: content.to_string(),
            token_estimate: 10,
            byte_start: 0,
            byte_end: content.len(),
            summary_kind: None,
        }
    }

    fn open_db() -> Db {
        Db::open_in_memory().expect("open_in_memory")
    }

    fn seed_lod(db: &Db, uri: &str, level: LodLevel, content: &str, source_hash: &str) {
        let store: &dyn LodChunkStore = db;
        store
            .upsert_lod_chunk(&UpsertLodChunk {
                object_uri: uri,
                brain_id: "brain-id",
                lod_level: level,
                content,
                token_est: None,
                method: LodMethod::Extractive,
                model_id: None,
                source_hash,
                expires_at: None,
                job_id: None,
            })
            .expect("upsert_lod_chunk");
    }

    // ---------------------------------------------------------------------------
    // L2 passthrough
    // ---------------------------------------------------------------------------

    #[test]
    fn test_l2_passthrough() {
        let db = open_db();
        let ranked = vec![make_ranked("chunk:abc:0", "hello world")];
        let (resolutions, diag) =
            resolve_lod_batch(&db, &ranked, LodLevel::L2, "brain", "brain-id");

        assert_eq!(resolutions.len(), 1);
        let r = &resolutions[0];
        assert_eq!(r.content, "hello world");
        assert_eq!(r.actual_lod, LodLevel::L2);
        assert!(r.lod_fresh);
        assert_eq!(r.generated_at, None);
        assert!(!r.enqueued);
        assert_eq!(diag.lod_hits, 1);
        assert_eq!(diag.lod_misses, 0);
    }

    // ---------------------------------------------------------------------------
    // L0 hit
    // ---------------------------------------------------------------------------

    #[test]
    fn test_l0_hit() {
        let db = open_db();
        let ranked = vec![make_ranked("chunk:abc:0", "source content")];
        let uri = build_object_uri(&ranked[0], "brain");
        let source_hash = crate::utils::content_hash("source content");

        seed_lod(&db, &uri, LodLevel::L0, "l0 abstract content", &source_hash);

        let (resolutions, diag) =
            resolve_lod_batch(&db, &ranked, LodLevel::L0, "brain", "brain-id");
        let r = &resolutions[0];
        assert_eq!(r.content, "l0 abstract content");
        assert_eq!(r.actual_lod, LodLevel::L0);
        assert!(r.lod_fresh);
        assert_eq!(diag.lod_hits, 1);
        assert_eq!(diag.lod_misses, 0);
    }

    // ---------------------------------------------------------------------------
    // L0 miss falls back to L2
    // ---------------------------------------------------------------------------

    #[test]
    fn test_l0_miss_falls_back_to_l2() {
        let db = open_db();
        let ranked = vec![make_ranked("chunk:abc:0", "source content")];
        let (resolutions, diag) =
            resolve_lod_batch(&db, &ranked, LodLevel::L0, "brain", "brain-id");

        let r = &resolutions[0];
        assert_eq!(r.content, "source content");
        assert_eq!(r.actual_lod, LodLevel::L2);
        assert!(r.lod_fresh);
        assert_eq!(diag.lod_hits, 0);
        assert_eq!(diag.lod_misses, 1);
    }

    // ---------------------------------------------------------------------------
    // L1 miss falls back to L0
    // ---------------------------------------------------------------------------

    #[test]
    fn test_l1_miss_falls_back_to_l0() {
        let db = open_db();
        let ranked = vec![make_ranked("chunk:abc:0", "source content")];
        let uri = build_object_uri(&ranked[0], "brain");
        let source_hash = crate::utils::content_hash("source content");

        // Seed only L0, no L1.
        seed_lod(&db, &uri, LodLevel::L0, "l0 fallback content", &source_hash);

        let (resolutions, _diag) =
            resolve_lod_batch(&db, &ranked, LodLevel::L1, "brain", "brain-id");
        let r = &resolutions[0];
        assert_eq!(r.content, "l0 fallback content");
        assert_eq!(r.actual_lod, LodLevel::L0);
        assert!(!r.lod_fresh); // came from miss path
    }

    // ---------------------------------------------------------------------------
    // Diagnostics counts
    // ---------------------------------------------------------------------------

    #[test]
    fn test_diagnostics_counts() {
        let db = open_db();

        // Two chunks: one with L0 seeded (hit), one without (miss).
        let ranked = vec![
            make_ranked("chunk:hit:0", "content hit"),
            make_ranked("chunk:miss:0", "content miss"),
        ];

        let uri_hit = build_object_uri(&ranked[0], "brain");
        let hash_hit = crate::utils::content_hash("content hit");
        seed_lod(&db, &uri_hit, LodLevel::L0, "abstract hit", &hash_hit);

        let (_, diag) = resolve_lod_batch(&db, &ranked, LodLevel::L0, "brain", "brain-id");
        assert_eq!(diag.lod_hits, 1);
        assert_eq!(diag.lod_misses, 1);
    }
}
