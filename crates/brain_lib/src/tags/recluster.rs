//! Synonym-clustering job orchestration (`brn-83a.7.2.3`).
//!
//! Wires the v43 schema (`tag_cluster_runs`, `tag_aliases` — sibling
//! `brn-83a.7.2.1`) and the pure clustering algorithm
//! ([`crate::tags::clustering`] — sibling `brn-83a.7.2.2`) into a runnable
//! per-brain function. Internal API only: no MCP/CLI surface, no daemon
//! scheduling — sibling `brn-83a.7.2.5` owns those.
//!
//! # Three-transaction model
//!
//! `tag_aliases.last_run_id` is a FK to `tag_cluster_runs(run_id)`, so the
//! run row must be committed before any alias upsert. Concretely:
//!
//! 1. **Tx-1** (short): INSERT the run row with `finished_at = NULL`.
//! 2. **Compute** (no DB locks): collect raw tags, snapshot `tag_aliases`,
//!    embed uncached entries, cluster, diff.
//! 3. **Tx-2** (atomic upsert + finalize): UPSERT every alias row and
//!    UPDATE the run row's `finished_at`, `source_count`, `cluster_count`.
//! 4. **Tx-3** (failure path only): UPDATE the run row's `notes` and
//!    `finished_at` if the function returns `Err`.
//!
//! Full design: `.omc/plans/brn-83a.7.2.3-plan.md`.

use std::collections::HashMap;
use std::sync::Arc;

use brain_persistence::db::tag_aliases::{
    AliasUpsert, DedupedRawTag, ExistingAlias, FinalizeRun, InsertRun,
};

use crate::embedder::{Embed, embed_batch_async};
use crate::error::Result;
use crate::ports::{TagAliasReader, TagAliasWriter};
use crate::stores::BrainStores;
use crate::tags::clustering::{ClusterParams, TagCandidate, cluster_tags};

/// Embedder identity stamped onto every cached embedding row in
/// `tag_aliases` and onto every `tag_cluster_runs` audit row.
///
/// **Known shortcut.** The [`Embed`] trait does not expose a version method,
/// so we hardcode the BGE-small-en-v1.5 identifier here. Bumping this
/// constant invalidates every cached embedding on the next [`run_recluster`]
/// call. Tracked for removal as `brn-83a.7.2.8` (add `fn version(&self) ->
/// &str` to the `Embed` trait).
const EMBEDDER_VERSION: &str = "bge-small-en-v1.5";

/// Hard cap on the size of `tag_cluster_runs.notes` the failure path
/// writes. Keeps a runaway error-message size from blowing up an audit row.
const MAX_NOTES_BYTES: usize = 4096;

/// Outcome summary of a single [`run_recluster`] invocation.
///
/// Field semantics mirror the v43 schema (`tag_cluster_runs` row + the diff
/// against `tag_aliases`) so callers can render or persist the report
/// without re-querying the database.
#[derive(Debug, Clone)]
pub struct ReclusterReport {
    /// ULID of the `tag_cluster_runs` row written for this invocation.
    pub run_id: String,
    /// Number of distinct raw tags observed in this brain at compute time.
    pub source_count: usize,
    /// Number of clusters produced.
    pub cluster_count: usize,
    /// Rows newly inserted into `tag_aliases`.
    pub new_aliases: usize,
    /// Rows whose `canonical_tag`, `cluster_id`, or `embedder_version` changed.
    pub updated_aliases: usize,
    /// Rows in the snapshot that were not observed in this run's raw tags.
    ///
    /// **Structurally always 0 in this implementation.** `tag_aliases` has
    /// no `brain_id` column today, so we cannot tell whether a missing row
    /// is genuinely stale or just owned by another brain — the conservative
    /// choice is to never count anything as stale. The field is retained in
    /// the public shape so the v44 schema migration (`brn-83a.7.2.7`)
    /// doesn't have to re-version this struct; the counter becomes
    /// meaningful once that migration lands. Introduced by `brn-83a.7.2.3`.
    pub stale_aliases: usize,
    /// Wall-clock duration of the run, milliseconds.
    pub duration_ms: u64,
    /// Embedder identity used for this run. Currently always [`EMBEDDER_VERSION`].
    pub embedder_version: String,
}

/// Run a synonym-clustering pass over the calling brain's raw tags.
///
/// See the module-level docs for the three-transaction model. This is the
/// only public-callable symbol in [`crate::tags::recluster`]; sibling task
/// `brn-83a.7.2.5` will wrap it for MCP/CLI exposure.
pub async fn run_recluster(
    stores: &BrainStores,
    embedder: &Arc<dyn Embed>,
    params: ClusterParams,
) -> Result<ReclusterReport> {
    let started_at = std::time::Instant::now();
    let run_id = ulid::Ulid::new().to_string();
    let started_at_iso = chrono::Utc::now().to_rfc3339();

    let outcome = run_inner(
        stores,
        embedder,
        params,
        run_id.clone(),
        started_at_iso,
        started_at,
    )
    .await;

    if let Err(ref e) = outcome {
        // Best-effort: record the failure on the run row so operators can
        // grep for `notes IS NOT NULL` to find broken runs. Tx-1 may or
        // may not have committed; if it didn't, this UPDATE matches zero
        // rows and is a harmless no-op.
        let now = chrono::Utc::now().to_rfc3339();
        let notes = truncate_utf8(&e.to_string(), MAX_NOTES_BYTES);
        let _ = stores.inner_db().record_run_failure(&run_id, &now, &notes);
    }

    outcome
}

async fn run_inner(
    stores: &BrainStores,
    embedder: &Arc<dyn Embed>,
    params: ClusterParams,
    run_id: String,
    started_at_iso: String,
    started_at: std::time::Instant,
) -> Result<ReclusterReport> {
    let db = stores.inner_db();

    // ---- Tx-1: insert the run row (FK precondition for Tx-2). -----------
    db.insert_run(InsertRun {
        run_id: run_id.clone(),
        started_at_iso,
        embedder_version: EMBEDDER_VERSION.to_string(),
        threshold: params.cosine_threshold,
        triggered_by: "manual".to_string(),
    })?;

    // ---- Compute phase (no DB locks held). ------------------------------
    let raw_tags: Vec<DedupedRawTag> = db.collect_raw_tags_for_brain(&stores.brain_id)?;
    let snapshot: HashMap<String, ExistingAlias> = db.read_alias_snapshot()?;

    let candidates = build_candidates(&raw_tags, &snapshot, embedder).await?;

    // Map each member back to its embedding for the diff phase. Cheap
    // (≤10k tags per brain) and lets the diff loop stay tag-keyed.
    let candidates_by_tag: HashMap<String, Vec<f32>> = candidates
        .iter()
        .map(|c| (c.tag.clone(), c.embedding.clone()))
        .collect();

    let clusters = cluster_tags(candidates, params);

    let (upserts, new_aliases, updated_aliases) =
        diff_clusters_against_snapshot(&clusters, &candidates_by_tag, &snapshot);

    // `stale_aliases` is structurally 0 — see ReclusterReport doc and the
    // follow-up `brn-83a.7.2.7`.
    let stale_aliases = 0usize;

    // ---- Tx-2: atomic UPSERT + finalize the run row. --------------------
    let finished_at_iso = chrono::Utc::now().to_rfc3339();
    db.apply_alias_upserts(
        upserts,
        FinalizeRun {
            run_id: run_id.clone(),
            finished_at_iso,
            source_count: raw_tags.len() as i64,
            cluster_count: clusters.len() as i64,
        },
    )?;

    Ok(ReclusterReport {
        run_id,
        source_count: raw_tags.len(),
        cluster_count: clusters.len(),
        new_aliases,
        updated_aliases,
        stale_aliases,
        duration_ms: started_at.elapsed().as_millis() as u64,
        embedder_version: EMBEDDER_VERSION.to_string(),
    })
}

/// Build the `Vec<TagCandidate>` for [`cluster_tags`] from the raw-tag set,
/// reusing cached embeddings on `tag_aliases` rows whose `embedder_version`
/// matches [`EMBEDDER_VERSION`] and embedding the rest in one batched call.
async fn build_candidates(
    raw_tags: &[DedupedRawTag],
    snapshot: &HashMap<String, ExistingAlias>,
    embedder: &Arc<dyn Embed>,
) -> Result<Vec<TagCandidate>> {
    let mut candidates: Vec<TagCandidate> = Vec::with_capacity(raw_tags.len());
    let mut to_embed_tags: Vec<String> = Vec::new();
    let mut to_embed_refs: Vec<i64> = Vec::new();

    for raw in raw_tags {
        let cache_hit = snapshot.get(&raw.tag).and_then(|prev| {
            if prev.embedder_version.as_deref() == Some(EMBEDDER_VERSION) {
                prev.embedding.clone()
            } else {
                None
            }
        });
        match cache_hit {
            Some(embedding) => candidates.push(TagCandidate {
                tag: raw.tag.clone(),
                embedding,
                reference_count: raw.total_reference_count,
            }),
            None => {
                to_embed_tags.push(raw.tag.clone());
                to_embed_refs.push(raw.total_reference_count);
            }
        }
    }

    if !to_embed_tags.is_empty() {
        let fresh = embed_batch_async(embedder, to_embed_tags.clone()).await?;
        for (idx, embedding) in fresh.into_iter().enumerate() {
            candidates.push(TagCandidate {
                tag: to_embed_tags[idx].clone(),
                embedding,
                reference_count: to_embed_refs[idx],
            });
        }
    }

    Ok(candidates)
}

/// Compare new clusters against the existing `tag_aliases` snapshot and
/// emit the `Vec<AliasUpsert>` plus `(new_aliases, updated_aliases)`
/// counters. Unchanged rows are skipped — that's how a re-run on identical
/// data produces zero upserts (idempotence).
fn diff_clusters_against_snapshot(
    clusters: &[crate::tags::clustering::TagCluster],
    candidates_by_tag: &HashMap<String, Vec<f32>>,
    snapshot: &HashMap<String, ExistingAlias>,
) -> (Vec<AliasUpsert>, usize, usize) {
    let mut new_aliases = 0usize;
    let mut updated_aliases = 0usize;
    let mut upserts: Vec<AliasUpsert> = Vec::new();

    for cluster in clusters {
        for member in &cluster.members {
            let embedding = candidates_by_tag
                .get(member)
                .cloned()
                .expect("cluster member must come from the candidates we just computed");
            let row = AliasUpsert {
                raw_tag: member.clone(),
                canonical_tag: cluster.canonical.clone(),
                cluster_id: cluster.cluster_id.clone(),
                embedding,
                embedder_version: EMBEDDER_VERSION.to_string(),
            };
            match snapshot.get(member) {
                None => {
                    new_aliases += 1;
                    upserts.push(row);
                }
                Some(prev)
                    if prev.canonical_tag != row.canonical_tag
                        || prev.cluster_id != row.cluster_id
                        || prev.embedder_version.as_deref() != Some(EMBEDDER_VERSION) =>
                {
                    updated_aliases += 1;
                    upserts.push(row);
                }
                Some(_) => { /* unchanged — skip upsert for idempotence */ }
            }
        }
    }

    (upserts, new_aliases, updated_aliases)
}

/// Clamp an error message to the given byte budget on a UTF-8 boundary.
fn truncate_utf8(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}
