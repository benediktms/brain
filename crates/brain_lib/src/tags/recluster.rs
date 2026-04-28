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
use tracing::{debug, info, warn};

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
///
/// # Concurrency
///
/// **Callers must serialize invocations per brain.** Concurrent calls
/// against the same brain are not data-unsafe — `with_write_conn`
/// serializes Tx-1, Tx-2, and Tx-3 on the writer mutex, and the UPSERT
/// makes last-write-wins safe — but they produce duplicate
/// `tag_cluster_runs` rows and report counters that double-count work
/// done by the racing call. The plan for sibling task `brn-83a.7.2.5`
/// (MCP/CLI surface) introduces a per-brain in-flight guard if needed;
/// until then, callers must enforce the "one run at a time per brain"
/// invariant themselves.
pub async fn run_recluster(
    stores: &BrainStores,
    embedder: &Arc<dyn Embed>,
    params: ClusterParams,
) -> Result<ReclusterReport> {
    let started_at = std::time::Instant::now();
    let run_id = ulid::Ulid::new().to_string();
    let started_at_iso = chrono::Utc::now().to_rfc3339();

    info!(
        brain_id = %stores.brain_id,
        run_id = %run_id,
        threshold = params.cosine_threshold,
        embedder_version = EMBEDDER_VERSION,
        "recluster run starting",
    );

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
        warn!(
            brain_id = %stores.brain_id,
            run_id = %run_id,
            error = %e,
            "recluster run failed; recording on tag_cluster_runs",
        );
        // Best-effort: record the failure on the run row so operators can
        // grep for `notes IS NOT NULL` to find broken runs. Tx-1 may or
        // may not have committed; if it didn't, this UPDATE matches zero
        // rows and is a harmless no-op. We still surface a Tx-3 failure
        // via tracing so an operator-visible signal exists when the
        // failure-recording itself is broken (e.g. mutex poisoned).
        let now = chrono::Utc::now().to_rfc3339();
        let notes = truncate_utf8(&e.to_string(), MAX_NOTES_BYTES);
        if let Err(tx3_err) = stores.inner_db().record_run_failure(&run_id, &now, &notes) {
            warn!(
                brain_id = %stores.brain_id,
                run_id = %run_id,
                tx3_error = %tx3_err,
                "Tx-3 failed to record run failure on tag_cluster_runs",
            );
        }
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

    debug!(
        brain_id = %stores.brain_id,
        run_id = %run_id,
        source_count = raw_tags.len(),
        snapshot_size = snapshot.len(),
        "compute phase: candidates built, clustering next",
    );

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

    let report = ReclusterReport {
        run_id,
        source_count: raw_tags.len(),
        cluster_count: clusters.len(),
        new_aliases,
        updated_aliases,
        stale_aliases,
        duration_ms: started_at.elapsed().as_millis() as u64,
        embedder_version: EMBEDDER_VERSION.to_string(),
    };

    info!(
        brain_id = %stores.brain_id,
        run_id = %report.run_id,
        duration_ms = report.duration_ms,
        source_count = report.source_count,
        cluster_count = report.cluster_count,
        new_aliases = report.new_aliases,
        updated_aliases = report.updated_aliases,
        "recluster run complete",
    );

    Ok(report)
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

    let cache_hits = candidates.len();
    let to_embed = to_embed_tags.len();

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

    debug!(
        cache_hits,
        to_embed,
        total_candidates = candidates.len(),
        "build_candidates: cache partition complete",
    );

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
            let embedding = candidates_by_tag.get(member).cloned().unwrap_or_else(|| {
                panic!(
                    "cluster member {member:?} missing from candidates_by_tag — \
                     cluster_tags must only emit members it received as input",
                )
            });
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedder::MockEmbedder;
    use brain_persistence::db::tag_aliases as ta;

    /// Seed three records and two tasks producing distinct raw tags
    /// `bug`, `perf`, `performance` after dedupe across (records, tasks).
    fn seed_demo_brain(stores: &BrainStores) {
        let brain_id = stores.brain_id.clone();
        stores
            .db_for_tests()
            .with_write_conn(move |conn| {
                ta::seed_record_with_tags(conn, "r1", &brain_id, 1000, &["bug", "perf"])?;
                ta::seed_record_with_tags(conn, "r2", &brain_id, 2000, &["bug"])?;
                ta::seed_record_with_tags(conn, "r3", &brain_id, 1500, &["bug"])?;
                ta::seed_task_with_labels(conn, "t1", &brain_id, 3000, &["bug", "perf"])?;
                ta::seed_task_with_labels(conn, "t2", &brain_id, 4000, &["performance"])?;
                Ok(())
            })
            .unwrap();
    }

    #[tokio::test]
    async fn recluster_happy_path() {
        let (_tmp, stores) = BrainStores::in_memory_with_brain_id("happy-brain").unwrap();
        seed_demo_brain(&stores);

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let report = run_recluster(&stores, &embedder, ClusterParams::default())
            .await
            .unwrap();

        assert_eq!(report.source_count, 3, "{report:?}");
        assert!(report.cluster_count >= 1);
        assert_eq!(report.new_aliases, 3);
        assert_eq!(report.updated_aliases, 0);
        assert_eq!(report.stale_aliases, 0);
        assert_eq!(report.embedder_version, EMBEDDER_VERSION);

        let snapshot = stores.inner_db().read_alias_snapshot().unwrap();
        assert_eq!(snapshot.len(), 3, "expected 3 alias rows, got {snapshot:?}");
        for tag in ["bug", "perf", "performance"] {
            let row = snapshot.get(tag).unwrap_or_else(|| panic!("missing {tag}"));
            assert_eq!(row.embedder_version.as_deref(), Some(EMBEDDER_VERSION));
            assert!(
                row.embedding.as_ref().is_some_and(|v| v.len() == 384),
                "expected 384-dim cached embedding for {tag}",
            );
        }

        let run = stores
            .db_for_tests()
            .with_read_conn(|conn| ta::get_run(conn, &report.run_id))
            .unwrap()
            .expect("run row should exist");
        assert!(run.finished_at.is_some());
        assert!(run.notes.is_none());
        assert_eq!(run.source_count, Some(3));
        assert_eq!(run.embedder_version, EMBEDDER_VERSION);
        assert_eq!(run.triggered_by, "manual");
    }

    #[tokio::test]
    async fn recluster_idempotent() {
        let (_tmp, stores) = BrainStores::in_memory_with_brain_id("idem-brain").unwrap();
        seed_demo_brain(&stores);

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

        let r1 = run_recluster(&stores, &embedder, ClusterParams::default())
            .await
            .unwrap();
        let snapshot_before = stores.inner_db().read_alias_snapshot().unwrap();

        let r2 = run_recluster(&stores, &embedder, ClusterParams::default())
            .await
            .unwrap();
        let snapshot_after = stores.inner_db().read_alias_snapshot().unwrap();

        assert_ne!(r1.run_id, r2.run_id);
        assert_eq!(r2.new_aliases, 0, "idempotent: no new rows on rerun");
        assert_eq!(r2.updated_aliases, 0, "idempotent: no updates on rerun");
        assert_eq!(snapshot_before.len(), snapshot_after.len());
        for (tag, before) in &snapshot_before {
            let after = snapshot_after
                .get(tag)
                .unwrap_or_else(|| panic!("alias {tag} disappeared on rerun"));
            assert_eq!(before.canonical_tag, after.canonical_tag);
            assert_eq!(before.cluster_id, after.cluster_id);
            assert_eq!(before.embedder_version, after.embedder_version);
        }
    }

    #[tokio::test]
    async fn recluster_invalidates_cache_on_version_change() {
        let (_tmp, stores) = BrainStores::in_memory_with_brain_id("ver-brain").unwrap();
        seed_demo_brain(&stores);

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

        let r1 = run_recluster(&stores, &embedder, ClusterParams::default())
            .await
            .unwrap();
        assert!(r1.new_aliases > 0);

        // Stamp every tag_aliases row with a stale embedder_version.
        let updated = stores
            .db_for_tests()
            .with_write_conn(|conn| ta::override_alias_embedder_version(conn, "outdated-v0"))
            .unwrap();
        assert_eq!(updated, r1.new_aliases);

        let r2 = run_recluster(&stores, &embedder, ClusterParams::default())
            .await
            .unwrap();
        assert_eq!(r2.new_aliases, 0, "rows already exist");
        assert_eq!(
            r2.updated_aliases, r1.source_count,
            "version mismatch should mark every row as updated",
        );

        let snapshot = stores.inner_db().read_alias_snapshot().unwrap();
        for alias in snapshot.values() {
            assert_eq!(
                alias.embedder_version.as_deref(),
                Some(EMBEDDER_VERSION),
                "expected re-stamped version on every row",
            );
        }
    }

    /// Returns `BrainCoreError::Embedding` from `embed_batch` so the
    /// failure path triggers between Tx-1 and Tx-2.
    struct FailingEmbedder;

    impl Embed for FailingEmbedder {
        fn embed_batch(&self, _texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
            Err(crate::error::BrainCoreError::Embedding(
                "simulated failure for brn-83a.7.2.3 test".to_string(),
            ))
        }

        fn hidden_size(&self) -> usize {
            384
        }
    }

    #[tokio::test]
    async fn recluster_failure_records_run_row() {
        let (_tmp, stores) = BrainStores::in_memory_with_brain_id("fail-brain").unwrap();
        seed_demo_brain(&stores);

        let embedder: Arc<dyn Embed> = Arc::new(FailingEmbedder);
        let outcome = run_recluster(&stores, &embedder, ClusterParams::default()).await;
        assert!(outcome.is_err(), "FailingEmbedder must propagate");

        let snapshot = stores.inner_db().read_alias_snapshot().unwrap();
        assert!(
            snapshot.is_empty(),
            "Tx-2 never opened, tag_aliases should be empty: {snapshot:?}",
        );

        let runs = stores.db_for_tests().with_read_conn(ta::list_runs).unwrap();
        assert_eq!(runs.len(), 1, "exactly one tag_cluster_runs row");
        let run = &runs[0];
        assert!(run.finished_at.is_some(), "Tx-3 must set finished_at");
        let notes = run.notes.as_deref().expect("Tx-3 must populate notes");
        assert!(
            notes.contains("simulated failure"),
            "notes should carry the embedder error, got {notes:?}",
        );
    }

    #[tokio::test]
    async fn recluster_brain_scoping() {
        let (_tmp, stores_a) = BrainStores::in_memory_with_brain_id("brain-a").unwrap();
        // Register brain-b directly so the FK from records.brain_id resolves.
        stores_a
            .db_for_tests()
            .ensure_brain_registered("brain-b", "brain-b")
            .unwrap();

        stores_a
            .db_for_tests()
            .with_write_conn(|conn| {
                ta::seed_record_with_tags(conn, "ra", "brain-a", 1000, &["alpha"])?;
                ta::seed_record_with_tags(conn, "rb", "brain-b", 1000, &["beta"])?;
                Ok(())
            })
            .unwrap();

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let report = run_recluster(&stores_a, &embedder, ClusterParams::default())
            .await
            .unwrap();

        assert_eq!(report.source_count, 1, "brain-a sees only its own tag");
        let snapshot = stores_a.inner_db().read_alias_snapshot().unwrap();
        assert!(snapshot.contains_key("alpha"));
        assert!(
            !snapshot.contains_key("beta"),
            "brain-b's tag must not leak into brain-a's run",
        );
    }
}
