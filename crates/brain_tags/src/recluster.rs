//! Synonym-clustering job orchestration.
//!
//! Wires the `tag_cluster_runs` / `tag_aliases` schema and the pure
//! clustering algorithm (see [`crate::clustering`]) into a runnable
//! per-brain function.
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

use std::collections::HashMap;
use std::sync::Arc;

use brain_core::error::Result;
use brain_core::ports::Embed;
use brain_persistence::db::Db;
use brain_persistence::db::tag_aliases::{
    AliasUpsert, DedupedRawTag, ExistingAlias, FinalizeRun, InsertRun,
};
use brain_persistence::ports::{TagAliasReader, TagAliasWriter};
use tracing::{debug, error, info, warn};

use crate::clustering::{ClusterParams, TagCandidate, cluster_tags};
use crate::embed_batch_async;

/// Hard cap on the size of `tag_cluster_runs.notes` the failure path
/// writes. Keeps a runaway error-message size from blowing up an audit row.
const MAX_NOTES_BYTES: usize = 4096;

/// Outcome summary of a single [`run_recluster`] invocation.
///
/// Field semantics mirror the underlying schema (`tag_cluster_runs` row +
/// the diff against `tag_aliases`) so callers can render or persist the
/// report without re-querying the database.
#[derive(Debug, Clone, serde::Serialize)]
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
    /// Rows in the brain's previous `tag_aliases` snapshot whose `raw_tag`
    /// is no longer referenced by any record-tag or task-label in this
    /// brain. Such rows are DELETEd inside Tx-2; this counter records how
    /// many were pruned.
    pub stale_aliases: usize,
    /// Wall-clock duration of the run, milliseconds.
    pub duration_ms: u64,
    /// Embedder identity used for this run, sourced from
    /// [`Embed::version`].
    pub embedder_version: String,
}

/// Run a synonym-clustering pass over the calling brain's raw tags.
///
/// See the module-level docs for the three-transaction model.
///
/// # Concurrency
///
/// **Callers must serialize invocations per brain.** Concurrent calls
/// against the same brain are not data-unsafe — `with_write_conn`
/// serializes Tx-1, Tx-2, and Tx-3 on the writer mutex, and the UPSERT
/// makes last-write-wins safe — but they produce duplicate
/// `tag_cluster_runs` rows and report counters that double-count work
/// done by the racing call. Callers must enforce the "one run at a time
/// per brain" invariant themselves.
#[tracing::instrument(
    skip_all,
    fields(run_id = tracing::field::Empty, threshold = params.cosine_threshold),
)]
pub async fn run_recluster(
    db: &Db,
    brain_id: &str,
    embedder: &Arc<dyn Embed>,
    params: ClusterParams,
) -> Result<ReclusterReport> {
    let started_at = std::time::Instant::now();
    let run_id = ulid::Ulid::new().to_string();
    tracing::Span::current().record("run_id", run_id.as_str());
    let started_at_iso = chrono::Utc::now().to_rfc3339();
    let embedder_version = embedder.version().to_string();

    info!(
        brain_id = %brain_id,
        run_id = %run_id,
        threshold = params.cosine_threshold,
        embedder_version = %embedder_version,
        "recluster run starting",
    );

    let outcome = run_inner(
        RunResources {
            db,
            brain_id,
            embedder,
        },
        RunMetadata {
            run_id: run_id.clone(),
            started_at_iso,
            started_at,
            embedder_version: embedder_version.clone(),
        },
        params,
    )
    .await;

    if let Err(ref e) = outcome {
        error!(
            brain_id = %brain_id,
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
        if let Err(tx3_err) = db.record_run_failure(&run_id, &now, &notes) {
            warn!(
                brain_id = %brain_id,
                run_id = %run_id,
                tx3_error = %tx3_err,
                "Tx-3 failed to record run failure on tag_cluster_runs",
            );
        }
    }

    outcome
}

/// Borrowed references to the resources `run_inner` operates over.
///
/// Kept separate from [`RunMetadata`] so the function signature stays at a
/// readable three parameters (resources, metadata, params) rather than the
/// eight individual values it threads through. Borrows + owned together
/// would also force a single lifetime parameter on the metadata.
struct RunResources<'a> {
    db: &'a Db,
    brain_id: &'a str,
    embedder: &'a Arc<dyn Embed>,
}

/// Owned metadata recorded on `tag_cluster_runs` and the final report.
struct RunMetadata {
    run_id: String,
    started_at_iso: String,
    started_at: std::time::Instant,
    embedder_version: String,
}

async fn run_inner(
    resources: RunResources<'_>,
    meta: RunMetadata,
    params: ClusterParams,
) -> Result<ReclusterReport> {
    let RunResources {
        db,
        brain_id,
        embedder,
    } = resources;
    let RunMetadata {
        run_id,
        started_at_iso,
        started_at,
        embedder_version,
    } = meta;

    // ---- Tx-1: insert the run row (FK precondition for Tx-2). -----------
    db.insert_run(InsertRun {
        run_id: run_id.clone(),
        brain_id: brain_id.to_string(),
        started_at_iso,
        embedder_version: embedder_version.clone(),
        threshold: params.cosine_threshold,
        triggered_by: "manual".to_string(),
    })?;

    // ---- Compute phase (no DB locks held). ------------------------------
    let raw_tags: Vec<DedupedRawTag> = db.collect_raw_tags(brain_id)?;
    let snapshot: HashMap<String, ExistingAlias> = db.read_alias_snapshot(brain_id)?;

    let candidates = build_candidates(&raw_tags, &snapshot, embedder, &embedder_version).await?;

    debug!(
        brain_id = %brain_id,
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

    let (upserts, new_aliases, updated_aliases) = diff_clusters_against_snapshot(
        &clusters,
        &candidates_by_tag,
        &snapshot,
        brain_id,
        &embedder_version,
    );

    // Stale rows: in this brain's snapshot but absent from the freshly
    // collected raw-tag set. Pruned inside Tx-2 (see `apply_alias_upserts`).
    let raw_tag_set: std::collections::HashSet<&str> =
        raw_tags.iter().map(|r| r.tag.as_str()).collect();
    let stale: Vec<String> = snapshot
        .keys()
        .filter(|k| !raw_tag_set.contains(k.as_str()))
        .cloned()
        .collect();
    let stale_aliases = stale.len();

    // ---- Tx-2: atomic UPSERT + DELETE stale + finalize the run row. ----
    let finished_at_iso = chrono::Utc::now().to_rfc3339();
    db.apply_alias_upserts(
        brain_id,
        upserts,
        stale,
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
        embedder_version,
    };

    if report.stale_aliases > 0 {
        warn!(
            brain_id = %brain_id,
            run_id = %report.run_id,
            stale = report.stale_aliases,
            "recluster: pruned stale aliases",
        );
    }

    info!(
        brain_id = %brain_id,
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
/// matches the supplied `embedder_version` and embedding the rest in one
/// batched call.
async fn build_candidates(
    raw_tags: &[DedupedRawTag],
    snapshot: &HashMap<String, ExistingAlias>,
    embedder: &Arc<dyn Embed>,
    embedder_version: &str,
) -> Result<Vec<TagCandidate>> {
    let mut candidates: Vec<TagCandidate> = Vec::with_capacity(raw_tags.len());
    let mut to_embed_tags: Vec<String> = Vec::new();
    let mut to_embed_refs: Vec<i64> = Vec::new();

    for raw in raw_tags {
        let cache_hit = snapshot.get(&raw.tag).and_then(|prev| {
            if prev.embedder_version.as_deref() == Some(embedder_version) {
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
    clusters: &[crate::clustering::TagCluster],
    candidates_by_tag: &HashMap<String, Vec<f32>>,
    snapshot: &HashMap<String, ExistingAlias>,
    brain_id: &str,
    embedder_version: &str,
) -> (Vec<AliasUpsert>, usize, usize) {
    let mut new_aliases = 0usize;
    let mut updated_aliases = 0usize;
    let mut upserts: Vec<AliasUpsert> = Vec::new();

    for cluster in clusters {
        debug!(
            cluster_id = %cluster.cluster_id,
            canonical = %cluster.canonical,
            members = cluster.members.len(),
            "recluster: cluster",
        );
        for member in &cluster.members {
            let embedding = candidates_by_tag.get(member).cloned().unwrap_or_else(|| {
                panic!(
                    "cluster member {member:?} missing from candidates_by_tag — \
                     cluster_tags must only emit members it received as input",
                )
            });
            let row = AliasUpsert {
                brain_id: brain_id.to_string(),
                raw_tag: member.clone(),
                canonical_tag: cluster.canonical.clone(),
                cluster_id: cluster.cluster_id.clone(),
                embedding,
                embedder_version: embedder_version.to_string(),
            };
            match snapshot.get(member) {
                None => {
                    new_aliases += 1;
                    upserts.push(row);
                }
                Some(prev)
                    if prev.canonical_tag != row.canonical_tag
                        || prev.cluster_id != row.cluster_id
                        || prev.embedder_version.as_deref() != Some(embedder_version) =>
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
    use brain_core::error::BrainCoreError;
    use brain_core::ports::mock::MockEmbedder;
    use brain_persistence::db::tag_aliases as ta;
    use brain_persistence::sql::{SqlError, SqlResultExt};

    /// Spin up an in-memory `Db` and register the brain so FK constraints
    /// on `records.brain_id` / `tasks.brain_id` resolve.
    fn new_db_with_brain(brain_id: &str) -> Db {
        let db = Db::open_in_memory().expect("open in-memory db");
        db.ensure_brain_registered(brain_id, brain_id)
            .expect("register brain");
        db
    }

    /// Seed three records and two tasks producing distinct raw tags
    /// `bug`, `perf`, `performance` after dedupe across (records, tasks).
    fn seed_demo_brain(db: &Db, brain_id: &str) {
        let brain_id_owned = brain_id.to_string();
        db.with_write_conn(move |conn| {
            ta::seed_record_with_tags(conn, "r1", &brain_id_owned, 1000, &["bug", "perf"])?;
            ta::seed_record_with_tags(conn, "r2", &brain_id_owned, 2000, &["bug"])?;
            ta::seed_record_with_tags(conn, "r3", &brain_id_owned, 1500, &["bug"])?;
            ta::seed_task_with_labels(conn, "t1", &brain_id_owned, 3000, &["bug", "perf"])?;
            ta::seed_task_with_labels(conn, "t2", &brain_id_owned, 4000, &["performance"])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();
    }

    #[tokio::test]
    async fn recluster_happy_path() {
        let db = new_db_with_brain("happy-brain");
        seed_demo_brain(&db, "happy-brain");

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let report = run_recluster(&db, "happy-brain", &embedder, ClusterParams::default())
            .await
            .unwrap();

        assert_eq!(report.source_count, 3, "{report:?}");
        assert!(report.cluster_count >= 1);
        assert_eq!(report.new_aliases, 3);
        assert_eq!(report.updated_aliases, 0);
        assert_eq!(report.stale_aliases, 0);
        assert_eq!(report.embedder_version, MockEmbedder.version());

        let snapshot = db.read_alias_snapshot("happy-brain").unwrap();
        assert_eq!(snapshot.len(), 3, "expected 3 alias rows, got {snapshot:?}");
        for tag in ["bug", "perf", "performance"] {
            let row = snapshot.get(tag).unwrap_or_else(|| panic!("missing {tag}"));
            assert_eq!(
                row.embedder_version.as_deref(),
                Some(MockEmbedder.version()),
            );
            assert!(
                row.embedding.as_ref().is_some_and(|v| v.len() == 384),
                "expected 384-dim cached embedding for {tag}",
            );
        }

        let run = db
            .with_read_conn(|conn| ta::get_run(conn, &report.run_id))
            .into_brain_core()
            .unwrap()
            .expect("run row should exist");
        assert!(run.finished_at.is_some());
        assert!(run.notes.is_none());
        assert_eq!(run.source_count, Some(3));
        assert_eq!(run.embedder_version, MockEmbedder.version());
        assert_eq!(run.triggered_by, "manual");
    }

    #[tokio::test]
    async fn recluster_idempotent() {
        let db = new_db_with_brain("idem-brain");
        seed_demo_brain(&db, "idem-brain");

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

        let r1 = run_recluster(&db, "idem-brain", &embedder, ClusterParams::default())
            .await
            .unwrap();
        let snapshot_before = db.read_alias_snapshot("idem-brain").unwrap();

        let r2 = run_recluster(&db, "idem-brain", &embedder, ClusterParams::default())
            .await
            .unwrap();
        let snapshot_after = db.read_alias_snapshot("idem-brain").unwrap();

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

    /// `MockEmbedder` clone that reports a caller-supplied [`Embed::version`]
    /// string. Used by the version-change test to swap embedders between two
    /// `run_recluster` calls and assert that cached rows are invalidated
    /// when the embedder identity changes.
    struct VersionedMockEmbedder(&'static str);

    impl Embed for VersionedMockEmbedder {
        fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
            MockEmbedder.embed_batch(texts)
        }

        fn hidden_size(&self) -> usize {
            384
        }

        fn version(&self) -> &str {
            self.0
        }
    }

    #[tokio::test]
    async fn recluster_invalidates_cache_on_version_change() {
        let db = new_db_with_brain("ver-brain");
        seed_demo_brain(&db, "ver-brain");

        // First run uses embedder version "v1": writes fresh alias rows.
        let v1: Arc<dyn Embed> = Arc::new(VersionedMockEmbedder("v1"));
        let r1 = run_recluster(&db, "ver-brain", &v1, ClusterParams::default())
            .await
            .unwrap();
        assert!(r1.new_aliases > 0);
        assert_eq!(r1.embedder_version, "v1");

        // Second run swaps to "v2": every cached row's embedder_version
        // mismatches, so all should be marked updated and re-stamped.
        let v2: Arc<dyn Embed> = Arc::new(VersionedMockEmbedder("v2"));
        let r2 = run_recluster(&db, "ver-brain", &v2, ClusterParams::default())
            .await
            .unwrap();
        assert_eq!(r2.new_aliases, 0, "rows already exist");
        assert_eq!(
            r2.updated_aliases, r1.source_count,
            "version mismatch should mark every row as updated",
        );
        assert_eq!(r2.embedder_version, "v2");

        let snapshot = db.read_alias_snapshot("ver-brain").unwrap();
        for alias in snapshot.values() {
            assert_eq!(
                alias.embedder_version.as_deref(),
                Some("v2"),
                "expected re-stamped version on every row",
            );
        }
    }

    /// Returns `BrainCoreError::Embedding` from `embed_batch` so the
    /// failure path triggers between Tx-1 and Tx-2.
    struct FailingEmbedder;

    impl Embed for FailingEmbedder {
        fn embed_batch(&self, _texts: &[&str]) -> Result<Vec<Vec<f32>>> {
            Err(BrainCoreError::Embedding(
                "simulated failure for recluster failure-path test".to_string(),
            ))
        }

        fn hidden_size(&self) -> usize {
            384
        }

        fn version(&self) -> &str {
            "failing-v0"
        }
    }

    #[tokio::test]
    async fn recluster_failure_records_run_row() {
        let db = new_db_with_brain("fail-brain");
        seed_demo_brain(&db, "fail-brain");

        let embedder: Arc<dyn Embed> = Arc::new(FailingEmbedder);
        let outcome = run_recluster(&db, "fail-brain", &embedder, ClusterParams::default()).await;
        assert!(outcome.is_err(), "FailingEmbedder must propagate");

        let snapshot = db.read_alias_snapshot("fail-brain").unwrap();
        assert!(
            snapshot.is_empty(),
            "Tx-2 never opened, tag_aliases should be empty: {snapshot:?}",
        );

        let runs = db.with_read_conn(ta::list_runs).into_brain_core().unwrap();
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
        let db = new_db_with_brain("brain-a");
        db.ensure_brain_registered("brain-b", "brain-b").unwrap();

        db.with_write_conn(|conn| {
            ta::seed_record_with_tags(conn, "ra", "brain-a", 1000, &["alpha"])?;
            ta::seed_record_with_tags(conn, "rb", "brain-b", 1000, &["beta"])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let report = run_recluster(&db, "brain-a", &embedder, ClusterParams::default())
            .await
            .unwrap();

        assert_eq!(report.source_count, 1, "brain-a sees only its own tag");

        // brain-a's snapshot has alpha, not beta.
        let snap_a = db.read_alias_snapshot("brain-a").unwrap();
        assert!(snap_a.contains_key("alpha"));
        assert!(
            !snap_a.contains_key("beta"),
            "brain-b's tag must not leak into brain-a's snapshot",
        );

        // brain-b's snapshot is empty — we never ran recluster against it,
        // so no `tag_aliases` rows were written for that brain.
        let snap_b = db.read_alias_snapshot("brain-b").unwrap();
        assert!(snap_b.is_empty(), "brain-b should have no alias rows yet");

        // Global confirmation: only brain-a rows exist in the table.
        let global_brains: Vec<String> = db
            .with_read_conn(|conn| {
                let mut stmt = conn.prepare("SELECT DISTINCT brain_id FROM tag_aliases")?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(SqlError::from)
            })
            .into_brain_core()
            .unwrap();
        assert_eq!(global_brains, vec!["brain-a".to_string()]);
    }

    #[tokio::test]
    async fn recluster_prunes_stale_aliases() {
        let db = new_db_with_brain("prune-brain");
        db.with_write_conn(|conn| {
            ta::seed_record_with_tags(conn, "r-alpha", "prune-brain", 1000, &["alpha"])?;
            ta::seed_record_with_tags(conn, "r-beta", "prune-brain", 1000, &["beta"])?;
            ta::seed_record_with_tags(conn, "r-gamma", "prune-brain", 1000, &["gamma"])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

        // First run: 3 new aliases, 0 stale.
        let r1 = run_recluster(&db, "prune-brain", &embedder, ClusterParams::default())
            .await
            .unwrap();
        assert_eq!(r1.source_count, 3);
        assert_eq!(r1.new_aliases, 3);
        assert_eq!(r1.stale_aliases, 0);
        let snap_after_r1 = db.read_alias_snapshot("prune-brain").unwrap();
        assert_eq!(snap_after_r1.len(), 3);
        for tag in ["alpha", "beta", "gamma"] {
            assert!(snap_after_r1.contains_key(tag), "missing {tag}");
        }

        // Drop the gamma source; rerun should prune. record_tags has no
        // ON DELETE CASCADE on records, so delete the tag rows first.
        db.with_write_conn(|conn| {
            conn.execute("DELETE FROM record_tags WHERE record_id = 'r-gamma'", [])?;
            conn.execute("DELETE FROM records WHERE record_id = 'r-gamma'", [])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();

        let r2 = run_recluster(&db, "prune-brain", &embedder, ClusterParams::default())
            .await
            .unwrap();
        assert_eq!(r2.source_count, 2);
        assert_eq!(r2.new_aliases, 0);
        assert_eq!(r2.stale_aliases, 1, "gamma should be pruned");

        let snap_after_r2 = db.read_alias_snapshot("prune-brain").unwrap();
        assert_eq!(snap_after_r2.len(), 2);
        assert!(snap_after_r2.contains_key("alpha"));
        assert!(snap_after_r2.contains_key("beta"));
        assert!(!snap_after_r2.contains_key("gamma"));
    }

    #[tokio::test]
    async fn recluster_does_not_touch_other_brain_stale() {
        let db = new_db_with_brain("brain-a");
        db.ensure_brain_registered("brain-b", "brain-b").unwrap();

        // Seed both brains, run recluster on each so both have alias rows.
        db.with_write_conn(|conn| {
            ta::seed_record_with_tags(conn, "ra", "brain-a", 1000, &["alpha"])?;
            ta::seed_record_with_tags(conn, "rb", "brain-b", 1000, &["beta"])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

        run_recluster(&db, "brain-a", &embedder, ClusterParams::default())
            .await
            .unwrap();
        run_recluster(&db, "brain-b", &embedder, ClusterParams::default())
            .await
            .unwrap();

        // Sanity: each brain has its own row.
        assert!(
            db.read_alias_snapshot("brain-a")
                .unwrap()
                .contains_key("alpha")
        );
        assert!(
            db.read_alias_snapshot("brain-b")
                .unwrap()
                .contains_key("beta")
        );

        // Now delete brain-b's source record (and its tags first — no
        // CASCADE on record_tags) and re-run recluster on brain-a.
        // brain-b's stale alias must remain untouched — the stale-DELETE
        // in apply_alias_upserts is brain-scoped.
        db.with_write_conn(|conn| {
            conn.execute("DELETE FROM record_tags WHERE record_id = 'rb'", [])?;
            conn.execute("DELETE FROM records WHERE record_id = 'rb'", [])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();

        let r = run_recluster(&db, "brain-a", &embedder, ClusterParams::default())
            .await
            .unwrap();
        assert_eq!(r.stale_aliases, 0, "brain-a has no stale tags");

        let snap_b = db.read_alias_snapshot("brain-b").unwrap();
        assert!(
            snap_b.contains_key("beta"),
            "brain-b's stale alias must survive a recluster on brain-a",
        );
    }

    #[tokio::test]
    async fn recluster_two_brains_same_tag_get_separate_rows() {
        let db = new_db_with_brain("brain-a");
        db.ensure_brain_registered("brain-b", "brain-b").unwrap();

        db.with_write_conn(|conn| {
            ta::seed_record_with_tags(conn, "ra", "brain-a", 1000, &["python"])?;
            ta::seed_record_with_tags(conn, "rb", "brain-b", 1000, &["python"])?;
            Ok(())
        })
        .into_brain_core()
        .unwrap();

        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);

        run_recluster(&db, "brain-a", &embedder, ClusterParams::default())
            .await
            .unwrap();
        run_recluster(&db, "brain-b", &embedder, ClusterParams::default())
            .await
            .unwrap();

        // Composite PK lets both brains coexist with the same raw_tag.
        let pairs: Vec<(String, String)> = db
            .with_read_conn(|conn| {
                let mut stmt =
                    conn.prepare("SELECT brain_id, raw_tag FROM tag_aliases ORDER BY brain_id")?;
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(SqlError::from)
            })
            .into_brain_core()
            .unwrap();

        assert_eq!(
            pairs,
            vec![
                ("brain-a".to_string(), "python".to_string()),
                ("brain-b".to_string(), "python".to_string()),
            ],
        );
    }
}
