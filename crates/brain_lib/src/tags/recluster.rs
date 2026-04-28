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

use std::sync::Arc;

use crate::embedder::Embed;
use crate::error::Result;
use crate::stores::BrainStores;
use crate::tags::clustering::ClusterParams;

/// Embedder identity stamped onto every cached embedding row in
/// `tag_aliases` and onto every `tag_cluster_runs` audit row.
///
/// **Known shortcut.** The [`Embed`] trait does not expose a version method,
/// so we hardcode the BGE-small-en-v1.5 identifier here. Bumping this
/// constant invalidates every cached embedding on the next [`run_recluster`]
/// call. Tracked for removal as `brn-83a.7.2.8` (add `fn version(&self) ->
/// &str` to the `Embed` trait).
#[allow(dead_code)] // wired in subsequent commits within `brn-83a.7.2.3`
const EMBEDDER_VERSION: &str = "bge-small-en-v1.5";

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
    /// Embedder identity used for this run. Currently always
    /// [`struct@EMBEDDER_VERSION`].
    pub embedder_version: String,
}

// Persistence types (`DedupedRawTag`, `ExistingAlias`, `AliasUpsert`) and
// the encode/decode codec live in `brain_persistence::db::tag_aliases`. The
// recluster module reaches them through the `TagAliasReader` /
// `TagAliasWriter` port traits in `crate::ports`. SQL belongs in
// `brain_persistence`; see `crates/brain_lib/clippy.toml`.

/// Run a synonym-clustering pass over the calling brain's raw tags.
///
/// See the module-level docs for the three-transaction model. This is the
/// only public-callable symbol in [`crate::tags::recluster`]; sibling task
/// `brn-83a.7.2.5` will wrap it for MCP/CLI exposure.
pub async fn run_recluster(
    _stores: &BrainStores,
    _embedder: &Arc<dyn Embed>,
    _params: ClusterParams,
) -> Result<ReclusterReport> {
    unimplemented!(
        "run_recluster body lands in subsequent commits of brn-83a.7.2.3 \
         (see .omc/plans/brn-83a.7.2.3-plan.md)"
    )
}
