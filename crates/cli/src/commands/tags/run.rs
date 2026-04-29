//! `brain tags …` — manual recluster trigger and alias inspection.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::json;

use brain_lib::embedder::{Embed, Embedder};
use brain_lib::stores::BrainStores;
use brain_lib::{ClusterParams, run_recluster};

use crate::markdown_table::MarkdownTable;

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

pub struct TagsCtx {
    pub(crate) stores: BrainStores,
    pub(crate) json: bool,
}

impl TagsCtx {
    pub fn new(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<Self> {
        Ok(Self {
            stores: BrainStores::from_path(sqlite_db, lance_db)?,
            json,
        })
    }
}

// ---------------------------------------------------------------------------
// recluster
// ---------------------------------------------------------------------------

pub async fn recluster(ctx: &TagsCtx, model_dir: &Path, threshold: f32) -> Result<()> {
    let embedder: Arc<dyn Embed> =
        Arc::new(Embedder::load(model_dir).context("Failed to load embedder model")?);

    let params = ClusterParams {
        cosine_threshold: threshold,
    };

    let report = run_recluster(&ctx.stores, &embedder, params)
        .await
        .context("recluster failed")?;

    if ctx.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Recluster complete (run_id {})", report.run_id);
        println!("  Source tags:    {}", report.source_count);
        println!("  Clusters:       {}", report.cluster_count);
        println!("  New aliases:    {}", report.new_aliases);
        println!("  Updated:        {}", report.updated_aliases);
        println!("  Stale pruned:   {}", report.stale_aliases);
        println!("  Embedder:       {}", report.embedder_version);
        println!("  Duration:       {} ms", report.duration_ms);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// aliases list
// ---------------------------------------------------------------------------

pub struct AliasesListParams {
    pub canonical: Option<String>,
    pub cluster_id: Option<String>,
    pub limit: i64,
    pub offset: i64,
}

pub fn aliases_list(ctx: &TagsCtx, params: AliasesListParams) -> Result<()> {
    let rows = ctx
        .stores
        .list_tag_aliases(
            params.canonical.as_deref(),
            params.cluster_id.as_deref(),
            params.limit,
            params.offset,
        )
        .context("Failed to list aliases")?;

    if ctx.json {
        let body = json!({
            "filters": {
                "canonical": params.canonical,
                "cluster_id": params.cluster_id,
                "limit": params.limit,
                "offset": params.offset,
            },
            "aliases": rows,
        });
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else if rows.is_empty() {
        println!("(no alias rows match)");
    } else {
        let mut table =
            MarkdownTable::new(vec!["raw_tag", "canonical", "cluster_id", "updated_at"]);
        for row in &rows {
            table.add_row(vec![
                row.raw_tag.clone(),
                row.canonical_tag.clone(),
                row.cluster_id.clone(),
                row.updated_at.clone(),
            ]);
        }
        print!("{table}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

pub fn status(ctx: &TagsCtx, model_dir: Option<&Path>) -> Result<()> {
    let last_run = ctx
        .stores
        .latest_tag_cluster_run()
        .context("Failed to read latest run")?;
    let counts = ctx
        .stores
        .count_tag_aliases()
        .context("Failed to count aliases")?;

    // Try to load the runtime embedder so we can report what the *next*
    // recluster would stamp. Failure is tolerated — the model may be
    // missing on a fresh checkout — but we surface the underlying error
    // on stderr so an operator troubleshooting "why didn't the new model
    // load?" sees the actual failure reason rather than a silent
    // "(not loaded)".
    let current_embedder_version = match model_dir {
        Some(dir) => match Embedder::load(dir) {
            Ok(e) => Some(e.version().to_string()),
            Err(err) => {
                eprintln!(
                    "warning: could not load runtime embedder from {}: {err}",
                    dir.display()
                );
                None
            }
        },
        None => None,
    };

    let ratio = if counts.raw_count > 0 {
        (counts.canonical_count as f64) / (counts.raw_count as f64)
    } else {
        0.0
    };

    if ctx.json {
        let body = json!({
            "last_run": last_run,
            "total_aliases": counts.raw_count,
            "total_clusters": counts.cluster_count,
            "current_embedder_version": current_embedder_version,
            "alias_coverage": {
                "canonical_count": counts.canonical_count,
                "raw_count": counts.raw_count,
                "ratio": ratio,
            },
        });
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else {
        println!("Tag clustering status for brain {}", ctx.stores.brain_name);
        match &last_run {
            None => println!("  Last run:      (none — no recluster has been performed)"),
            Some(run) => {
                println!("  Last run:      {}", run.run_id);
                println!("    started_at:    {}", run.started_at);
                println!(
                    "    finished_at:   {}",
                    run.finished_at.as_deref().unwrap_or("(in flight)")
                );
                if let Some(notes) = &run.notes {
                    println!("    notes:         {notes}");
                }
                println!("    embedder:      {}", run.embedder_version);
                println!("    threshold:     {:.2}", run.threshold);
            }
        }
        println!("  Total aliases:   {}", counts.raw_count);
        println!("  Canonical tags:  {}", counts.canonical_count);
        println!("  Clusters:        {}", counts.cluster_count);
        println!("  Coverage ratio:  {ratio:.3}");
        match &current_embedder_version {
            Some(v) => println!("  Runtime embedder: {v}"),
            None => println!("  Runtime embedder: (not loaded)"),
        }
    }
    Ok(())
}
