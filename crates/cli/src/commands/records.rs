use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

use brain_lib::query_pipeline::{QueryPipeline, SearchParams};
use brain_lib::records::integrity;
use brain_lib::stores::BrainStores;
use brain_lib::uri::SynapseUri;

use crate::commands::memory::run::MemoryCtx;
use crate::markdown_table::MarkdownTable;

pub struct RecordsCtx {
    pub record_store: brain_lib::records::RecordStore,
    pub object_store: brain_lib::records::objects::ObjectStore,
    pub json: bool,
}

impl RecordsCtx {
    pub fn new(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<Self> {
        let stores = BrainStores::from_path(sqlite_db, lance_db)?;
        Ok(Self {
            record_store: stores.records,
            object_store: stores.objects,
            json,
        })
    }
}

pub fn verify(ctx: &RecordsCtx, verbose: bool) -> Result<()> {
    let report = integrity::verify_integrity(&ctx.record_store, &ctx.object_store)
        .context("Failed to verify integrity")?;

    if ctx.json {
        let out = json!({
            "clean": report.is_clean(),
            "records_checked": report.records_checked,
            "blobs_checked": report.blobs_checked,
            "missing": report.missing.len(),
            "corrupt": report.corrupt.len(),
            "orphans": report.orphans.len(),
            "stale_flags": report.stale_flags.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        print!("{report}");
        if verbose && !report.is_clean() {
            for m in &report.missing {
                println!(
                    "  MISSING: record={} hash={}",
                    m.record_id,
                    &m.content_hash[..16]
                );
            }
            for c in &report.corrupt {
                println!(
                    "  CORRUPT: expected={} actual={}",
                    &c.expected_hash[..16],
                    &c.actual_hash[..16]
                );
            }
            for o in &report.orphans {
                println!("  ORPHAN: hash={}", &o.hash[..16]);
            }
            for s in &report.stale_flags {
                println!(
                    "  STALE: record={} hash={}",
                    s.record_id,
                    &s.content_hash[..16]
                );
            }
        }
    }

    if !report.is_clean() {
        std::process::exit(1);
    }

    Ok(())
}

pub fn gc(ctx: &RecordsCtx, dry_run: bool) -> Result<()> {
    let report = integrity::verify_integrity(&ctx.record_store, &ctx.object_store)
        .context("Failed to verify integrity")?;

    if report.orphans.is_empty() {
        if ctx.json {
            let out = json!({ "orphans_removed": 0, "bytes_freed": 0 });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No orphan blobs found.");
        }
        return Ok(());
    }

    let result = integrity::cleanup_orphans(&report, &ctx.object_store, dry_run)
        .context("Failed to clean up orphans")?;

    if ctx.json {
        let out = json!({
            "orphans_removed": result.orphans_removed,
            "bytes_freed": result.bytes_freed,
            "dry_run": dry_run,
            "errors": result.errors.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if dry_run {
        println!(
            "Dry run — would remove {} orphan blob(s)",
            report.orphans.len()
        );
    } else {
        print!("{result}");
    }

    Ok(())
}

pub fn evict(ctx: &RecordsCtx, id: &str, reason: Option<String>) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve record ID: {id}"))?;

    let reason = reason.unwrap_or_else(|| "manual eviction".to_string());

    ctx.record_store
        .evict_payload(&record_id, &reason, "cli", &ctx.object_store)
        .context("Failed to evict payload")?;

    if ctx.json {
        let out = json!({ "evicted": record_id, "reason": reason });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Evicted payload for record {record_id}");
        println!("  Reason: {reason}");
    }

    Ok(())
}

pub fn pin(ctx: &RecordsCtx, id: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve record ID: {id}"))?;

    ctx.record_store
        .pin_record(&record_id, "cli")
        .context("Failed to pin record")?;

    if ctx.json {
        let out = json!({ "pinned": record_id });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Pinned record {record_id}");
    }

    Ok(())
}

pub fn unpin(ctx: &RecordsCtx, id: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve record ID: {id}"))?;

    ctx.record_store
        .unpin_record(&record_id, "cli")
        .context("Failed to unpin record")?;

    if ctx.json {
        let out = json!({ "unpinned": record_id });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Unpinned record {record_id}");
    }

    Ok(())
}

pub struct RecordsSearchParams {
    pub query: String,
    pub k: usize,
    pub budget: usize,
    pub tags: Vec<String>,
    pub brains: Vec<String>,
}

pub async fn search(ctx: &MemoryCtx, params: RecordsSearchParams) -> Result<()> {
    // Over-request to account for post-filter attrition.
    let over_k = params.k * 3;

    let search_params =
        SearchParams::new(&params.query, "lookup", params.budget, over_k, &params.tags);

    let search_result = if params.brains.is_empty() {
        let pipeline = QueryPipeline::new(
            ctx.stores.db(),
            &ctx.search.store,
            &ctx.search.embedder,
            &ctx.metrics,
        );
        pipeline.search(&search_params).await?
    } else {
        use brain_lib::config::{list_brain_keys, open_remote_search_context};
        use brain_lib::query_pipeline::FederatedPipeline;
        use brain_lib::store::StoreReader;

        let brain_keys: Vec<String> = if params.brains.iter().any(|b| b == "all") {
            list_brain_keys(&ctx.stores.brain_home)?
                .into_iter()
                .map(|(name, _id)| name)
                .collect()
        } else {
            params.brains.clone()
        };

        let mut brains: Vec<(String, Option<StoreReader>)> = Vec::new();
        brains.push((
            ctx.stores.brain_name.clone(),
            Some(ctx.search.store.clone()),
        ));

        for key in &brain_keys {
            if key == &ctx.stores.brain_name {
                continue;
            }
            match open_remote_search_context(
                &ctx.stores.brain_home,
                key,
                std::path::Path::new(""),
                &ctx.search.embedder,
            )
            .await?
            {
                Some(remote) => {
                    brains.push((remote.brain_name, remote.store));
                }
                None => {
                    eprintln!("warning: brain '{key}' not found in registry, skipping");
                }
            }
        }

        let federated = FederatedPipeline {
            db: ctx.stores.db(),
            brains,
            embedder: &ctx.search.embedder,
            metrics: &ctx.metrics,
        };
        federated.search(&search_params).await?
    };

    // Filter to record-kind only, then truncate to k.
    let record_stubs: Vec<_> = search_result
        .results
        .iter()
        .filter(|stub| stub.kind == "record")
        .take(params.k)
        .collect();

    let used_tokens_est: usize = record_stubs.iter().map(|s| s.token_estimate).sum();
    let num_results = record_stubs.len();

    if ctx.json {
        let results_json: Vec<serde_json::Value> = record_stubs
            .iter()
            .map(|stub| {
                // Extract record_id from memory_id: "record:<ID>:<chunk>" → "<ID>"
                let record_id = stub
                    .memory_id
                    .strip_prefix("record:")
                    .and_then(|s| s.rsplit_once(':').map(|(id, _)| id))
                    .unwrap_or(&stub.memory_id);

                let mut result_json = json!({
                    "record_id": record_id,
                    "memory_id": stub.memory_id,
                    "title": stub.title,
                    "summary": stub.summary_2sent,
                    "score": stub.hybrid_score,
                    "kind": stub.kind,
                });
                let uri_brain = stub.brain_name.as_deref().unwrap_or(&ctx.stores.brain_name);
                result_json["uri"] =
                    json!(SynapseUri::for_record(uri_brain, record_id).to_string());
                if let Some(ref bn) = stub.brain_name {
                    result_json["brain_name"] = json!(bn);
                }
                result_json
            })
            .collect();

        let out = json!({
            "budget_tokens": params.budget,
            "used_tokens_est": used_tokens_est,
            "result_count": num_results,
            "total_available": search_result.total_available,
            "results": results_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if record_stubs.is_empty() {
            println!("No record results found.");
            return Ok(());
        }
        let mut table = MarkdownTable::new(vec!["RECORD ID", "TITLE", "SCORE"]);
        for stub in &record_stubs {
            let record_id = stub
                .memory_id
                .strip_prefix("record:")
                .and_then(|s| s.rsplit_once(':').map(|(id, _)| id))
                .unwrap_or(&stub.memory_id);
            table.add_row(vec![
                record_id.to_string(),
                stub.title.clone(),
                format!("{:.4}", stub.hybrid_score),
            ]);
        }
        print!("{table}");
        println!();
        println!(
            "{}/{} record results | {}-token budget",
            num_results, search_result.total_available, params.budget
        );
    }

    Ok(())
}
