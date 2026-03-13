use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

use brain_lib::records::RecordStore;
use brain_lib::records::integrity;
use brain_lib::records::objects::ObjectStore;

pub struct RecordsCtx {
    pub record_store: RecordStore,
    pub object_store: ObjectStore,
    pub json: bool,
}

impl RecordsCtx {
    pub fn new(sqlite_db: &Path, json: bool) -> Result<Self> {
        let resolved = crate::commands::db_routing::resolve_dbs(sqlite_db)?;
        let brain_dir = sqlite_db.parent().unwrap_or_else(|| Path::new("."));
        let records_dir = brain_dir.join("records");
        // Use unified objects dir if it exists, fall back to per-brain
        let unified_objects = resolved.brain_home.join("objects");
        let objects_dir = if unified_objects.exists() {
            unified_objects
        } else {
            brain_dir.join("objects")
        };
        let record_store = if resolved.brain_id.is_empty() {
            RecordStore::new(&records_dir, resolved.unified)?
        } else {
            RecordStore::with_brain_id(&records_dir, resolved.unified, &resolved.brain_id)?
        }
        .with_meta_db(resolved.per_brain);
        let object_store = ObjectStore::new(&objects_dir).context("Failed to open object store")?;
        Ok(Self {
            record_store,
            object_store,
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
