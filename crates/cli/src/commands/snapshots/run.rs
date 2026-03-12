use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::json;

use brain_lib::db::Db;
use brain_lib::records::RecordStore;
use brain_lib::records::events::{
    ContentRefPayload, LinkPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
    RecordEventType, TagPayload,
};
use brain_lib::records::objects::{COMPRESSION_THRESHOLD, ObjectStore};
use brain_lib::records::queries::RecordFilter;

use crate::markdown_table::MarkdownTable;

// -- shared context --

pub struct SnapshotCtx {
    pub(crate) record_store: RecordStore,
    pub(crate) object_store: ObjectStore,
    pub(crate) json: bool,
}

impl SnapshotCtx {
    pub fn new(sqlite_db: &Path, json: bool) -> Result<Self> {
        let db = Db::open(sqlite_db).context("Failed to open SQLite database")?;
        let brain_dir = sqlite_db.parent().unwrap_or_else(|| Path::new("."));
        let records_dir = brain_dir.join("records");
        let objects_dir = brain_dir.join("objects");
        let record_store =
            RecordStore::new(&records_dir, db).context("Failed to open record store")?;
        let object_store = ObjectStore::new(&objects_dir).context("Failed to open object store")?;
        Ok(Self {
            record_store,
            object_store,
            json,
        })
    }
}

// -- helpers --

fn format_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn format_size(bytes: i64) -> String {
    let bytes = bytes as f64;
    if bytes < 1024.0 {
        format!("{bytes:.0} B")
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1} KB", bytes / 1024.0)
    } else {
        format!("{:.1} MB", bytes / (1024.0 * 1024.0))
    }
}

// -- save --

pub struct SaveParams {
    pub title: String,
    pub file: Option<std::path::PathBuf>,
    pub stdin: bool,
    pub description: Option<String>,
    pub task: Option<String>,
    pub tags: Vec<String>,
    pub media_type: Option<String>,
}

pub fn save(ctx: &SnapshotCtx, params: SaveParams) -> Result<()> {
    let data: Vec<u8> = if let Some(ref path) = params.file {
        std::fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?
    } else if params.stdin {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)
            .context("Failed to read from stdin")?;
        buf
    } else {
        bail!("Must provide either --file <path> or --stdin to supply payload");
    };

    let media_type = params
        .media_type
        .or_else(|| Some("application/octet-stream".to_string()));

    let (content_ref, encoding, original_size) = ctx
        .object_store
        .write_compressed(&data, media_type, COMPRESSION_THRESHOLD)
        .context("Failed to write to object store")?;

    let content_ref_payload = ContentRefPayload::compressed(
        content_ref.hash.clone(),
        content_ref.size,
        content_ref.media_type.clone(),
        encoding,
        original_size,
    );

    let prefix = ctx
        .record_store
        .get_project_prefix()
        .context("Failed to get project prefix")?;
    let record_id = brain_lib::records::events::new_record_id(&prefix);

    let event = RecordEvent::from_payload(
        &record_id,
        "cli",
        RecordCreatedPayload {
            title: params.title.clone(),
            kind: "snapshot".to_string(),
            content_ref: content_ref_payload,
            description: params.description.clone(),
            task_id: params.task.clone(),
            tags: params.tags.clone(),
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
        },
    );

    ctx.record_store
        .apply_and_append(&event)
        .context("Failed to apply and append record event")?;

    if ctx.json {
        let out = json!({
            "record_id": record_id,
            "content_hash": content_ref.hash,
            "size": content_ref.size,
            "media_type": content_ref.media_type,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Saved snapshot {record_id}");
        println!("  Title: {}", params.title);
        println!("  Size:  {}", format_size(content_ref.size as i64));
        println!("  Hash:  {}", &content_ref.hash[..16]);
        if !params.tags.is_empty() {
            println!("  Tags:  {}", params.tags.join(", "));
        }
    }

    Ok(())
}

// -- list --

pub struct ListParams {
    pub tag: Option<String>,
    pub status: String,
    pub limit: usize,
}

pub fn list(ctx: &SnapshotCtx, params: &ListParams) -> Result<()> {
    let filter = RecordFilter {
        kind: Some("snapshot".to_string()),
        status: Some(params.status.clone()),
        tag: params.tag.clone(),
        task_id: None,
        limit: Some(params.limit),
        brain_id: None,
    };

    let records = ctx
        .record_store
        .list_records(&filter)
        .context("Failed to list snapshots")?;

    if ctx.json {
        let items: Vec<serde_json::Value> = records
            .iter()
            .map(|r| {
                json!({
                    "record_id": r.record_id,
                    "title": r.title,
                    "status": r.status,
                    "content_size": r.content_size,
                    "media_type": r.media_type,
                    "task_id": r.task_id,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                })
            })
            .collect();
        let out = json!({ "snapshots": items, "count": records.len() });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if records.is_empty() {
            println!("No snapshots found.");
            return Ok(());
        }

        let short_ids = ctx.record_store.compact_record_ids().unwrap_or_default();

        let mut table = MarkdownTable::new(vec!["ID", "TITLE", "STATUS", "SIZE", "CREATED"]);
        for r in &records {
            let short = short_ids
                .get(&r.record_id)
                .cloned()
                .unwrap_or_else(|| r.record_id.clone());
            table.add_row(vec![
                short,
                r.title.clone(),
                r.status.clone(),
                format_size(r.content_size),
                format_ts(r.created_at),
            ]);
        }
        print!("{table}");
        println!();
        println!("{} snapshot(s) shown", records.len());
    }

    Ok(())
}

// -- get --

pub fn get(ctx: &SnapshotCtx, id: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let record = ctx
        .record_store
        .get_record(&record_id)
        .context("Failed to get snapshot")?
        .with_context(|| format!("Snapshot not found: {record_id}"))?;

    let tags = ctx
        .record_store
        .get_record_tags(&record_id)
        .unwrap_or_default();

    let links = ctx
        .record_store
        .get_record_links(&record_id)
        .unwrap_or_default();

    if ctx.json {
        let out = json!({
            "record_id": record.record_id,
            "title": record.title,
            "kind": record.kind,
            "status": record.status,
            "description": record.description,
            "content_hash": record.content_hash,
            "content_size": record.content_size,
            "media_type": record.media_type,
            "task_id": record.task_id,
            "actor": record.actor,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "tags": tags,
            "links": links.iter().map(|l| json!({
                "task_id": l.task_id,
                "chunk_id": l.chunk_id,
                "created_at": l.created_at,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Snapshot: {}", record.record_id);
        println!("  Title:   {}", record.title);
        println!("  Status:  {}", record.status);
        if let Some(ref desc) = record.description {
            println!("  Desc:    {desc}");
        }
        println!("  Size:    {}", format_size(record.content_size));
        println!("  Hash:    {}", &record.content_hash[..16]);
        if let Some(ref mt) = record.media_type {
            println!("  Type:    {mt}");
        }
        if let Some(ref tid) = record.task_id {
            println!("  Task:    {tid}");
        }
        println!("  Actor:   {}", record.actor);
        println!("  Created: {}", format_ts(record.created_at));
        println!("  Updated: {}", format_ts(record.updated_at));
        if !tags.is_empty() {
            println!("  Tags:    {}", tags.join(", "));
        }
        if !links.is_empty() {
            println!("  Links:");
            for l in &links {
                if let Some(ref tid) = l.task_id {
                    println!("    task:{tid}");
                }
                if let Some(ref cid) = l.chunk_id {
                    println!("    chunk:{cid}");
                }
            }
        }
    }

    Ok(())
}

// -- restore --

pub fn restore(ctx: &SnapshotCtx, id: &str, output: Option<std::path::PathBuf>) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let record = ctx
        .record_store
        .get_record(&record_id)
        .context("Failed to get snapshot")?
        .with_context(|| format!("Snapshot not found: {record_id}"))?;

    let bytes = ctx
        .object_store
        .read_auto(&record.content_hash)
        .with_context(|| format!("Failed to read object for snapshot {record_id}"))?;

    match output {
        Some(path) => {
            std::fs::write(&path, &bytes)
                .with_context(|| format!("Failed to write to {}", path.display()))?;
            if ctx.json {
                let out = json!({
                    "record_id": record_id,
                    "bytes": bytes.len(),
                    "output": path.to_string_lossy(),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "Restored {} to {}",
                    format_size(bytes.len() as i64),
                    path.display()
                );
            }
        }
        None => {
            std::io::stdout()
                .write_all(&bytes)
                .context("Failed to write to stdout")?;
        }
    }

    Ok(())
}

// -- tag --

pub fn tag_add(ctx: &SnapshotCtx, id: &str, tag: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let event = RecordEvent::new(
        &record_id,
        "cli",
        RecordEventType::TagAdded,
        &TagPayload {
            tag: tag.to_string(),
        },
    );

    ctx.record_store
        .apply_and_append(&event)
        .context("Failed to apply tag_add event")?;

    if ctx.json {
        let out = serde_json::json!({ "record_id": record_id, "tag": tag, "action": "added" });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added tag '{tag}' to snapshot {record_id}");
    }

    Ok(())
}

pub fn tag_remove(ctx: &SnapshotCtx, id: &str, tag: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let event = RecordEvent::new(
        &record_id,
        "cli",
        RecordEventType::TagRemoved,
        &TagPayload {
            tag: tag.to_string(),
        },
    );

    ctx.record_store
        .apply_and_append(&event)
        .context("Failed to apply tag_remove event")?;

    if ctx.json {
        let out = serde_json::json!({ "record_id": record_id, "tag": tag, "action": "removed" });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed tag '{tag}' from snapshot {record_id}");
    }

    Ok(())
}

// -- link --

pub fn link_add(
    ctx: &SnapshotCtx,
    id: &str,
    task: Option<String>,
    chunk: Option<String>,
) -> Result<()> {
    if task.is_none() && chunk.is_none() {
        anyhow::bail!("Must specify at least one of --task or --chunk");
    }

    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let event = RecordEvent::new(
        &record_id,
        "cli",
        RecordEventType::LinkAdded,
        &LinkPayload {
            task_id: task.clone(),
            chunk_id: chunk.clone(),
        },
    );

    ctx.record_store
        .apply_and_append(&event)
        .context("Failed to apply link_add event")?;

    if ctx.json {
        let out = serde_json::json!({
            "record_id": record_id,
            "task_id": task,
            "chunk_id": chunk,
            "action": "linked",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if let Some(ref t) = task {
            println!("Linked task '{t}' to snapshot {record_id}");
        }
        if let Some(ref c) = chunk {
            println!("Linked chunk '{c}' to snapshot {record_id}");
        }
    }

    Ok(())
}

pub fn link_remove(
    ctx: &SnapshotCtx,
    id: &str,
    task: Option<String>,
    chunk: Option<String>,
) -> Result<()> {
    if task.is_none() && chunk.is_none() {
        anyhow::bail!("Must specify at least one of --task or --chunk");
    }

    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let event = RecordEvent::new(
        &record_id,
        "cli",
        RecordEventType::LinkRemoved,
        &LinkPayload {
            task_id: task.clone(),
            chunk_id: chunk.clone(),
        },
    );

    ctx.record_store
        .apply_and_append(&event)
        .context("Failed to apply link_remove event")?;

    if ctx.json {
        let out = serde_json::json!({
            "record_id": record_id,
            "task_id": task,
            "chunk_id": chunk,
            "action": "unlinked",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if let Some(ref t) = task {
            println!("Unlinked task '{t}' from snapshot {record_id}");
        }
        if let Some(ref c) = chunk {
            println!("Unlinked chunk '{c}' from snapshot {record_id}");
        }
    }

    Ok(())
}

// -- archive --

pub fn archive(ctx: &SnapshotCtx, id: &str, reason: Option<String>) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve snapshot ID: {id}"))?;

    let event = RecordEvent::from_payload(
        &record_id,
        "cli",
        RecordArchivedPayload {
            reason: reason.clone(),
        },
    );

    ctx.record_store
        .apply_and_append(&event)
        .context("Failed to apply archive event")?;

    if ctx.json {
        let out = json!({ "archived": record_id, "reason": reason });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Archived snapshot {record_id}");
        if let Some(ref r) = reason {
            println!("  Reason: {r}");
        }
    }

    Ok(())
}
