use std::io::Read;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::DateTime;
use chrono::Utc;
use serde_json::json;

use brain_lib::records::events::{
    ContentRefPayload, LinkPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
    RecordEventType, TagPayload,
};
use brain_lib::records::objects::COMPRESSION_THRESHOLD;
use brain_lib::records::queries::RecordFilter;
use brain_lib::stores::BrainStores;

use crate::markdown_table::MarkdownTable;

// -- shared context --

pub struct ArtifactCtx {
    pub(crate) record_store: brain_lib::records::RecordStore,
    pub(crate) object_store: brain_lib::records::objects::ObjectStore,
    pub(crate) json: bool,
}

impl ArtifactCtx {
    pub fn new(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<Self> {
        let stores = BrainStores::from_path(sqlite_db, lance_db)?;
        Ok(Self {
            record_store: stores.records,
            object_store: stores.objects,
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
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB", bytes / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}

// -- create --

pub struct CreateParams {
    pub title: String,
    pub kind: String,
    pub file: Option<std::path::PathBuf>,
    pub stdin: bool,
    pub description: Option<String>,
    pub task: Option<String>,
    pub tags: Vec<String>,
    pub media_type: Option<String>,
    pub brain: Option<String>,
}

pub fn create(ctx: &ArtifactCtx, params: CreateParams) -> Result<()> {
    // Read payload
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

    // Write to object store (with transparent zstd compression)
    let (content_ref, encoding, original_size) = ctx
        .object_store
        .write_compressed(&data, params.media_type.clone(), COMPRESSION_THRESHOLD)
        .context("Failed to write to object store")?;

    // Convert ContentRef to ContentRefPayload
    let content_ref_payload = ContentRefPayload::compressed(
        content_ref.hash.clone(),
        content_ref.size,
        content_ref.media_type.clone(),
        encoding,
        original_size,
    );

    // Resolve target brain (if --brain provided)
    let remote_brain_info: Option<(String, String)>;
    let remote_records = if let Some(ref brain) = params.brain {
        let db = ctx.record_store.db();
        let (bid, bname) = db
            .resolve_brain(brain)
            .with_context(|| format!("Failed to resolve brain: {brain}"))?;
        // Guard: reject writes to archived brains
        let archived = db.with_read_conn(|conn| {
            let mut stmt =
                conn.prepare_cached("SELECT archived FROM brains WHERE brain_id = ?1")?;
            let mut rows = stmt.query([bid.as_str()])?;
            if let Some(row) = rows.next()? {
                let v: i64 = row.get(0)?;
                Ok(v != 0)
            } else {
                Ok(false)
            }
        })?;
        if archived {
            bail!("Target brain '{bname}' is archived");
        }
        remote_brain_info = Some((bname.clone(), bid.clone()));
        Some(brain_lib::records::RecordStore::with_brain_id(
            db.clone(),
            &bid,
            &bname,
        )?)
    } else {
        remote_brain_info = None;
        None
    };
    let record_store = remote_records.as_ref().unwrap_or(&ctx.record_store);

    // Generate record ID
    let prefix = record_store
        .get_project_prefix()
        .context("Failed to get project prefix")?;
    let record_id = brain_lib::records::events::new_record_id(&prefix);

    // Build event
    let event = RecordEvent::from_payload(
        &record_id,
        "cli",
        RecordCreatedPayload {
            title: params.title.clone(),
            kind: params.kind.clone(),
            content_ref: content_ref_payload,
            description: params.description.clone(),
            task_id: params.task.clone(),
            tags: params.tags.clone(),
            scope_type: params.task.as_ref().map(|_| "task".to_string()),
            scope_id: params.task.clone(),
            retention_class: None,
            producer: None,
        },
    );

    record_store
        .apply_event(&event)
        .context("Failed to apply and append record event")?;

    if ctx.json {
        let mut out = json!({
            "record_id": record_id,
            "content_hash": content_ref.hash,
            "size": content_ref.size,
            "media_type": content_ref.media_type,
        });
        if let Some((ref bname, ref bid)) = remote_brain_info {
            out["brain_name"] = json!(bname);
            out["brain_id"] = json!(bid);
        }
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created artifact {record_id}");
        if let Some((ref bname, _)) = remote_brain_info {
            println!("  Brain: {bname}");
        }
        println!("  Title: {}", params.title);
        println!("  Kind:  {}", params.kind);
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
    pub kind: Option<String>,
    pub tag: Option<String>,
    pub status: String,
    pub limit: usize,
}

pub fn list(ctx: &ArtifactCtx, params: &ListParams) -> Result<()> {
    let filter = RecordFilter {
        kind: params.kind.clone(),
        status: Some(params.status.clone()),
        tag: params.tag.clone(),
        task_id: None,
        limit: Some(params.limit),
        brain_id: None,
    };

    let records = ctx
        .record_store
        .list_records(&filter)
        .context("Failed to list records")?;

    if ctx.json {
        let items: Vec<serde_json::Value> = records
            .iter()
            .map(|r| {
                json!({
                    "record_id": r.record_id,
                    "title": r.title,
                    "kind": r.kind,
                    "status": r.status,
                    "description": r.description,
                    "content_hash": r.content_hash,
                    "content_size": r.content_size,
                    "media_type": r.media_type,
                    "task_id": r.task_id,
                    "actor": r.actor,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                })
            })
            .collect();
        let out = json!({ "artifacts": items, "count": records.len() });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if records.is_empty() {
            println!("No artifacts found.");
            return Ok(());
        }

        let short_ids = ctx.record_store.compact_record_ids().unwrap_or_default();

        let mut table =
            MarkdownTable::new(vec!["ID", "TITLE", "KIND", "STATUS", "SIZE", "CREATED"]);
        for r in &records {
            let short = short_ids
                .get(&r.record_id)
                .cloned()
                .unwrap_or_else(|| r.record_id.clone());
            table.add_row(vec![
                short,
                r.title.clone(),
                r.kind.clone(),
                r.status.clone(),
                format_size(r.content_size),
                format_ts(r.created_at),
            ]);
        }
        print!("{table}");
        println!();
        println!("{} artifact(s) shown", records.len());
    }

    Ok(())
}

// -- get --

pub fn get(ctx: &ArtifactCtx, id: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;

    let record = ctx
        .record_store
        .get_record(&record_id)
        .context("Failed to get record")?
        .with_context(|| format!("Artifact not found: {record_id}"))?;

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
        println!("Artifact: {}", record.record_id);
        println!("  Title:   {}", record.title);
        println!("  Kind:    {}", record.kind);
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

// -- tag --

pub fn tag_add(ctx: &ArtifactCtx, id: &str, tag: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;

    let event = RecordEvent::new(
        &record_id,
        "cli",
        RecordEventType::TagAdded,
        &TagPayload {
            tag: tag.to_string(),
        },
    );

    ctx.record_store
        .apply_event(&event)
        .context("Failed to apply tag_add event")?;

    if ctx.json {
        let out = serde_json::json!({ "record_id": record_id, "tag": tag, "action": "added" });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added tag '{tag}' to artifact {record_id}");
    }

    Ok(())
}

pub fn tag_remove(ctx: &ArtifactCtx, id: &str, tag: &str) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;

    let event = RecordEvent::new(
        &record_id,
        "cli",
        RecordEventType::TagRemoved,
        &TagPayload {
            tag: tag.to_string(),
        },
    );

    ctx.record_store
        .apply_event(&event)
        .context("Failed to apply tag_remove event")?;

    if ctx.json {
        let out = serde_json::json!({ "record_id": record_id, "tag": tag, "action": "removed" });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed tag '{tag}' from artifact {record_id}");
    }

    Ok(())
}

// -- link --

pub fn link_add(
    ctx: &ArtifactCtx,
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
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;

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
        .apply_event(&event)
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
            println!("Linked task '{t}' to artifact {record_id}");
        }
        if let Some(ref c) = chunk {
            println!("Linked chunk '{c}' to artifact {record_id}");
        }
    }

    Ok(())
}

pub fn link_remove(
    ctx: &ArtifactCtx,
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
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;

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
        .apply_event(&event)
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
            println!("Unlinked task '{t}' from artifact {record_id}");
        }
        if let Some(ref c) = chunk {
            println!("Unlinked chunk '{c}' from artifact {record_id}");
        }
    }

    Ok(())
}

// -- archive --

pub fn archive(ctx: &ArtifactCtx, id: &str, reason: Option<String>) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;

    let event = RecordEvent::from_payload(
        &record_id,
        "cli",
        RecordArchivedPayload {
            reason: reason.clone(),
        },
    );

    ctx.record_store
        .apply_event(&event)
        .context("Failed to apply archive event")?;

    if ctx.json {
        let out = json!({ "archived": record_id, "reason": reason });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Archived artifact {record_id}");
        if let Some(ref r) = reason {
            println!("  Reason: {r}");
        }
    }

    Ok(())
}

// -- restore --

pub fn restore(ctx: &ArtifactCtx, id: &str, output: Option<std::path::PathBuf>) -> Result<()> {
    let record_id = ctx
        .record_store
        .resolve_record_id(id)
        .with_context(|| format!("Could not resolve artifact ID: {id}"))?;
    let record = ctx
        .record_store
        .get_record(&record_id)
        .context("Failed to get artifact")?
        .with_context(|| format!("Artifact not found: {record_id}"))?;
    let bytes = ctx
        .object_store
        .read_auto(&record.content_hash)
        .with_context(|| format!("Failed to read object for artifact {record_id}"))?;

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
