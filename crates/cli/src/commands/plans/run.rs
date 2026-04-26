use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::json;

use brain_lib::l0_abstract::generate_l0_abstract;
use brain_lib::pipeline::embed_poll::upsert_domain_lod_l0;
use brain_lib::records::events::{
    ContentRefPayload, RecordCreatedPayload, RecordEvent, new_record_id,
};
use brain_lib::records::objects::COMPRESSION_THRESHOLD;
use brain_lib::stores::BrainStores;
use brain_lib::uri::SynapseUri;

pub struct PlanCtx {
    pub(crate) stores: BrainStores,
    pub(crate) json: bool,
}

impl PlanCtx {
    pub fn new(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<Self> {
        Ok(Self {
            stores: BrainStores::from_path(sqlite_db, lance_db)?,
            json,
        })
    }
}

pub struct CreateParams {
    pub title: String,
    pub file: Option<std::path::PathBuf>,
    pub stdin: bool,
    pub text: Option<String>,
    pub description: Option<String>,
    pub task: Option<String>,
    pub tags: Vec<String>,
    pub media_type: Option<String>,
    pub brain: Option<String>,
}

pub fn create(ctx: &PlanCtx, params: CreateParams) -> Result<()> {
    let provided_sources = usize::from(params.file.is_some())
        + usize::from(params.stdin)
        + usize::from(params.text.is_some());
    if provided_sources != 1 {
        bail!("Provide exactly one payload source: --file <path>, --stdin, or --text <body>");
    }

    let (raw_bytes, media_type) = match (&params.file, params.stdin, &params.text) {
        (Some(path), false, None) => (
            std::fs::read(path)
                .with_context(|| format!("Failed to read file: {}", path.display()))?,
            params
                .media_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        ),
        (None, true, None) => {
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("Failed to read from stdin")?;
            (
                buf,
                params
                    .media_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
            )
        }
        (None, false, Some(text)) => (
            text.as_bytes().to_vec(),
            params
                .media_type
                .clone()
                .unwrap_or_else(|| "text/plain".to_string()),
        ),
        _ => unreachable!("validated exactly one payload source"),
    };

    let mut target_stores = None;
    if let Some(ref brain) = params.brain {
        let (bid, brain_name) = ctx.stores.resolve_brain(brain)?;
        if ctx.stores.is_brain_archived(&bid)? {
            bail!("Target brain '{brain_name}' is archived");
        }
        if bid != ctx.stores.brain_id {
            target_stores = Some(ctx.stores.with_brain_id(&bid, &brain_name)?);
        }
    }
    let stores = target_stores.as_ref().unwrap_or(&ctx.stores);

    let (content_ref, encoding, original_size) = stores
        .objects
        .write_compressed(&raw_bytes, Some(media_type.clone()), COMPRESSION_THRESHOLD)
        .context("Failed to write object")?;

    let prefix = stores
        .records
        .get_project_prefix()
        .context("Failed to get project prefix")?;
    let record_id = new_record_id(&prefix);

    let title_for_capsule = params.title.clone();
    let tags_for_capsule = params.tags.clone();

    let payload = RecordCreatedPayload {
        title: params.title.clone(),
        kind: "plan".to_string(),
        content_ref: ContentRefPayload::compressed(
            content_ref.hash.clone(),
            content_ref.size,
            Some(media_type),
            encoding,
            original_size,
        ),
        description: params.description,
        task_id: params.task,
        tags: params.tags,
        scope_type: None,
        scope_id: None,
        retention_class: None,
        producer: None,
    };

    let event = RecordEvent::from_payload(&record_id, "cli", payload);
    stores
        .records
        .apply_event(&event)
        .context("Failed to save record")?;

    let content = String::from_utf8_lossy(&raw_bytes);
    let tags_refs: Vec<&str> = tags_for_capsule.iter().map(|s| s.as_str()).collect();
    let abstract_text = generate_l0_abstract(&title_for_capsule, &content, &tags_refs);
    let record_file_id = format!("record:{record_id}");
    stores
        .upsert_record_chunk(&record_file_id, &abstract_text)
        .context("Failed to write L0 abstract to FTS")?;
    upsert_domain_lod_l0(
        stores,
        &record_file_id,
        &abstract_text,
        &stores.brain_id,
        "record",
    );

    let uri = SynapseUri::for_record(&stores.brain_name, &record_id).to_string();

    if ctx.json {
        let mut out = json!({
            "kind": "plan",
            "record_id": record_id,
            "uri": uri,
            "content_hash": content_ref.hash,
            "size": content_ref.size,
        });
        if target_stores.is_some() {
            out["brain_name"] = json!(stores.brain_name.clone());
            out["brain_id"] = json!(stores.brain_id.clone());
        }
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created plan {record_id}");
        if target_stores.is_some() {
            println!("  Brain: {}", stores.brain_name);
        }
        println!("  Title: {}", params.title);
        println!("  URI:   {uri}");
        println!("  Size:  {} B", content_ref.size);
        println!("  Hash:  {}", &content_ref.hash[..16]);
        if !tags_for_capsule.is_empty() {
            println!("  Tags:  {}", tags_for_capsule.join(", "));
        }
    }

    Ok(())
}
