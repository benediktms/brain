//! Periodic poll loops that embed stale tasks and chunks into LanceDB.
//!
//! Called from the daemon watch loop on a 10-second interval. Each poll cycle
//! processes up to 256 items to prevent memory spikes on the first run after
//! `embedded_at` is introduced.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, warn};

use crate::embedder::{Embed, embed_batch_async};
use crate::l0_generate::{generate_episode_l0, generate_procedure_l0};
use crate::ports::{ChunkIndexWriter, ChunkMetaWriter, EmbeddingOps, EmbeddingResetter};
use crate::records::capsule::build_record_capsule;
use crate::tasks::capsule::{build_outcome_capsule, build_task_capsule};
use crate::tasks::queries::{TaskPollRow, get_labels_for_tasks};
use crate::tokens::estimate_tokens;
use crate::uri::SynapseUri;
use brain_persistence::db::Db;
use brain_persistence::db::chunks::ChunkPollRow;
use brain_persistence::db::summaries::SummaryPollRow;

// ── Tasks ───────────────────────────────────────────────────────────────────

/// Poll for tasks whose capsule is stale (updated_at > embedded_at or embedded_at IS NULL).
///
/// Builds task + outcome capsules, batch-embeds them, upserts to LanceDB and
/// SQLite FTS, then marks the tasks as embedded.
///
/// `brain_id` — when non-empty, filters tasks to this brain only; when empty,
/// processes all tasks.
///
/// Returns the number of tasks successfully embedded.
pub async fn poll_stale_tasks(
    db: &Db,
    store: &impl ChunkIndexWriter,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize {
    debug!("embed_poll: scanning stale tasks");

    // ── 1. Fetch stale task rows ─────────────────────────────────────────
    let rows: Vec<TaskPollRow> = match db.find_stale_tasks_for_embedding(brain_id) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to query stale tasks");
            return 0;
        }
    };

    if rows.is_empty() {
        debug!("embed_poll: no stale tasks");
        return 0;
    }

    debug!(count = rows.len(), "embed_poll: embedding stale tasks");

    // ── 2. Fetch labels for each task ────────────────────────────────────
    let task_id_refs: Vec<&str> = rows.iter().map(|r| r.task_id.as_str()).collect();

    let label_map: std::collections::HashMap<String, Vec<String>> =
        match db.with_read_conn(|conn| get_labels_for_tasks(conn, &task_id_refs)) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "embed_poll: failed to fetch labels for stale tasks");
                std::collections::HashMap::new()
            }
        };

    // ── 3. Build capsule texts ────────────────────────────────────────────
    struct CapsuleEntry {
        task_id: String,
        title: String,
        file_id: String,
        capsule_text: String,
    }

    let mut capsules: Vec<CapsuleEntry> = Vec::new();

    for row in &rows {
        let labels = label_map.get(&row.task_id).cloned().unwrap_or_default();
        let capsule_text = build_task_capsule(
            &row.title,
            row.description.as_deref(),
            &labels,
            row.priority,
        );
        let file_id = format!("task:{}", row.task_id);
        capsules.push(CapsuleEntry {
            task_id: row.task_id.clone(),
            title: row.title.clone(),
            file_id,
            capsule_text,
        });

        // Outcome capsule for terminal tasks
        let status = row.status.as_str();
        if status == "done" || status == "cancelled" {
            let reason = if status == "cancelled" {
                row.blocked_reason.as_deref()
            } else {
                None
            };
            let outcome_text = build_outcome_capsule(&row.title, reason);
            let outcome_file_id = format!("task-outcome:{}", row.task_id);
            capsules.push(CapsuleEntry {
                task_id: row.task_id.clone(),
                title: row.title.clone(),
                file_id: outcome_file_id,
                capsule_text: outcome_text,
            });
        }
    }

    // ── 4. Batch embed ────────────────────────────────────────────────────
    let texts: Vec<String> = capsules.iter().map(|c| c.capsule_text.clone()).collect();

    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed task capsules");
            return 0;
        }
    };

    // ── 5. Upsert to LanceDB + SQLite FTS ────────────────────────────────
    let mut embedded_task_ids: HashSet<String> = HashSet::new();

    for (entry, embedding) in capsules.iter().zip(embeddings.iter()) {
        // LanceDB upsert
        if let Err(e) = store
            .upsert_chunks(
                &entry.file_id,
                &entry.title,
                brain_id,
                &[(0, entry.capsule_text.as_str())],
                std::slice::from_ref(embedding),
            )
            .await
        {
            warn!(
                task_id = %entry.task_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for task capsule"
            );
            continue;
        }

        // SQLite FTS upsert via ChunkMetaWriter port
        if let Err(e) = db.upsert_task_chunk(&entry.file_id, &entry.capsule_text, brain_id) {
            warn!(
                task_id = %entry.task_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: SQLite FTS upsert failed for task capsule"
            );
            continue;
        }

        // L0 LOD upsert via shared helper (uses chunk_id format with :0 suffix).
        crate::tasks::capsule::upsert_task_lod_l0(
            db,
            &entry.file_id,
            &entry.capsule_text,
            brain_id,
        );

        // Track unique task IDs (task + outcome capsule both count once)
        embedded_task_ids.insert(entry.task_id.clone());
    }

    // ── 6. Mark embedded ─────────────────────────────────────────────────
    if !embedded_task_ids.is_empty() {
        let ids_ref: Vec<&str> = embedded_task_ids.iter().map(|s| s.as_str()).collect();
        if let Err(e) = db.mark_tasks_embedded(&ids_ref) {
            warn!(error = %e, "embed_poll: failed to mark tasks as embedded");
        }
    }

    let count = embedded_task_ids.len();
    debug!(count, "embed_poll: tasks embedded");
    count
}

// ── Chunks ──────────────────────────────────────────────────────────────────

/// Poll for file chunks that have not yet been embedded into LanceDB.
///
/// Batch-embeds up to 256 chunks, upserts to LanceDB, then marks them
/// as embedded in SQLite.
///
/// Returns the number of chunks successfully embedded.
pub async fn poll_stale_chunks(
    db: &Db,
    store: &impl ChunkIndexWriter,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize {
    debug!("embed_poll: scanning stale chunks");
    let rows: Vec<ChunkPollRow> = match db.find_stale_chunks_for_embedding(brain_id) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to query stale chunks");
            return 0;
        }
    };

    if rows.is_empty() {
        debug!("embed_poll: no stale chunks");
        return 0;
    }

    debug!(count = rows.len(), "embed_poll: embedding stale chunks");

    // ── Batch embed ───────────────────────────────────────────────────────
    let texts: Vec<String> = rows.iter().map(|r| r.content.clone()).collect();
    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed chunks");
            return 0;
        }
    };

    // ── Group by file_id for upsert ───────────────────────────────────────
    // LanceDB upsert works per-file. We group chunks, upsert each file's
    // chunks as a batch, then mark all chunk_ids as embedded.
    use std::collections::HashMap;

    struct FileGroup<'a> {
        file_path: &'a str,
        chunks: Vec<(usize, &'a str)>,
        embeddings: Vec<Vec<f32>>,
        chunk_ids: Vec<&'a str>,
    }

    let mut file_groups: HashMap<&str, FileGroup<'_>> = HashMap::new();

    for (row, embedding) in rows.iter().zip(embeddings.iter()) {
        let group = file_groups
            .entry(row.file_id.as_str())
            .or_insert_with(|| FileGroup {
                file_path: row.file_path.as_str(),
                chunks: Vec::new(),
                embeddings: Vec::new(),
                chunk_ids: Vec::new(),
            });
        group
            .chunks
            .push((row.chunk_ord as usize, row.content.as_str()));
        group.embeddings.push(embedding.clone());
        group.chunk_ids.push(row.chunk_id.as_str());
    }

    // ── Upsert each file group ────────────────────────────────────────────
    let mut embedded_chunk_ids: Vec<&str> = Vec::new();

    for (file_id, group) in &file_groups {
        if let Err(e) = store
            .upsert_chunks(
                file_id,
                group.file_path,
                brain_id,
                &group.chunks,
                &group.embeddings,
            )
            .await
        {
            warn!(
                file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for file chunks"
            );
            continue;
        }
        embedded_chunk_ids.extend_from_slice(&group.chunk_ids);
    }

    // ── Mark embedded via ChunkMetaWriter port ────────────────────────────
    if !embedded_chunk_ids.is_empty() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if let Err(e) = ChunkMetaWriter::mark_chunks_embedded(db, &embedded_chunk_ids, now) {
            warn!(error = %e, "embed_poll: failed to mark chunks as embedded");
        }
    }

    let count = embedded_chunk_ids.len();
    debug!(count, "embed_poll: chunks embedded");
    count
}

// ── Records ─────────────────────────────────────────────────────────────────

/// Poll for records whose capsule is stale (searchable=1, active, and
/// `updated_at > embedded_at` or `embedded_at IS NULL`).
///
/// Builds record capsules, batch-embeds them, upserts to LanceDB and
/// SQLite FTS, then marks the records as embedded.
///
/// `brain_id` — when non-empty, filters records to this brain only; when
/// empty, processes all records.
///
/// Returns the number of records successfully embedded.
pub async fn poll_stale_records(
    db: &Db,
    store: &impl ChunkIndexWriter,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize {
    debug!("embed_poll: scanning stale records");

    let rows = match db.find_stale_records_for_embedding(brain_id) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to query stale records");
            return 0;
        }
    };

    if rows.is_empty() {
        debug!("embed_poll: no stale records");
        return 0;
    }

    debug!(count = rows.len(), "embed_poll: embedding stale records");

    // ── 2. Fetch tags for each record ────────────────────────────────────
    let record_id_refs: Vec<&str> = rows.iter().map(|r| r.record_id.as_str()).collect();
    let record_ids_owned: Vec<String> = record_id_refs.iter().map(|s| s.to_string()).collect();

    let tag_map: std::collections::HashMap<String, Vec<String>> =
        match db.with_read_conn(move |conn| {
            let refs: Vec<&str> = record_ids_owned.iter().map(String::as_str).collect();
            brain_persistence::db::records::queries::get_tags_for_records(conn, &refs)
        }) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "embed_poll: failed to fetch tags for stale records");
                std::collections::HashMap::new()
            }
        };

    // ── 3. Build capsule texts ───────────────────────────────────────────
    struct CapsuleEntry {
        record_id: String,
        title: String,
        file_id: String,
        capsule_text: String,
    }

    let mut capsules: Vec<CapsuleEntry> = Vec::new();

    for row in &rows {
        let tags = tag_map.get(&row.record_id).cloned().unwrap_or_default();
        let capsule_text =
            build_record_capsule(&row.title, &row.kind, row.description.as_deref(), &tags);
        let file_id = format!("record:{}", row.record_id);
        capsules.push(CapsuleEntry {
            record_id: row.record_id.clone(),
            title: row.title.clone(),
            file_id,
            capsule_text,
        });
    }

    // ── 4. Batch embed ───────────────────────────────────────────────────
    let texts: Vec<String> = capsules.iter().map(|c| c.capsule_text.clone()).collect();

    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed record capsules");
            return 0;
        }
    };

    // ── 5. Upsert to LanceDB + SQLite FTS ───────────────────────────────
    let mut embedded_record_ids: HashSet<String> = HashSet::new();

    for (entry, embedding) in capsules.iter().zip(embeddings.iter()) {
        // LanceDB upsert
        if let Err(e) = store
            .upsert_chunks(
                &entry.file_id,
                &entry.title,
                brain_id,
                &[(0, entry.capsule_text.as_str())],
                std::slice::from_ref(embedding),
            )
            .await
        {
            warn!(
                record_id = %entry.record_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for record capsule"
            );
            continue;
        }

        // SQLite FTS upsert via ChunkMetaWriter port
        if let Err(e) = db.upsert_record_chunk(&entry.file_id, &entry.capsule_text, brain_id) {
            warn!(
                record_id = %entry.record_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: SQLite FTS upsert failed for record capsule"
            );
            continue;
        }

        embedded_record_ids.insert(entry.record_id.clone());
    }

    // ── 6. Mark embedded ─────────────────────────────────────────────────
    if !embedded_record_ids.is_empty() {
        let ids_owned: Vec<String> = embedded_record_ids.iter().cloned().collect();
        let ids_ref: Vec<&str> = ids_owned.iter().map(String::as_str).collect();
        if let Err(e) = db.mark_records_embedded(&ids_ref) {
            warn!(error = %e, "embed_poll: failed to mark records as embedded");
        }
    }

    let count = embedded_record_ids.len();
    debug!(count, "embed_poll: records embedded");
    count
}

// ── Shared LOD helper ──────────────────────────────────────────────────────

/// Best-effort L0 LOD upsert for a domain capsule (records, etc.).
///
/// Uses the chunk_id format (`{file_id}:0`) for the URI to match the lookup
/// path in `lod_resolver::build_object_uri`.
pub fn upsert_domain_lod_l0(
    store: &dyn crate::lod::LodChunkStore,
    file_id: &str,
    capsule_text: &str,
    brain_id: &str,
    domain: &str,
) {
    let chunk_id = format!("{file_id}:0");
    let lod_uri = match domain {
        "record" => SynapseUri::for_record(brain_id, &chunk_id).to_string(),
        "episode" => SynapseUri::for_episode(brain_id, &chunk_id).to_string(),
        "reflection" => SynapseUri::for_reflection(brain_id, &chunk_id).to_string(),
        "procedure" => SynapseUri::for_procedure(brain_id, &chunk_id).to_string(),
        _ => SynapseUri::for_memory(brain_id, &chunk_id).to_string(),
    };
    let source_hash = crate::utils::content_hash(capsule_text);
    let token_est = estimate_tokens(capsule_text) as i64;
    if let Err(e) = store.upsert_lod_chunk(&crate::lod::UpsertLodChunk {
        object_uri: &lod_uri,
        brain_id,
        lod_level: crate::lod::LodLevel::L0,
        content: capsule_text,
        token_est: Some(token_est),
        method: crate::lod::LodMethod::Extractive,
        model_id: None,
        source_hash: &source_hash,
        expires_at: None,
        job_id: None,
    }) {
        warn!(
            file_id = %file_id,
            domain = %domain,
            error = %e,
            "embed_poll: LOD L0 upsert failed"
        );
    }
}

pub async fn poll_stale_summaries<D>(
    db: &D,
    store: &impl crate::ports::SummaryStoreWriter,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize
where
    D: crate::ports::EmbeddingOps
        + crate::lod::LodChunkStore
        + crate::ports::JobQueue
        + Send
        + Sync,
{
    debug!("embed_poll: scanning stale summaries");

    let rows: Vec<SummaryPollRow> = match db.find_stale_summaries_for_embedding(brain_id) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to query stale summaries");
            return 0;
        }
    };

    if rows.is_empty() {
        debug!("embed_poll: no stale summaries");
        return 0;
    }

    debug!(count = rows.len(), "embed_poll: embedding stale summaries");

    let texts: Vec<String> = rows.iter().map(|r| r.content.clone()).collect();
    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed summaries");
            return 0;
        }
    };

    let mut embedded_summary_ids: HashSet<String> = HashSet::new();

    for (row, embedding) in rows.iter().zip(embeddings.iter()) {
        if let Err(e) = store
            .upsert_summary(&row.summary_id, &row.content, brain_id, embedding)
            .await
        {
            warn!(
                summary_id = %row.summary_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for summary"
            );
            continue;
        }
        // For episodes, generate L0 and upsert to lod_chunks
        if row.kind == "episode" {
            let l0_content = generate_episode_l0(&row.content);
            if !l0_content.is_empty() {
                // Use summary_id as the chunk_id (with :0 suffix)
                let file_id = format!("summary:{}", row.summary_id);
                upsert_domain_lod_l0(db, &file_id, &l0_content, brain_id, "episode");
            }

            // Enqueue L1 if content > 300 chars
            if row.content.len() > 300 {
                let file_id = format!("summary:{}", row.summary_id);
                let chunk_id = format!("{file_id}:0");
                let lod_uri = SynapseUri::for_episode(brain_id, &chunk_id).to_string();
                let source_hash = crate::utils::content_hash(&row.content);
                if let Err(e) = crate::pipeline::job_worker::enqueue_l1_summarize(
                    db,
                    &lod_uri,
                    brain_id,
                    &row.content,
                    &source_hash,
                ) {
                    warn!(
                        error = %e,
                        summary_id = %row.summary_id,
                        "embed_poll: failed to enqueue L1 for episode"
                    );
                }
            }
        }
        // For reflections, generate L0 and upsert to lod_chunks
        if row.kind == "reflection" {
            let title = row.title.as_deref().unwrap_or("");
            let l0_content = crate::l0_generate::generate_reflection_l0(title, &row.content);
            if !l0_content.is_empty() {
                let file_id = format!("summary:{}", row.summary_id);
                upsert_domain_lod_l0(db, &file_id, &l0_content, brain_id, "reflection");
            }

            // Enqueue L1 if content > 300 chars
            if row.content.len() > 300 {
                let file_id = format!("summary:{}", row.summary_id);
                let chunk_id = format!("{file_id}:0");
                let lod_uri = SynapseUri::for_reflection(brain_id, &chunk_id).to_string();
                let source_hash = crate::utils::content_hash(&row.content);
                if let Err(e) = crate::pipeline::job_worker::enqueue_l1_summarize(
                    db,
                    &lod_uri,
                    brain_id,
                    &row.content,
                    &source_hash,
                ) {
                    warn!(
                        error = %e,
                        summary_id = %row.summary_id,
                        "embed_poll: failed to enqueue L1 for reflection"
                    );
                }
            }
        }
        // For procedures, generate L0 and upsert to lod_chunks
        if row.kind == "procedure" {
            let title = row.title.as_deref().unwrap_or("");
            let l0_content = generate_procedure_l0(title, &row.content);
            if !l0_content.is_empty() {
                let file_id = format!("summary:{}", row.summary_id);
                upsert_domain_lod_l0(db, &file_id, &l0_content, brain_id, "procedure");
            }

            // Enqueue L1 if content > 300 chars
            if row.content.len() > 300 {
                let file_id = format!("summary:{}", row.summary_id);
                let chunk_id = format!("{file_id}:0");
                let lod_uri = SynapseUri::for_procedure(brain_id, &chunk_id).to_string();
                let source_hash = crate::utils::content_hash(&row.content);
                if let Err(e) = crate::pipeline::job_worker::enqueue_l1_summarize(
                    db,
                    &lod_uri,
                    brain_id,
                    &row.content,
                    &source_hash,
                ) {
                    warn!(
                        error = %e,
                        summary_id = %row.summary_id,
                        "embed_poll: failed to enqueue L1 for procedure"
                    );
                }
            }
        }

        embedded_summary_ids.insert(row.summary_id.clone());
    }

    if !embedded_summary_ids.is_empty() {
        let ids_owned: Vec<String> = embedded_summary_ids.iter().cloned().collect();
        let ids_ref: Vec<&str> = ids_owned.iter().map(String::as_str).collect();
        if let Err(e) = db.mark_summaries_embedded(&ids_ref) {
            warn!(error = %e, "embed_poll: failed to mark summaries as embedded");
        }
    }

    let count = embedded_summary_ids.len();
    debug!(count, "embed_poll: summaries embedded");
    count
}

// ── Self-heal ────────────────────────────────────────────────────────────────

/// Check if LanceDB is accessible. If not, reset all `embedded_at` columns so
/// items will be re-embedded on the next poll cycle.
///
/// Returns `true` if a reset occurred (LanceDB was missing/inaccessible).
pub async fn self_heal_if_lance_missing(
    resetter: &(impl EmbeddingResetter + ?Sized),
    store: &(impl crate::ports::SchemaMeta + ?Sized),
) -> bool {
    // Use schema() as a lightweight accessibility probe — it sends a trivial
    // request to the underlying table handle.
    if store.current_schema_matches_expected().await {
        return false;
    }

    warn!("LanceDB not found — resetting embedded_at for full re-embed");

    if let Err(e) = resetter.reset_tasks_embedded_at() {
        warn!(error = %e, "embed_poll: failed to reset tasks.embedded_at");
    }

    if let Err(e) = resetter.reset_chunks_embedded_at() {
        warn!(error = %e, "embed_poll: failed to reset chunks.embedded_at");
    }

    if let Err(e) = resetter.reset_records_embedded_at() {
        warn!(error = %e, "embed_poll: failed to reset records.embedded_at");
    }

    true
}
