//! `RecordStore` — SQLite-backed records repository.
//!
//! SQLite is the sole source of truth. Public methods append validated events
//! to the projection or read from it. Persistence row types stay behind this
//! boundary: query helpers in `crate::queries` return `RecordRow`, this store
//! wraps them and (when applicable) maps to domain types from `crate::domain`.

use brain_persistence::db::Db;
use brain_persistence::sql::SqlResultExt;

use brain_core::error::Result;

use crate::domain::{ContentRef, Record, RecordKind, RecordLink, RecordStatus};
use crate::events;
use crate::objects;
use crate::projections;
use crate::queries;

/// Domain-level query for listing records. Replaces the persistence
/// `RecordFilter` shape on the public surface of `RecordStore`. The store
/// maps `RecordQuery` → `RecordFilter` internally.
#[derive(Debug, Clone, Default)]
pub struct RecordQuery {
    /// Filter by record kind (typed). `None` = no filter.
    pub kind: Option<RecordKind>,
    /// Filter by lifecycle status (typed). `None` = no filter.
    pub status: Option<RecordStatus>,
    /// Filter by tag (exact match on a single tag).
    pub tag: Option<String>,
    /// Filter by linked task ID.
    pub task_id: Option<String>,
    /// Result row limit. `None` = no limit.
    pub limit: Option<usize>,
}

/// Parameters for `RecordStore::create_*` methods.
#[derive(Debug, Clone)]
pub struct CreateRecordParams {
    pub title: String,
    pub description: Option<String>,
    /// Raw payload bytes. The store writes them to the object store
    /// (compressing past `objects::COMPRESSION_THRESHOLD`) and constructs the
    /// `ContentRefPayload` internally — callers do not see persistence event
    /// types.
    pub body: Vec<u8>,
    pub media_type: Option<String>,
    pub task_id: Option<String>,
    pub tags: Vec<String>,
    pub scope_type: Option<String>,
    pub scope_id: Option<String>,
    pub retention_class: Option<String>,
    pub producer: Option<String>,
    /// Actor stamped onto the event (e.g. `"cli"`, `"mcp"`, agent name).
    pub actor: String,
}

/// The record store: SQLite is the sole source of truth.
#[derive(Clone)]
pub struct RecordStore {
    db: Db,
    /// Brain ID that scopes this store's reads and writes.
    ///
    /// Empty string means "all brains" (legacy / single-brain mode).
    /// Non-empty means filter reads by this brain_id and stamp it on writes.
    pub(crate) brain_id: String,
}

impl RecordStore {
    /// Create a new `RecordStore`.
    pub fn new(db: Db) -> Self {
        Self {
            db,
            brain_id: String::new(),
        }
    }

    /// Create a new `RecordStore` with an explicit brain_id scope.
    pub fn with_brain_id(db: Db, brain_id: &str, brain_name: &str) -> Result<Self> {
        // Ensure the brain is registered — FK on brain_id requires it.
        if !brain_id.is_empty() {
            db.ensure_brain_registered(brain_id, brain_name)?;
        }
        Ok(Self {
            db,
            brain_id: brain_id.to_string(),
        })
    }

    /// Test-only accessor for the underlying `Db` handle. Production code
    /// must use the inherent delegation methods on `RecordStore` (or call
    /// methods through port traits implemented on `Db`).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn db_for_tests(&self) -> &Db {
        &self.db
    }

    /// Resolve a brain by name, brain_id, alias, or root path.
    pub fn resolve_brain(&self, input: &str) -> Result<(String, String)> {
        self.db.resolve_brain(input)
    }

    /// Check whether a brain has been archived.
    pub fn is_brain_archived(&self, brain_id: &str) -> Result<bool> {
        self.db.is_brain_archived(brain_id)
    }

    /// Construct a `RecordStore` scoped to a different brain on the same
    /// underlying database. Used for cross-brain writes from CLI/MCP.
    pub fn with_remote_brain_id(&self, brain_id: &str, brain_name: &str) -> Result<Self> {
        Self::with_brain_id(self.db.clone(), brain_id, brain_name)
    }

    /// Returns the brain ID this store is scoped to. Empty string for unscoped/legacy mode.
    pub fn brain_id(&self) -> &str {
        &self.brain_id
    }

    /// Import events from a JSONL file into the unified SQLite database.
    ///
    /// Reads the given path as a JSONL event log and replays all events into
    /// SQLite via `projections::apply_event`. Events that already exist (by
    /// `event_id`) or would violate constraints are silently skipped to make
    /// this safe to call multiple times (idempotent).
    ///
    /// This is intended for migration from per-brain JSONL event logs into
    /// the unified `~/.brain/brain.db`.
    pub fn import_from_jsonl(&self, path: &std::path::Path) -> Result<usize> {
        if !path.exists() {
            return Ok(0);
        }
        let all_events = events::read_all_events(path)?;
        if all_events.is_empty() {
            return Ok(0);
        }
        let brain_id = self.brain_id.clone();
        let mut imported = 0usize;
        self.db
            .with_write_conn(|conn| {
                for event in &all_events {
                    match projections::apply_event(conn, event, &brain_id) {
                        Ok(()) => imported += 1,
                        Err(e) => {
                            tracing::debug!(
                                event_id = %event.event_id,
                                "skipping record event during import: {e}"
                            );
                        }
                    }
                }
                Ok(())
            })
            .into_brain_core()?;
        Ok(imported)
    }

    /// Apply a single event to the SQLite projection.
    pub fn apply_event(&self, event: &events::RecordEvent) -> Result<()> {
        let brain_id = self.brain_id.clone();
        self.db
            .with_write_conn(|conn| projections::apply_event(conn, event, &brain_id))
            .into_brain_core()
    }

    // -- Typed record-creation methods --
    //
    // Each `create_*` wraps the boilerplate of:
    //   1. allocate a new record ID using the brain's project prefix,
    //   2. write the body to the object store (compressing past the
    //      threshold),
    //   3. construct `RecordCreatedPayload` + `ContentRefPayload` internally,
    //   4. wrap in a `RecordEvent` with the caller's `actor`,
    //   5. apply the event, then
    //   6. reload + return the typed `Record`.
    //
    // CLI / MCP callers should prefer these over hand-rolled
    // `RecordEvent::from_payload(...)` + `apply_event(...)` so the wire-format
    // event types do not leak into application code.

    /// Create a document record.
    pub fn create_document(
        &self,
        params: CreateRecordParams,
        objects: &objects::ObjectStore,
    ) -> Result<Record> {
        self.create_with_kind(params, "document", objects)
    }

    /// Create an analysis record.
    pub fn create_analysis(
        &self,
        params: CreateRecordParams,
        objects: &objects::ObjectStore,
    ) -> Result<Record> {
        self.create_with_kind(params, "analysis", objects)
    }

    /// Create a plan record.
    pub fn create_plan(
        &self,
        params: CreateRecordParams,
        objects: &objects::ObjectStore,
    ) -> Result<Record> {
        self.create_with_kind(params, "plan", objects)
    }

    /// Create a snapshot record.
    pub fn create_snapshot(
        &self,
        params: CreateRecordParams,
        objects: &objects::ObjectStore,
    ) -> Result<Record> {
        self.create_with_kind(params, "snapshot", objects)
    }

    /// # Crash recovery
    ///
    /// The blob is written to the object store BEFORE the `RecordCreated` event
    /// is applied. If `apply_event` fails (FK violation, transaction error), the
    /// blob remains on disk with no projection row referencing it (a "stale
    /// blob"). This is the safe direction — no data loss. Running
    /// `brain records gc` (which calls `integrity::cleanup_orphans`) detects and
    /// reclaims the stale blob on next sweep.
    fn create_with_kind(
        &self,
        params: CreateRecordParams,
        kind: &str,
        objects: &objects::ObjectStore,
    ) -> Result<Record> {
        if params.actor.trim().is_empty() {
            return Err(brain_core::error::BrainCoreError::Config(
                "actor must be non-empty".into(),
            ));
        }
        if params.title.trim().is_empty() {
            return Err(brain_core::error::BrainCoreError::Config(
                "title must be non-empty".into(),
            ));
        }

        let prefix = self.get_project_prefix()?;
        let record_id = events::new_record_id(&prefix);

        // Clone fields we need after params is partially moved into payload.
        let title = params.title.clone();
        let description = params.description.clone();
        let task_id = params.task_id.clone();
        let actor = params.actor.clone();
        let retention_class = params.retention_class.clone();

        let (content_ref, encoding, original_size) = objects
            .write_compressed(
                &params.body,
                params.media_type.clone(),
                objects::COMPRESSION_THRESHOLD,
            )
            .map_err(|e| {
                brain_core::error::BrainCoreError::Internal(format!(
                    "create_{kind}: write object: {e}"
                ))
            })?;

        let domain_content_ref = ContentRef {
            hash: content_ref.hash.clone(),
            size: content_ref.size,
            media_type: content_ref.media_type.clone(),
            content_encoding: encoding.clone(),
            original_size: Some(original_size),
        };

        let content_ref_payload = events::ContentRefPayload::compressed(
            content_ref.hash,
            content_ref.size,
            params.media_type,
            encoding,
            original_size,
        );

        let payload = events::RecordCreatedPayload {
            title: params.title,
            kind: kind.to_string(),
            content_ref: content_ref_payload,
            description: params.description,
            task_id: params.task_id,
            tags: params.tags,
            scope_type: params.scope_type,
            scope_id: params.scope_id,
            retention_class: params.retention_class,
            producer: params.producer,
        };

        let event = events::RecordEvent::from_payload(&record_id, &actor, payload);
        let event_timestamp = event.timestamp;
        self.apply_event(&event)?;

        Ok(Record {
            record_id,
            title,
            kind: RecordKind::from(kind),
            status: RecordStatus::Active,
            description,
            content_ref: domain_content_ref,
            task_id,
            actor,
            created_at: event_timestamp,
            updated_at: event_timestamp,
            retention_class,
            pinned: false,
            payload_available: true,
            trust: "untrusted".to_string(),
            source_tool: None,
        })
    }

    // -- Query methods --

    /// Get a single record by ID, mapped to the domain `Record` type.
    ///
    /// `RecordRow` (the persistence wire shape) is kept behind this boundary
    /// — callers receive a typed `Record` with parsed `status` and bundled
    /// `content_ref`.
    pub fn get_record(&self, record_id: &str) -> Result<Option<Record>> {
        self.db
            .with_read_conn(|conn| queries::get_record(conn, record_id))
            .into_brain_core()
            .map(|opt| opt.map(Record::from))
    }

    /// Return `Some(brain_id)` when this store is scoped to a specific brain,
    /// or `None` when it operates in unscoped mode.
    fn brain_id_filter(&self) -> Option<String> {
        if self.brain_id.is_empty() {
            None
        } else {
            Some(self.brain_id.clone())
        }
    }

    /// List records matching a query, mapped to the domain `Record` type.
    pub fn list_records(&self, query: &RecordQuery) -> Result<Vec<Record>> {
        let filter = crate::queries::RecordFilter {
            brain_id: self.brain_id_filter(),
            kind: query.kind.as_ref().map(|k| k.as_str().to_string()),
            status: query.status.as_ref().map(|s| s.as_str().to_string()),
            tag: query.tag.clone(),
            task_id: query.task_id.clone(),
            limit: query.limit,
        };
        self.db
            .with_read_conn(|conn| crate::queries::list_records(conn, &filter))
            .into_brain_core()
            .map(|rows| rows.into_iter().map(Record::from).collect())
    }

    pub fn get_record_tags(&self, record_id: &str) -> Result<Vec<String>> {
        self.db
            .with_read_conn(|conn| queries::get_record_tags(conn, record_id))
            .into_brain_core()
    }

    pub fn get_record_links(&self, record_id: &str) -> Result<Vec<RecordLink>> {
        self.db
            .with_read_conn(|conn| queries::get_record_links(conn, record_id))
            .into_brain_core()
            .map(|rows| rows.into_iter().map(RecordLink::from).collect())
    }

    pub fn resolve_record_id(&self, input: &str) -> Result<String> {
        let brain_id = self.brain_id.clone();
        self.db
            .with_read_conn(|conn| queries::resolve_record_id(conn, input, &brain_id))
            .into_brain_core()
    }

    pub fn compact_record_id(&self, record_id: &str) -> Result<String> {
        self.db
            .with_read_conn(|conn| queries::compact_record_id(conn, record_id))
            .into_brain_core()
    }

    pub fn compact_record_ids(&self) -> Result<std::collections::HashMap<String, String>> {
        self.db
            .with_read_conn(queries::compact_record_ids)
            .into_brain_core()
    }

    /// Get the project prefix for record ID generation.
    ///
    /// Reads from `brains.prefix` (per-brain column). If `brain_id` is not
    /// set or the prefix is missing/invalid, returns an error.
    pub fn get_project_prefix(&self) -> Result<String> {
        if !self.brain_id.is_empty() {
            let brain_id = self.brain_id.clone();
            let result = self
                .db
                .with_read_conn(|conn| {
                    let prefix: Option<String> = conn
                        .query_row(
                            "SELECT prefix FROM brains WHERE brain_id = ?1",
                            [&brain_id],
                            |row| row.get::<_, Option<String>>(0),
                        )
                        .ok()
                        .flatten();
                    Ok(prefix)
                })
                .into_brain_core()?;
            if let Some(ref prefix) =
                result.filter(|p| p.len() == 3 && p.chars().all(|c| c.is_ascii_uppercase()))
            {
                return Ok(prefix.clone());
            }
            return Err(brain_core::error::BrainCoreError::Config(
                "brains.prefix not set for this brain".into(),
            ));
        }
        // Unscoped/legacy mode: fall back to brain_meta
        self.db
            .with_write_conn(|conn| {
                brain_persistence::db::meta::get_or_init_project_prefix(
                    conn,
                    std::path::Path::new("."),
                )
            })
            .into_brain_core()
    }

    pub fn get_all_content_refs(&self) -> Result<Vec<(String, String, bool)>> {
        self.db
            .with_read_conn(queries::get_all_content_refs)
            .into_brain_core()
    }

    pub fn count_payload_refs(&self, content_hash: &str, exclude_record_id: &str) -> Result<i64> {
        self.db
            .with_read_conn(|conn| {
                queries::count_payload_refs(conn, content_hash, exclude_record_id)
            })
            .into_brain_core()
    }

    // -- Eviction, pinning, and retention class methods --

    /// Evict a record's payload blob from the object store.
    ///
    /// Validates:
    /// - Record exists
    /// - `payload_available == true` (not already evicted)
    /// - `pinned == false` (pinned records cannot be evicted)
    ///
    /// Uses ref-counting: only deletes the blob if no OTHER records reference
    /// the same content_hash with payload_available = 1.
    ///
    /// Appends a `PayloadEvicted` event and updates the projection.
    ///
    /// # Crash recovery
    ///
    /// The event is committed **before** the blob is deleted. This is
    /// intentional: if the process crashes between the two operations the
    /// projection will show `payload_available = false` while the blob still
    /// exists on disk (a "stale flag"). This is the safe direction — no data
    /// is lost. Running `brain records gc` (which calls
    /// `crate::integrity::cleanup_orphans`) will detect and remove the stale
    /// blob.
    pub fn evict_payload(
        &self,
        record_id: &str,
        reason: &str,
        actor: &str,
        objects: &objects::ObjectStore,
    ) -> Result<()> {
        let record = self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        if !record.payload_available {
            return Err(brain_core::error::BrainCoreError::RecordEvent(
                "payload already evicted".to_string(),
            ));
        }
        if record.pinned {
            return Err(brain_core::error::BrainCoreError::RecordEvent(
                "cannot evict pinned record".to_string(),
            ));
        }

        let payload = events::PayloadEvictedPayload {
            content_hash: record.content_ref.hash.clone(),
            reason: reason.to_string(),
        };
        let event = events::RecordEvent::from_payload(record_id, actor, payload);
        self.apply_event(&event)?;

        let other_refs = self.count_payload_refs(&record.content_ref.hash, record_id)?;
        if other_refs == 0 && objects.exists(&record.content_ref.hash) {
            objects.delete(&record.content_ref.hash)?;
        }

        Ok(())
    }

    /// Set or clear the retention class for a record.
    pub fn set_retention_class(
        &self,
        record_id: &str,
        retention_class: Option<&str>,
        actor: &str,
    ) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let payload = events::RetentionClassSetPayload {
            retention_class: retention_class.map(|s| s.to_string()),
        };
        let event = events::RecordEvent::from_payload(record_id, actor, payload);
        self.apply_event(&event)
    }

    /// Pin a record, preventing it from being evicted.
    pub fn pin_record(&self, record_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = events::RecordEvent::new(
            record_id,
            actor,
            events::RecordEventType::RecordPinned,
            &events::PinPayload {},
        );
        self.apply_event(&event)
    }

    /// Unpin a record, allowing it to be evicted again.
    pub fn unpin_record(&self, record_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = events::RecordEvent::new(
            record_id,
            actor,
            events::RecordEventType::RecordUnpinned,
            &events::PinPayload {},
        );
        self.apply_event(&event)
    }

    // -- Typed mutation methods --

    /// Mark a record as archived.
    ///
    /// Idempotent at the **status level**: calling on an already-archived
    /// record leaves `status = "archived"` unchanged. Every call appends a
    /// new `RecordArchived` event to the audit log and bumps `updated_at`,
    /// so callers that need true single-shot semantics must guard externally.
    pub fn archive_record(
        &self,
        record_id: &str,
        reason: Option<String>,
        actor: &str,
    ) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::from_payload(
            record_id,
            actor,
            crate::events::RecordArchivedPayload { reason },
        );
        self.apply_event(&event)
    }

    /// Update mutable record metadata. `title` and `description` are applied
    /// only when `Some(_)`. Empty title or empty description is rejected.
    pub fn update_record(
        &self,
        record_id: &str,
        title: Option<String>,
        description: Option<String>,
        actor: &str,
    ) -> Result<()> {
        if let Some(ref t) = title
            && t.trim().is_empty()
        {
            return Err(brain_core::error::BrainCoreError::Config(
                "title must be non-empty".into(),
            ));
        }
        if let Some(ref d) = description
            && d.trim().is_empty()
        {
            return Err(brain_core::error::BrainCoreError::Config(
                "description must be non-empty (use None to skip the update instead)".into(),
            ));
        }
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let payload = crate::events::RecordUpdatedPayload { title, description };
        let event = crate::events::RecordEvent::from_payload(record_id, actor, payload);
        self.apply_event(&event)
    }

    /// Add a tag to a record. Idempotent.
    pub fn add_tag(&self, record_id: &str, tag: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::new(
            record_id,
            actor,
            crate::events::RecordEventType::TagAdded,
            &crate::events::TagPayload {
                tag: tag.to_string(),
            },
        );
        self.apply_event(&event)
    }

    /// Remove a tag from a record. Idempotent.
    pub fn remove_tag(&self, record_id: &str, tag: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::new(
            record_id,
            actor,
            crate::events::RecordEventType::TagRemoved,
            &crate::events::TagPayload {
                tag: tag.to_string(),
            },
        );
        self.apply_event(&event)
    }

    /// Link a record to a task. Idempotent.
    pub fn link_task(&self, record_id: &str, task_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::new(
            record_id,
            actor,
            crate::events::RecordEventType::LinkAdded,
            &crate::events::LinkPayload {
                task_id: Some(task_id.to_string()),
                chunk_id: None,
            },
        );
        self.apply_event(&event)
    }

    /// Unlink a record from a task. Idempotent.
    pub fn unlink_task(&self, record_id: &str, task_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::new(
            record_id,
            actor,
            crate::events::RecordEventType::LinkRemoved,
            &crate::events::LinkPayload {
                task_id: Some(task_id.to_string()),
                chunk_id: None,
            },
        );
        self.apply_event(&event)
    }

    /// Link a record to a chunk (memory chunk). Idempotent.
    pub fn link_chunk(&self, record_id: &str, chunk_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::new(
            record_id,
            actor,
            crate::events::RecordEventType::LinkAdded,
            &crate::events::LinkPayload {
                task_id: None,
                chunk_id: Some(chunk_id.to_string()),
            },
        );
        self.apply_event(&event)
    }

    /// Unlink a record from a chunk. Idempotent.
    pub fn unlink_chunk(&self, record_id: &str, chunk_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?.ok_or_else(|| {
            brain_core::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}"))
        })?;

        let event = crate::events::RecordEvent::new(
            record_id,
            actor,
            crate::events::RecordEventType::LinkRemoved,
            &crate::events::LinkPayload {
                task_id: None,
                chunk_id: Some(chunk_id.to_string()),
            },
        );
        self.apply_event(&event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::*;

    #[test]
    fn test_record_store_new() {
        let db = brain_persistence::db::Db::open_in_memory().unwrap();
        let store = RecordStore::new(db);
        assert!(store.brain_id().is_empty());
    }

    #[test]
    fn test_import_from_jsonl_missing_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = brain_persistence::db::Db::open_in_memory().unwrap();
        let store = RecordStore::with_brain_id(db, "test-brain", "test-brain").unwrap();

        let missing = dir.path().join("nonexistent.jsonl");
        let count = store.import_from_jsonl(&missing).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_record_store_apply_event_and_query() {
        let db = brain_persistence::db::Db::open_in_memory().unwrap();
        let store = RecordStore::new(db);

        let ev = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordCreatedPayload {
                title: "My Artifact".to_string(),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                    10,
                    None,
                ),
                description: None,
                task_id: None,
                tags: vec!["q1".to_string()],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        store.apply_event(&ev).unwrap();

        let row = store.get_record("r1").unwrap();
        assert!(row.is_some(), "r1 should exist after apply_event");
        assert_eq!(row.unwrap().title, "My Artifact");
    }

    // -- Helper for eviction/pin/retention tests --

    fn make_store_with_objects(
        dir: &tempfile::TempDir,
    ) -> (RecordStore, crate::objects::ObjectStore) {
        let db = brain_persistence::db::Db::open_in_memory().unwrap();
        let store = RecordStore::new(db);
        let objects = crate::objects::ObjectStore::new(dir.path().join("objects")).unwrap();
        (store, objects)
    }

    fn create_record_in_store(store: &RecordStore, record_id: &str, content_hash: &str, size: u64) {
        let ev = RecordEvent::from_payload(
            record_id,
            "agent",
            RecordCreatedPayload {
                title: "Test Record".to_string(),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new(content_hash.to_string(), size, None),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        store.apply_event(&ev).unwrap();
    }

    // -- evict_payload tests --

    #[test]
    fn test_evict_payload_basic() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let data = b"hello eviction world";
        let content_ref = objects.write(data).unwrap();
        create_record_in_store(&store, "r1", &content_ref.hash, content_ref.size);

        assert!(objects.exists(&content_ref.hash));
        let row = store.get_record("r1").unwrap().unwrap();
        assert!(row.payload_available);

        store
            .evict_payload("r1", "gc", "gc-agent", &objects)
            .unwrap();

        let row = store.get_record("r1").unwrap().unwrap();
        assert!(!row.payload_available);
        // Blob should be deleted since r1 was the only reference
        assert!(!objects.exists(&content_ref.hash));
    }

    #[test]
    fn test_evict_pinned_record_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let data = b"pinned payload";
        let content_ref = objects.write(data).unwrap();
        create_record_in_store(&store, "r1", &content_ref.hash, content_ref.size);

        store.pin_record("r1", "agent").unwrap();

        let result = store.evict_payload("r1", "gc", "gc-agent", &objects);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot evict pinned record")
        );

        // Blob must still exist
        assert!(objects.exists(&content_ref.hash));
    }

    #[test]
    fn test_evict_already_evicted_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let data = b"evict me twice";
        let content_ref = objects.write(data).unwrap();
        create_record_in_store(&store, "r1", &content_ref.hash, content_ref.size);

        store
            .evict_payload("r1", "gc", "gc-agent", &objects)
            .unwrap();

        let result = store.evict_payload("r1", "gc", "gc-agent", &objects);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("payload already evicted")
        );
    }

    #[test]
    fn test_evict_shared_blob_survives() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let data = b"shared payload data";
        let content_ref = objects.write(data).unwrap();
        // Two records sharing the same hash
        create_record_in_store(&store, "r1", &content_ref.hash, content_ref.size);
        create_record_in_store(&store, "r2", &content_ref.hash, content_ref.size);

        // Evict r1 — blob should survive because r2 still references it
        store
            .evict_payload("r1", "gc", "gc-agent", &objects)
            .unwrap();

        let row1 = store.get_record("r1").unwrap().unwrap();
        assert!(!row1.payload_available);
        let row2 = store.get_record("r2").unwrap().unwrap();
        assert!(row2.payload_available);
        assert!(objects.exists(&content_ref.hash));
    }

    #[test]
    fn test_evict_shared_blob_both_evicted() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let data = b"shared payload both evicted";
        let content_ref = objects.write(data).unwrap();
        create_record_in_store(&store, "r1", &content_ref.hash, content_ref.size);
        create_record_in_store(&store, "r2", &content_ref.hash, content_ref.size);

        // Evict r1 first — blob survives
        store
            .evict_payload("r1", "gc", "gc-agent", &objects)
            .unwrap();
        assert!(objects.exists(&content_ref.hash));

        // Evict r2 — now blob can be deleted
        store
            .evict_payload("r2", "gc", "gc-agent", &objects)
            .unwrap();
        assert!(!objects.exists(&content_ref.hash));
    }

    #[test]
    fn test_evict_nonexistent_record_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let result = store.evict_payload("nonexistent", "gc", "gc-agent", &objects);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("record not found"));
    }

    // -- set_retention_class tests --

    #[test]
    fn test_set_retention_class() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        let row = store.get_record("r1").unwrap().unwrap();
        assert!(row.retention_class.is_none());

        store
            .set_retention_class("r1", Some("permanent"), "agent")
            .unwrap();

        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.retention_class.as_deref(), Some("permanent"));
    }

    #[test]
    fn test_set_retention_class_clear() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        store
            .set_retention_class("r1", Some("ephemeral"), "agent")
            .unwrap();
        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.retention_class.as_deref(), Some("ephemeral"));

        store.set_retention_class("r1", None, "agent").unwrap();
        let row = store.get_record("r1").unwrap().unwrap();
        assert!(row.retention_class.is_none());
    }

    // -- pin/unpin tests --

    // -- create_* typed write API tests --

    fn create_params(title: &str, body: &[u8]) -> CreateRecordParams {
        CreateRecordParams {
            title: title.to_string(),
            description: None,
            body: body.to_vec(),
            media_type: Some("text/plain".to_string()),
            task_id: None,
            tags: vec![],
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
            actor: "test".to_string(),
        }
    }

    #[test]
    fn test_create_document_returns_typed_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let params = create_params("hello document", b"document body");
        let record = store.create_document(params, &objects).unwrap();

        assert_eq!(record.title, "hello document");
        assert_eq!(record.kind, crate::domain::RecordKind::Document);
        assert_eq!(record.status, crate::domain::RecordStatus::Active);
        assert_eq!(record.actor, "test");
        // Persisted via apply_event — round-trip query confirms it.
        let reloaded = store.get_record(&record.record_id).unwrap().unwrap();
        assert_eq!(reloaded.record_id, record.record_id);
    }

    #[test]
    fn test_create_analysis_uses_correct_kind() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let record = store
            .create_analysis(create_params("findings", b"analysis body"), &objects)
            .unwrap();
        assert_eq!(record.kind, crate::domain::RecordKind::Analysis);
    }

    #[test]
    fn test_create_plan_uses_correct_kind() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let record = store
            .create_plan(create_params("rollout", b"plan body"), &objects)
            .unwrap();
        assert_eq!(record.kind, crate::domain::RecordKind::Plan);
    }

    #[test]
    fn test_create_snapshot_uses_correct_kind() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let record = store
            .create_snapshot(create_params("state-cap", b"snapshot body"), &objects)
            .unwrap();
        assert_eq!(record.kind, crate::domain::RecordKind::Snapshot);
    }

    #[test]
    fn test_create_document_populates_content_ref() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let body = b"some content body";
        let record = store
            .create_document(create_params("with-content", body), &objects)
            .unwrap();
        // BLAKE3 hex digest is 64 chars.
        assert_eq!(record.content_ref.hash.len(), 64);
        // The blob exists on disk under that hash.
        assert!(objects.exists(&record.content_ref.hash));
    }

    #[test]
    fn test_pin_unpin_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        let row = store.get_record("r1").unwrap().unwrap();
        assert!(!row.pinned);

        store.pin_record("r1", "agent").unwrap();
        let row = store.get_record("r1").unwrap().unwrap();
        assert!(row.pinned);

        store.unpin_record("r1", "agent").unwrap();
        let row = store.get_record("r1").unwrap().unwrap();
        assert!(!row.pinned);
    }

    #[test]
    fn test_cross_brain_create_uses_correct_prefix() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("brain.db");
        let db = brain_persistence::db::Db::open(&db_path).unwrap();

        // Register two brains with distinct prefixes.
        db.upsert_brain(&brain_persistence::db::schema::BrainUpsert {
            brain_id: "test-a",
            name: "test-a",
            prefix: "AAA",
            roots_json: "[]",
            notes_json: "[]",
            aliases_json: "[]",
            archived: false,
        })
        .unwrap();
        db.upsert_brain(&brain_persistence::db::schema::BrainUpsert {
            brain_id: "test-b",
            name: "test-b",
            prefix: "BBB",
            roots_json: "[]",
            notes_json: "[]",
            aliases_json: "[]",
            archived: false,
        })
        .unwrap();

        let objects_dir = dir.path().join("objects");
        let objects = crate::objects::ObjectStore::new(&objects_dir).unwrap();

        // Brain A store.
        let store_a = RecordStore::with_brain_id(db.clone(), "test-a", "test-a").unwrap();
        let record_a = store_a
            .create_document(create_params("doc-a", b"body-a"), &objects)
            .unwrap();
        assert!(
            record_a.record_id.starts_with("AAA-"),
            "expected AAA- prefix, got {}",
            record_a.record_id
        );

        // Cross-brain handle scoped to brain B.
        let store_b = store_a.with_remote_brain_id("test-b", "test-b").unwrap();
        let record_b = store_b
            .create_document(create_params("doc-b", b"body-b"), &objects)
            .unwrap();
        assert!(
            record_b.record_id.starts_with("BBB-"),
            "expected BBB- prefix, got {}",
            record_b.record_id
        );
    }

    // -- Typed mutation method tests --

    #[test]
    fn test_archive_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.status, crate::domain::RecordStatus::Active);

        store
            .archive_record("r1", Some("superseded".to_string()), "agent")
            .unwrap();

        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.status, crate::domain::RecordStatus::Archived);
    }

    #[test]
    fn test_update_record_title_and_description() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        store
            .update_record(
                "r1",
                Some("Updated Title".to_string()),
                Some("Updated description".to_string()),
                "agent",
            )
            .unwrap();

        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.title, "Updated Title");
        assert_eq!(row.description.as_deref(), Some("Updated description"));
    }

    #[test]
    fn test_update_record_rejects_empty_title() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        let result = store.update_record("r1", Some("".into()), None, "agent");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("title must be non-empty"),
            "expected non-empty title error"
        );
    }

    #[test]
    fn test_add_tag_then_remove_tag() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        store.add_tag("r1", "important", "agent").unwrap();
        let tags = store.get_record_tags("r1").unwrap();
        assert!(
            tags.contains(&"important".to_string()),
            "tag should be present after add"
        );

        store.remove_tag("r1", "important", "agent").unwrap();
        let tags = store.get_record_tags("r1").unwrap();
        assert!(
            !tags.contains(&"important".to_string()),
            "tag should be absent after remove"
        );
    }

    #[test]
    fn test_link_task_and_unlink_task() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        store.link_task("r1", "t1", "agent").unwrap();
        let links = store.get_record_links("r1").unwrap();
        assert!(
            links.iter().any(|l| l.task_id.as_deref() == Some("t1")),
            "task link should be present after link_task"
        );

        store.unlink_task("r1", "t1", "agent").unwrap();
        let links = store.get_record_links("r1").unwrap();
        assert!(
            !links.iter().any(|l| l.task_id.as_deref() == Some("t1")),
            "task link should be absent after unlink_task"
        );
    }

    #[test]
    fn test_link_chunk_and_unlink_chunk() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        store.link_chunk("r1", "chunk-abc", "agent").unwrap();
        let links = store.get_record_links("r1").unwrap();
        assert!(
            links
                .iter()
                .any(|l| l.chunk_id.as_deref() == Some("chunk-abc")),
            "chunk link should be present after link_chunk"
        );

        store.unlink_chunk("r1", "chunk-abc", "agent").unwrap();
        let links = store.get_record_links("r1").unwrap();
        assert!(
            !links
                .iter()
                .any(|l| l.chunk_id.as_deref() == Some("chunk-abc")),
            "chunk link should be absent after unlink_chunk"
        );
    }

    #[test]
    fn test_archive_record_on_nonexistent_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let result = store.archive_record("BRN-nonexistent", None, "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("record not found"));
    }

    #[test]
    fn test_add_tag_on_nonexistent_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let result = store.add_tag("BRN-nonexistent", "mytag", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("record not found"));
    }

    #[test]
    fn test_link_task_on_nonexistent_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let result = store.link_task("BRN-nonexistent", "BRN-task-1", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("record not found"));
    }

    #[test]
    fn test_update_record_rejects_empty_description() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);
        let record = store
            .create_document(create_params("doc", b"body"), &objects)
            .unwrap();
        let result = store.update_record(&record.record_id, None, Some("".to_string()), "test");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("description must be non-empty")
        );
    }

    #[test]
    fn test_list_records_by_query_filters_kind() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        store
            .create_document(create_params("doc-1", b"body"), &objects)
            .unwrap();
        store
            .create_analysis(create_params("analysis-1", b"body"), &objects)
            .unwrap();
        store
            .create_plan(create_params("plan-1", b"body"), &objects)
            .unwrap();

        let query = RecordQuery {
            kind: Some(crate::domain::RecordKind::Document),
            ..Default::default()
        };
        let results = store.list_records(&query).unwrap();
        assert_eq!(results.len(), 1, "should return exactly one document");
        assert_eq!(results[0].kind, crate::domain::RecordKind::Document);
        assert_eq!(results[0].title, "doc-1");
    }

    #[test]
    fn test_list_records_by_query_filters_status() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, objects) = make_store_with_objects(&dir);

        let r1 = store
            .create_document(create_params("active-doc", b"body"), &objects)
            .unwrap();
        let r2 = store
            .create_document(create_params("archived-doc", b"body"), &objects)
            .unwrap();

        store.archive_record(&r2.record_id, None, "agent").unwrap();

        let query = RecordQuery {
            status: Some(crate::domain::RecordStatus::Active),
            ..Default::default()
        };
        let results = store.list_records(&query).unwrap();
        assert_eq!(results.len(), 1, "should return only the active record");
        assert_eq!(results[0].record_id, r1.record_id);
        assert_eq!(results[0].status, crate::domain::RecordStatus::Active);
    }
}
