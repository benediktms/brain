pub mod events;
pub mod integrity;
pub mod objects;
pub mod projections;
pub mod queries;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::error::Result;

// -- Domain types --

/// A ULID-based record ID, prefixed with the brain's project prefix.
///
/// Format: `"{PREFIX}-{ULID}"`, e.g. `"BRN-01KK7XXXXXXXXXXXXXXXXXXXX"`.
/// This is a newtype wrapper around `String` for type safety.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordId(pub String);

impl RecordId {
    /// Create a new `RecordId` from a string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RecordId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for RecordId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for RecordId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Domain classification for a record's scope.
///
/// Identifies which part of the Brain system produced or owns the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordDomain {
    /// Produced by or scoped to a specific task.
    Task,
    /// Scoped to the entire brain instance.
    Brain,
    /// Globally scoped, not tied to any specific entity.
    Global,
    /// Custom domain with an arbitrary string identifier.
    Custom(String),
}

impl RecordDomain {
    pub fn as_str(&self) -> &str {
        match self {
            RecordDomain::Task => "task",
            RecordDomain::Brain => "brain",
            RecordDomain::Global => "global",
            RecordDomain::Custom(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for RecordDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The category/kind of a record.
///
/// Stored as a string in both the event log and SQLite projection so the list
/// is open for extension without schema changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    /// Structured analysis or summary produced by an agent.
    Report,
    /// A patch or change set (text or structured).
    Diff,
    /// A serialized data export (JSON, CSV, etc.).
    Export,
    /// Quantitative or qualitative analysis result.
    Analysis,
    /// A generated prose document.
    Document,
    /// Opaque saved state bundle.
    Snapshot,
    /// Custom kind with an arbitrary string identifier.
    Custom(String),
}

impl RecordKind {
    pub fn as_str(&self) -> &str {
        match self {
            RecordKind::Report => "report",
            RecordKind::Diff => "diff",
            RecordKind::Export => "export",
            RecordKind::Analysis => "analysis",
            RecordKind::Document => "document",
            RecordKind::Snapshot => "snapshot",
            RecordKind::Custom(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for RecordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for RecordKind {
    fn from(s: &str) -> Self {
        match s {
            "report" => RecordKind::Report,
            "diff" => RecordKind::Diff,
            "export" => RecordKind::Export,
            "analysis" => RecordKind::Analysis,
            "document" => RecordKind::Document,
            "snapshot" => RecordKind::Snapshot,
            other => RecordKind::Custom(other.to_string()),
        }
    }
}

impl From<String> for RecordKind {
    fn from(s: String) -> Self {
        RecordKind::from(s.as_str())
    }
}

/// Lifecycle status of a record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStatus {
    /// The record is current and accessible.
    #[default]
    Active,
    /// The record has been superseded or explicitly archived.
    Archived,
}

impl RecordStatus {
    pub fn as_str(&self) -> &str {
        match self {
            RecordStatus::Active => "active",
            RecordStatus::Archived => "archived",
        }
    }
}

impl std::fmt::Display for RecordStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for RecordStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "active" => Ok(RecordStatus::Active),
            "archived" => Ok(RecordStatus::Archived),
            _ => Err(format!("invalid record status: '{s}'")),
        }
    }
}

/// A reference to an object in the content-addressed object store.
///
/// The `hash` field is the storage key. Two records with identical payloads
/// share one object on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRef {
    /// BLAKE3 hex digest of the raw payload bytes (64 hex chars, 256 bits).
    pub hash: String,
    /// Byte length of the payload.
    pub size: u64,
    /// Optional MIME type hint (e.g. `text/plain`, `application/json`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

impl ContentRef {
    pub fn new(hash: impl Into<String>, size: u64, media_type: Option<String>) -> Self {
        Self {
            hash: hash.into(),
            size,
            media_type,
        }
    }
}

/// The materialized/projected view of a record (artifact or snapshot).
///
/// This is the read model — derived from the event log and stored in the SQLite
/// projection. It is the struct returned by queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub record_id: String,
    pub title: String,
    pub kind: String,
    pub status: RecordStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub content_ref: ContentRef,
    /// Soft reference to the task that produced this record.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Producer identifier (agent name, tool name, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    /// Scope type (e.g. "task", "brain", "global").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_type: Option<String>,
    /// ID of the scoped entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,
    /// Retention class hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_class: Option<String>,
    /// Whether the record is pinned (exempt from GC).
    pub pinned: bool,
    /// Whether the payload object is currently available in the object store.
    pub payload_available: bool,
    /// The actor who created this record.
    pub actor: String,
    /// Unix seconds when the record was created.
    pub created_at: i64,
    /// Unix seconds when the record metadata was last updated.
    pub updated_at: i64,
    /// Tags on this record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// The materialized/projected view of an artifact record.
///
/// Artifacts are durable work products with known structure and semantics.
/// The payload is immutable after creation; only metadata is updatable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    /// Category of the artifact (report, diff, export, analysis, document).
    pub kind: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    /// MIME type of the content (e.g. `application/json`, `text/plain`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Content encoding applied to the stored bytes (e.g. `gzip`, `identity`).
    #[serde(default = "default_identity_encoding")]
    pub content_encoding: String,
    /// BLAKE3 hex digest of the raw payload bytes.
    pub content_hash: String,
    /// Byte length of the stored (possibly encoded) payload.
    pub size_bytes: u64,
    /// Byte length of the original (pre-encoding) payload.
    pub original_size_bytes: u64,
    /// Scope type (e.g. "task", "brain", "global").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_type: Option<String>,
    /// ID of the scoped entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,
    /// Retention class hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_class: Option<String>,
    /// Whether the artifact is pinned (exempt from GC).
    pub pinned: bool,
    /// Whether the payload object is currently available in the object store.
    pub payload_available: bool,
    /// Unix seconds when the artifact was created.
    pub created_at: i64,
    /// Unix seconds when the artifact was soft-deleted / archived, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<i64>,
}

fn default_identity_encoding() -> String {
    "identity".to_string()
}

/// The materialized/projected view of a snapshot record.
///
/// Snapshots are opaque saved state bundles. Brain stores the bytes and
/// metadata but does not parse or interpret the content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub snapshot_id: String,
    /// Always `RecordKind::Snapshot` for snapshots.
    pub kind: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    /// Schema version of the snapshot's internal format (opaque to Brain core).
    #[serde(default)]
    pub schema_version: u32,
    /// ID of the snapshot this was derived from, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<String>,
    /// MIME type of the content (e.g. `application/octet-stream`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Content encoding applied to the stored bytes.
    #[serde(default = "default_identity_encoding")]
    pub content_encoding: String,
    /// BLAKE3 hex digest of the raw payload bytes.
    pub content_hash: String,
    /// Byte length of the stored (possibly encoded) payload.
    pub size_bytes: u64,
    /// Byte length of the original (pre-encoding) payload.
    pub original_size_bytes: u64,
    /// Scope type (e.g. "task", "brain", "global").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_type: Option<String>,
    /// ID of the scoped entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,
    /// Retention class hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_class: Option<String>,
    /// Whether the snapshot is pinned (exempt from GC).
    pub pinned: bool,
    /// Whether the payload object is currently available in the object store.
    pub payload_available: bool,
    /// Unix seconds when the snapshot was created.
    pub created_at: i64,
    /// ID of the snapshot that superseded this one, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by_snapshot_id: Option<String>,
    /// Unix seconds when the snapshot was soft-deleted / archived, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<i64>,
}

// -- RecordStore skeleton --

/// The record store: event log (JSONL) as source of truth, SQLite as projection.
///
/// This is the skeleton — append/query logic will be added in subsequent tasks
/// when the event log writer and SQLite schema migrations are in place.
pub struct RecordStore {
    events_path: PathBuf,
    db: Db,
}

impl RecordStore {
    /// Create a new `RecordStore`.
    ///
    /// `records_dir` is the directory containing (or that will contain)
    /// `events.jsonl`. It will be created if it does not exist.
    pub fn new(records_dir: &std::path::Path, db: Db) -> Result<Self> {
        std::fs::create_dir_all(records_dir)?;
        Ok(Self {
            events_path: records_dir.join("events.jsonl"),
            db,
        })
    }

    /// Append a validated event to the log.
    ///
    /// Full projection apply logic will be added in a subsequent task when
    /// the SQLite schema and migrations are in place.
    pub fn append(&self, event: &events::RecordEvent) -> Result<()> {
        events::append_event(&self.events_path, event)
    }

    /// Read all events from the event log.
    pub fn read_all_events(&self) -> Result<Vec<events::RecordEvent>> {
        events::read_all_events(&self.events_path)
    }

    /// Access the underlying database handle.
    ///
    /// Used by projection and query layers (added in subsequent tasks).
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Return the path to the events JSONL file.
    pub fn events_path(&self) -> &std::path::Path {
        &self.events_path
    }

    /// Rebuild all records projection tables from the JSONL event log.
    ///
    /// Wipes the four records tables (in FK-safe order), then replays every
    /// event in `events.jsonl` in order. Returns the number of events applied.
    pub fn rebuild_projections(&self) -> Result<usize> {
        let events_path = self.events_path.clone();
        self.db
            .with_write_conn(|conn| projections::rebuild(conn, &events_path))
    }

    /// Apply a single event to the SQLite projection and append it to the log.
    ///
    /// The event is written to the JSONL log first (durable), then projected
    /// into SQLite. If the process crashes between the two writes, the next
    /// call to `rebuild_projections` will recover the correct state.
    pub fn apply_and_append(&self, event: &events::RecordEvent) -> Result<()> {
        // Persist to the durable log first
        events::append_event(&self.events_path, event)?;
        // Then update the SQLite projection
        self.db
            .with_write_conn(|conn| projections::apply_event(conn, event))
    }

    // -- Query methods --

    pub fn get_record(&self, record_id: &str) -> Result<Option<queries::RecordRow>> {
        self.db.with_read_conn(|conn| queries::get_record(conn, record_id))
    }

    pub fn list_records(&self, filter: &queries::RecordFilter) -> Result<Vec<queries::RecordRow>> {
        self.db.with_read_conn(|conn| queries::list_records(conn, filter))
    }

    pub fn get_record_tags(&self, record_id: &str) -> Result<Vec<String>> {
        self.db.with_read_conn(|conn| queries::get_record_tags(conn, record_id))
    }

    pub fn get_record_links(&self, record_id: &str) -> Result<Vec<queries::RecordLink>> {
        self.db.with_read_conn(|conn| queries::get_record_links(conn, record_id))
    }

    pub fn resolve_record_id(&self, input: &str) -> Result<String> {
        self.db.with_read_conn(|conn| queries::resolve_record_id(conn, input))
    }

    pub fn compact_record_id(&self, record_id: &str) -> Result<String> {
        self.db.with_read_conn(|conn| queries::compact_record_id(conn, record_id))
    }

    pub fn compact_record_ids(&self) -> Result<std::collections::HashMap<String, String>> {
        self.db.with_read_conn(queries::compact_record_ids)
    }

    pub fn get_project_prefix(&self) -> Result<String> {
        self.db.with_read_conn(|conn| {
            Ok(crate::db::meta::get_meta(conn, "project_prefix")?.unwrap_or_else(|| "BRN".to_string()))
        })
    }

    pub fn get_all_content_refs(&self) -> Result<Vec<(String, String, bool)>> {
        self.db.with_read_conn(queries::get_all_content_refs)
    }

    pub fn count_payload_refs(
        &self,
        content_hash: &str,
        exclude_record_id: &str,
    ) -> Result<i64> {
        self.db.with_read_conn(|conn| {
            queries::count_payload_refs(conn, content_hash, exclude_record_id)
        })
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
    /// is lost. Running `brain records gc` (which calls [`integrity::cleanup_orphans`])
    /// will detect and remove the stale blob.
    pub fn evict_payload(
        &self,
        record_id: &str,
        reason: &str,
        actor: &str,
        objects: &objects::ObjectStore,
    ) -> Result<()> {
        let record = self
            .get_record(record_id)?
            .ok_or_else(|| crate::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}")))?;

        if !record.payload_available {
            return Err(crate::error::BrainCoreError::RecordEvent(
                "payload already evicted".to_string(),
            ));
        }
        if record.pinned {
            return Err(crate::error::BrainCoreError::RecordEvent(
                "cannot evict pinned record".to_string(),
            ));
        }

        let payload = events::PayloadEvictedPayload {
            content_hash: record.content_hash.clone(),
            reason: reason.to_string(),
        };
        let event = events::RecordEvent::from_payload(record_id, actor, payload);
        self.apply_and_append(&event)?;

        let other_refs = self.count_payload_refs(&record.content_hash, record_id)?;
        if other_refs == 0 && objects.exists(&record.content_hash) {
            objects.delete(&record.content_hash)?;
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
        self.get_record(record_id)?
            .ok_or_else(|| crate::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}")))?;

        let payload = events::RetentionClassSetPayload {
            retention_class: retention_class.map(|s| s.to_string()),
        };
        let event = events::RecordEvent::from_payload(record_id, actor, payload);
        self.apply_and_append(&event)
    }

    /// Pin a record, preventing it from being evicted.
    pub fn pin_record(&self, record_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?
            .ok_or_else(|| crate::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}")))?;

        let event = events::RecordEvent::new(
            record_id,
            actor,
            events::RecordEventType::RecordPinned,
            &events::PinPayload {},
        );
        self.apply_and_append(&event)
    }

    /// Unpin a record, allowing it to be evicted again.
    pub fn unpin_record(&self, record_id: &str, actor: &str) -> Result<()> {
        self.get_record(record_id)?
            .ok_or_else(|| crate::error::BrainCoreError::RecordEvent(format!("record not found: {record_id}")))?;

        let event = events::RecordEvent::new(
            record_id,
            actor,
            events::RecordEventType::RecordUnpinned,
            &events::PinPayload {},
        );
        self.apply_and_append(&event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::events::*;

    #[test]
    fn test_record_id_display() {
        let id = RecordId::new("BRN-01XXXXXXXXXXXXXXXXXXXXXXXX");
        assert_eq!(id.to_string(), "BRN-01XXXXXXXXXXXXXXXXXXXXXXXX");
        assert_eq!(id.as_str(), "BRN-01XXXXXXXXXXXXXXXXXXXXXXXX");
    }

    #[test]
    fn test_record_id_from_string() {
        let id: RecordId = "BRN-01XXX".into();
        assert_eq!(id.as_str(), "BRN-01XXX");
    }

    #[test]
    fn test_record_kind_as_str() {
        assert_eq!(RecordKind::Report.as_str(), "report");
        assert_eq!(RecordKind::Diff.as_str(), "diff");
        assert_eq!(RecordKind::Export.as_str(), "export");
        assert_eq!(RecordKind::Analysis.as_str(), "analysis");
        assert_eq!(RecordKind::Document.as_str(), "document");
        assert_eq!(RecordKind::Snapshot.as_str(), "snapshot");
        assert_eq!(RecordKind::Custom("custom_kind".to_string()).as_str(), "custom_kind");
    }

    #[test]
    fn test_record_kind_from_str() {
        assert_eq!(RecordKind::from("report"), RecordKind::Report);
        assert_eq!(RecordKind::from("snapshot"), RecordKind::Snapshot);
        assert_eq!(RecordKind::from("unknown"), RecordKind::Custom("unknown".to_string()));
    }

    #[test]
    fn test_record_kind_serde_round_trip() {
        let kind = RecordKind::Analysis;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, "\"analysis\"");
        let back: RecordKind = serde_json::from_str(&json).unwrap();
        assert_eq!(back, kind);
    }

    #[test]
    fn test_record_status_default() {
        let status = RecordStatus::default();
        assert_eq!(status, RecordStatus::Active);
        assert_eq!(status.as_str(), "active");
    }

    #[test]
    fn test_record_status_parse() {
        use std::str::FromStr;
        assert_eq!(RecordStatus::from_str("active").unwrap(), RecordStatus::Active);
        assert_eq!(RecordStatus::from_str("archived").unwrap(), RecordStatus::Archived);
        assert!(RecordStatus::from_str("invalid").is_err());
    }

    #[test]
    fn test_record_status_serde_round_trip() {
        let status = RecordStatus::Archived;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"archived\"");
        let back: RecordStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn test_content_ref_serde_no_media_type() {
        let cr = ContentRef::new(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            1024,
            None,
        );
        let json = serde_json::to_string(&cr).unwrap();
        assert!(!json.contains("media_type"), "media_type should be absent when None");
        let back: ContentRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.hash, cr.hash);
        assert_eq!(back.size, cr.size);
        assert!(back.media_type.is_none());
    }

    #[test]
    fn test_content_ref_serde_with_media_type() {
        let cr = ContentRef::new(
            "abc123def456abc123def456abc123def456abc123def456abc123def456abc1",
            512,
            Some("application/json".to_string()),
        );
        let json = serde_json::to_string(&cr).unwrap();
        assert!(json.contains("application/json"));
        let back: ContentRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.media_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn test_record_domain_serde() {
        let domain = RecordDomain::Task;
        let json = serde_json::to_string(&domain).unwrap();
        assert_eq!(json, "\"task\"");

        let domain = RecordDomain::Custom("my_domain".to_string());
        let json = serde_json::to_string(&domain).unwrap();
        // Custom variant serializes as {"Custom":"my_domain"} due to Rust enum encoding
        let back: RecordDomain = serde_json::from_str(&json).unwrap();
        assert_eq!(back, domain);
    }

    #[test]
    fn test_artifact_record_serde_round_trip() {
        let artifact = ArtifactRecord {
            artifact_id: "BRN-01XXX".to_string(),
            kind: "report".to_string(),
            title: "Test Artifact".to_string(),
            summary: Some("A test artifact".to_string()),
            producer: Some("test-agent".to_string()),
            content_type: Some("application/json".to_string()),
            content_encoding: "identity".to_string(),
            content_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            size_bytes: 100,
            original_size_bytes: 100,
            scope_type: Some("task".to_string()),
            scope_id: Some("BRN-01YYY".to_string()),
            retention_class: Some("permanent".to_string()),
            pinned: false,
            payload_available: true,
            created_at: 1700000000,
            deleted_at: None,
        };

        let json = serde_json::to_string(&artifact).unwrap();
        let back: ArtifactRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.artifact_id, artifact.artifact_id);
        assert_eq!(back.title, artifact.title);
        assert_eq!(back.content_hash, artifact.content_hash);
        assert!(back.deleted_at.is_none());
    }

    #[test]
    fn test_snapshot_record_serde_round_trip() {
        let snapshot = SnapshotRecord {
            snapshot_id: "BRN-01SNAP".to_string(),
            kind: "snapshot".to_string(),
            title: "Test Snapshot".to_string(),
            summary: None,
            producer: Some("brain-daemon".to_string()),
            schema_version: 1,
            parent_snapshot_id: None,
            content_type: Some("application/octet-stream".to_string()),
            content_encoding: "identity".to_string(),
            content_hash: "abc123def456abc123def456abc123def456abc123def456abc123def456abc1"
                .to_string(),
            size_bytes: 4096,
            original_size_bytes: 4096,
            scope_type: None,
            scope_id: None,
            retention_class: None,
            pinned: true,
            payload_available: true,
            created_at: 1700000000,
            superseded_by_snapshot_id: None,
            deleted_at: None,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let back: SnapshotRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.snapshot_id, snapshot.snapshot_id);
        assert_eq!(back.schema_version, 1);
        assert!(back.parent_snapshot_id.is_none());
        assert!(back.superseded_by_snapshot_id.is_none());
    }

    #[test]
    fn test_snapshot_schema_version_default() {
        // When deserializing from JSON without schema_version, it should default to 0
        let json = r#"{"snapshot_id":"s1","kind":"snapshot","title":"T","content_hash":"abc","size_bytes":0,"original_size_bytes":0,"pinned":false,"payload_available":true,"created_at":0,"content_encoding":"identity","actor":"a"}"#;
        let snap: SnapshotRecord = serde_json::from_str(json).unwrap();
        assert_eq!(snap.schema_version, 0);
    }

    #[test]
    fn test_record_store_new_creates_dir() {
        let dir = tempfile::TempDir::new().unwrap();
        let records_dir = dir.path().join("records");
        assert!(!records_dir.exists());

        let db = crate::db::Db::open_in_memory().unwrap();
        let store = RecordStore::new(&records_dir, db).unwrap();
        assert!(records_dir.exists());
        assert_eq!(store.events_path(), records_dir.join("events.jsonl").as_path());
    }

    #[test]
    fn test_record_store_append_and_read() {
        let dir = tempfile::TempDir::new().unwrap();
        let records_dir = dir.path().join("records");
        let db = crate::db::Db::open_in_memory().unwrap();
        let store = RecordStore::new(&records_dir, db).unwrap();

        let ev = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordCreatedPayload {
                title: "My Artifact".to_string(),
                kind: "report".to_string(),
                content_ref: ContentRefPayload {
                    hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
                    size: 10,
                    media_type: None,
                },
                description: None,
                task_id: None,
                tags: vec!["q1".to_string()],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        store.append(&ev).unwrap();

        let events = store.read_all_events().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].record_id, "r1");
        assert_eq!(events[0].event_type, RecordEventType::RecordCreated);
    }

    // -- Helper for eviction/pin/retention tests --

    fn make_store_with_objects(
        dir: &tempfile::TempDir,
    ) -> (RecordStore, objects::ObjectStore) {
        let records_dir = dir.path().join("records");
        let db = crate::db::Db::open_in_memory().unwrap();
        let store = RecordStore::new(&records_dir, db).unwrap();
        let objects = objects::ObjectStore::new(dir.path().join("objects")).unwrap();
        (store, objects)
    }

    fn create_record_in_store(store: &RecordStore, record_id: &str, content_hash: &str, size: u64) {
        let ev = RecordEvent::from_payload(
            record_id,
            "agent",
            RecordCreatedPayload {
                title: "Test Record".to_string(),
                kind: "report".to_string(),
                content_ref: ContentRefPayload {
                    hash: content_hash.to_string(),
                    size,
                    media_type: None,
                },
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        store.apply_and_append(&ev).unwrap();
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

        store.evict_payload("r1", "gc", "gc-agent", &objects).unwrap();

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
        assert!(result.unwrap_err().to_string().contains("cannot evict pinned record"));

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

        store.evict_payload("r1", "gc", "gc-agent", &objects).unwrap();

        let result = store.evict_payload("r1", "gc", "gc-agent", &objects);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("payload already evicted"));
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
        store.evict_payload("r1", "gc", "gc-agent", &objects).unwrap();

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
        store.evict_payload("r1", "gc", "gc-agent", &objects).unwrap();
        assert!(objects.exists(&content_ref.hash));

        // Evict r2 — now blob can be deleted
        store.evict_payload("r2", "gc", "gc-agent", &objects).unwrap();
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

        store.set_retention_class("r1", Some("permanent"), "agent").unwrap();

        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.retention_class.as_deref(), Some("permanent"));
    }

    #[test]
    fn test_set_retention_class_clear() {
        let dir = tempfile::TempDir::new().unwrap();
        let (store, _objects) = make_store_with_objects(&dir);
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        create_record_in_store(&store, "r1", hash, 42);

        store.set_retention_class("r1", Some("ephemeral"), "agent").unwrap();
        let row = store.get_record("r1").unwrap().unwrap();
        assert_eq!(row.retention_class.as_deref(), Some("ephemeral"));

        store.set_retention_class("r1", None, "agent").unwrap();
        let row = store.get_record("r1").unwrap().unwrap();
        assert!(row.retention_class.is_none());
    }

    // -- pin/unpin tests --

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
}
