//! Records domain types.
//!
//! Newtypes and enums that represent the records domain in its parsed form,
//! independent of the persistence row shape. `From<RecordRow>` lives here as
//! the boundary impl — public Store APIs return these types, never raw rows.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use brain_persistence::db::records::queries::RecordRow;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordKind {
    /// A generated prose document.
    Document,
    /// Structured analysis or summary produced by an agent.
    Analysis,
    /// A plan or roadmap.
    Plan,
    /// Opaque saved state bundle.
    Snapshot,
    /// An implementation note or build artifact.
    Implementation,
    /// A review or critique artifact.
    Review,
    /// A short synthesized summary.
    Summary,
    /// Custom kind with an arbitrary string identifier.
    Custom(String),
}

impl RecordKind {
    pub fn as_str(&self) -> &str {
        match self {
            RecordKind::Document => "document",
            RecordKind::Analysis => "analysis",
            RecordKind::Plan => "plan",
            RecordKind::Snapshot => "snapshot",
            RecordKind::Implementation => "implementation",
            RecordKind::Review => "review",
            RecordKind::Summary => "summary",
            RecordKind::Custom(s) => s.as_str(),
        }
    }

    pub fn policy(&self) -> KindPolicy {
        match self {
            RecordKind::Document
            | RecordKind::Analysis
            | RecordKind::Plan
            | RecordKind::Summary => KindPolicy {
                embed: true,
                summarize: true,
                searchable: true,
            },
            RecordKind::Implementation | RecordKind::Review => KindPolicy {
                embed: true,
                summarize: false,
                searchable: true,
            },
            RecordKind::Snapshot => KindPolicy {
                embed: false,
                summarize: false,
                searchable: false,
            },
            RecordKind::Custom(_) => KindPolicy {
                embed: true,
                summarize: false,
                searchable: true,
            },
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
            "document" => RecordKind::Document,
            "analysis" => RecordKind::Analysis,
            "plan" => RecordKind::Plan,
            "snapshot" => RecordKind::Snapshot,
            "implementation" => RecordKind::Implementation,
            "review" => RecordKind::Review,
            "summary" => RecordKind::Summary,
            other => RecordKind::Custom(other.to_string()),
        }
    }
}

impl From<String> for RecordKind {
    fn from(s: String) -> Self {
        RecordKind::from(s.as_str())
    }
}

impl Serialize for RecordKind {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RecordKind {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(RecordKind::from(s))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindPolicy {
    pub embed: bool,
    pub summarize: bool,
    pub searchable: bool,
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
/// share one object on disk. `content_encoding` and `original_size` describe
/// the transport encoding applied to the bytes on disk (e.g. zstd-compressed
/// payloads carry `content_encoding = "zstd"` and `original_size = Some(..)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRef {
    /// BLAKE3 hex digest of the raw payload bytes (64 hex chars, 256 bits).
    pub hash: String,
    /// Byte length of the stored (possibly encoded) payload.
    pub size: u64,
    /// Optional MIME type hint (e.g. `text/plain`, `application/json`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Content encoding applied to the bytes (`identity` for uncompressed,
    /// `zstd` for compressed). Defaults to `identity` for the 3-arg `new()`
    /// shorthand.
    #[serde(default = "default_identity_encoding")]
    pub content_encoding: String,
    /// Byte length of the original (pre-encoding) payload. `Some` only when
    /// `content_encoding != "identity"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_size: Option<u64>,
}

impl ContentRef {
    /// Construct a `ContentRef` for an unencoded payload. Defaults
    /// `content_encoding = "identity"` and `original_size = None`. Encoded
    /// payloads should use struct-init syntax with all fields set.
    pub fn new(hash: impl Into<String>, size: u64, media_type: Option<String>) -> Self {
        Self {
            hash: hash.into(),
            size,
            media_type,
            content_encoding: default_identity_encoding(),
            original_size: None,
        }
    }
}

/// The materialized/projected view of a record.
///
/// This is the read model — derived from the event log and stored in the
/// SQLite projection. `RecordStore` returns this type from `get_record` /
/// `list_records`; the persistence `RecordRow` is the wire shape and stays
/// behind the boundary.
///
/// Tags and links live in side-tables and are loaded via dedicated
/// `get_record_tags` / `get_record_links` calls — they are not bundled here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub record_id: String,
    pub title: String,
    /// Free-form kind tag (`document`, `analysis`, `plan`, `snapshot`,
    /// `implementation`, `review`, `summary`, or a custom string). String
    /// because the open `Custom(String)` variant of `RecordKind` would not
    /// add type-safety here.
    pub kind: String,
    pub status: RecordStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub content_ref: ContentRef,
    /// Soft reference to the task that produced this record.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// The actor who created this record.
    pub actor: String,
    /// Unix seconds when the record was created.
    pub created_at: i64,
    /// Unix seconds when the record metadata was last updated.
    pub updated_at: i64,
    /// Retention class hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_class: Option<String>,
    /// Whether the record is pinned (exempt from GC).
    pub pinned: bool,
    /// Whether the payload object is currently available in the object store.
    pub payload_available: bool,
    /// Provenance trust level. `trusted` for legacy rows.
    pub trust: String,
    /// Originating tool. `None` for system-internal rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_tool: Option<String>,
}

impl From<RecordRow> for Record {
    fn from(row: RecordRow) -> Self {
        let status = row.status.parse().unwrap_or(RecordStatus::Active);
        let content_ref = ContentRef {
            hash: row.content_hash,
            size: row.content_size as u64,
            media_type: row.media_type,
            content_encoding: row.content_encoding,
            original_size: row.original_size.map(|v| v as u64),
        };
        Record {
            record_id: row.record_id,
            title: row.title,
            kind: row.kind,
            status,
            description: row.description,
            content_ref,
            task_id: row.task_id,
            actor: row.actor,
            created_at: row.created_at,
            updated_at: row.updated_at,
            retention_class: row.retention_class,
            pinned: row.pinned,
            payload_available: row.payload_available,
            trust: row.trust,
            source_tool: row.source_tool,
        }
    }
}

impl From<&RecordRow> for Record {
    fn from(row: &RecordRow) -> Self {
        Record::from(row.clone())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(RecordKind::Document.as_str(), "document");
        assert_eq!(RecordKind::Analysis.as_str(), "analysis");
        assert_eq!(RecordKind::Plan.as_str(), "plan");
        assert_eq!(RecordKind::Snapshot.as_str(), "snapshot");
        assert_eq!(RecordKind::Implementation.as_str(), "implementation");
        assert_eq!(RecordKind::Review.as_str(), "review");
        assert_eq!(RecordKind::Summary.as_str(), "summary");
        assert_eq!(
            RecordKind::Custom("custom_kind".to_string()).as_str(),
            "custom_kind"
        );
    }

    #[test]
    fn test_record_kind_from_str() {
        assert_eq!(RecordKind::from("document"), RecordKind::Document);
        assert_eq!(RecordKind::from("analysis"), RecordKind::Analysis);
        assert_eq!(RecordKind::from("plan"), RecordKind::Plan);
        assert_eq!(RecordKind::from("snapshot"), RecordKind::Snapshot);
        assert_eq!(
            RecordKind::from("implementation"),
            RecordKind::Implementation
        );
        assert_eq!(RecordKind::from("review"), RecordKind::Review);
        assert_eq!(RecordKind::from("summary"), RecordKind::Summary);
        assert_eq!(
            RecordKind::from("unknown"),
            RecordKind::Custom("unknown".to_string())
        );
    }

    #[test]
    fn test_record_kind_serde_round_trip() {
        for kind in [
            RecordKind::Document,
            RecordKind::Analysis,
            RecordKind::Plan,
            RecordKind::Snapshot,
            RecordKind::Implementation,
            RecordKind::Review,
            RecordKind::Summary,
            RecordKind::Custom("custom_kind".to_string()),
        ] {
            let json = serde_json::to_string(&kind).unwrap();
            let back: RecordKind = serde_json::from_str(&json).unwrap();
            assert_eq!(back, kind);
        }
    }

    #[test]
    fn test_record_kind_policy() {
        assert_eq!(
            RecordKind::Document.policy(),
            KindPolicy {
                embed: true,
                summarize: true,
                searchable: true,
            }
        );
        assert_eq!(
            RecordKind::Analysis.policy(),
            KindPolicy {
                embed: true,
                summarize: true,
                searchable: true,
            }
        );
        assert_eq!(
            RecordKind::Plan.policy(),
            KindPolicy {
                embed: true,
                summarize: true,
                searchable: true,
            }
        );
        assert_eq!(
            RecordKind::Snapshot.policy(),
            KindPolicy {
                embed: false,
                summarize: false,
                searchable: false,
            }
        );
        assert_eq!(
            RecordKind::Implementation.policy(),
            KindPolicy {
                embed: true,
                summarize: false,
                searchable: true,
            }
        );
        assert_eq!(
            RecordKind::Review.policy(),
            KindPolicy {
                embed: true,
                summarize: false,
                searchable: true,
            }
        );
        assert_eq!(
            RecordKind::Summary.policy(),
            KindPolicy {
                embed: true,
                summarize: true,
                searchable: true,
            }
        );
        assert_eq!(
            RecordKind::Custom("custom_kind".to_string()).policy(),
            KindPolicy {
                embed: true,
                summarize: false,
                searchable: true,
            }
        );
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
        assert_eq!(
            RecordStatus::from_str("active").unwrap(),
            RecordStatus::Active
        );
        assert_eq!(
            RecordStatus::from_str("archived").unwrap(),
            RecordStatus::Archived
        );
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
        assert!(
            !json.contains("media_type"),
            "media_type should be absent when None"
        );
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
}
