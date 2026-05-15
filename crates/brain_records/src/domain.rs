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
///
/// The `Unknown(String)` variant preserves unrecognised status strings
/// round-trip so that a future `"quarantined"` (or any other new status)
/// is never silently downgraded to `Active`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RecordStatus {
    /// The record is current and accessible.
    #[default]
    Active,
    /// The record has been superseded or explicitly archived.
    Archived,
    /// An unrecognised status string, preserved verbatim.
    Unknown(String),
}

impl RecordStatus {
    pub fn as_str(&self) -> &str {
        match self {
            RecordStatus::Active => "active",
            RecordStatus::Archived => "archived",
            RecordStatus::Unknown(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for RecordStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for RecordStatus {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "active" => Ok(RecordStatus::Active),
            "archived" => Ok(RecordStatus::Archived),
            other => Ok(RecordStatus::Unknown(other.to_string())),
        }
    }
}

impl Serialize for RecordStatus {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RecordStatus {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(s.parse().unwrap_or_else(|e| match e {}))
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
    /// Typed kind classification. Round-trips through the open `Custom(String)`
    /// variant for unknown strings. Serializes as a plain string (e.g.
    /// `"document"`) — JSON output is unchanged from the previous `String` field.
    pub kind: RecordKind,
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
        let status: RecordStatus = row.status.parse().unwrap_or_else(|e| match e {});
        let content_ref = ContentRef {
            hash: row.content_hash,
            size: row.content_size.max(0) as u64,
            media_type: row.media_type,
            content_encoding: row.content_encoding,
            original_size: row.original_size.map(|v| v.max(0) as u64),
        };
        Record {
            record_id: row.record_id,
            title: row.title,
            kind: RecordKind::from(row.kind),
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

fn default_identity_encoding() -> String {
    "identity".to_string()
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
        assert_eq!(
            RecordStatus::from_str("invalid").unwrap(),
            RecordStatus::Unknown("invalid".to_string())
        );
    }

    #[test]
    fn test_record_status_serde_round_trip() {
        let status = RecordStatus::Archived;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"archived\"");
        let back: RecordStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);

        // Unknown variant round-trips as its raw string
        let unknown = RecordStatus::Unknown("quarantined".to_string());
        let json = serde_json::to_string(&unknown).unwrap();
        assert_eq!(json, "\"quarantined\"");
        let back: RecordStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, unknown);
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
    fn test_from_record_row_happy_path() {
        let hash = format!("abc123{}", "0".repeat(58));
        let row = RecordRow {
            record_id: "BRN-01XYZ".to_string(),
            title: "Test".to_string(),
            kind: "document".to_string(),
            status: "active".to_string(),
            description: Some("a description".to_string()),
            content_hash: hash.clone(),
            content_size: 100,
            media_type: Some("text/plain".to_string()),
            task_id: Some("BRN-task-1".to_string()),
            actor: "cli".to_string(),
            created_at: 1_700_000_000,
            updated_at: 1_700_000_100,
            retention_class: Some("permanent".to_string()),
            pinned: true,
            payload_available: true,
            content_encoding: "zstd".to_string(),
            original_size: Some(200),
            trust: "trusted".to_string(),
            source_tool: Some("brain".to_string()),
        };
        let record = Record::from(row);
        assert_eq!(record.record_id, "BRN-01XYZ");
        assert_eq!(record.title, "Test");
        assert_eq!(record.status, RecordStatus::Active);
        assert_eq!(record.content_ref.hash, hash);
        assert_eq!(record.content_ref.size, 100u64);
        assert_eq!(record.content_ref.original_size, Some(200u64));
        assert_eq!(record.task_id.as_deref(), Some("BRN-task-1"));
        assert_eq!(record.actor, "cli");
        assert_eq!(record.created_at, 1_700_000_000);
        assert_eq!(record.updated_at, 1_700_000_100);
        assert_eq!(record.retention_class.as_deref(), Some("permanent"));
        assert!(record.pinned);
        assert!(record.payload_available);
        assert_eq!(record.trust, "trusted");
        assert_eq!(record.source_tool.as_deref(), Some("brain"));
    }

    #[test]
    fn test_from_record_row_unknown_status_preserved() {
        let row = RecordRow {
            record_id: "BRN-01".to_string(),
            title: "T".to_string(),
            kind: "document".to_string(),
            status: "bogus_unknown_value".to_string(),
            description: None,
            content_hash: "a".repeat(64),
            content_size: 0,
            media_type: None,
            task_id: None,
            actor: "cli".to_string(),
            created_at: 0,
            updated_at: 0,
            retention_class: None,
            pinned: false,
            payload_available: false,
            content_encoding: "identity".to_string(),
            original_size: None,
            trust: "trusted".to_string(),
            source_tool: None,
        };
        let record = Record::from(row);
        assert_eq!(
            record.status,
            RecordStatus::Unknown("bogus_unknown_value".to_string())
        );
    }

    #[test]
    fn test_from_record_row_bundles_content_into_content_ref() {
        let hash = format!("deadbeef{}", "0".repeat(56));
        let row = RecordRow {
            record_id: "BRN-02".to_string(),
            title: "T".to_string(),
            kind: "snapshot".to_string(),
            status: "active".to_string(),
            description: None,
            content_hash: hash.clone(),
            content_size: 4096,
            media_type: Some("application/zstd".to_string()),
            task_id: None,
            actor: "daemon".to_string(),
            created_at: 0,
            updated_at: 0,
            retention_class: None,
            pinned: false,
            payload_available: true,
            content_encoding: "zstd".to_string(),
            original_size: Some(8192),
            trust: "trusted".to_string(),
            source_tool: None,
        };
        let record = Record::from(row);
        assert_eq!(record.content_ref.hash, hash);
        assert_eq!(record.content_ref.size, 4096);
        assert_eq!(
            record.content_ref.media_type.as_deref(),
            Some("application/zstd")
        );
        assert_eq!(record.content_ref.content_encoding, "zstd");
        assert_eq!(record.content_ref.original_size, Some(8192));
    }
}
