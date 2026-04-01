//! Level-of-Detail (LOD) chunk types and port trait.
//!
//! LOD chunks are pre-computed representations of source objects at different
//! detail levels: L0 (extractive/deterministic), L1 (LLM-summarized), and
//! L2 (passthrough, never stored).

use brain_persistence::error::Result;

/// LOD level discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LodLevel {
    /// Extractive/deterministic (~100 tokens). Always stored.
    L0,
    /// LLM-summarized (~2000 tokens). Stored with optional expiry.
    L1,
    /// Passthrough from source. NEVER stored in `lod_chunks`.
    L2,
}

impl LodLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LodLevel::L0 => "L0",
            LodLevel::L1 => "L1",
            LodLevel::L2 => "L2",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "L0" => Some(LodLevel::L0),
            "L1" => Some(LodLevel::L1),
            "L2" => Some(LodLevel::L2),
            _ => None,
        }
    }

    /// Whether this level is persisted to `lod_chunks`.
    pub fn is_stored(&self) -> bool {
        !matches!(self, LodLevel::L2)
    }
}

/// Generation method discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LodMethod {
    Extractive,
    Llm,
}

impl LodMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            LodMethod::Extractive => "extractive",
            LodMethod::Llm => "llm",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "extractive" => Some(LodMethod::Extractive),
            "llm" => Some(LodMethod::Llm),
            _ => None,
        }
    }
}

/// A domain-level LOD chunk (mapped from persistence `LodChunkRow`).
#[derive(Debug, Clone)]
pub struct LodChunk {
    pub id: String,
    pub object_uri: String,
    pub brain_id: String,
    pub lod_level: LodLevel,
    pub content: String,
    pub token_est: Option<i64>,
    pub method: LodMethod,
    pub model_id: Option<String>,
    pub source_hash: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub job_id: Option<String>,
}

/// Input for upserting an LOD chunk at the domain level.
pub struct UpsertLodChunk<'a> {
    pub object_uri: &'a str,
    pub brain_id: &'a str,
    pub lod_level: LodLevel,
    pub content: &'a str,
    pub token_est: Option<i64>,
    pub method: LodMethod,
    pub model_id: Option<&'a str>,
    pub source_hash: &'a str,
    pub expires_at: Option<&'a str>,
    pub job_id: Option<&'a str>,
}

/// Persistence port for LOD chunk operations.
///
/// Implementations live in `brain_lib::ports` for the concrete `Db` type.
pub trait LodChunkStore: Send + Sync {
    /// Insert or replace an LOD chunk for `(object_uri, lod_level)`.
    /// Returns the ULID of the inserted/replaced row.
    ///
    /// On conflict (same `object_uri` + `lod_level`), the existing row's id is
    /// replaced with a fresh ULID. Callers must not cache old ids across upserts.
    ///
    /// Returns an error if `lod_level` is `L2` (passthrough, never stored).
    fn upsert_lod_chunk(&self, input: &UpsertLodChunk<'_>) -> Result<String>;

    /// Get LOD chunk by `(object_uri, lod_level)`.
    fn get_lod_chunk(&self, object_uri: &str, lod_level: LodLevel) -> Result<Option<LodChunk>>;

    /// Get all stored LOD chunks for an object URI.
    fn get_lod_chunks_for_uri(&self, object_uri: &str) -> Result<Vec<LodChunk>>;

    /// Delete all LOD chunks for an object URI. Returns count deleted.
    fn delete_lod_chunks_for_uri(&self, object_uri: &str) -> Result<usize>;

    /// Delete expired LOD chunks. Returns count deleted.
    fn delete_expired_lod_chunks(&self, now_iso: &str) -> Result<usize>;

    /// Check if an LOD chunk's `source_hash` matches the given hash.
    /// Returns `true` if fresh (hashes match), `false` if stale or missing.
    fn is_lod_fresh(
        &self,
        object_uri: &str,
        lod_level: LodLevel,
        current_source_hash: &str,
    ) -> Result<bool>;

    /// Count LOD chunks for a brain, optionally filtered by level.
    fn count_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        lod_level: Option<LodLevel>,
    ) -> Result<usize>;

    /// List LOD chunks for a brain with pagination, ordered by `created_at` DESC.
    fn list_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<LodChunk>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lod_level_round_trip() {
        for level in [LodLevel::L0, LodLevel::L1, LodLevel::L2] {
            assert_eq!(LodLevel::parse(level.as_str()), Some(level));
        }
    }

    #[test]
    fn test_lod_level_is_stored() {
        assert!(LodLevel::L0.is_stored());
        assert!(LodLevel::L1.is_stored());
        assert!(!LodLevel::L2.is_stored());
    }

    #[test]
    fn test_lod_level_parse_invalid() {
        assert_eq!(LodLevel::parse("L3"), None);
        assert_eq!(LodLevel::parse(""), None);
    }

    #[test]
    fn test_lod_method_round_trip() {
        for method in [LodMethod::Extractive, LodMethod::Llm] {
            assert_eq!(LodMethod::parse(method.as_str()), Some(method));
        }
    }

    #[test]
    fn test_lod_method_parse_invalid() {
        assert_eq!(LodMethod::parse("passthrough"), None);
        assert_eq!(LodMethod::parse(""), None);
    }
}
