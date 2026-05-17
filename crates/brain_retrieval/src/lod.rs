//! Level-of-Detail (LOD) chunk types and port trait.
//!
//! LOD chunks are pre-computed representations of source objects at different
//! detail levels: L0 (extractive/deterministic), L1 (LLM-summarized), and
//! L2 (passthrough, never stored).

use brain_core::error::{BrainCoreError, Result};
use brain_persistence::db::Db;
use brain_persistence::sql::SqlResultExt;

/// TTL for L1 chunks (days). After this period, the chunk is considered stale
/// even if the source hash matches, and regeneration is triggered.
pub const L1_TTL_DAYS: i64 = 30;

/// Minimum acceptable length (chars, trimmed) for LLM-generated L1 content.
/// Outputs shorter than this are rejected and trigger a retry.
pub const L1_MIN_CONTENT_LEN: usize = 50;

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

    /// Check if an L1 chunk is fresh: source_hash matches AND (expires_at is
    /// NULL OR expires_at > now).
    ///
    /// Returns `true` only when all three conditions hold. Returns `false` when
    /// the chunk is missing, has a different source hash, or has expired.
    fn is_l1_fresh(&self, object_uri: &str, current_source_hash: &str) -> Result<bool>;

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

// ─── `LodChunkStore` impl for the concrete `Db` ─────────────────────────────
//
// The trait lives in this module; per Rust's orphan rule, the impl for
// brain_persistence's `Db` must live in this crate. Delegates to typed SQL
// writers in `brain_persistence::db::lod_chunks`.

impl LodChunkStore for Db {
    fn upsert_lod_chunk(&self, input: &UpsertLodChunk<'_>) -> Result<String> {
        if !input.lod_level.is_stored() {
            return Err(BrainCoreError::Database(
                "L2 chunks are passthrough and must not be stored".into(),
            ));
        }
        let id = ulid::Ulid::new().to_string();
        let now = chrono::Utc::now().to_rfc3339();
        let persist = brain_persistence::db::lod_chunks::InsertLodChunk {
            id: &id,
            object_uri: input.object_uri,
            brain_id: input.brain_id,
            lod_level: input.lod_level.as_str(),
            content: input.content,
            token_est: input.token_est,
            method: input.method.as_str(),
            model_id: input.model_id,
            source_hash: input.source_hash,
            created_at: &now,
            expires_at: input.expires_at,
            job_id: input.job_id,
        };
        self.with_write_conn(|conn| {
            brain_persistence::db::lod_chunks::upsert_lod_chunk(conn, &persist)
        })
        .into_brain_core()?;
        Ok(id)
    }

    fn get_lod_chunk(&self, object_uri: &str, lod_level: LodLevel) -> Result<Option<LodChunk>> {
        let uri = object_uri.to_string();
        let level = lod_level.as_str().to_string();
        let row = self
            .with_read_conn(move |conn| {
                brain_persistence::db::lod_chunks::get_lod_chunk(conn, &uri, &level)
            })
            .into_brain_core()?;
        row.map(row_to_lod_chunk).transpose()
    }

    fn get_lod_chunks_for_uri(&self, object_uri: &str) -> Result<Vec<LodChunk>> {
        let uri = object_uri.to_string();
        let rows = self
            .with_read_conn(move |conn| {
                brain_persistence::db::lod_chunks::get_lod_chunks_for_uri(conn, &uri)
            })
            .into_brain_core()?;
        rows.into_iter().map(row_to_lod_chunk).collect()
    }

    fn delete_lod_chunks_for_uri(&self, object_uri: &str) -> Result<usize> {
        let uri = object_uri.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::lod_chunks::delete_lod_chunks_for_uri(conn, &uri)
        })
        .into_brain_core()
    }

    fn delete_expired_lod_chunks(&self, now_iso: &str) -> Result<usize> {
        let now = now_iso.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::lod_chunks::delete_expired_lod_chunks(conn, &now)
        })
        .into_brain_core()
    }

    fn is_lod_fresh(
        &self,
        object_uri: &str,
        lod_level: LodLevel,
        current_source_hash: &str,
    ) -> Result<bool> {
        let chunk = LodChunkStore::get_lod_chunk(self, object_uri, lod_level)?;
        Ok(chunk.is_some_and(|c| c.source_hash == current_source_hash))
    }

    fn is_l1_fresh(&self, object_uri: &str, current_source_hash: &str) -> Result<bool> {
        let chunk = LodChunkStore::get_lod_chunk(self, object_uri, LodLevel::L1)?;
        Ok(chunk.is_some_and(|c| {
            if c.source_hash != current_source_hash {
                return false;
            }
            match &c.expires_at {
                None => true,
                Some(exp) => chrono::DateTime::parse_from_rfc3339(exp)
                    .map(|e| e > chrono::Utc::now())
                    .unwrap_or(false),
            }
        }))
    }

    fn count_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        lod_level: Option<LodLevel>,
    ) -> Result<usize> {
        let bid = brain_id.to_string();
        let level = lod_level.map(|l| l.as_str().to_string());
        self.with_read_conn(move |conn| {
            brain_persistence::db::lod_chunks::count_lod_chunks_by_brain(
                conn,
                &bid,
                level.as_deref(),
            )
        })
        .into_brain_core()
    }

    fn list_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<LodChunk>> {
        let bid = brain_id.to_string();
        let rows = self
            .with_read_conn(move |conn| {
                brain_persistence::db::lod_chunks::list_lod_chunks_by_brain(
                    conn, &bid, limit, offset,
                )
            })
            .into_brain_core()?;
        rows.into_iter().map(row_to_lod_chunk).collect()
    }
}

fn row_to_lod_chunk(row: brain_persistence::db::lod_chunks::LodChunkRow) -> Result<LodChunk> {
    let lod_level = LodLevel::parse(&row.lod_level).ok_or_else(|| {
        BrainCoreError::Database(format!(
            "unknown lod_level '{}' for chunk {}",
            row.lod_level, row.id
        ))
    })?;
    let method = LodMethod::parse(&row.method).ok_or_else(|| {
        BrainCoreError::Database(format!(
            "unknown method '{}' for chunk {}",
            row.method, row.id
        ))
    })?;
    Ok(LodChunk {
        id: row.id,
        object_uri: row.object_uri,
        brain_id: row.brain_id,
        lod_level,
        content: row.content,
        token_est: row.token_est,
        method,
        model_id: row.model_id,
        source_hash: row.source_hash,
        created_at: row.created_at,
        expires_at: row.expires_at,
        job_id: row.job_id,
    })
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
