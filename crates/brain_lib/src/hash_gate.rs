use crate::db::Db;
use crate::db::files;
use crate::utils::content_hash;

/// Result of a hash gate check: should this file be (re-)indexed?
pub struct GateVerdict {
    pub file_id: String,
    pub hash: String,
    pub should_index: bool,
}

/// Owns the full file-state lifecycle: check → in_progress → passed.
///
/// Consolidates identity resolution, hash comparison, and state transitions
/// so the pipeline can focus on chunking/embedding/upserting.
pub struct HashGate<'a> {
    db: &'a Db,
}

impl<'a> HashGate<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    /// Resolve file identity (get_or_create) + compare stored hash with
    /// current content hash. Single DB transaction for atomicity.
    pub fn check(&self, path: &str, content: &str) -> crate::error::Result<GateVerdict> {
        let hash = content_hash(content);

        let (file_id, should_index) = self.db.with_write_conn(|conn| {
            let (file_id, _is_new) = files::get_or_create_file_id(conn, path)?;
            let stored_hash = files::get_content_hash(conn, &file_id)?;
            let should_index = needs_reindex(stored_hash.as_deref(), content);
            Ok((file_id, should_index))
        })?;

        Ok(GateVerdict {
            file_id,
            hash,
            should_index,
        })
    }

    /// Mark file as in-flight for crash recovery. Call after check()
    /// returns should_index=true, before starting the actual work.
    pub fn mark_in_progress(&self, file_id: &str) -> crate::error::Result<()> {
        self.db
            .with_write_conn(|conn| files::set_indexing_state(conn, file_id, "indexing_started"))
    }

    /// Stamp the hash + mark indexed after successful indexing.
    pub fn mark_passed(&self, file_id: &str, hash: &str) -> crate::error::Result<()> {
        self.db
            .with_write_conn(|conn| files::mark_indexed(conn, file_id, hash))
    }
}

/// Check if a file needs re-indexing by comparing stored hash with current content.
fn needs_reindex(stored_hash: Option<&str>, current_content: &str) -> bool {
    match stored_hash {
        None => true,
        Some(stored) => stored != content_hash(current_content),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needs_reindex_no_stored_hash() {
        assert!(needs_reindex(None, "content"));
    }

    #[test]
    fn test_needs_reindex_same_content() {
        let hash = content_hash("content");
        assert!(!needs_reindex(Some(&hash), "content"));
    }

    #[test]
    fn test_needs_reindex_different_content() {
        let hash = content_hash("old content");
        assert!(needs_reindex(Some(&hash), "new content"));
    }
}
