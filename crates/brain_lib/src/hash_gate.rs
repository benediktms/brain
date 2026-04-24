use tracing::instrument;

use crate::chunker::CHUNKER_VERSION;
use crate::ports::{FileMetaReader, FileMetaWriter};
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
pub struct HashGate<'a, W, R>
where
    W: FileMetaWriter,
    R: FileMetaReader,
{
    writer: &'a W,
    reader: &'a R,
}

impl<'a, W, R> HashGate<'a, W, R>
where
    W: FileMetaWriter,
    R: FileMetaReader,
{
    pub fn new(writer: &'a W, reader: &'a R) -> Self {
        Self { writer, reader }
    }

    /// Resolve file identity (get_or_create) + compare stored hash with
    /// current content hash + check chunker version.
    ///
    /// `brain_id` is forwarded to `get_or_create_file_id` so new files are
    /// stamped with the owning brain.
    #[instrument(skip_all)]
    pub fn check(
        &self,
        path: &str,
        content: &str,
        brain_id: &str,
    ) -> crate::error::Result<GateVerdict> {
        let hash = content_hash(content);

        let (file_id, _is_new) = self.writer.get_or_create_file_id(path, brain_id)?;
        let stored_hash = self.reader.get_content_hash(&file_id)?;
        let stored_version = self.reader.get_chunker_version(&file_id)?;
        let should_index = needs_reindex(stored_hash.as_deref(), content, stored_version);

        Ok(GateVerdict {
            file_id,
            hash,
            should_index,
        })
    }

    /// Mark file as in-flight for crash recovery. Call after check()
    /// returns should_index=true, before starting the actual work.
    pub fn mark_in_progress(&self, file_id: &str) -> crate::error::Result<()> {
        self.writer.set_indexing_state(file_id, "indexing_started")
    }

    /// Stamp the hash + chunker version + mark indexed after successful indexing.
    ///
    /// `disk_modified_at` is the file's OS-level mtime (Unix seconds).
    pub fn mark_passed(
        &self,
        file_id: &str,
        hash: &str,
        disk_modified_at: Option<i64>,
    ) -> crate::error::Result<()> {
        self.writer
            .mark_indexed(file_id, hash, CHUNKER_VERSION, disk_modified_at)
    }
}

/// Check if a file needs re-indexing by comparing stored hash with current content
/// and checking if the chunker version is current.
fn needs_reindex(
    stored_hash: Option<&str>,
    current_content: &str,
    stored_version: Option<u32>,
) -> bool {
    match stored_hash {
        None => true,
        Some(stored) => {
            if stored != content_hash(current_content) {
                return true;
            }
            // Content unchanged — but chunker version may have changed
            match stored_version {
                None => true,
                Some(v) => v != CHUNKER_VERSION,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needs_reindex_no_stored_hash() {
        assert!(needs_reindex(None, "content", None));
    }

    #[test]
    fn test_needs_reindex_same_content_same_version() {
        let hash = content_hash("content");
        assert!(!needs_reindex(
            Some(&hash),
            "content",
            Some(CHUNKER_VERSION)
        ));
    }

    #[test]
    fn test_needs_reindex_different_content() {
        let hash = content_hash("old content");
        assert!(needs_reindex(
            Some(&hash),
            "new content",
            Some(CHUNKER_VERSION)
        ));
    }

    #[test]
    fn test_needs_reindex_chunker_version_changed() {
        let hash = content_hash("content");
        assert!(needs_reindex(
            Some(&hash),
            "content",
            Some(CHUNKER_VERSION - 1)
        ));
    }

    #[test]
    fn test_needs_reindex_chunker_version_null() {
        let hash = content_hash("content");
        assert!(needs_reindex(Some(&hash), "content", None));
    }

    #[test]
    fn test_needs_reindex_all_match() {
        let hash = content_hash("content");
        assert!(!needs_reindex(
            Some(&hash),
            "content",
            Some(CHUNKER_VERSION)
        ));
    }
}
