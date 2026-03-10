use std::path::{Path, PathBuf};

use crate::error::{BrainCoreError, Result};
use crate::records::ContentRef;

/// A content-addressed object store for record payloads (artifacts and snapshots).
///
/// Objects are addressed by their BLAKE3 hash. The store uses a 2-character
/// prefix sharding scheme (identical to Git's object store) to limit directory
/// entry counts on filesystems that degrade with large flat directories.
///
/// Layout: `<root>/<2-char prefix>/<full 64-char BLAKE3 hex>`
///
/// Example: hash `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
/// is stored at `<root>/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
pub struct ObjectStore {
    root: PathBuf,
}

impl ObjectStore {
    /// Create a new `ObjectStore` rooted at `root`.
    ///
    /// The root directory will be created if it does not exist.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Return the filesystem path for a blob with the given hex hash.
    ///
    /// Validates that the hash is exactly 64 lowercase hex characters to
    /// prevent path traversal attacks.
    fn blob_path(&self, hash: &str) -> Result<PathBuf> {
        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(BrainCoreError::ObjectStore(format!(
                "invalid hash: expected 64 hex chars, got '{hash}'"
            )));
        }
        let prefix = &hash[..2];
        Ok(self.root.join(prefix).join(hash))
    }

    /// Write `data` to the store and return a [`ContentRef`].
    ///
    /// The BLAKE3 hash is computed from the raw bytes. If an object with the
    /// same hash already exists the write is skipped (deduplication). Writes
    /// to a temporary file that is atomically renamed into place so that
    /// partial writes are never visible to readers.
    pub fn write(&self, data: &[u8]) -> Result<ContentRef> {
        self.write_with_media_type(data, None)
    }

    /// Write `data` to the store with an optional MIME type hint.
    ///
    /// Identical to [`write`](Self::write) but also records the `media_type`
    /// in the returned [`ContentRef`].
    pub fn write_with_media_type(
        &self,
        data: &[u8],
        media_type: Option<String>,
    ) -> Result<ContentRef> {
        let hash = blake3::hash(data);
        let hex = hash.to_hex().to_string();
        let size = data.len() as u64;

        // Fast path: blob already exists — skip write (deduplication).
        let dest = self.blob_path(&hex)?;
        if dest.exists() {
            return Ok(ContentRef::new(hex, size, media_type));
        }

        // Ensure the 2-char prefix directory exists.
        let dir = dest
            .parent()
            .expect("blob path always has a parent directory");
        std::fs::create_dir_all(dir)?;

        // Write to a temp file in the same directory, then atomically rename.
        let tmp_path = dir.join(format!("{hex}.tmp"));
        std::fs::write(&tmp_path, data).map_err(|e| {
            BrainCoreError::Internal(format!("object store: write temp file: {e}"))
        })?;
        std::fs::rename(&tmp_path, &dest).map_err(|e| {
            // Best-effort cleanup of the temp file if rename fails.
            let _ = std::fs::remove_file(&tmp_path);
            BrainCoreError::Internal(format!("object store: atomic rename: {e}"))
        })?;

        Ok(ContentRef::new(hex, size, media_type))
    }

    /// Read and return the raw bytes for the blob identified by `hash`.
    ///
    /// Returns [`BrainCoreError::ObjectStore`] if the blob does not exist.
    pub fn read(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.blob_path(hash)?;
        std::fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BrainCoreError::ObjectStore(format!(
                    "blob not found: {hash}"
                ))
            } else {
                BrainCoreError::Internal(format!("object store: read blob {hash}: {e}"))
            }
        })
    }

    /// Return `true` if a blob with the given hash exists in the store.
    pub fn exists(&self, hash: &str) -> bool {
        self.blob_path(hash).map(|p| p.exists()).unwrap_or(false)
    }

    /// Delete the blob identified by `hash`.
    ///
    /// Returns [`BrainCoreError::ObjectStore`] if the blob does not exist.
    pub fn delete(&self, hash: &str) -> Result<()> {
        let path = self.blob_path(hash)?;
        std::fs::remove_file(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BrainCoreError::ObjectStore(format!(
                    "blob not found: {hash}"
                ))
            } else {
                BrainCoreError::Internal(format!("object store: delete blob {hash}: {e}"))
            }
        })
    }

    /// Return the root directory of the object store.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_store() -> (TempDir, ObjectStore) {
        let dir = TempDir::new().unwrap();
        let store = ObjectStore::new(dir.path().join("objects")).unwrap();
        (dir, store)
    }

    #[test]
    fn test_write_read_round_trip() {
        let (_dir, store) = make_store();
        let data = b"hello, world!";
        let content_ref = store.write(data).unwrap();

        assert_eq!(content_ref.size, data.len() as u64);
        assert_eq!(content_ref.hash.len(), 64);
        assert!(content_ref.media_type.is_none());

        let read_back = store.read(&content_ref.hash).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_write_with_media_type() {
        let (_dir, store) = make_store();
        let data = b"{\"key\": \"value\"}";
        let content_ref = store
            .write_with_media_type(data, Some("application/json".to_string()))
            .unwrap();

        assert_eq!(content_ref.media_type.as_deref(), Some("application/json"));
        let read_back = store.read(&content_ref.hash).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_deduplication_skips_second_write() {
        let (_dir, store) = make_store();
        let data = b"duplicate payload";

        let ref1 = store.write(data).unwrap();
        // Confirm blob exists after first write.
        assert!(store.exists(&ref1.hash));

        // A second write of identical bytes must return an equivalent ContentRef.
        let ref2 = store.write(data).unwrap();
        assert_eq!(ref1.hash, ref2.hash);
        assert_eq!(ref1.size, ref2.size);

        // There should still be exactly one file on disk (no duplicates).
        let blob_path = store.blob_path(&ref1.hash).unwrap();
        assert!(blob_path.exists());
    }

    #[test]
    fn test_exists_returns_false_for_missing_blob() {
        let (_dir, store) = make_store();
        // A well-formed hash that has never been written.
        let fake_hash = "a".repeat(64);
        assert!(!store.exists(&fake_hash));
    }

    #[test]
    fn test_exists_returns_true_after_write() {
        let (_dir, store) = make_store();
        let content_ref = store.write(b"some data").unwrap();
        assert!(store.exists(&content_ref.hash));
    }

    #[test]
    fn test_read_missing_blob_returns_error() {
        let (_dir, store) = make_store();
        let fake_hash = "b".repeat(64);
        let result = store.read(&fake_hash);
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should surface as ObjectStore error mentioning the hash.
        assert!(
            err.to_string().contains("blob not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_delete_removes_blob() {
        let (_dir, store) = make_store();
        let content_ref = store.write(b"to be deleted").unwrap();
        assert!(store.exists(&content_ref.hash));

        store.delete(&content_ref.hash).unwrap();
        assert!(!store.exists(&content_ref.hash));
    }

    #[test]
    fn test_delete_missing_blob_returns_error() {
        let (_dir, store) = make_store();
        let fake_hash = "c".repeat(64);
        let result = store.delete(&fake_hash);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("blob not found"));
    }

    #[test]
    fn test_storage_layout_uses_2char_prefix_sharding() {
        let (_dir, store) = make_store();
        let data = b"layout test";
        let content_ref = store.write(data).unwrap();

        let prefix = &content_ref.hash[..2];
        let expected_path = store.root().join(prefix).join(&content_ref.hash);
        assert!(
            expected_path.exists(),
            "blob should be at {expected_path:?}"
        );

        // The prefix directory must exist.
        let prefix_dir = store.root().join(prefix);
        assert!(prefix_dir.is_dir());
    }

    #[test]
    fn test_empty_payload() {
        let (_dir, store) = make_store();
        let content_ref = store.write(b"").unwrap();
        assert_eq!(content_ref.size, 0);
        let read_back = store.read(&content_ref.hash).unwrap();
        assert!(read_back.is_empty());
    }

    #[test]
    fn test_binary_payload() {
        let (_dir, store) = make_store();
        let data: Vec<u8> = (0u8..=255u8).collect();
        let content_ref = store.write(&data).unwrap();
        let read_back = store.read(&content_ref.hash).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_hash_is_valid_blake3() {
        let (_dir, store) = make_store();
        let data = b"verify hash";
        let content_ref = store.write(data).unwrap();

        // Recompute hash independently and verify it matches.
        let expected = blake3::hash(data).to_hex().to_string();
        assert_eq!(content_ref.hash, expected);
    }

    #[test]
    fn test_object_store_creates_root_dir() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("deep/nested/objects");
        assert!(!root.exists());
        let store = ObjectStore::new(&root).unwrap();
        assert!(store.root().exists());
    }
}
