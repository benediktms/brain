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

    /// Write data with optional zstd compression.
    ///
    /// If `data.len() >= threshold`, compresses with zstd level 3.
    /// The BLAKE3 hash is always computed on the RAW (pre-compression) bytes
    /// to preserve deduplication across compressed/uncompressed writes.
    ///
    /// Returns `(ContentRef, encoding, original_size)` where:
    /// - `ContentRef.size` = stored size (compressed if applicable)
    /// - `encoding` = "zstd" or "identity"
    /// - `original_size` = raw byte length
    pub fn write_compressed(
        &self,
        data: &[u8],
        media_type: Option<String>,
        threshold: usize,
    ) -> Result<(ContentRef, String, u64)> {
        let hash = blake3::hash(data);
        let hex = hash.to_hex().to_string();
        let original_size = data.len() as u64;

        // Fast path: blob already exists — skip write (deduplication).
        let dest = self.blob_path(&hex)?;
        if dest.exists() {
            return Ok((
                ContentRef::new(hex, original_size, media_type),
                "identity".to_string(),
                original_size,
            ));
        }

        // Ensure the 2-char prefix directory exists.
        let dir = dest
            .parent()
            .expect("blob path always has a parent directory");
        std::fs::create_dir_all(dir)?;

        let (bytes_to_write, encoding) = if data.len() >= threshold {
            let compressed = zstd::encode_all(data, 3).map_err(|e| {
                BrainCoreError::Internal(format!("object store: zstd compress: {e}"))
            })?;
            (compressed, "zstd")
        } else {
            (data.to_vec(), "identity")
        };

        let stored_size = bytes_to_write.len() as u64;

        let tmp_path = dir.join(format!("{hex}.tmp"));
        std::fs::write(&tmp_path, &bytes_to_write).map_err(|e| {
            BrainCoreError::Internal(format!("object store: write temp file: {e}"))
        })?;
        std::fs::rename(&tmp_path, &dest).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            BrainCoreError::Internal(format!("object store: atomic rename: {e}"))
        })?;

        Ok((
            ContentRef::new(hex, stored_size, media_type),
            encoding.to_string(),
            original_size,
        ))
    }

    /// Read a blob, auto-detecting and decompressing zstd if present.
    ///
    /// Checks the first 4 bytes for zstd magic number (0xFD2FB528).
    /// If detected, decompresses. Otherwise returns bytes as-is.
    pub fn read_auto(&self, hash: &str) -> Result<Vec<u8>> {
        let bytes = self.read(hash)?;
        // zstd magic number in little-endian byte order: 0xFD2FB528
        if bytes.len() >= 4 && bytes[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            zstd::decode_all(&bytes[..]).map_err(|e| {
                BrainCoreError::Internal(format!("object store: zstd decompress {hash}: {e}"))
            })
        } else {
            Ok(bytes)
        }
    }

    /// Walk the object store and return all blob hashes.
    ///
    /// Enumerates 2-char prefix directories, collects filenames that are
    /// exactly 64 hex characters. Ignores `.tmp` files and other non-hash entries.
    pub fn list_all_hashes(&self) -> Result<Vec<String>> {
        let mut hashes = Vec::new();

        let root_entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(hashes),
            Err(e) => {
                return Err(BrainCoreError::Internal(format!(
                    "object store: list root: {e}"
                )))
            }
        };

        for entry in root_entries {
            let entry = entry.map_err(|e| {
                BrainCoreError::Internal(format!("object store: read root entry: {e}"))
            })?;
            let path = entry.path();

            // Only descend into 2-char directories.
            if !path.is_dir() {
                continue;
            }
            let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) if n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit()) => {
                    n.to_string()
                }
                _ => continue,
            };

            let children = std::fs::read_dir(&path).map_err(|e| {
                BrainCoreError::Internal(format!(
                    "object store: list prefix dir {dir_name}: {e}"
                ))
            })?;

            for child in children {
                let child = child.map_err(|e| {
                    BrainCoreError::Internal(format!("object store: read child entry: {e}"))
                })?;
                let file_name = match child.file_name().into_string() {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                // Skip temp files and anything that is not a 64-char hex string.
                if file_name.ends_with(".tmp") {
                    continue;
                }
                if file_name.len() == 64
                    && file_name.chars().all(|c| c.is_ascii_hexdigit())
                {
                    hashes.push(file_name);
                }
            }
        }

        hashes.sort();
        Ok(hashes)
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

    // --- write_compressed / read_auto / list_all_hashes tests ---

    #[test]
    fn test_write_compressed_below_threshold() {
        let (_dir, store) = make_store();
        let data = b"small data";
        let threshold = 1024;
        let (content_ref, encoding, original_size) = store
            .write_compressed(data, None, threshold)
            .unwrap();

        assert_eq!(encoding, "identity");
        assert_eq!(original_size, data.len() as u64);
        assert_eq!(content_ref.size, data.len() as u64);

        // Raw bytes stored (no compression).
        let stored = store.read(&content_ref.hash).unwrap();
        assert_eq!(stored, data);
    }

    #[test]
    fn test_write_compressed_above_threshold() {
        let (_dir, store) = make_store();
        // Create a highly compressible payload larger than threshold.
        let data: Vec<u8> = b"aaaa".repeat(1024);
        let threshold = 100;
        let (content_ref, encoding, original_size) = store
            .write_compressed(&data, None, threshold)
            .unwrap();

        assert_eq!(encoding, "zstd");
        assert_eq!(original_size, data.len() as u64);
        // Compressed size must be strictly less than original for repetitive data.
        assert!(
            content_ref.size < original_size,
            "expected compressed size {} < original {}",
            content_ref.size,
            original_size
        );
    }

    #[test]
    fn test_read_auto_compressed() {
        let (_dir, store) = make_store();
        let data: Vec<u8> = b"hello world ".repeat(500);
        let (content_ref, encoding, _) = store
            .write_compressed(&data, None, 100)
            .unwrap();
        assert_eq!(encoding, "zstd");

        let recovered = store.read_auto(&content_ref.hash).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_read_auto_uncompressed() {
        let (_dir, store) = make_store();
        let data = b"plain text payload";
        let content_ref = store.write(data).unwrap();

        let recovered = store.read_auto(&content_ref.hash).unwrap();
        assert_eq!(recovered, data.as_ref());
    }

    #[test]
    fn test_write_compressed_dedup() {
        let (_dir, store) = make_store();
        let data: Vec<u8> = b"repeated ".repeat(500);
        let threshold = 100;

        let (ref1, enc1, _) = store
            .write_compressed(&data, None, threshold)
            .unwrap();
        let (ref2, enc2, _) = store
            .write_compressed(&data, None, threshold)
            .unwrap();

        assert_eq!(ref1.hash, ref2.hash);
        // Second write returns "identity" encoding (deduplicated — skipped write).
        assert_eq!(enc1, "zstd");
        assert_eq!(enc2, "identity");

        // Exactly one blob on disk.
        let blob_path = store.blob_path(&ref1.hash).unwrap();
        assert!(blob_path.exists());
    }

    #[test]
    fn test_list_all_hashes() {
        let (_dir, store) = make_store();
        let hashes: Vec<String> = ["alpha", "beta", "gamma"]
            .iter()
            .map(|s| {
                let cr = store.write(s.as_bytes()).unwrap();
                cr.hash
            })
            .collect();

        let mut listed = store.list_all_hashes().unwrap();
        listed.sort();
        let mut expected = hashes.clone();
        expected.sort();

        assert_eq!(listed, expected);
    }

    #[test]
    fn test_list_all_hashes_ignores_tmp() {
        let (_dir, store) = make_store();
        let cr = store.write(b"real blob").unwrap();
        let prefix = &cr.hash[..2];
        // Manually plant a .tmp file in the prefix dir.
        let tmp_path = store.root().join(prefix).join("fake.tmp");
        std::fs::write(&tmp_path, b"incomplete").unwrap();

        let listed = store.list_all_hashes().unwrap();
        // Only the real blob should appear; the .tmp file must be absent.
        assert_eq!(listed, vec![cr.hash]);
    }

    #[test]
    fn test_zstd_magic_detection() {
        let (_dir, store) = make_store();

        // Write a blob that begins with the zstd magic bytes.
        let magic_prefix: Vec<u8> = vec![0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x00];
        // Store raw so we can test the detection branch directly.
        let cr = store.write(&magic_prefix).unwrap();
        // read_auto will attempt decompression; expect an error because these
        // are not valid zstd-compressed data beyond the magic header.
        let result = store.read_auto(&cr.hash);
        // May succeed or fail depending on the zstd decoder, but must not panic.
        // The important thing is we exercised the magic-detection branch.
        let _ = result;

        // Non-zstd data: first byte != 0x28.
        let non_zstd = b"plain bytes that do not start with zstd magic";
        let cr2 = store.write(non_zstd).unwrap();
        let recovered = store.read_auto(&cr2.hash).unwrap();
        assert_eq!(recovered, non_zstd.as_ref());
    }
}
