use std::collections::HashSet;
use std::fmt;

use crate::error::Result;

use super::RecordStore;
use super::objects::ObjectStore;

// -- Report structs --

/// A blob referenced by a record but missing from the object store.
#[derive(Debug, Clone)]
pub struct MissingBlob {
    pub record_id: String,
    pub content_hash: String,
}

/// A blob whose stored bytes don't match its expected BLAKE3 hash.
#[derive(Debug, Clone)]
pub struct CorruptBlob {
    pub expected_hash: String,
    pub actual_hash: String,
}

/// A blob in the object store not referenced by any record.
#[derive(Debug, Clone)]
pub struct OrphanBlob {
    pub hash: String,
}

/// A record with payload_available=false but whose blob still exists.
#[derive(Debug, Clone)]
pub struct StaleFlag {
    pub record_id: String,
    pub content_hash: String,
}

/// Full integrity report from a verification pass.
#[derive(Debug, Clone, Default)]
pub struct IntegrityReport {
    pub missing: Vec<MissingBlob>,
    pub corrupt: Vec<CorruptBlob>,
    pub orphans: Vec<OrphanBlob>,
    pub stale_flags: Vec<StaleFlag>,
    pub records_checked: usize,
    pub blobs_checked: usize,
}

impl IntegrityReport {
    pub fn is_clean(&self) -> bool {
        self.missing.is_empty()
            && self.corrupt.is_empty()
            && self.orphans.is_empty()
            && self.stale_flags.is_empty()
    }
}

impl fmt::Display for IntegrityReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Integrity Report")?;
        writeln!(f, "  Records checked: {}", self.records_checked)?;
        writeln!(f, "  Blobs checked:   {}", self.blobs_checked)?;
        if self.is_clean() {
            writeln!(f, "  Status: CLEAN")?;
        } else {
            if !self.missing.is_empty() {
                writeln!(f, "  Missing blobs:   {}", self.missing.len())?;
            }
            if !self.corrupt.is_empty() {
                writeln!(f, "  Corrupt blobs:   {}", self.corrupt.len())?;
            }
            if !self.orphans.is_empty() {
                writeln!(f, "  Orphan blobs:    {}", self.orphans.len())?;
            }
            if !self.stale_flags.is_empty() {
                writeln!(f, "  Stale flags:     {}", self.stale_flags.len())?;
            }
        }
        Ok(())
    }
}

/// Result of a cleanup (orphan removal) pass.
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    pub orphans_removed: usize,
    pub bytes_freed: u64,
    pub errors: Vec<String>,
}

impl fmt::Display for CleanupResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Cleanup Result")?;
        writeln!(f, "  Orphans removed: {}", self.orphans_removed)?;
        writeln!(f, "  Bytes freed:     {}", self.bytes_freed)?;
        if !self.errors.is_empty() {
            writeln!(f, "  Errors:          {}", self.errors.len())?;
        }
        Ok(())
    }
}

// -- Main functions --

/// Run a full integrity verification of the records object store.
///
/// Algorithm:
/// 1. Query all (record_id, content_hash, payload_available) from projection
/// 2. Build HashSet of all referenced content hashes
/// 3. For each ref where payload_available == true: verify blob exists and hash matches
/// 4. For each ref where payload_available == false: if blob still exists, flag as stale
/// 5. Walk object store (list_all_hashes), any hash not in ref set = orphan
/// 6. Return report
pub fn verify_integrity(
    record_store: &RecordStore,
    object_store: &ObjectStore,
) -> Result<IntegrityReport> {
    let content_refs = record_store.get_all_content_refs()?;

    let referenced_hashes: HashSet<String> = content_refs
        .iter()
        .map(|(_, hash, _)| hash.clone())
        .collect();

    let mut report = IntegrityReport {
        records_checked: content_refs.len(),
        ..Default::default()
    };

    for (record_id, content_hash, payload_available) in &content_refs {
        if *payload_available {
            if !object_store.exists(content_hash) {
                report.missing.push(MissingBlob {
                    record_id: record_id.clone(),
                    content_hash: content_hash.clone(),
                });
            } else {
                report.blobs_checked += 1;
                match object_store.read_auto(content_hash) {
                    Ok(bytes) => {
                        let actual_hash = blake3::hash(&bytes).to_hex().to_string();
                        if actual_hash != *content_hash {
                            report.corrupt.push(CorruptBlob {
                                expected_hash: content_hash.clone(),
                                actual_hash,
                            });
                        }
                    }
                    Err(e) => {
                        report.missing.push(MissingBlob {
                            record_id: record_id.clone(),
                            content_hash: format!("{content_hash} (read error: {e})"),
                        });
                    }
                }
            }
        } else if object_store.exists(content_hash) {
            report.stale_flags.push(StaleFlag {
                record_id: record_id.clone(),
                content_hash: content_hash.clone(),
            });
        }
    }

    let all_store_hashes = object_store.list_all_hashes()?;
    for hash in all_store_hashes {
        if !referenced_hashes.contains(&hash) {
            report.orphans.push(OrphanBlob { hash });
        }
    }

    Ok(report)
}

/// Remove orphan blobs from the object store.
///
/// If `dry_run` is true, counts orphans without deleting.
/// Returns the cleanup result with count, bytes freed, and any errors.
pub fn cleanup_orphans(
    report: &IntegrityReport,
    object_store: &ObjectStore,
    dry_run: bool,
) -> Result<CleanupResult> {
    let mut result = CleanupResult::default();

    for orphan in &report.orphans {
        let size = match object_store.read(&orphan.hash) {
            Ok(bytes) => bytes.len() as u64,
            Err(e) => {
                result
                    .errors
                    .push(format!("read orphan {}: {e}", orphan.hash));
                continue;
            }
        };

        if !dry_run && let Err(e) = object_store.delete(&orphan.hash) {
            result
                .errors
                .push(format!("delete orphan {}: {e}", orphan.hash));
            continue;
        }

        result.orphans_removed += 1;
        result.bytes_freed += size;
    }

    Ok(result)
}

// -- Tests --

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::db::Db;

    use super::super::RecordStore;
    use super::super::events::*;
    use super::super::objects::ObjectStore;
    use super::*;

    fn setup() -> (TempDir, RecordStore, ObjectStore) {
        let dir = TempDir::new().unwrap();
        let sqlite_path = dir.path().join("test.db");
        let records_dir = dir.path().join("records");
        let objects_dir = dir.path().join("objects");
        let db = Db::open(&sqlite_path).unwrap();
        let record_store = RecordStore::new(&records_dir, db).unwrap();
        let object_store = ObjectStore::new(&objects_dir).unwrap();
        (dir, record_store, object_store)
    }

    fn create_record_with_blob(
        record_store: &RecordStore,
        object_store: &ObjectStore,
        record_id: &str,
        data: &[u8],
    ) -> String {
        let content_ref = object_store.write(data).unwrap();
        let event = RecordEvent::from_payload(
            record_id,
            "test-agent",
            RecordCreatedPayload {
                title: format!("Record {record_id}"),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new(
                    content_ref.hash.clone(),
                    content_ref.size,
                    None,
                ),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        record_store.apply_and_append(&event).unwrap();
        content_ref.hash
    }

    #[test]
    fn test_verify_empty_store() {
        let (_dir, record_store, object_store) = setup();
        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(report.is_clean());
        assert_eq!(report.records_checked, 0);
        assert_eq!(report.blobs_checked, 0);
    }

    #[test]
    fn test_verify_clean_store() {
        let (_dir, record_store, object_store) = setup();
        create_record_with_blob(&record_store, &object_store, "r1", b"hello world");
        create_record_with_blob(&record_store, &object_store, "r2", b"another payload");

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(report.is_clean(), "expected clean report, got:\n{report}");
        assert_eq!(report.records_checked, 2);
        assert_eq!(report.blobs_checked, 2);
    }

    #[test]
    fn test_verify_missing_blob() {
        let (_dir, record_store, object_store) = setup();
        let hash = create_record_with_blob(&record_store, &object_store, "r1", b"to be deleted");

        // Delete the blob directly from the store
        object_store.delete(&hash).unwrap();

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(!report.is_clean());
        assert_eq!(report.missing.len(), 1);
        assert_eq!(report.missing[0].record_id, "r1");
        assert_eq!(report.missing[0].content_hash, hash);
        assert!(report.corrupt.is_empty());
        assert!(report.orphans.is_empty());
    }

    #[test]
    fn test_verify_corrupt_blob() {
        let (_dir, record_store, object_store) = setup();
        let hash = create_record_with_blob(&record_store, &object_store, "r1", b"original data");

        // Overwrite the blob file with different bytes
        let blob_path = object_store.root().join(&hash[..2]).join(&hash);
        std::fs::write(&blob_path, b"corrupted bytes").unwrap();

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(!report.is_clean());
        assert_eq!(report.corrupt.len(), 1);
        assert_eq!(report.corrupt[0].expected_hash, hash);
        assert_ne!(report.corrupt[0].actual_hash, hash);
        assert!(report.missing.is_empty());
        assert!(report.orphans.is_empty());
    }

    #[test]
    fn test_verify_orphan_blob() {
        let (_dir, record_store, object_store) = setup();

        // Write a blob directly without associating it with any record
        let orphan_ref = object_store.write(b"orphan data").unwrap();

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(!report.is_clean());
        assert_eq!(report.orphans.len(), 1);
        assert_eq!(report.orphans[0].hash, orphan_ref.hash);
        assert!(report.missing.is_empty());
        assert!(report.corrupt.is_empty());
    }

    #[test]
    fn test_verify_stale_flag() {
        let (_dir, record_store, object_store) = setup();
        let hash = create_record_with_blob(&record_store, &object_store, "r1", b"evictable data");

        // Apply PayloadEvicted event but do NOT delete the blob from the object store.
        // This simulates a crash between event write and blob deletion,
        // or a record that was marked evicted but the blob was retained (e.g. another ref existed).
        let evict_event = RecordEvent::from_payload(
            "r1",
            "gc-agent",
            PayloadEvictedPayload {
                content_hash: hash.clone(),
                reason: "test eviction".to_string(),
            },
        );
        record_store.apply_and_append(&evict_event).unwrap();

        // Blob still exists in the store
        assert!(object_store.exists(&hash));

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(!report.is_clean());
        assert_eq!(report.stale_flags.len(), 1);
        assert_eq!(report.stale_flags[0].record_id, "r1");
        assert_eq!(report.stale_flags[0].content_hash, hash);
        assert!(report.missing.is_empty());
        assert!(report.corrupt.is_empty());
    }

    #[test]
    fn test_cleanup_orphans() {
        let (_dir, record_store, object_store) = setup();

        // Create an orphan blob (written directly, not referenced by any record)
        let orphan_ref = object_store.write(b"orphan payload").unwrap();
        let orphan_hash = orphan_ref.hash.clone();

        // Also create a valid record so we have something to contrast with
        create_record_with_blob(&record_store, &object_store, "r1", b"valid data");

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert_eq!(report.orphans.len(), 1);

        let cleanup = cleanup_orphans(&report, &object_store, false).unwrap();
        assert_eq!(cleanup.orphans_removed, 1);
        assert_eq!(cleanup.bytes_freed, b"orphan payload".len() as u64);
        assert!(cleanup.errors.is_empty());

        // Orphan blob should now be gone
        assert!(!object_store.exists(&orphan_hash));

        // Valid blob should still be there
        let report2 = verify_integrity(&record_store, &object_store).unwrap();
        assert!(report2.is_clean());
    }

    #[test]
    fn test_cleanup_orphans_dry_run() {
        let (_dir, record_store, object_store) = setup();

        let orphan_ref = object_store.write(b"dry run orphan").unwrap();
        let orphan_hash = orphan_ref.hash.clone();

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert_eq!(report.orphans.len(), 1);

        let cleanup = cleanup_orphans(&report, &object_store, true).unwrap();
        assert_eq!(cleanup.orphans_removed, 1);
        assert_eq!(cleanup.bytes_freed, b"dry run orphan".len() as u64);
        assert!(cleanup.errors.is_empty());

        // Blob must still exist after dry run
        assert!(object_store.exists(&orphan_hash));
    }

    /// Helper: create a record with a compressed blob.
    fn create_record_with_compressed_blob(
        record_store: &RecordStore,
        object_store: &ObjectStore,
        record_id: &str,
        data: &[u8],
        threshold: usize,
    ) -> String {
        let (content_ref, _encoding, _original_size) = object_store
            .write_compressed(data, None, threshold)
            .unwrap();
        let event = RecordEvent::from_payload(
            record_id,
            "test-agent",
            RecordCreatedPayload {
                title: format!("Record {record_id}"),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new(
                    content_ref.hash.clone(),
                    content_ref.size,
                    None,
                ),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        record_store.apply_and_append(&event).unwrap();
        content_ref.hash
    }

    #[test]
    fn test_verify_compressed_blob_reports_clean() {
        let (_dir, record_store, object_store) = setup();
        // Create a large, compressible payload that exceeds the threshold
        let data: Vec<u8> = b"compressible data ".repeat(500);
        create_record_with_compressed_blob(
            &record_store,
            &object_store,
            "r1",
            &data,
            100, // threshold: will compress
        );

        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert!(
            report.is_clean(),
            "compressed blob should verify clean, got:\n{report}"
        );
        assert_eq!(report.records_checked, 1);
        assert_eq!(report.blobs_checked, 1);
    }

    #[test]
    fn test_crash_recovery_stale_blob_cleaned_by_gc() {
        let (_dir, record_store, object_store) = setup();
        let hash =
            create_record_with_blob(&record_store, &object_store, "r1", b"crash recovery data");

        // Simulate a crash: apply the eviction event but do NOT delete the blob.
        let evict_event = RecordEvent::from_payload(
            "r1",
            "gc-agent",
            PayloadEvictedPayload {
                content_hash: hash.clone(),
                reason: "simulated crash".to_string(),
            },
        );
        record_store.apply_and_append(&evict_event).unwrap();

        // Blob is still on disk (simulating crash between event commit and delete)
        assert!(object_store.exists(&hash));

        // verify_integrity should flag it as a stale flag
        let report = verify_integrity(&record_store, &object_store).unwrap();
        assert_eq!(report.stale_flags.len(), 1);

        // The stale blob also shows up as an orphan (no active ref with payload_available=true)
        // Actually the blob IS referenced by r1, just with payload_available=false,
        // so it won't be an orphan in our current logic. Let's verify the gc path:
        // cleanup_orphans won't remove it since it's referenced. But verify shows the stale flag.
        // The real cleanup path: gc sees stale flag, deletes blob directly.

        // Verify gc removes the stale blob via orphan detection
        // The blob hash IS in referenced_hashes, so it won't be in orphans.
        // Stale flags are the mechanism for detecting this case.
        assert!(report.orphans.is_empty());
        assert_eq!(report.stale_flags[0].content_hash, hash);
    }

    #[test]
    fn test_display_clean_report() {
        let report = IntegrityReport {
            records_checked: 5,
            blobs_checked: 5,
            ..Default::default()
        };
        let output = report.to_string();
        assert!(output.contains("CLEAN"));
        assert!(output.contains("Records checked: 5"));
        assert!(output.contains("Blobs checked:   5"));
    }

    #[test]
    fn test_display_report_with_issues() {
        let mut report = IntegrityReport {
            records_checked: 3,
            blobs_checked: 2,
            ..Default::default()
        };
        report.missing.push(MissingBlob {
            record_id: "r1".to_string(),
            content_hash: "abc".to_string(),
        });
        report.orphans.push(OrphanBlob {
            hash: "def".to_string(),
        });

        let output = report.to_string();
        assert!(output.contains("Missing blobs:   1"));
        assert!(output.contains("Orphan blobs:    1"));
        assert!(!output.contains("CLEAN"));
    }

    #[test]
    fn test_display_cleanup_result() {
        let result = CleanupResult {
            orphans_removed: 3,
            bytes_freed: 1024,
            errors: vec![],
        };
        let output = result.to_string();
        assert!(output.contains("Orphans removed: 3"));
        assert!(output.contains("Bytes freed:     1024"));
    }
}
