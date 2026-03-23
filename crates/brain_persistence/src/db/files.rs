use rusqlite::{Connection, OptionalExtension};
use ulid::Ulid;

use crate::error::Result;

/// Get or create a file_id for the given path. Returns (file_id, is_new).
pub fn get_or_create_file_id(conn: &Connection, path: &str) -> Result<(String, bool)> {
    // Try to find existing (including soft-deleted — resurrect it)
    let existing: Option<(String, Option<i64>)> = conn
        .query_row(
            "SELECT file_id, deleted_at FROM files WHERE path = ?1",
            [path],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    if let Some((file_id, deleted_at)) = existing {
        if deleted_at.is_some() {
            // Resurrect soft-deleted file (clear chunker_version to force re-chunk)
            conn.execute(
                "UPDATE files SET deleted_at = NULL, indexing_state = 'idle', content_hash = NULL, chunker_version = NULL WHERE file_id = ?1",
                [&file_id],
            )?;
        }
        return Ok((file_id, false));
    }

    // Create new entry with ULID
    let file_id = Ulid::new().to_string();
    conn.execute(
        "INSERT INTO files (file_id, path, indexing_state) VALUES (?1, ?2, 'idle')",
        rusqlite::params![file_id, path],
    )?;

    Ok((file_id, true))
}

/// Get the stored content hash for a file.
pub fn get_content_hash(conn: &Connection, file_id: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT content_hash FROM files WHERE file_id = ?1",
            [file_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

/// Get the stored chunker version for a file.
pub fn get_chunker_version(conn: &Connection, file_id: &str) -> Result<Option<u32>> {
    Ok(conn
        .query_row(
            "SELECT chunker_version FROM files WHERE file_id = ?1",
            [file_id],
            |row| row.get::<_, Option<u32>>(0),
        )
        .optional()?
        .flatten())
}

/// Count files where chunker_version doesn't match the current version (stale or NULL).
pub fn count_stale_chunker_version(conn: &Connection, current_version: u32) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE deleted_at IS NULL AND (chunker_version IS NULL OR chunker_version != ?1)",
        [current_version],
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

/// Set the indexing state for a file (idle | indexing_started | indexed).
pub fn set_indexing_state(conn: &Connection, file_id: &str, state: &str) -> Result<()> {
    conn.execute(
        "UPDATE files SET indexing_state = ?1 WHERE file_id = ?2",
        rusqlite::params![state, file_id],
    )?;
    Ok(())
}

/// Mark a file as fully indexed: update hash, chunker version, timestamp, and state.
pub fn mark_indexed(
    conn: &Connection,
    file_id: &str,
    content_hash: &str,
    chunker_version: u32,
) -> Result<()> {
    let now = crate::utils::now_ts();
    conn.execute(
        "UPDATE files SET content_hash = ?1, last_indexed_at = ?2, indexing_state = 'indexed', chunker_version = ?3 WHERE file_id = ?4",
        rusqlite::params![content_hash, now, chunker_version, file_id],
    )?;
    Ok(())
}

/// Update the path for a renamed file.
pub fn handle_rename(conn: &Connection, file_id: &str, new_path: &str) -> Result<()> {
    conn.execute(
        "UPDATE files SET path = ?1 WHERE file_id = ?2",
        rusqlite::params![new_path, file_id],
    )?;
    Ok(())
}

/// Soft-delete a file by path. Returns the file_id if found.
pub fn handle_delete(conn: &Connection, path: &str) -> Result<Option<String>> {
    let file_id: Option<String> = conn
        .query_row(
            "SELECT file_id FROM files WHERE path = ?1 AND deleted_at IS NULL",
            [path],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(ref fid) = file_id {
        let now = crate::utils::now_ts();
        conn.execute(
            "UPDATE files SET deleted_at = ?1 WHERE file_id = ?2",
            rusqlite::params![now, fid],
        )?;
        conn.execute(
            "DELETE FROM chunks WHERE file_id = ?1",
            rusqlite::params![fid],
        )?;
    }

    Ok(file_id)
}

/// Find files stuck in 'indexing_started' state (crash recovery).
pub fn find_stuck_files(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT file_id, path FROM files WHERE indexing_state = 'indexing_started' AND deleted_at IS NULL",
    )?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    super::collect_rows(rows)
}

/// Get all active (non-deleted) file paths for startup deletion detection.
pub fn get_all_active_paths(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare("SELECT file_id, path FROM files WHERE deleted_at IS NULL")?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    super::collect_rows(rows)
}

/// Clear all content hashes and chunker versions (forces full re-index on next scan).
pub fn clear_all_content_hashes(conn: &Connection) -> Result<usize> {
    let count = conn.execute(
        "UPDATE files SET content_hash = NULL, chunker_version = NULL, indexing_state = 'idle' WHERE deleted_at IS NULL",
        [],
    )?;
    Ok(count)
}

/// Clear content hash and chunker version for a specific file path (forces re-index of that file).
pub fn clear_content_hash_by_path(conn: &Connection, path: &str) -> Result<bool> {
    let count = conn.execute(
        "UPDATE files SET content_hash = NULL, chunker_version = NULL, indexing_state = 'idle' WHERE path = ?1 AND deleted_at IS NULL",
        [path],
    )?;
    Ok(count > 0)
}

/// Hard-delete files where `deleted_at` is older than the given threshold (Unix seconds).
/// Returns the list of file_ids that were purged.
pub fn purge_deleted_files(conn: &Connection, older_than_ts: i64) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT file_id FROM files WHERE deleted_at IS NOT NULL AND deleted_at < ?1")?;
    let rows = stmt.query_map([older_than_ts], |row| row.get::<_, String>(0))?;
    let file_ids: Vec<String> = super::collect_rows(rows)?;

    if !file_ids.is_empty() {
        conn.execute(
            "DELETE FROM files WHERE deleted_at IS NOT NULL AND deleted_at < ?1",
            [older_than_ts],
        )?;
    }

    Ok(file_ids)
}

/// Get all active file_ids with their content hashes for doctor verification.
pub fn get_files_with_hashes(conn: &Connection) -> Result<Vec<(String, String, Option<String>)>> {
    let mut stmt =
        conn.prepare("SELECT file_id, path, content_hash FROM files WHERE deleted_at IS NULL")?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    super::collect_rows(rows)
}

// ---------------------------------------------------------------------------
// Additional helpers used by ports layer
// ---------------------------------------------------------------------------

/// Count files stuck in `indexing_started` state.
pub fn count_stuck_indexing(conn: &Connection) -> Result<u64> {
    let count: u64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE indexing_state = 'indexing_started' AND deleted_at IS NULL",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    Ok(count)
}

/// Look up a file by path, rename it, and return its file_id.
pub fn rename_by_path(conn: &Connection, from_path: &str, to_path: &str) -> Result<Option<String>> {
    let file_id: Option<String> = conn
        .query_row(
            "SELECT file_id FROM files WHERE path = ?1 AND deleted_at IS NULL",
            [from_path],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(ref fid) = file_id {
        handle_rename(conn, fid, to_path)?;
    }
    Ok(file_id)
}

/// Run SQLite VACUUM.
pub fn vacuum(conn: &Connection) -> Result<()> {
    conn.execute_batch("VACUUM")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn test_get_or_create_new_file() {
        let conn = setup();
        let (file_id, is_new) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();
        assert!(is_new);
        assert!(!file_id.is_empty());

        // Second call returns same id
        let (file_id2, is_new2) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();
        assert!(!is_new2);
        assert_eq!(file_id, file_id2);
    }

    #[test]
    fn test_content_hash_lifecycle() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();

        // Initially no hash
        assert_eq!(get_content_hash(&conn, &file_id).unwrap(), None);

        // After marking indexed
        mark_indexed(&conn, &file_id, "abc123", 1).unwrap();
        assert_eq!(
            get_content_hash(&conn, &file_id).unwrap(),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_indexing_state_transitions() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();

        set_indexing_state(&conn, &file_id, "indexing_started").unwrap();

        let stuck = find_stuck_files(&conn).unwrap();
        assert_eq!(stuck.len(), 1);
        assert_eq!(stuck[0].0, file_id);

        mark_indexed(&conn, &file_id, "hash123", 1).unwrap();
        let stuck = find_stuck_files(&conn).unwrap();
        assert!(stuck.is_empty());
    }

    #[test]
    fn test_handle_delete_and_resurrect() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();

        // Soft-delete
        let deleted_id = handle_delete(&conn, "/notes/test.md").unwrap();
        assert_eq!(deleted_id, Some(file_id.clone()));

        // Not in active paths
        let active = get_all_active_paths(&conn).unwrap();
        assert!(active.is_empty());

        // Resurrect by get_or_create
        let (file_id2, is_new) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();
        assert!(!is_new);
        assert_eq!(file_id, file_id2);

        // Back in active paths
        let active = get_all_active_paths(&conn).unwrap();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn test_handle_delete_removes_orphan_chunks() {
        use crate::db::chunks::{ChunkMeta, replace_chunk_metadata};

        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();

        let chunks = vec![
            ChunkMeta::test(&format!("{file_id}:0"), 0, "h0"),
            ChunkMeta::test(&format!("{file_id}:1"), 1, "h1"),
        ];
        replace_chunk_metadata(&conn, &file_id, &chunks).unwrap();

        // Verify chunks exist
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Soft-delete the file
        handle_delete(&conn, "/notes/test.md").unwrap();

        // Chunks must be gone
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "chunks should be deleted on soft-delete");
    }

    #[test]
    fn test_handle_delete_chunks_does_not_affect_other_files() {
        use crate::db::chunks::{ChunkMeta, replace_chunk_metadata};

        let conn = setup();
        let (file_a, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();
        let (file_b, _) = get_or_create_file_id(&conn, "/notes/b.md").unwrap();

        replace_chunk_metadata(
            &conn,
            &file_a,
            &[ChunkMeta::test(&format!("{file_a}:0"), 0, "ha")],
        )
        .unwrap();
        replace_chunk_metadata(
            &conn,
            &file_b,
            &[ChunkMeta::test(&format!("{file_b}:0"), 0, "hb")],
        )
        .unwrap();

        // Delete file A
        handle_delete(&conn, "/notes/a.md").unwrap();

        // File B's chunks are untouched
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_b],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "other file's chunks must not be affected");
    }

    #[test]
    fn test_clear_all_content_hashes() {
        let conn = setup();
        let (fid1, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();
        let (fid2, _) = get_or_create_file_id(&conn, "/notes/b.md").unwrap();
        mark_indexed(&conn, &fid1, "hash1", 1).unwrap();
        mark_indexed(&conn, &fid2, "hash2", 1).unwrap();

        let cleared = clear_all_content_hashes(&conn).unwrap();
        assert_eq!(cleared, 2);

        assert_eq!(get_content_hash(&conn, &fid1).unwrap(), None);
        assert_eq!(get_content_hash(&conn, &fid2).unwrap(), None);
    }

    #[test]
    fn test_clear_content_hash_by_path() {
        let conn = setup();
        let (fid1, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();
        let (fid2, _) = get_or_create_file_id(&conn, "/notes/b.md").unwrap();
        mark_indexed(&conn, &fid1, "hash1", 1).unwrap();
        mark_indexed(&conn, &fid2, "hash2", 1).unwrap();

        let found = clear_content_hash_by_path(&conn, "/notes/a.md").unwrap();
        assert!(found);

        assert_eq!(get_content_hash(&conn, &fid1).unwrap(), None);
        // b.md untouched
        assert_eq!(
            get_content_hash(&conn, &fid2).unwrap(),
            Some("hash2".to_string())
        );

        // Non-existent path returns false
        let found = clear_content_hash_by_path(&conn, "/notes/nope.md").unwrap();
        assert!(!found);
    }

    #[test]
    fn test_purge_deleted_files() {
        let conn = setup();
        let (fid, _) = get_or_create_file_id(&conn, "/notes/old.md").unwrap();
        handle_delete(&conn, "/notes/old.md").unwrap();

        // Set deleted_at to a very old timestamp
        conn.execute(
            "UPDATE files SET deleted_at = 1000 WHERE file_id = ?1",
            [&fid],
        )
        .unwrap();

        // Purge with a cutoff well after the deletion
        let purged = purge_deleted_files(&conn, 2000).unwrap();
        assert_eq!(purged.len(), 1);
        assert_eq!(purged[0], fid);

        // File should be completely gone
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE file_id = ?1",
                [&fid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_get_files_with_hashes() {
        let conn = setup();
        let (fid1, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();
        let (fid2, _) = get_or_create_file_id(&conn, "/notes/b.md").unwrap();
        mark_indexed(&conn, &fid1, "hash1", 1).unwrap();

        let files = get_files_with_hashes(&conn).unwrap();
        assert_eq!(files.len(), 2);

        let a = files.iter().find(|(fid, _, _)| fid == &fid1).unwrap();
        assert_eq!(a.2, Some("hash1".to_string()));

        let b = files.iter().find(|(fid, _, _)| fid == &fid2).unwrap();
        assert_eq!(b.2, None);
    }

    #[test]
    fn test_handle_rename() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/old.md").unwrap();

        handle_rename(&conn, &file_id, "/notes/new.md").unwrap();

        // Old path no longer found
        let (_, is_new) = get_or_create_file_id(&conn, "/notes/old.md").unwrap();
        assert!(is_new); // creates a new entry

        // New path returns the original file_id
        let (found_id, is_new) = get_or_create_file_id(&conn, "/notes/new.md").unwrap();
        assert!(!is_new);
        assert_eq!(found_id, file_id);
    }

    #[test]
    fn test_chunker_version_lifecycle() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();

        // Initially no version
        assert_eq!(get_chunker_version(&conn, &file_id).unwrap(), None);

        // After marking indexed with version 2
        mark_indexed(&conn, &file_id, "hash1", 2).unwrap();
        assert_eq!(get_chunker_version(&conn, &file_id).unwrap(), Some(2));

        // Update to version 3
        mark_indexed(&conn, &file_id, "hash1", 3).unwrap();
        assert_eq!(get_chunker_version(&conn, &file_id).unwrap(), Some(3));
    }

    #[test]
    fn test_resurrect_clears_chunker_version() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();
        mark_indexed(&conn, &file_id, "hash1", 2).unwrap();
        assert_eq!(get_chunker_version(&conn, &file_id).unwrap(), Some(2));

        // Soft-delete
        handle_delete(&conn, "/notes/test.md").unwrap();

        // Resurrect
        let (file_id2, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();
        assert_eq!(file_id, file_id2);

        // chunker_version should be cleared
        assert_eq!(get_chunker_version(&conn, &file_id).unwrap(), None);
    }

    #[test]
    fn test_count_stale_chunker_version() {
        let conn = setup();
        let (fid1, _) = get_or_create_file_id(&conn, "/notes/a.md").unwrap();
        let (fid2, _) = get_or_create_file_id(&conn, "/notes/b.md").unwrap();
        let (fid3, _) = get_or_create_file_id(&conn, "/notes/c.md").unwrap();

        // All NULL → all stale
        assert_eq!(count_stale_chunker_version(&conn, 2).unwrap(), 3);

        // Mark one as current
        mark_indexed(&conn, &fid1, "h1", 2).unwrap();
        assert_eq!(count_stale_chunker_version(&conn, 2).unwrap(), 2);

        // Mark another with old version
        mark_indexed(&conn, &fid2, "h2", 1).unwrap();
        assert_eq!(count_stale_chunker_version(&conn, 2).unwrap(), 2);

        // Mark last as current
        mark_indexed(&conn, &fid3, "h3", 2).unwrap();
        assert_eq!(count_stale_chunker_version(&conn, 2).unwrap(), 1); // fid2 still stale
    }
}
