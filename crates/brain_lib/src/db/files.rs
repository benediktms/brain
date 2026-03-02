use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

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
            // Resurrect soft-deleted file
            conn.execute(
                "UPDATE files SET deleted_at = NULL, indexing_state = 'idle', content_hash = NULL WHERE file_id = ?1",
                [&file_id],
            )?;
        }
        return Ok((file_id, false));
    }

    // Create new entry with UUID v7
    let file_id = Uuid::now_v7().to_string();
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

/// Set the indexing state for a file (idle | indexing_started | indexed).
pub fn set_indexing_state(conn: &Connection, file_id: &str, state: &str) -> Result<()> {
    conn.execute(
        "UPDATE files SET indexing_state = ?1 WHERE file_id = ?2",
        rusqlite::params![state, file_id],
    )?;
    Ok(())
}

/// Mark a file as fully indexed: update hash, timestamp, and state.
pub fn mark_indexed(conn: &Connection, file_id: &str, content_hash: &str) -> Result<()> {
    let now = chrono_now();
    conn.execute(
        "UPDATE files SET content_hash = ?1, last_indexed_at = ?2, indexing_state = 'indexed' WHERE file_id = ?3",
        rusqlite::params![content_hash, now, file_id],
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
        let now = chrono_now();
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

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
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
        mark_indexed(&conn, &file_id, "abc123").unwrap();
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

        mark_indexed(&conn, &file_id, "hash123").unwrap();
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
}
