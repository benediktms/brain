use rusqlite::Connection;

use crate::error::Result;

/// A chunk's metadata for SQLite bookkeeping.
pub struct ChunkMeta {
    pub chunk_id: String,
    pub chunk_ord: usize,
    pub chunk_hash: String,
    pub chunker_version: u32,
    pub content: String,
    pub heading_path: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub token_estimate: usize,
}

/// Replace all chunk metadata for a file in a single transaction.
/// Deletes existing chunks for the file_id and inserts new ones.
pub fn replace_chunk_metadata(
    conn: &Connection,
    file_id: &str,
    chunks: &[ChunkMeta],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    tx.execute("DELETE FROM chunks WHERE file_id = ?1", [file_id])?;

    let mut stmt = tx.prepare_cached(
        "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, chunker_version,
                             content, heading_path, byte_start, byte_end, token_estimate)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;

    for chunk in chunks {
        stmt.execute(rusqlite::params![
            chunk.chunk_id,
            file_id,
            chunk.chunk_ord as i64,
            chunk.chunk_hash,
            chunk.chunker_version,
            chunk.content,
            chunk.heading_path,
            chunk.byte_start as i64,
            chunk.byte_end as i64,
            chunk.token_estimate as i64,
        ])?;
    }

    drop(stmt);
    tx.commit()?;
    Ok(())
}

/// A chunk row retrieved from SQLite (with joined file path).
#[derive(Debug, Clone)]
pub struct ChunkRow {
    pub chunk_id: String,
    pub file_id: String,
    pub file_path: String,
    pub content: String,
    pub heading_path: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub token_estimate: usize,
    pub last_indexed_at: Option<i64>,
}

/// Look up chunks by their IDs, joining with the files table for path and timestamp.
pub fn get_chunks_by_ids(conn: &Connection, chunk_ids: &[String]) -> Result<Vec<ChunkRow>> {
    if chunk_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Build a parameterized IN clause
    let placeholders: Vec<String> = (1..=chunk_ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT c.chunk_id, c.file_id, f.path, c.content, c.heading_path,
                c.byte_start, c.byte_end, c.token_estimate, f.last_indexed_at
         FROM chunks c
         JOIN files f ON f.file_id = c.file_id
         WHERE c.chunk_id IN ({})
         AND f.deleted_at IS NULL",
        placeholders.join(", ")
    );

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::types::ToSql> = chunk_ids
        .iter()
        .map(|id| id as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = stmt.query_map(params.as_slice(), |row| {
        Ok(ChunkRow {
            chunk_id: row.get(0)?,
            file_id: row.get(1)?,
            file_path: row.get(2)?,
            content: row.get(3)?,
            heading_path: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            byte_start: row.get::<_, i64>(5)? as usize,
            byte_end: row.get::<_, i64>(6)? as usize,
            token_estimate: row.get::<_, i64>(7)? as usize,
            last_indexed_at: row.get(8)?,
        })
    })?;

    super::collect_rows(rows)
}

/// Upsert a task capsule chunk into SQLite.
///
/// Creates a synthetic `files` row if needed (with path = file_id),
/// then replaces the chunk metadata. This ensures task capsule chunks
/// are found by both FTS5 and the enrichment JOIN in get_chunks_by_ids.
pub fn upsert_task_chunk(
    conn: &Connection,
    task_file_id: &str, // e.g. "task:BRN-01KK" or "task-outcome:BRN-01KK"
    capsule_text: &str,
) -> Result<()> {
    // Ensure a synthetic files row exists
    conn.execute(
        "INSERT OR IGNORE INTO files (file_id, path, indexing_state) VALUES (?1, ?1, 'idle')",
        [task_file_id],
    )?;
    // Update last_indexed_at for recency signal
    conn.execute(
        "UPDATE files SET last_indexed_at = strftime('%s','now') WHERE file_id = ?1",
        [task_file_id],
    )?;
    // Replace chunk (delete + insert for FTS trigger)
    conn.execute("DELETE FROM chunks WHERE file_id = ?1", [task_file_id])?;
    let chunk_id = format!("{task_file_id}:0");
    conn.execute(
        "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, heading_path, byte_start, byte_end, token_estimate)
         VALUES (?1, ?2, 0, '', ?3, '', 0, 0, ?4)",
        rusqlite::params![
            chunk_id,
            task_file_id,
            capsule_text,
            crate::tokens::estimate_tokens(capsule_text) as i64,
        ],
    )?;
    Ok(())
}

/// Get ordered chunk hashes for a file (by chunk_ord).
pub fn get_chunk_hashes(conn: &Connection, file_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT chunk_hash FROM chunks WHERE file_id = ?1 ORDER BY chunk_ord")?;
    let rows = stmt.query_map([file_id], |row| row.get::<_, String>(0))?;
    super::collect_rows(rows)
}

#[cfg(test)]
impl ChunkMeta {
    /// Create a test ChunkMeta with sensible defaults for the new fields.
    pub fn test(chunk_id: &str, ord: usize, hash: &str) -> Self {
        Self {
            chunk_id: chunk_id.to_string(),
            chunk_ord: ord,
            chunk_hash: hash.to_string(),
            chunker_version: 1,
            content: format!("test content {ord}"),
            heading_path: String::new(),
            byte_start: 0,
            byte_end: 0,
            token_estimate: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::files::get_or_create_file_id;
    use crate::db::schema::init_schema;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn test_replace_chunk_metadata() {
        let conn = setup();
        let (file_id, _) = get_or_create_file_id(&conn, "/notes/test.md").unwrap();

        let chunks = vec![
            ChunkMeta::test(&format!("{file_id}:0"), 0, "hash0"),
            ChunkMeta::test(&format!("{file_id}:1"), 1, "hash1"),
        ];

        replace_chunk_metadata(&conn, &file_id, &chunks).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Replace with fewer chunks
        let chunks2 = vec![ChunkMeta::test(&format!("{file_id}:0"), 0, "newhash")];
        replace_chunk_metadata(&conn, &file_id, &chunks2).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_replace_does_not_affect_other_files() {
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

        // Replace A's chunks
        replace_chunk_metadata(&conn, &file_a, &[]).unwrap();

        // B's chunks still intact
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_b],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
