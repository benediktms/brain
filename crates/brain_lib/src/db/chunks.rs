use rusqlite::Connection;

use crate::error::Result;

/// A chunk's metadata for SQLite bookkeeping.
pub struct ChunkMeta {
    pub chunk_id: String,
    pub chunk_ord: usize,
    pub chunk_hash: String,
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
        "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash) VALUES (?1, ?2, ?3, ?4)",
    )?;

    for chunk in chunks {
        stmt.execute(rusqlite::params![
            chunk.chunk_id,
            file_id,
            chunk.chunk_ord as i64,
            chunk.chunk_hash,
        ])?;
    }

    drop(stmt);
    tx.commit()?;
    Ok(())
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
            ChunkMeta {
                chunk_id: format!("{file_id}:0"),
                chunk_ord: 0,
                chunk_hash: "hash0".to_string(),
            },
            ChunkMeta {
                chunk_id: format!("{file_id}:1"),
                chunk_ord: 1,
                chunk_hash: "hash1".to_string(),
            },
        ];

        replace_chunk_metadata(&conn, &file_id, &chunks).unwrap();

        // Verify chunks inserted
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                [&file_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Replace with fewer chunks
        let chunks2 = vec![ChunkMeta {
            chunk_id: format!("{file_id}:0"),
            chunk_ord: 0,
            chunk_hash: "newhash".to_string(),
        }];

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

        let chunks_a = vec![ChunkMeta {
            chunk_id: format!("{file_a}:0"),
            chunk_ord: 0,
            chunk_hash: "ha".to_string(),
        }];
        let chunks_b = vec![ChunkMeta {
            chunk_id: format!("{file_b}:0"),
            chunk_ord: 0,
            chunk_hash: "hb".to_string(),
        }];

        replace_chunk_metadata(&conn, &file_a, &chunks_a).unwrap();
        replace_chunk_metadata(&conn, &file_b, &chunks_b).unwrap();

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
