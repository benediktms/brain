use rusqlite::Connection;

use crate::error::Result;

/// A full-text search result with normalized BM25 score.
#[derive(Debug, Clone)]
pub struct FtsResult {
    pub chunk_id: String,
    /// BM25 score normalized to [0, 1] (1.0 = best match in result set).
    pub score: f64,
}

/// Search the FTS5 index for chunks matching the query.
///
/// Returns results ranked by BM25 relevance, with scores normalized
/// to [0, 1] by dividing by the maximum score in the result set.
/// Supports FTS5 query syntax (phrases, boolean operators).
pub fn search_fts(conn: &Connection, query: &str, limit: usize) -> Result<Vec<FtsResult>> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(Vec::new());
    }

    // BM25 returns negative values where more negative = more relevant.
    // We negate to make higher = more relevant.
    let mut stmt = conn.prepare(
        "SELECT c.chunk_id, -bm25(fts_chunks) AS score
         FROM fts_chunks
         JOIN chunks c ON c.rowid = fts_chunks.rowid
         WHERE fts_chunks MATCH ?1
         ORDER BY score DESC
         LIMIT ?2",
    )?;

    let rows = stmt.query_map(rusqlite::params![query, limit as i64], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })?;

    let mut raw: Vec<(String, f64)> = Vec::new();
    for row in rows {
        raw.push(row?);
    }

    if raw.is_empty() {
        return Ok(Vec::new());
    }

    // Normalize scores to [0, 1]
    let max_score = raw
        .iter()
        .map(|(_, s)| *s)
        .fold(f64::NEG_INFINITY, f64::max);

    let results = if max_score <= 0.0 {
        // All scores are zero or negative — assign 0.0
        raw.into_iter()
            .map(|(chunk_id, _)| FtsResult {
                chunk_id,
                score: 0.0,
            })
            .collect()
    } else {
        raw.into_iter()
            .map(|(chunk_id, score)| FtsResult {
                chunk_id,
                score: score / max_score,
            })
            .collect()
    };

    Ok(results)
}

/// Rebuild the FTS5 index by re-reading all content from the chunks table.
///
/// Use this for doctor/repair operations when the FTS5 index may be
/// out of sync with the chunks table.
pub fn reindex_fts(conn: &Connection) -> Result<()> {
    conn.execute("INSERT INTO fts_chunks(fts_chunks) VALUES('rebuild')", [])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;

    fn setup_with_data(conn: &Connection) {
        init_schema(conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/test.md', 'idle')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:0', 'f1', 0, 'h0', 'rust programming language systems programming')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:1', 'f1', 1, 'h1', 'python machine learning data science')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:2', 'f1', 2, 'h2', 'rust ownership borrowing memory safety')",
            [],
        )
        .unwrap();
    }

    #[test]
    fn test_reindex_fts_on_empty_table() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        reindex_fts(&conn).unwrap();
    }

    #[test]
    fn test_reindex_fts_rebuilds_index() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        reindex_fts(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'rust'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_search_fts_basic() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "rust", 10).unwrap();
        assert_eq!(results.len(), 2);

        // Both should be rust-related chunks
        let ids: Vec<&str> = results.iter().map(|r| r.chunk_id.as_str()).collect();
        assert!(ids.contains(&"f1:0"));
        assert!(ids.contains(&"f1:2"));
    }

    #[test]
    fn test_search_fts_scores_normalized() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "rust", 10).unwrap();
        assert!(!results.is_empty());

        // Best result should have score 1.0
        assert!(
            (results[0].score - 1.0).abs() < f64::EPSILON,
            "top result should be normalized to 1.0, got {}",
            results[0].score
        );

        // All scores should be in [0, 1]
        for r in &results {
            assert!(
                r.score >= 0.0 && r.score <= 1.0,
                "score out of range: {}",
                r.score
            );
        }
    }

    #[test]
    fn test_search_fts_limit() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "rust", 1).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_fts_no_results() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "javascript", 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_fts_empty_query() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "", 10).unwrap();
        assert!(results.is_empty());

        let results = search_fts(&conn, "   ", 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_fts_phrase_query() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        // Phrase query — only f1:1 has "machine learning" adjacent
        let results = search_fts(&conn, "\"machine learning\"", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_id, "f1:1");
    }

    #[test]
    fn test_search_fts_consistent_after_delete() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        // Delete a chunk — FTS trigger should clean up
        conn.execute("DELETE FROM chunks WHERE chunk_id = 'f1:0'", [])
            .unwrap();

        let results = search_fts(&conn, "rust", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_id, "f1:2");
    }
}
