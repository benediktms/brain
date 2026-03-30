use rusqlite::Connection;

use crate::error::Result;

/// Sanitize user input for FTS5 MATCH queries by quoting unquoted tokens.
///
/// FTS5 interprets hyphens as column negation and words like AND/OR/NOT as
/// boolean operators. This function wraps every unquoted token in double quotes
/// to force literal matching, while preserving already-quoted phrase strings.
pub fn sanitize_fts_query(input: &str) -> String {
    let input = input.trim();
    if input.is_empty() {
        return String::new();
    }

    let mut result = String::with_capacity(input.len() + 16);
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        if ch == '"' {
            // Pass through quoted phrase as-is
            result.push(chars.next().unwrap());
            loop {
                match chars.next() {
                    Some('"') => {
                        result.push('"');
                        break;
                    }
                    Some(c) => result.push(c),
                    None => {
                        // Unterminated quote — close it
                        result.push('"');
                        break;
                    }
                }
            }
        } else if ch.is_whitespace() {
            result.push(chars.next().unwrap());
        } else {
            // Collect token until whitespace or quote
            let mut token = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() || c == '"' {
                    break;
                }
                token.push(chars.next().unwrap());
            }
            // Escape embedded double quotes by doubling them, then wrap
            result.push('"');
            result.push_str(&token.replace('"', "\"\""));
            result.push('"');
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Brain-scoped query builders
// ---------------------------------------------------------------------------

/// Build the SQL + params for a brain-scoped FTS chunks query.
fn build_fts_chunks_query(
    query: &str,
    limit: usize,
    brain_ids: Option<&[String]>,
) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params.push(Box::new(query.to_string()));

    let brain_filter = match brain_ids {
        None => String::new(),
        Some(ids) if ids.is_empty() => String::new(),
        Some(ids) => {
            let placeholders: Vec<String> = ids
                .iter()
                .map(|id| {
                    params.push(Box::new(id.clone()));
                    format!("?{}", params.len())
                })
                .collect();
            format!(" AND c.brain_id IN ({})", placeholders.join(", "))
        }
    };

    params.push(Box::new(limit as i64));
    let limit_placeholder = format!("?{}", params.len());

    let sql = format!(
        "SELECT c.chunk_id, -bm25(fts_chunks) AS score
         FROM fts_chunks
         JOIN chunks c ON c.rowid = fts_chunks.rowid
         WHERE fts_chunks MATCH ?1{brain_filter}
         ORDER BY score DESC
         LIMIT {limit_placeholder}"
    );

    (sql, params)
}

/// Build the SQL + params for a brain-scoped FTS summaries query.
fn build_fts_summaries_query(
    query: &str,
    limit: usize,
    brain_ids: Option<&[String]>,
) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params.push(Box::new(query.to_string()));

    let brain_filter = match brain_ids {
        None => String::new(),
        Some(ids) if ids.is_empty() => String::new(),
        Some(ids) => {
            let placeholders: Vec<String> = ids
                .iter()
                .map(|id| {
                    params.push(Box::new(id.clone()));
                    format!("?{}", params.len())
                })
                .collect();
            format!(" AND s.brain_id IN ({})", placeholders.join(", "))
        }
    };

    params.push(Box::new(limit as i64));
    let limit_placeholder = format!("?{}", params.len());

    let sql = format!(
        "SELECT s.summary_id, -bm25(fts_summaries) AS score
         FROM fts_summaries
         JOIN summaries s ON s.rowid = fts_summaries.rowid
         WHERE fts_summaries MATCH ?1{brain_filter}
         ORDER BY score DESC
         LIMIT {limit_placeholder}"
    );

    (sql, params)
}

/// A full-text search result with normalized BM25 score.
#[derive(Debug, Clone)]
pub struct FtsResult {
    pub chunk_id: String,
    /// BM25 score normalized to [0, 1] (1.0 = best match in result set).
    pub score: f64,
}

/// Search the FTS5 index for chunks matching the query.
///
/// When `brain_ids` is `Some`, results are filtered to chunks belonging to
/// the specified brains.  `None` means no brain filter (workspace-global).
/// Scores are normalized to [0, 1] by dividing by the maximum score.
pub fn search_fts(
    conn: &Connection,
    query: &str,
    limit: usize,
    brain_ids: Option<&[String]>,
) -> Result<Vec<FtsResult>> {
    let query = sanitize_fts_query(query);
    if query.is_empty() {
        return Ok(Vec::new());
    }

    // BM25 returns negative values where more negative = more relevant.
    // We negate to make higher = more relevant.
    let (sql, params) = build_fts_chunks_query(&query, limit, brain_ids);
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
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

        let results = search_fts(&conn, "rust", 10, None).unwrap();
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

        let results = search_fts(&conn, "rust", 10, None).unwrap();
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

        let results = search_fts(&conn, "rust", 1, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_fts_no_results() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "javascript", 10, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_fts_empty_query() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        let results = search_fts(&conn, "", 10, None).unwrap();
        assert!(results.is_empty());

        let results = search_fts(&conn, "   ", 10, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_fts_phrase_query() {
        let conn = Connection::open_in_memory().unwrap();
        setup_with_data(&conn);

        // Phrase query — only f1:1 has "machine learning" adjacent
        let results = search_fts(&conn, "\"machine learning\"", 10, None).unwrap();
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

        let results = search_fts(&conn, "rust", 10, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_id, "f1:2");
    }

    // ── sanitize_fts_query unit tests ───────────────────────────────────

    #[test]
    fn test_sanitize_simple_token() {
        assert_eq!(sanitize_fts_query("rust"), "\"rust\"");
    }

    #[test]
    fn test_sanitize_multiple_tokens() {
        assert_eq!(
            sanitize_fts_query("remediation post-migration"),
            "\"remediation\" \"post-migration\""
        );
    }

    #[test]
    fn test_sanitize_hyphenated_term() {
        assert_eq!(sanitize_fts_query("post-migration"), "\"post-migration\"");
    }

    #[test]
    fn test_sanitize_preserves_quoted_phrase() {
        assert_eq!(
            sanitize_fts_query("\"machine learning\""),
            "\"machine learning\""
        );
    }

    #[test]
    fn test_sanitize_mixed_quoted_and_unquoted() {
        assert_eq!(
            sanitize_fts_query("rust \"machine learning\" safety"),
            "\"rust\" \"machine learning\" \"safety\""
        );
    }

    #[test]
    fn test_sanitize_boolean_operators_neutralized() {
        assert_eq!(
            sanitize_fts_query("rust AND memory"),
            "\"rust\" \"AND\" \"memory\""
        );
        assert_eq!(
            sanitize_fts_query("rust OR python"),
            "\"rust\" \"OR\" \"python\""
        );
        assert_eq!(sanitize_fts_query("NOT rust"), "\"NOT\" \"rust\"");
    }

    #[test]
    fn test_sanitize_empty_input() {
        assert_eq!(sanitize_fts_query(""), "");
        assert_eq!(sanitize_fts_query("   "), "");
    }

    #[test]
    fn test_sanitize_unterminated_quote() {
        // Unterminated quote gets closed
        assert_eq!(sanitize_fts_query("\"open phrase"), "\"open phrase\"");
    }

    #[test]
    fn test_sanitize_special_chars() {
        // Parentheses, asterisks — all get quoted as tokens
        assert_eq!(sanitize_fts_query("test*"), "\"test*\"");
        assert_eq!(sanitize_fts_query("(group)"), "\"(group)\"");
    }

    // ── Integration: search with hyphenated terms ───────────────────────

    #[test]
    fn test_search_fts_hyphenated_query() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/test.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
             VALUES ('f1:0', 'f1', 0, 'h0', 'remediation post-migration steps')",
            [],
        )
        .unwrap();

        // This would fail without sanitization: "no such column: migration"
        let results = search_fts(&conn, "post-migration", 10, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_id, "f1:0");
    }

    // --- brain_id filtering tests ---

    /// Insert chunks belonging to two different brains.
    fn setup_multi_brain(conn: &Connection) {
        init_schema(conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, brain_id) VALUES ('fa', '/brain-a/notes.md', 'brain-a')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (file_id, path, brain_id) VALUES ('fb', '/brain-b/notes.md', 'brain-b')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, brain_id)
             VALUES ('fa:0', 'fa', 0, 'ha', 'rust programming language', 'brain-a')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, brain_id)
             VALUES ('fb:0', 'fb', 0, 'hb', 'rust ownership borrowing', 'brain-b')",
            [],
        )
        .unwrap();
    }

    #[test]
    fn test_search_fts_brain_id_single_filter() {
        let conn = Connection::open_in_memory().unwrap();
        setup_multi_brain(&conn);

        let brain_a = vec!["brain-a".to_string()];
        let results = search_fts(&conn, "rust", 10, Some(&brain_a)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_id, "fa:0");

        let brain_b = vec!["brain-b".to_string()];
        let results = search_fts(&conn, "rust", 10, Some(&brain_b)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].chunk_id, "fb:0");
    }

    #[test]
    fn test_search_fts_brain_id_multi_filter() {
        let conn = Connection::open_in_memory().unwrap();
        setup_multi_brain(&conn);

        let both = vec!["brain-a".to_string(), "brain-b".to_string()];
        let results = search_fts(&conn, "rust", 10, Some(&both)).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_fts_brain_id_none_returns_all() {
        let conn = Connection::open_in_memory().unwrap();
        setup_multi_brain(&conn);

        let results = search_fts(&conn, "rust", 10, None).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_fts_brain_id_empty_slice_returns_all() {
        let conn = Connection::open_in_memory().unwrap();
        setup_multi_brain(&conn);

        let empty: Vec<String> = vec![];
        let results = search_fts(&conn, "rust", 10, Some(&empty)).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_fts_brain_id_no_match() {
        let conn = Connection::open_in_memory().unwrap();
        setup_multi_brain(&conn);

        let unknown = vec!["brain-x".to_string()];
        let results = search_fts(&conn, "rust", 10, Some(&unknown)).unwrap();
        assert!(results.is_empty());
    }
}

/// A full-text search result from the summaries FTS5 index.
#[derive(Debug, Clone)]
pub struct FtsSummaryResult {
    pub summary_id: String,
    /// BM25 score normalized to [0, 1] (1.0 = best match in result set).
    pub score: f64,
}

/// Search the FTS5 summaries index for episodes/reflections matching the query.
///
/// Returns results ranked by BM25 relevance, with scores normalized
/// to [0, 1] by dividing by the maximum score in the result set.
pub fn search_summaries_fts(
    conn: &Connection,
    query: &str,
    limit: usize,
    brain_ids: Option<&[String]>,
) -> Result<Vec<FtsSummaryResult>> {
    let query = sanitize_fts_query(query);
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let (sql, params) = build_fts_summaries_query(&query, limit, brain_ids);
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })?;

    let mut raw: Vec<(String, f64)> = Vec::new();
    for row in rows {
        raw.push(row?);
    }

    if raw.is_empty() {
        return Ok(Vec::new());
    }

    let max_score = raw
        .iter()
        .map(|(_, s)| *s)
        .fold(f64::NEG_INFINITY, f64::max);

    let results = if max_score <= 0.0 {
        raw.into_iter()
            .map(|(summary_id, _)| FtsSummaryResult {
                summary_id,
                score: 0.0,
            })
            .collect()
    } else {
        raw.into_iter()
            .map(|(summary_id, score)| FtsSummaryResult {
                summary_id,
                score: score / max_score,
            })
            .collect()
    };

    Ok(results)
}

/// Check FTS5 consistency: return (chunk_count, fts_count).
pub fn fts_consistency(conn: &Connection) -> Result<(i64, i64)> {
    let chunk_count: i64 = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
    let fts_count: i64 = conn.query_row("SELECT COUNT(*) FROM fts_chunks", [], |row| row.get(0))?;
    Ok((chunk_count, fts_count))
}

/// Rebuild the FTS5 summaries index by re-reading all content from the summaries table.
///
/// Use this for doctor/repair operations when the FTS5 index may be
/// out of sync with the summaries table. Returns the count of summaries indexed.
pub fn reindex_summaries_fts(conn: &Connection) -> Result<usize> {
    conn.execute(
        "INSERT INTO fts_summaries(fts_summaries) VALUES('rebuild')",
        [],
    )?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM summaries", [], |row| row.get(0))?;
    Ok(count as usize)
}
