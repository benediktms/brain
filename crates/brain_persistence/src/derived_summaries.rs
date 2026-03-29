use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{BrainCoreError, Result};

#[derive(Debug, Clone)]
pub struct DerivedSummaryRow {
    pub id: String,
    pub scope_type: String,
    pub scope_value: String,
    pub content: String,
    pub stale: bool,
    pub generated_at: i64,
}

#[derive(Debug, Clone)]
pub struct GeneratedScopeSummaryRow {
    pub id: String,
    pub source_content: String,
    pub content_changed: bool,
}

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DerivedSummaryRow> {
    Ok(DerivedSummaryRow {
        id: row.get(0)?,
        scope_type: row.get(1)?,
        scope_value: row.get(2)?,
        content: row.get(3)?,
        stale: row.get::<_, i64>(4)? != 0,
        generated_at: row.get(5)?,
    })
}

pub fn generate_scope_summary(
    conn: &Connection,
    scope_type: &str,
    scope_value: &str,
) -> Result<GeneratedScopeSummaryRow> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let sources: Vec<(String, String)> = match scope_type {
        "directory" => {
            let pattern = format!("{}%", scope_value);
            let mut stmt = conn.prepare(
                "SELECT c.chunk_id, c.content
                 FROM chunks c
                 JOIN files f ON c.file_id = f.file_id
                 WHERE f.path LIKE ?1
                 ORDER BY f.path, c.chunk_ord",
            )?;
            stmt.query_map(params![pattern], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| BrainCoreError::Database(e.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| BrainCoreError::Database(e.to_string()))?
        }
        "tag" => {
            let pattern = format!("%{}%", scope_value);
            let mut stmt = conn.prepare(
                "SELECT summary_id, content
                 FROM summaries
                 WHERE tags LIKE ?1
                 ORDER BY created_at",
            )?;
            stmt.query_map(params![pattern], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| BrainCoreError::Database(e.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| BrainCoreError::Database(e.to_string()))?
        }
        other => {
            return Err(BrainCoreError::Parse(format!(
                "unknown derived summary scope_type: {other}"
            )));
        }
    };

    let source_ids: Vec<&str> = sources.iter().map(|(id, _)| id.as_str()).collect();
    let contents: Vec<&str> = sources.iter().map(|(_, c)| c.as_str()).collect();
    let source_content = contents.join("\n\n");
    let new_hash = blake3::hash(source_content.as_bytes()).to_hex().to_string();

    let existing_hash: Option<String> = conn
        .query_row(
            "SELECT source_content_hash FROM derived_summaries
             WHERE scope_type = ?1 AND scope_value = ?2",
            params![scope_type, scope_value],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(|opt| opt.flatten())
        .unwrap_or(None);

    if existing_hash.as_deref() == Some(&new_hash) {
        conn.execute(
            "UPDATE derived_summaries SET stale = 0
             WHERE scope_type = ?1 AND scope_value = ?2 AND source_content_hash = ?3",
            params![scope_type, scope_value, new_hash],
        )
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;

        let id: String = conn
            .query_row(
                "SELECT id FROM derived_summaries
                 WHERE scope_type = ?1 AND scope_value = ?2",
                params![scope_type, scope_value],
                |row| row.get(0),
            )
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;

        return Ok(GeneratedScopeSummaryRow {
            id,
            source_content,
            content_changed: false,
        });
    }

    let summary_content: String = contents
        .iter()
        .map(|c| c.get(..200).unwrap_or(c))
        .collect::<Vec<&str>>()
        .join("\n");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let existing_id: Option<String> = conn
        .query_row(
            "SELECT id FROM derived_summaries WHERE scope_type = ?1 AND scope_value = ?2",
            params![scope_type, scope_value],
            |row| row.get(0),
        )
        .optional()
        .unwrap_or(None);

    let id = existing_id.unwrap_or_else(|| ulid::Ulid::new().to_string());

    let source_type = match scope_type {
        "directory" => "chunk",
        "tag" => "episode",
        _ => unreachable!("validated scope_type in source fetch"),
    };

    let tx = conn
        .unchecked_transaction()
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;

    let updated = tx
        .execute(
            "UPDATE derived_summaries
             SET content = ?1, stale = 0, generated_at = ?2, source_content_hash = ?3
             WHERE id = ?4",
            params![summary_content, now, new_hash, id],
        )
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;

    if updated == 0 {
        tx.execute(
            "INSERT INTO derived_summaries
                 (id, scope_type, scope_value, content, stale, generated_at, source_content_hash)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
            params![id, scope_type, scope_value, summary_content, now, new_hash],
        )
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;
    }

    tx.execute(
        "DELETE FROM summary_sources WHERE summary_id = ?1",
        params![id],
    )
    .map_err(|e| BrainCoreError::Database(e.to_string()))?;

    {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO summary_sources (summary_id, source_id, source_type, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
        for src_id in source_ids {
            stmt.execute(params![id, src_id, source_type, now])
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
        }
    }

    tx.commit()
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;

    Ok(GeneratedScopeSummaryRow {
        id,
        source_content,
        content_changed: true,
    })
}

pub fn get_scope_summary(
    conn: &Connection,
    scope_type: &str,
    scope_value: &str,
) -> Result<Option<DerivedSummaryRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, scope_type, scope_value, content, stale, generated_at
         FROM derived_summaries
         WHERE scope_type = ?1 AND scope_value = ?2",
    )?;

    let mut rows = stmt
        .query_map(params![scope_type, scope_value], map_row)
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;

    match rows.next() {
        Some(Ok(summary)) => Ok(Some(summary)),
        Some(Err(e)) => Err(BrainCoreError::Database(e.to_string())),
        None => Ok(None),
    }
}

pub fn mark_scope_stale(conn: &Connection, scope_type: &str, scope_value: &str) -> Result<usize> {
    let n = conn
        .execute(
            "UPDATE derived_summaries SET stale = 1
             WHERE scope_type = ?1 AND scope_value = ?2",
            params![scope_type, scope_value],
        )
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;
    Ok(n)
}

pub fn search_derived_summaries(
    conn: &Connection,
    query: &str,
    limit: usize,
) -> Result<Vec<DerivedSummaryRow>> {
    let fts_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='table' AND name='fts_derived_summaries'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)
        .unwrap_or(false);

    if fts_exists {
        let mut stmt = conn.prepare(
            "SELECT ds.id, ds.scope_type, ds.scope_value, ds.content,
                    ds.stale, ds.generated_at
             FROM fts_derived_summaries fts
             JOIN derived_summaries ds ON ds.rowid = fts.rowid
             WHERE fts_derived_summaries MATCH ?1
             LIMIT ?2",
        )?;
        let summaries = stmt
            .query_map(params![query, limit as i64], map_row)
            .map_err(|e| BrainCoreError::Database(e.to_string()))?
            .collect::<std::result::Result<Vec<DerivedSummaryRow>, _>>()
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
        Ok(summaries)
    } else {
        let pattern = format!("%{}%", query);
        let mut stmt = conn.prepare(
            "SELECT id, scope_type, scope_value, content, stale, generated_at
             FROM derived_summaries
             WHERE content LIKE ?1
             LIMIT ?2",
        )?;
        let summaries = stmt
            .query_map(params![pattern, limit as i64], map_row)
            .map_err(|e| BrainCoreError::Database(e.to_string()))?
            .collect::<std::result::Result<Vec<DerivedSummaryRow>, _>>()
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
        Ok(summaries)
    }
}

pub fn list_stale_summaries(conn: &Connection, limit: usize) -> Result<Vec<DerivedSummaryRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, scope_type, scope_value, content, stale, generated_at
         FROM derived_summaries
         WHERE stale = 1
         ORDER BY generated_at ASC
         LIMIT ?1",
    )?;
    let summaries = stmt
        .query_map(params![limit as i64], map_row)
        .map_err(|e| BrainCoreError::Database(e.to_string()))?
        .collect::<std::result::Result<Vec<DerivedSummaryRow>, _>>()
        .map_err(|e| BrainCoreError::Database(e.to_string()))?;
    Ok(summaries)
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

    fn insert_chunk(conn: &Connection, file_id: &str, path: &str, chunk_id: &str, content: &str) {
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES (?1, ?2, 'idle')",
            params![file_id, path],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks
                 (chunk_id, file_id, chunk_ord, chunk_hash, content, heading_path,
                  byte_start, byte_end, token_estimate)
             VALUES (?1, ?2, 0, 'h', ?3, '', 0, 0, 0)",
            params![chunk_id, file_id, content],
        )
        .unwrap();
    }

    #[test]
    fn test_generate_scope_summary_directory_round_trip_and_no_change_short_circuit() {
        let conn = setup();
        insert_chunk(
            &conn,
            "file-1",
            "notes/a.md",
            "chunk-1",
            "first chunk content",
        );
        insert_chunk(
            &conn,
            "file-2",
            "notes/b.md",
            "chunk-2",
            "second chunk content",
        );

        let first = generate_scope_summary(&conn, "directory", "notes/").unwrap();
        assert!(first.content_changed);
        assert!(!first.id.is_empty());
        assert!(first.source_content.contains("first chunk content"));
        assert!(first.source_content.contains("second chunk content"));

        let second = generate_scope_summary(&conn, "directory", "notes/").unwrap();
        assert!(!second.content_changed);
        assert_eq!(first.id, second.id);
    }

    #[test]
    fn test_get_mark_search_and_list_stale() {
        let conn = setup();
        insert_chunk(&conn, "file-3", "src/lib.rs", "chunk-3", "alpha beta gamma");
        let created = generate_scope_summary(&conn, "directory", "src/").unwrap();

        let fetched = get_scope_summary(&conn, "directory", "src/")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.id, created.id);
        assert!(!fetched.stale);

        let updated = mark_scope_stale(&conn, "directory", "src/").unwrap();
        assert_eq!(updated, 1);

        let stale = list_stale_summaries(&conn, 10).unwrap();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].id, created.id);

        let found = search_derived_summaries(&conn, "alpha", 10).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, created.id);
    }
}
