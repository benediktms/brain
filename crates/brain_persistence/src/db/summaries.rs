use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};
use ulid::Ulid;

use crate::error::Result;

/// An episode record for the summaries table.
pub struct Episode {
    pub brain_id: String,
    pub goal: String,
    pub actions: String,
    pub outcome: String,
    pub tags: Vec<String>,
    pub importance: f64,
}

/// A stored summary row.
#[derive(Debug, Clone)]
pub struct SummaryRow {
    pub summary_id: String,
    pub brain_id: String,
    pub kind: String,
    pub title: Option<String>,
    pub content: String,
    pub tags: Vec<String>,
    pub importance: f64,
    pub created_at: i64,
    pub updated_at: i64,
    pub parent_id: Option<String>,
    pub source_hash: Option<String>,
    pub confidence: f64,
    pub valid_from: Option<i64>,
}

/// Store an episode in the summaries table.
/// Returns the summary_id.
pub fn store_episode(conn: &Connection, episode: &Episode) -> Result<String> {
    let summary_id = Ulid::new().to_string();
    let now = crate::utils::now_ts();
    let tags_json = serde_json::to_string(&episode.tags).unwrap_or_else(|_| "[]".into());

    let content = format!(
        "Goal: {}\nActions: {}\nOutcome: {}",
        episode.goal, episode.actions, episode.outcome
    );

    conn.execute(
        "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, brain_id, valid_from, created_at, updated_at)
         VALUES (?1, 'episode', ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?7)",
        rusqlite::params![
            summary_id,
            episode.goal,
            content,
            tags_json,
            episode.importance,
            episode.brain_id,
            now,
        ],
    )?;

    Ok(summary_id)
}

/// Store a reflection in the summaries table, linked to source summaries.
/// Returns the summary_id.
pub fn store_reflection(
    conn: &Connection,
    title: &str,
    content: &str,
    source_ids: &[String],
    tags: &[String],
    importance: f64,
    brain_id: &str,
) -> Result<String> {
    let summary_id = Ulid::new().to_string();
    let now = crate::utils::now_ts();
    let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".into());

    let tx = conn.unchecked_transaction()?;

    tx.execute(
        "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, brain_id, valid_from, created_at, updated_at)
         VALUES (?1, 'reflection', ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?7)",
        rusqlite::params![summary_id, title, content, tags_json, importance, brain_id, now],
    )?;

    // Link reflection to sources
    let mut stmt = tx.prepare_cached(
        "INSERT INTO reflection_sources (reflection_id, source_id) VALUES (?1, ?2)",
    )?;
    for source_id in source_ids {
        stmt.execute(rusqlite::params![summary_id, source_id])?;
    }

    drop(stmt);
    tx.commit()?;

    Ok(summary_id)
}

/// Get a summary by ID.
/// No brain_id filter — PK lookup, intentional for cross-brain references.
pub fn get_summary(conn: &Connection, summary_id: &str) -> Result<Option<SummaryRow>> {
    let result = conn
        .query_row(
            "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at,
                    brain_id, parent_id, source_hash, confidence, valid_from
             FROM summaries WHERE summary_id = ?1",
            [summary_id],
            |row| {
                let tags_json: String = row.get(4)?;
                let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
                Ok(SummaryRow {
                    summary_id: row.get(0)?,
                    kind: row.get(1)?,
                    title: row.get(2)?,
                    content: row.get(3)?,
                    tags,
                    importance: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                    brain_id: row.get(8)?,
                    parent_id: row.get(9)?,
                    source_hash: row.get(10)?,
                    confidence: row.get(11)?,
                    valid_from: row.get(12)?,
                })
            },
        )
        .optional()?;

    Ok(result)
}

/// Get summaries by prefixed IDs (strips `sum:` prefix before lookup).
/// No brain_id filter — allows cross-brain references.
pub fn get_summaries_by_prefixed_ids(
    conn: &Connection,
    prefixed_ids: &[String],
) -> Result<Vec<SummaryRow>> {
    if prefixed_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<String> = prefixed_ids
        .iter()
        .map(|id| id.strip_prefix("sum:").unwrap_or(id).to_string())
        .collect();
    let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at,
                brain_id, parent_id, source_hash, confidence, valid_from
         FROM summaries WHERE summary_id IN ({})",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::types::ToSql> = ids
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();
    let rows = stmt.query_map(params.as_slice(), |row| {
        let tags_json: String = row.get(4)?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
        Ok(SummaryRow {
            summary_id: row.get(0)?,
            kind: row.get(1)?,
            title: row.get(2)?,
            content: row.get(3)?,
            tags,
            importance: row.get(5)?,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
            brain_id: row.get(8)?,
            parent_id: row.get(9)?,
            source_hash: row.get(10)?,
            confidence: row.get(11)?,
            valid_from: row.get(12)?,
        })
    })?;
    super::collect_rows(rows)
}

/// Store an ML-generated summary for a chunk.
/// Uses UPSERT to replace existing summary from the same summarizer.
pub fn store_ml_summary(
    conn: &Connection,
    chunk_id: &str,
    summary_text: &str,
    summarizer: &str,
) -> Result<String> {
    let summary_id = Ulid::new().to_string();
    let now = crate::utils::now_ts();
    conn.execute(
        "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, created_at, updated_at, summarizer, chunk_id)
         VALUES (?1, 'summary', NULL, ?2, '[]', 1.0, ?3, ?4, ?5, ?6)
         ON CONFLICT(chunk_id, summarizer) WHERE kind = 'summary'
         DO UPDATE SET content = excluded.content, updated_at = excluded.updated_at",
        rusqlite::params![summary_id, summary_text, now, now, summarizer, chunk_id],
    )?;
    Ok(summary_id)
}

/// Find chunk_ids that have no ML summary from the given summarizer.
/// Returns (chunk_id, content) pairs ordered by most recently indexed first.
pub fn find_chunks_lacking_summary(
    conn: &Connection,
    summarizer: &str,
    limit: usize,
) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT c.chunk_id, c.content FROM chunks c
         WHERE NOT EXISTS (
             SELECT 1 FROM summaries s
             WHERE s.chunk_id = c.chunk_id AND s.summarizer = ?1 AND s.kind = 'summary'
         )
         ORDER BY c.rowid DESC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![summarizer, limit as i64], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    super::collect_rows(rows)
}

/// Batch-load ML summaries for a set of chunk_ids.
/// Returns a map from chunk_id to summary text.
/// Prefers the most recent summary if multiple summarizers exist.
pub fn get_ml_summaries_for_chunks(
    conn: &Connection,
    chunk_ids: &[&str],
) -> Result<HashMap<String, String>> {
    if chunk_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders: Vec<String> = (1..=chunk_ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT chunk_id, content FROM summaries
         WHERE kind = 'summary' AND chunk_id IN ({})
         ORDER BY updated_at DESC",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::types::ToSql> = chunk_ids
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();
    let rows = stmt.query_map(params.as_slice(), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut map = HashMap::new();
    for row in rows {
        let (chunk_id, content) = row?;
        map.entry(chunk_id).or_insert(content);
    }
    Ok(map)
}

/// List recent episodes.
/// When `brain_id` is non-empty, filters to that brain only. Empty string returns all brains.
pub fn list_episodes(conn: &Connection, limit: usize, brain_id: &str) -> Result<Vec<SummaryRow>> {
    let row_mapper = |row: &rusqlite::Row<'_>| {
        let tags_json: String = row.get(4)?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
        Ok(SummaryRow {
            summary_id: row.get(0)?,
            kind: row.get(1)?,
            title: row.get(2)?,
            content: row.get(3)?,
            tags,
            importance: row.get(5)?,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
            brain_id: row.get(8)?,
            parent_id: row.get(9)?,
            source_hash: row.get(10)?,
            confidence: row.get(11)?,
            valid_from: row.get(12)?,
        })
    };

    if brain_id.is_empty() {
        let mut stmt = conn.prepare(
            "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at,
                    brain_id, parent_id, source_hash, confidence, valid_from
             FROM summaries WHERE kind = 'episode'
             ORDER BY created_at DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], row_mapper)?;
        super::collect_rows(rows)
    } else {
        let mut stmt = conn.prepare(
            "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at,
                    brain_id, parent_id, source_hash, confidence, valid_from
             FROM summaries WHERE kind = 'episode' AND brain_id = ?2
             ORDER BY created_at DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(rusqlite::params![limit as i64, brain_id], row_mapper)?;
        super::collect_rows(rows)
    }
}

/// List recent episodes across multiple brains.
/// Returns episodes where brain_id is in the provided list, newest first.
pub fn list_episodes_multi_brain(
    conn: &Connection,
    limit: usize,
    brain_ids: &[String],
) -> Result<Vec<SummaryRow>> {
    if brain_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders: Vec<String> = (1..=brain_ids.len()).map(|i| format!("?{}", i + 1)).collect();
    let sql = format!(
        "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at,
                brain_id, parent_id, source_hash, confidence, valid_from
         FROM summaries WHERE kind = 'episode' AND brain_id IN ({})
         ORDER BY created_at DESC
         LIMIT ?1",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::with_capacity(brain_ids.len() + 1);
    params.push(Box::new(limit as i64));
    for id in brain_ids {
        params.push(Box::new(id.clone()));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|b| b.as_ref()).collect();
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let tags_json: String = row.get(4)?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
        Ok(SummaryRow {
            summary_id: row.get(0)?,
            kind: row.get(1)?,
            title: row.get(2)?,
            content: row.get(3)?,
            tags,
            importance: row.get(5)?,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
            brain_id: row.get(8)?,
            parent_id: row.get(9)?,
            source_hash: row.get(10)?,
            confidence: row.get(11)?,
            valid_from: row.get(12)?,
        })
    })?;
    super::collect_rows(rows)
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
    fn test_store_and_get_episode() {
        let conn = setup();

        let episode = Episode {
            brain_id: "brain-test".into(),
            goal: "Fix the bug".into(),
            actions: "Debugged and patched".into(),
            outcome: "Bug fixed".into(),
            tags: vec!["debugging".into(), "rust".into()],
            importance: 0.8,
        };

        let id = store_episode(&conn, &episode).unwrap();
        assert!(!id.is_empty());

        let summary = get_summary(&conn, &id).unwrap().unwrap();
        assert_eq!(summary.kind, "episode");
        assert_eq!(summary.brain_id, "brain-test");
        assert_eq!(summary.title.as_deref(), Some("Fix the bug"));
        assert!(summary.content.contains("Fix the bug"));
        assert!(summary.content.contains("Bug fixed"));
        assert_eq!(summary.tags, vec!["debugging", "rust"]);
        assert!((summary.importance - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_store_reflection_with_sources() {
        let conn = setup();

        // Create two episodes as sources
        let ep1 = store_episode(
            &conn,
            &Episode {
                brain_id: "brain-a".into(),
                goal: "Goal 1".into(),
                actions: "Actions 1".into(),
                outcome: "Outcome 1".into(),
                tags: vec![],
                importance: 1.0,
            },
        )
        .unwrap();
        let ep2 = store_episode(
            &conn,
            &Episode {
                brain_id: "brain-a".into(),
                goal: "Goal 2".into(),
                actions: "Actions 2".into(),
                outcome: "Outcome 2".into(),
                tags: vec![],
                importance: 1.0,
            },
        )
        .unwrap();

        // Create a reflection
        let ref_id = store_reflection(
            &conn,
            "My Reflection",
            "I learned that...",
            &[ep1.clone(), ep2.clone()],
            &["learning".into()],
            0.9,
            "brain-a",
        )
        .unwrap();

        let summary = get_summary(&conn, &ref_id).unwrap().unwrap();
        assert_eq!(summary.kind, "reflection");
        assert_eq!(summary.title.as_deref(), Some("My Reflection"));

        // Verify sources linked
        let source_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM reflection_sources WHERE reflection_id = ?1",
                [&ref_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 2);
    }

    #[test]
    fn test_list_episodes() {
        let conn = setup();

        for i in 0..5 {
            store_episode(
                &conn,
                &Episode {
                    brain_id: "brain-x".into(),
                    goal: format!("Goal {i}"),
                    actions: format!("Actions {i}"),
                    outcome: format!("Outcome {i}"),
                    tags: vec![],
                    importance: 1.0,
                },
            )
            .unwrap();
        }

        let episodes = list_episodes(&conn, 3, "brain-x").unwrap();
        assert_eq!(episodes.len(), 3);
        // All should be episodes
        for ep in &episodes {
            assert_eq!(ep.kind, "episode");
        }
    }

    #[test]
    fn test_list_episodes_brain_id_filter() {
        let conn = setup();

        store_episode(
            &conn,
            &Episode {
                brain_id: "brain-a".into(),
                goal: "A goal".into(),
                actions: "A actions".into(),
                outcome: "A outcome".into(),
                tags: vec![],
                importance: 1.0,
            },
        )
        .unwrap();
        store_episode(
            &conn,
            &Episode {
                brain_id: "brain-b".into(),
                goal: "B goal".into(),
                actions: "B actions".into(),
                outcome: "B outcome".into(),
                tags: vec![],
                importance: 1.0,
            },
        )
        .unwrap();

        let a_eps = list_episodes(&conn, 10, "brain-a").unwrap();
        assert_eq!(a_eps.len(), 1);
        assert_eq!(a_eps[0].brain_id, "brain-a");

        let b_eps = list_episodes(&conn, 10, "brain-b").unwrap();
        assert_eq!(b_eps.len(), 1);
        assert_eq!(b_eps[0].brain_id, "brain-b");

        let all_eps = list_episodes(&conn, 10, "").unwrap();
        assert_eq!(all_eps.len(), 2);
    }

    #[test]
    fn test_list_episodes_multi_brain() {
        let conn = setup();

        for bid in ["brain-1", "brain-2", "brain-3"] {
            store_episode(
                &conn,
                &Episode {
                    brain_id: bid.into(),
                    goal: format!("Goal {bid}"),
                    actions: "actions".into(),
                    outcome: "outcome".into(),
                    tags: vec![],
                    importance: 1.0,
                },
            )
            .unwrap();
        }

        let multi = list_episodes_multi_brain(
            &conn,
            10,
            &["brain-1".to_string(), "brain-2".to_string()],
        )
        .unwrap();
        assert_eq!(multi.len(), 2);
        let ids: Vec<&str> = multi.iter().map(|r| r.brain_id.as_str()).collect();
        assert!(ids.contains(&"brain-1"));
        assert!(ids.contains(&"brain-2"));
        assert!(!ids.contains(&"brain-3"));
    }

    #[test]
    fn test_get_nonexistent_summary() {
        let conn = setup();
        let result = get_summary(&conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    // --- ML summary tests ---

    fn insert_chunk(conn: &Connection, chunk_id: &str, file_id: &str, content: &str) {
        conn.execute(
            "INSERT OR IGNORE INTO files (file_id, path, indexing_state) VALUES (?1, ?1, 'idle')",
            [file_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content, heading_path, byte_start, byte_end, token_estimate)
             VALUES (?1, ?2, 0, '', ?3, '', 0, 0, 0)",
            rusqlite::params![chunk_id, file_id, content],
        )
        .unwrap();
    }

    #[test]
    fn test_store_ml_summary_and_get_round_trip() {
        let conn = setup();
        insert_chunk(&conn, "chunk:1", "file:1", "chunk content one");

        let id = store_ml_summary(&conn, "chunk:1", "ML summary text", "flan-t5-small").unwrap();
        assert!(!id.is_empty());

        let map = get_ml_summaries_for_chunks(&conn, &["chunk:1"]).unwrap();
        assert_eq!(
            map.get("chunk:1").map(String::as_str),
            Some("ML summary text")
        );
    }

    #[test]
    fn test_store_ml_summary_upsert_overwrites_same_summarizer() {
        let conn = setup();
        insert_chunk(&conn, "chunk:1", "file:1", "chunk content");

        store_ml_summary(&conn, "chunk:1", "first summary", "flan-t5-small").unwrap();
        store_ml_summary(&conn, "chunk:1", "updated summary", "flan-t5-small").unwrap();

        let map = get_ml_summaries_for_chunks(&conn, &["chunk:1"]).unwrap();
        assert_eq!(
            map.get("chunk:1").map(String::as_str),
            Some("updated summary"),
            "second store should overwrite first"
        );

        // Only one row in summaries for this chunk+summarizer
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summaries WHERE chunk_id = 'chunk:1' AND kind = 'summary' AND summarizer = 'flan-t5-small'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_store_ml_summary_different_summarizers_coexist() {
        let conn = setup();
        insert_chunk(&conn, "chunk:1", "file:1", "chunk content");

        store_ml_summary(&conn, "chunk:1", "flan summary", "flan-t5-small").unwrap();
        store_ml_summary(&conn, "chunk:1", "remote summary", "remote-llm").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summaries WHERE chunk_id = 'chunk:1' AND kind = 'summary'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_find_chunks_lacking_summary_returns_unsummarized() {
        let conn = setup();
        insert_chunk(&conn, "chunk:1", "file:1", "content one");
        insert_chunk(&conn, "chunk:2", "file:1", "content two");
        insert_chunk(&conn, "chunk:3", "file:1", "content three");

        // Summarize chunk:1 only
        store_ml_summary(&conn, "chunk:1", "summary for one", "flan-t5-small").unwrap();

        let lacking = find_chunks_lacking_summary(&conn, "flan-t5-small", 10).unwrap();
        let ids: Vec<&str> = lacking.iter().map(|(id, _)| id.as_str()).collect();

        assert!(ids.contains(&"chunk:2"), "chunk:2 should be returned");
        assert!(ids.contains(&"chunk:3"), "chunk:3 should be returned");
        assert!(!ids.contains(&"chunk:1"), "chunk:1 is already summarized");
    }

    #[test]
    fn test_find_chunks_lacking_summary_respects_limit() {
        let conn = setup();
        for i in 0..5 {
            insert_chunk(
                &conn,
                &format!("chunk:{i}"),
                "file:1",
                &format!("content {i}"),
            );
        }

        let lacking = find_chunks_lacking_summary(&conn, "flan-t5-small", 3).unwrap();
        assert_eq!(lacking.len(), 3);
    }

    #[test]
    fn test_find_chunks_lacking_summary_excludes_summarized() {
        let conn = setup();
        insert_chunk(&conn, "chunk:1", "file:1", "content one");

        store_ml_summary(&conn, "chunk:1", "summary", "flan-t5-small").unwrap();

        let lacking = find_chunks_lacking_summary(&conn, "flan-t5-small", 10).unwrap();
        assert!(lacking.is_empty(), "all chunks are summarized");
    }

    #[test]
    fn test_get_ml_summaries_for_chunks_empty_input() {
        let conn = setup();
        let map = get_ml_summaries_for_chunks(&conn, &[]).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_get_ml_summaries_for_chunks_batch() {
        let conn = setup();
        insert_chunk(&conn, "chunk:1", "file:1", "content one");
        insert_chunk(&conn, "chunk:2", "file:1", "content two");

        store_ml_summary(&conn, "chunk:1", "summary one", "flan-t5-small").unwrap();
        store_ml_summary(&conn, "chunk:2", "summary two", "flan-t5-small").unwrap();

        let map = get_ml_summaries_for_chunks(&conn, &["chunk:1", "chunk:2"]).unwrap();
        assert_eq!(map.get("chunk:1").map(String::as_str), Some("summary one"));
        assert_eq!(map.get("chunk:2").map(String::as_str), Some("summary two"));
    }

    #[test]
    fn test_get_ml_summaries_for_chunks_missing_chunk_not_in_result() {
        let conn = setup();
        let map = get_ml_summaries_for_chunks(&conn, &["chunk:nonexistent"]).unwrap();
        assert!(!map.contains_key("chunk:nonexistent"));
    }
}
