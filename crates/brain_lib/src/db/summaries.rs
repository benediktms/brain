use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::error::Result;

/// An episode record for the summaries table.
pub struct Episode {
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
    pub kind: String,
    pub title: Option<String>,
    pub content: String,
    pub tags: Vec<String>,
    pub importance: f64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Store an episode in the summaries table.
/// Returns the summary_id.
pub fn store_episode(conn: &Connection, episode: &Episode) -> Result<String> {
    let summary_id = Uuid::now_v7().to_string();
    let now = chrono_now();
    let tags_json = serde_json::to_string(&episode.tags).unwrap_or_else(|_| "[]".into());

    let content = format!(
        "Goal: {}\nActions: {}\nOutcome: {}",
        episode.goal, episode.actions, episode.outcome
    );

    conn.execute(
        "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, created_at, updated_at)
         VALUES (?1, 'episode', ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            summary_id,
            episode.goal,
            content,
            tags_json,
            episode.importance,
            now,
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
) -> Result<String> {
    let summary_id = Uuid::now_v7().to_string();
    let now = chrono_now();
    let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".into());

    let tx = conn.unchecked_transaction()?;

    tx.execute(
        "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, created_at, updated_at)
         VALUES (?1, 'reflection', ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![summary_id, title, content, tags_json, importance, now, now],
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
pub fn get_summary(conn: &Connection, summary_id: &str) -> Result<Option<SummaryRow>> {
    let result = conn
        .query_row(
            "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at
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
                })
            },
        )
        .optional()?;

    Ok(result)
}

/// List recent episodes.
pub fn list_episodes(conn: &Connection, limit: usize) -> Result<Vec<SummaryRow>> {
    let mut stmt = conn.prepare(
        "SELECT summary_id, kind, title, content, tags, importance, created_at, updated_at
         FROM summaries WHERE kind = 'episode'
         ORDER BY created_at DESC
         LIMIT ?1",
    )?;

    let rows = stmt.query_map([limit as i64], |row| {
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
        })
    })?;

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
    fn test_store_and_get_episode() {
        let conn = setup();

        let episode = Episode {
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
                    goal: format!("Goal {i}"),
                    actions: format!("Actions {i}"),
                    outcome: format!("Outcome {i}"),
                    tags: vec![],
                    importance: 1.0,
                },
            )
            .unwrap();
        }

        let episodes = list_episodes(&conn, 3).unwrap();
        assert_eq!(episodes.len(), 3);
        // All should be episodes
        for ep in &episodes {
            assert_eq!(ep.kind, "episode");
        }
    }

    #[test]
    fn test_get_nonexistent_summary() {
        let conn = setup();
        let result = get_summary(&conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }
}
