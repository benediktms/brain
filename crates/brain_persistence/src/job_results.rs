use crate::db::Connection;
use crate::error::Result;
use ulid::Ulid;

pub fn persist_scope_summary_result(
    conn: &Connection,
    summary_id: &str,
    result: &str,
) -> Result<()> {
    let now = crate::utils::now_ts();
    conn.execute(
        "UPDATE derived_summaries
         SET content = ?1, stale = 0, generated_at = ?2
         WHERE id = ?3",
        rusqlite::params![result, now, summary_id],
    )?;
    Ok(())
}

pub fn persist_consolidation_result(
    conn: &Connection,
    suggested_title: &str,
    result: &str,
    episode_ids: &[String],
    brain_id: &str,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let now = crate::utils::now_ts();

    let reflection_id = Ulid::new().to_string();
    tx.execute(
        "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, brain_id, valid_from, created_at, updated_at)
         VALUES (?1, 'reflection', ?2, ?3, '[]', 1.0, ?4, ?5, ?5, ?5)",
        rusqlite::params![reflection_id, suggested_title, result, brain_id, now],
    )?;

    for ep_id in episode_ids {
        tx.execute(
            "INSERT OR IGNORE INTO reflection_sources (reflection_id, source_id) VALUES (?1, ?2)",
            rusqlite::params![reflection_id, ep_id],
        )?;
    }

    for ep_id in episode_ids {
        tx.execute(
            "INSERT OR IGNORE INTO summary_sources (summary_id, source_id, source_type, created_at)
             VALUES (?1, ?2, 'episode', ?3)",
            rusqlite::params![reflection_id, ep_id, now],
        )?;
    }

    for ep_id in episode_ids {
        tx.execute(
            "UPDATE summaries SET consolidated_by = ?1
             WHERE summary_id = ?2 AND consolidated_by IS NULL",
            rusqlite::params![reflection_id, ep_id],
        )?;
    }

    tx.commit()?;
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
    fn persist_scope_summary_result_updates_row() {
        let conn = setup();
        conn.execute(
            "INSERT INTO derived_summaries (id, scope_type, scope_value, content, stale, generated_at)
             VALUES (?1, ?2, ?3, ?4, 1, 0)",
            rusqlite::params!["sum-1", "directory", "src/", "old"],
        )
        .unwrap();

        persist_scope_summary_result(&conn, "sum-1", "new summary").unwrap();

        let (content, stale): (String, i64) = conn
            .query_row(
                "SELECT content, stale FROM derived_summaries WHERE id = ?1",
                ["sum-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(content, "new summary");
        assert_eq!(stale, 0);
    }

    #[test]
    fn persist_consolidation_result_writes_reflection_and_lineage() {
        let conn = setup();
        let now = crate::utils::now_ts();

        for ep_id in ["ep-1", "ep-2"] {
            conn.execute(
                "INSERT INTO summaries (summary_id, kind, title, content, tags, importance, brain_id, valid_from, created_at, updated_at)
                 VALUES (?1, 'episode', ?2, ?3, '[]', 1.0, ?4, ?5, ?5, ?5)",
                rusqlite::params![ep_id, ep_id, format!("content-{ep_id}"), "brain-1", now],
            )
            .unwrap();
        }

        let source_ids = vec!["ep-1".to_string(), "ep-2".to_string()];
        persist_consolidation_result(
            &conn,
            "Cluster Summary",
            "Synthesized reflection",
            &source_ids,
            "brain-1",
        )
        .unwrap();

        let reflection_id: String = conn
            .query_row(
                "SELECT summary_id FROM summaries WHERE kind = 'reflection' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let source_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summary_sources WHERE summary_id = ?1",
                [&reflection_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 2);

        let reflection_source_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM reflection_sources WHERE reflection_id = ?1",
                [&reflection_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reflection_source_count, 2);

        let consolidated_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM summaries WHERE summary_id IN ('ep-1', 'ep-2') AND consolidated_by = ?1",
                [&reflection_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(consolidated_count, 2);
    }
}
