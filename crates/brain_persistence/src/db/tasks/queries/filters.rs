use rusqlite::Connection;

use crate::db::fts::sanitize_fts_query;
use crate::sql::SqlResult;

/// Full-text search on task title and description via FTS5.
/// Returns matching task_ids ordered by relevance.
pub fn search_tasks_fts(conn: &Connection, query: &str, limit: usize) -> SqlResult<Vec<String>> {
    let query = sanitize_fts_query(query);
    if query.is_empty() {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT t.task_id
         FROM fts_tasks
         JOIN tasks t ON t.rowid = fts_tasks.rowid
         WHERE fts_tasks MATCH ?1
         ORDER BY rank
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![query, limit as i64], |row| row.get(0))?;
    crate::db::collect_rows(rows)
}
