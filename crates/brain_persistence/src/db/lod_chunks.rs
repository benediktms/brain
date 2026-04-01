//! CRUD operations for the `lod_chunks` table.
//!
//! LOD chunks store pre-computed representations of source objects at different
//! fidelity levels (L0 extractive, L1 LLM-summarized). L2 is passthrough and
//! never stored in this table.

use rusqlite::{Connection, params};

use crate::error::Result;

/// A row from the `lod_chunks` table.
#[derive(Debug, Clone)]
pub struct LodChunkRow {
    pub id: String,
    pub object_uri: String,
    pub brain_id: String,
    pub lod_level: String,
    pub content: String,
    pub token_est: Option<i64>,
    pub method: String,
    pub model_id: Option<String>,
    pub source_hash: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub job_id: Option<String>,
}

/// Input for inserting or upserting an LOD chunk.
pub struct InsertLodChunk<'a> {
    pub id: &'a str,
    pub object_uri: &'a str,
    pub brain_id: &'a str,
    pub lod_level: &'a str,
    pub content: &'a str,
    pub token_est: Option<i64>,
    pub method: &'a str,
    pub model_id: Option<&'a str>,
    pub source_hash: &'a str,
    pub created_at: &'a str,
    pub expires_at: Option<&'a str>,
    pub job_id: Option<&'a str>,
}

fn row_from_rusqlite(row: &rusqlite::Row<'_>) -> rusqlite::Result<LodChunkRow> {
    Ok(LodChunkRow {
        id: row.get("id")?,
        object_uri: row.get("object_uri")?,
        brain_id: row.get("brain_id")?,
        lod_level: row.get("lod_level")?,
        content: row.get("content")?,
        token_est: row.get("token_est")?,
        method: row.get("method")?,
        model_id: row.get("model_id")?,
        source_hash: row.get("source_hash")?,
        created_at: row.get("created_at")?,
        expires_at: row.get("expires_at")?,
        job_id: row.get("job_id")?,
    })
}

/// Insert or replace an LOD chunk keyed on `(object_uri, lod_level)`.
///
/// On conflict the existing row is updated in place (new id, content, hash, etc.).
pub fn upsert_lod_chunk(conn: &Connection, input: &InsertLodChunk) -> Result<()> {
    conn.execute(
        "INSERT INTO lod_chunks
             (id, object_uri, brain_id, lod_level, content, token_est,
              method, model_id, source_hash, created_at, expires_at, job_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(object_uri, lod_level) DO UPDATE SET
             id          = excluded.id,
             brain_id    = excluded.brain_id,
             content     = excluded.content,
             token_est   = excluded.token_est,
             method      = excluded.method,
             model_id    = excluded.model_id,
             source_hash = excluded.source_hash,
             created_at  = excluded.created_at,
             expires_at  = excluded.expires_at,
             job_id      = excluded.job_id",
        params![
            input.id,
            input.object_uri,
            input.brain_id,
            input.lod_level,
            input.content,
            input.token_est,
            input.method,
            input.model_id,
            input.source_hash,
            input.created_at,
            input.expires_at,
            input.job_id,
        ],
    )?;
    Ok(())
}

/// Get a single LOD chunk by `(object_uri, lod_level)`.
pub fn get_lod_chunk(
    conn: &Connection,
    object_uri: &str,
    lod_level: &str,
) -> Result<Option<LodChunkRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, object_uri, brain_id, lod_level, content, token_est,
                method, model_id, source_hash, created_at, expires_at, job_id
         FROM lod_chunks
         WHERE object_uri = ?1 AND lod_level = ?2",
    )?;
    let row = stmt.query_row(params![object_uri, lod_level], row_from_rusqlite);
    match row {
        Ok(r) => Ok(Some(r)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Get all LOD chunks for an object URI, ordered by level.
pub fn get_lod_chunks_for_uri(conn: &Connection, object_uri: &str) -> Result<Vec<LodChunkRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, object_uri, brain_id, lod_level, content, token_est,
                method, model_id, source_hash, created_at, expires_at, job_id
         FROM lod_chunks
         WHERE object_uri = ?1
         ORDER BY lod_level",
    )?;
    let rows = stmt
        .query_map(params![object_uri], row_from_rusqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Delete all LOD chunks for an object URI. Returns the number of rows deleted.
pub fn delete_lod_chunks_for_uri(conn: &Connection, object_uri: &str) -> Result<usize> {
    let count = conn.execute(
        "DELETE FROM lod_chunks WHERE object_uri = ?1",
        params![object_uri],
    )?;
    Ok(count)
}

/// Delete all LOD chunks whose `object_uri` matches a LIKE pattern.
///
/// Used to clean up LOD entries when a file is deleted or emptied.
/// Pattern example: `"synapse://my-brain/memory/file123:%"` deletes all
/// LOD entries for chunks of that file.
pub fn delete_lod_chunks_by_uri_pattern(conn: &Connection, uri_pattern: &str) -> Result<usize> {
    let count = conn.execute(
        "DELETE FROM lod_chunks WHERE object_uri LIKE ?1",
        params![uri_pattern],
    )?;
    Ok(count)
}

/// Delete expired LOD chunks where `expires_at < now_iso`. Returns count deleted.
pub fn delete_expired_lod_chunks(conn: &Connection, now_iso: &str) -> Result<usize> {
    let count = conn.execute(
        "DELETE FROM lod_chunks WHERE expires_at IS NOT NULL AND expires_at < ?1",
        params![now_iso],
    )?;
    Ok(count)
}

/// Count LOD chunks for a brain, optionally filtered by level.
pub fn count_lod_chunks_by_brain(
    conn: &Connection,
    brain_id: &str,
    lod_level: Option<&str>,
) -> Result<usize> {
    let count: i64 = match lod_level {
        Some(level) => conn.query_row(
            "SELECT COUNT(*) FROM lod_chunks WHERE brain_id = ?1 AND lod_level = ?2",
            params![brain_id, level],
            |row| row.get(0),
        )?,
        None => conn.query_row(
            "SELECT COUNT(*) FROM lod_chunks WHERE brain_id = ?1",
            params![brain_id],
            |row| row.get(0),
        )?,
    };
    Ok(count as usize)
}

/// List LOD chunks for a brain with pagination.
pub fn list_lod_chunks_by_brain(
    conn: &Connection,
    brain_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<LodChunkRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, object_uri, brain_id, lod_level, content, token_est,
                method, model_id, source_hash, created_at, expires_at, job_id
         FROM lod_chunks
         WHERE brain_id = ?1
         ORDER BY created_at DESC
         LIMIT ?2 OFFSET ?3",
    )?;
    let rows = stmt
        .query_map(
            params![brain_id, limit as i64, offset as i64],
            row_from_rusqlite,
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
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

    fn make_input<'a>(
        id: &'a str,
        uri: &'a str,
        brain: &'a str,
        level: &'a str,
        content: &'a str,
        hash: &'a str,
    ) -> InsertLodChunk<'a> {
        InsertLodChunk {
            id,
            object_uri: uri,
            brain_id: brain,
            lod_level: level,
            content,
            token_est: Some(10),
            method: "extractive",
            model_id: None,
            source_hash: hash,
            created_at: "2026-03-30T00:00:00Z",
            expires_at: None,
            job_id: None,
        }
    }

    #[test]
    fn test_upsert_insert() {
        let conn = setup();
        let input = make_input("01A", "synapse://b/memory/c1", "b", "L0", "hello", "h1");
        upsert_lod_chunk(&conn, &input).unwrap();

        let row = get_lod_chunk(&conn, "synapse://b/memory/c1", "L0")
            .unwrap()
            .expect("should find inserted row");
        assert_eq!(row.id, "01A");
        assert_eq!(row.content, "hello");
        assert_eq!(row.source_hash, "h1");
    }

    #[test]
    fn test_upsert_replace() {
        let conn = setup();
        let input1 = make_input("01A", "synapse://b/memory/c1", "b", "L0", "first", "h1");
        upsert_lod_chunk(&conn, &input1).unwrap();

        let input2 = make_input("01B", "synapse://b/memory/c1", "b", "L0", "second", "h2");
        upsert_lod_chunk(&conn, &input2).unwrap();

        let row = get_lod_chunk(&conn, "synapse://b/memory/c1", "L0")
            .unwrap()
            .expect("should find upserted row");
        assert_eq!(row.id, "01B", "id should be replaced on upsert");
        assert_eq!(row.content, "second");
        assert_eq!(row.source_hash, "h2");
    }

    #[test]
    fn test_get_not_found() {
        let conn = setup();
        let row = get_lod_chunk(&conn, "synapse://b/memory/missing", "L0").unwrap();
        assert!(row.is_none());
    }

    #[test]
    fn test_get_all_levels() {
        let conn = setup();
        let l0 = make_input("01A", "synapse://b/memory/c1", "b", "L0", "short", "h1");
        let mut l1 = make_input(
            "01B",
            "synapse://b/memory/c1",
            "b",
            "L1",
            "long summary",
            "h1",
        );
        l1.method = "llm";
        l1.model_id = Some("test-model");

        upsert_lod_chunk(&conn, &l0).unwrap();
        upsert_lod_chunk(&conn, &l1).unwrap();

        let rows = get_lod_chunks_for_uri(&conn, "synapse://b/memory/c1").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].lod_level, "L0");
        assert_eq!(rows[1].lod_level, "L1");
    }

    #[test]
    fn test_delete_for_uri() {
        let conn = setup();
        let l0 = make_input("01A", "synapse://b/memory/c1", "b", "L0", "short", "h1");
        let mut l1 = make_input("01B", "synapse://b/memory/c1", "b", "L1", "long", "h1");
        l1.method = "llm";
        upsert_lod_chunk(&conn, &l0).unwrap();
        upsert_lod_chunk(&conn, &l1).unwrap();

        let deleted = delete_lod_chunks_for_uri(&conn, "synapse://b/memory/c1").unwrap();
        assert_eq!(deleted, 2);

        let rows = get_lod_chunks_for_uri(&conn, "synapse://b/memory/c1").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_delete_expired() {
        let conn = setup();
        let mut expired = make_input("01A", "synapse://b/memory/c1", "b", "L0", "old", "h1");
        expired.expires_at = Some("2026-01-01T00:00:00Z");

        let mut fresh = make_input("01B", "synapse://b/memory/c2", "b", "L0", "new", "h2");
        fresh.expires_at = Some("2099-01-01T00:00:00Z");

        let no_expiry = make_input("01C", "synapse://b/memory/c3", "b", "L0", "permanent", "h3");

        upsert_lod_chunk(&conn, &expired).unwrap();
        upsert_lod_chunk(&conn, &fresh).unwrap();
        upsert_lod_chunk(&conn, &no_expiry).unwrap();

        let deleted = delete_expired_lod_chunks(&conn, "2026-06-01T00:00:00Z").unwrap();
        assert_eq!(deleted, 1, "only the expired chunk should be deleted");

        // fresh and no_expiry should survive
        assert!(
            get_lod_chunk(&conn, "synapse://b/memory/c2", "L0")
                .unwrap()
                .is_some()
        );
        assert!(
            get_lod_chunk(&conn, "synapse://b/memory/c3", "L0")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_count_by_brain() {
        let conn = setup();
        let a1 = make_input("01A", "synapse://a/memory/c1", "brain-a", "L0", "a", "h1");
        let mut a2 = make_input("01B", "synapse://a/memory/c2", "brain-a", "L1", "b", "h2");
        a2.method = "llm";
        let b1 = make_input("01C", "synapse://b/memory/c3", "brain-b", "L0", "c", "h3");

        upsert_lod_chunk(&conn, &a1).unwrap();
        upsert_lod_chunk(&conn, &a2).unwrap();
        upsert_lod_chunk(&conn, &b1).unwrap();

        assert_eq!(
            count_lod_chunks_by_brain(&conn, "brain-a", None).unwrap(),
            2
        );
        assert_eq!(
            count_lod_chunks_by_brain(&conn, "brain-a", Some("L0")).unwrap(),
            1
        );
        assert_eq!(
            count_lod_chunks_by_brain(&conn, "brain-b", None).unwrap(),
            1
        );
        assert_eq!(
            count_lod_chunks_by_brain(&conn, "brain-c", None).unwrap(),
            0
        );
    }

    #[test]
    fn test_list_by_brain() {
        let conn = setup();
        let a1 = make_input("01A", "synapse://a/memory/c1", "brain-a", "L0", "a", "h1");
        let b1 = make_input("01B", "synapse://b/memory/c2", "brain-b", "L0", "b", "h2");

        upsert_lod_chunk(&conn, &a1).unwrap();
        upsert_lod_chunk(&conn, &b1).unwrap();

        let rows = list_lod_chunks_by_brain(&conn, "brain-a", 10, 0).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].brain_id, "brain-a");
    }
}
