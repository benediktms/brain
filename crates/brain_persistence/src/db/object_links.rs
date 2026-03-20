use rusqlite::Connection;

use crate::error::Result;

/// A cross-domain object link between two brain:// URIs.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectLink {
    pub source_uri: String,
    pub target_uri: String,
    pub link_type: String,
    pub created_at: i64,
}

/// Add a directional link between two brain:// URIs.
///
/// Inserts a row into `object_links`. Fails if the (source_uri, target_uri)
/// pair already exists (PRIMARY KEY constraint).
pub fn add_object_link(
    conn: &Connection,
    source_uri: &str,
    target_uri: &str,
    link_type: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO object_links (source_uri, target_uri, link_type, created_at)
         VALUES (?1, ?2, ?3, strftime('%s', 'now'))",
        rusqlite::params![source_uri, target_uri, link_type],
    )?;
    Ok(())
}

/// Remove a directional link between two brain:// URIs.
///
/// No-op if the link does not exist.
pub fn remove_object_link(conn: &Connection, source_uri: &str, target_uri: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM object_links WHERE source_uri = ?1 AND target_uri = ?2",
        rusqlite::params![source_uri, target_uri],
    )?;
    Ok(())
}

/// Return all links where `uri` appears as either source or target.
///
/// Both outgoing (uri is source) and incoming (uri is target) links are included.
pub fn get_object_links(conn: &Connection, uri: &str) -> Result<Vec<ObjectLink>> {
    let mut stmt = conn.prepare_cached(
        "SELECT source_uri, target_uri, link_type, created_at
         FROM object_links
         WHERE source_uri = ?1 OR target_uri = ?1
         ORDER BY created_at ASC",
    )?;

    let rows = stmt.query_map(rusqlite::params![uri], |row| {
        Ok(ObjectLink {
            source_uri: row.get(0)?,
            target_uri: row.get(1)?,
            link_type: row.get(2)?,
            created_at: row.get(3)?,
        })
    })?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
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
    fn test_add_and_get_link() {
        let conn = setup();

        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r1",
            "related",
        )
        .unwrap();

        let links = get_object_links(&conn, "brain://b1/tasks/t1").unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].source_uri, "brain://b1/tasks/t1");
        assert_eq!(links[0].target_uri, "brain://b1/records/r1");
        assert_eq!(links[0].link_type, "related");
    }

    #[test]
    fn test_get_returns_both_incoming_and_outgoing() {
        let conn = setup();

        // t1 → r1 (outgoing from t1)
        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r1",
            "related",
        )
        .unwrap();

        // r2 → t1 (incoming to t1)
        add_object_link(
            &conn,
            "brain://b1/records/r2",
            "brain://b1/tasks/t1",
            "derived_from",
        )
        .unwrap();

        let links = get_object_links(&conn, "brain://b1/tasks/t1").unwrap();
        assert_eq!(links.len(), 2);

        let sources: Vec<&str> = links.iter().map(|l| l.source_uri.as_str()).collect();
        assert!(sources.contains(&"brain://b1/tasks/t1"));
        assert!(sources.contains(&"brain://b1/records/r2"));
    }

    #[test]
    fn test_remove_link() {
        let conn = setup();

        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r1",
            "related",
        )
        .unwrap();

        remove_object_link(&conn, "brain://b1/tasks/t1", "brain://b1/records/r1").unwrap();

        let links = get_object_links(&conn, "brain://b1/tasks/t1").unwrap();
        assert_eq!(links.len(), 0);
    }

    #[test]
    fn test_remove_nonexistent_is_noop() {
        let conn = setup();

        // Should not error
        remove_object_link(&conn, "brain://b1/tasks/t99", "brain://b1/records/r99").unwrap();
    }

    #[test]
    fn test_duplicate_add_fails() {
        let conn = setup();

        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r1",
            "related",
        )
        .unwrap();

        let result = add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r1",
            "supersedes",
        );
        assert!(result.is_err(), "duplicate link should fail");
    }

    #[test]
    fn test_get_returns_empty_for_unknown_uri() {
        let conn = setup();

        let links = get_object_links(&conn, "brain://b1/tasks/unknown").unwrap();
        assert_eq!(links.len(), 0);
    }

    #[test]
    fn test_multiple_link_types() {
        let conn = setup();

        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r1",
            "related",
        )
        .unwrap();
        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/records/r2",
            "derived_from",
        )
        .unwrap();
        add_object_link(
            &conn,
            "brain://b1/tasks/t1",
            "brain://b1/episodes/e1",
            "supersedes",
        )
        .unwrap();

        let links = get_object_links(&conn, "brain://b1/tasks/t1").unwrap();
        assert_eq!(links.len(), 3);

        let types: Vec<&str> = links.iter().map(|l| l.link_type.as_str()).collect();
        assert!(types.contains(&"related"));
        assert!(types.contains(&"derived_from"));
        assert!(types.contains(&"supersedes"));
    }
}
