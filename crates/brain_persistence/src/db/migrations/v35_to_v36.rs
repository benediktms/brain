use rusqlite::Connection;

use crate::error::Result;

pub fn migrate_v35_to_v36(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "ALTER TABLE summaries ADD COLUMN embedded_at INTEGER;
         PRAGMA user_version = 36;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::{migrate_v0_to_v1, migrate_v29_to_v30, migrate_v34_to_v35};

    fn setup_v35(conn: &Connection) {
        migrate_v0_to_v1(conn).unwrap();
        migrate_v29_to_v30(conn).unwrap();
        migrate_v34_to_v35(conn).unwrap();
        conn.execute_batch("PRAGMA user_version = 35;").unwrap();
    }

    #[test]
    fn test_version_stamped() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v35(&conn);

        migrate_v35_to_v36(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 36);
    }

    #[test]
    fn test_embedded_at_defaults_to_null() {
        let conn = Connection::open_in_memory().unwrap();
        setup_v35(&conn);
        migrate_v35_to_v36(&conn).unwrap();

        conn.execute(
            "INSERT INTO summaries (summary_id, kind, content, created_at, updated_at)
             VALUES ('s-new', 'episode', 'test', 1000, 1000)",
            [],
        )
        .unwrap();

        let embedded_at: Option<i64> = conn
            .query_row(
                "SELECT embedded_at FROM summaries WHERE summary_id = 's-new'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(embedded_at.is_none());
    }
}
