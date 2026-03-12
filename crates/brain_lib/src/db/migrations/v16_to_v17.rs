use rusqlite::Connection;

use crate::error::Result;

/// v16 → v17: B2 transition marker.
///
/// No schema changes. This version marks the transition to SQLite-as-truth.
/// Event logs are now audit trails, not the source of truth.
pub fn migrate_v16_to_v17(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA user_version = 17;")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version_is_17_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "user_version", 16).unwrap();

        migrate_v16_to_v17(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 17);
    }
}
