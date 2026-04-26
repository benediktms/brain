use rusqlite::Connection;

use crate::error::Result;

/// v41 → v42: Normalize free-form record kinds and re-derive searchable.
pub fn migrate_v41_to_v42(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "BEGIN;
         UPDATE records SET kind = 'document' WHERE kind = 'dispatch-brief';
         UPDATE records SET kind = 'snapshot' WHERE kind = 'conversation';
         UPDATE records SET kind = 'document' WHERE kind = 'report';
         UPDATE records SET searchable = CASE WHEN kind = 'snapshot' THEN 0 ELSE 1 END;
         PRAGMA user_version = 42;
         COMMIT;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_v41() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(
            "CREATE TABLE records (
                 record_id  TEXT PRIMARY KEY,
                 kind       TEXT NOT NULL,
                 searchable INTEGER NOT NULL DEFAULT 1
             );
             PRAGMA user_version = 41;",
        )
        .unwrap();
        conn
    }

    fn insert_record(conn: &Connection, record_id: &str, kind: &str, searchable: i32) {
        conn.execute(
            "INSERT INTO records (record_id, kind, searchable) VALUES (?1, ?2, ?3)",
            rusqlite::params![record_id, kind, searchable],
        )
        .unwrap();
    }

    fn record_state(conn: &Connection, record_id: &str) -> (String, i32) {
        conn.query_row(
            "SELECT kind, searchable FROM records WHERE record_id = ?1",
            [record_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    #[test]
    fn test_version_stamp() {
        let conn = setup_v41();
        migrate_v41_to_v42(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 42);
    }

    #[test]
    fn test_kind_renames_and_searchable_derived() {
        let conn = setup_v41();
        insert_record(&conn, "r-dispatch", "dispatch-brief", 0);
        insert_record(&conn, "r-conversation", "conversation", 1);
        insert_record(&conn, "r-report", "report", 0);
        insert_record(&conn, "r-snapshot", "snapshot", 1);
        insert_record(&conn, "r-document", "document", 1);
        insert_record(&conn, "r-analysis", "analysis", 1);
        insert_record(&conn, "r-plan", "plan", 1);
        insert_record(&conn, "r-custom", "my-custom-kind", 1);

        migrate_v41_to_v42(&conn).unwrap();

        assert_eq!(record_state(&conn, "r-dispatch"), ("document".into(), 1));
        assert_eq!(
            record_state(&conn, "r-conversation"),
            ("snapshot".into(), 0)
        );
        assert_eq!(record_state(&conn, "r-report"), ("document".into(), 1));
        assert_eq!(record_state(&conn, "r-snapshot"), ("snapshot".into(), 0));
        assert_eq!(record_state(&conn, "r-document"), ("document".into(), 1));
        assert_eq!(record_state(&conn, "r-analysis"), ("analysis".into(), 1));
        assert_eq!(record_state(&conn, "r-plan"), ("plan".into(), 1));
        assert_eq!(
            record_state(&conn, "r-custom"),
            ("my-custom-kind".into(), 1)
        );
    }

    #[test]
    fn test_idempotent() {
        let conn = setup_v41();
        insert_record(&conn, "r-dispatch", "dispatch-brief", 0);
        insert_record(&conn, "r-conversation", "conversation", 1);
        insert_record(&conn, "r-report", "report", 0);
        insert_record(&conn, "r-custom", "my-custom-kind", 1);

        migrate_v41_to_v42(&conn).unwrap();
        migrate_v41_to_v42(&conn).unwrap();

        assert_eq!(record_state(&conn, "r-dispatch"), ("document".into(), 1));
        assert_eq!(
            record_state(&conn, "r-conversation"),
            ("snapshot".into(), 0)
        );
        assert_eq!(record_state(&conn, "r-report"), ("document".into(), 1));
        assert_eq!(
            record_state(&conn, "r-custom"),
            ("my-custom-kind".into(), 1)
        );
    }
}
