//! v53 → v54: add `display_id` to `sagas` for compact `saga-<hex>` IDs.
//!
//! Mirrors the task-side `display_id` pattern (introduced in v30→v31) but
//! with global uniqueness instead of per-brain — sagas are cross-brain by
//! design. The column is `NOT NULL` from the start with an empty-string
//! sentinel; the entire backfill + index creation happens inside one
//! transaction so the sentinel is never visible to readers.

use std::collections::HashSet;

use rusqlite::Connection;

use crate::db::short_id::{blake3_short_hex, pick_unique_prefix};
use crate::error::Result;

pub fn migrate_v53_to_v54(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    // 1. Add the column. NOT NULL with an empty-string sentinel that the
    //    backfill loop overwrites before the UNIQUE index is created.
    tx.execute_batch("ALTER TABLE sagas ADD COLUMN display_id TEXT NOT NULL DEFAULT '';")?;

    // 2. Backfill in deterministic order (sort by saga_id ASC) so the result
    //    is reproducible across replays.
    let saga_ids: Vec<String> = {
        let mut stmt = tx.prepare("SELECT saga_id FROM sagas ORDER BY saga_id ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let mut used: HashSet<String> = HashSet::new();
    for saga_id in &saga_ids {
        let full_hex = blake3_short_hex(saga_id);
        let display_id = pick_unique_prefix(&full_hex, &used);
        tx.execute(
            "UPDATE sagas SET display_id = ?1 WHERE saga_id = ?2",
            rusqlite::params![display_id, saga_id],
        )?;
        used.insert(display_id);
    }

    // 3. Create the global UNIQUE index. Safe because every row now has a
    //    unique non-empty display_id; no row carries the empty sentinel.
    tx.execute_batch("CREATE UNIQUE INDEX idx_sagas_display_id ON sagas(display_id);")?;

    tx.pragma_update(None, "user_version", 54i32)?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_v52_to_v53;
    use rusqlite::Connection;

    fn fresh_v53(conn: &Connection) {
        // v52 baseline + run v52→v53 to materialise the sagas tables, then
        // step back to user_version 53 so the v53→v54 migration is the next
        // step to apply.
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA user_version = 52;",
        )
        .unwrap();
        migrate_v52_to_v53(conn).unwrap();
    }

    fn insert_saga(conn: &Connection, saga_id: &str) {
        conn.execute(
            "INSERT INTO sagas (saga_id, title, status, created_at, updated_at)
             VALUES (?1, ?2, 'planning', 1000, 1000)",
            rusqlite::params![saga_id, format!("title for {saga_id}")],
        )
        .unwrap();
    }

    #[test]
    fn empty_table_bumps_version_and_creates_index() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v53(&conn);

        migrate_v53_to_v54(&conn).unwrap();

        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 54);

        let indexes: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_sagas_display_id'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn single_row_gets_three_char_display_id() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v53(&conn);
        insert_saga(&conn, "01KR16ZJRDVNF5D463QMVD9PH0");

        migrate_v53_to_v54(&conn).unwrap();

        let display_id: String = conn
            .query_row(
                "SELECT display_id FROM sagas WHERE saga_id = ?1",
                ["01KR16ZJRDVNF5D463QMVD9PH0"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            display_id.len(),
            3,
            "first saga should get the minimum length"
        );
        assert!(
            display_id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn many_rows_all_unique() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v53(&conn);
        for i in 0..50 {
            insert_saga(&conn, &format!("01KR16ZJRDVNF5D463QMVD9P{i:02}"));
        }

        migrate_v53_to_v54(&conn).unwrap();

        let display_ids: Vec<String> = conn
            .prepare("SELECT display_id FROM sagas")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(display_ids.len(), 50);
        let unique: HashSet<&String> = display_ids.iter().collect();
        assert_eq!(unique.len(), 50, "all display_ids must be unique");
        for id in &display_ids {
            assert!(id.len() >= 3, "display_id length must be at least 3");
        }
    }

    #[test]
    fn forced_collision_extends_to_4_or_more_chars() {
        // Find two saga_ids whose BLAKE3 hashes share the same first-3-hex-chars
        // prefix by brute force, then assert the migration extends one to 4+.
        // Birthday bound: 4096 slots → expected pair around sqrt(4096*2) ≈ 90.
        let mut by_prefix: std::collections::HashMap<String, String> = Default::default();
        let mut collide_a: Option<String> = None;
        let mut collide_b: Option<String> = None;
        for i in 0..5000u32 {
            let id = format!("01TEST{i:020}");
            let prefix = blake3_short_hex(&id)[..3].to_string();
            if let Some(existing) = by_prefix.get(&prefix).cloned() {
                collide_a = Some(existing);
                collide_b = Some(id);
                break;
            }
            by_prefix.insert(prefix, id);
        }
        let a = collide_a
            .expect("brute force should find a 3-char-prefix collision within 5000 iterations");
        let b = collide_b.unwrap();

        let conn = Connection::open_in_memory().unwrap();
        fresh_v53(&conn);
        // Sort the inserts so the smaller saga_id is processed first and
        // claims the 3-char slot; the other should extend.
        let (first, second) = if a < b { (&a, &b) } else { (&b, &a) };
        insert_saga(&conn, first);
        insert_saga(&conn, second);

        migrate_v53_to_v54(&conn).unwrap();

        let display_first: String = conn
            .query_row(
                "SELECT display_id FROM sagas WHERE saga_id = ?1",
                [first],
                |row| row.get(0),
            )
            .unwrap();
        let display_second: String = conn
            .query_row(
                "SELECT display_id FROM sagas WHERE saga_id = ?1",
                [second],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(display_first.len(), 3);
        assert!(
            display_second.len() >= 4,
            "colliding saga should extend to 4 or more chars, got {}",
            display_second.len()
        );
        assert_ne!(display_first, display_second);
    }

    #[test]
    fn index_rejects_duplicate_after_migration() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v53(&conn);
        insert_saga(&conn, "01KR16ZJRDVNF5D463QMVD9PH0");

        migrate_v53_to_v54(&conn).unwrap();

        // Attempting to set a second saga's display_id to the same value
        // should fail the UNIQUE constraint.
        let existing: String = conn
            .query_row(
                "SELECT display_id FROM sagas WHERE saga_id = ?1",
                ["01KR16ZJRDVNF5D463QMVD9PH0"],
                |row| row.get(0),
            )
            .unwrap();
        insert_saga(&conn, "01KR16ZJRDVNF5D463QMVD9PH1");
        let result = conn.execute(
            "UPDATE sagas SET display_id = ?1 WHERE saga_id = ?2",
            rusqlite::params![existing, "01KR16ZJRDVNF5D463QMVD9PH1"],
        );
        assert!(
            result.is_err(),
            "duplicate display_id update should fail UNIQUE constraint"
        );
    }
}
