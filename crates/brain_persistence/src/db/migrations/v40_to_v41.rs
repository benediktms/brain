use rusqlite::Connection;

use crate::error::Result;

/// v40 → v41: Add `tags` and `importance` columns to the `files` table.
///
/// These columns store per-file metadata extracted from YAML frontmatter
/// during indexing, enabling the `tag_match` and `importance` ranking
/// signals to be truly independent (rather than derived from heading
/// paths and PageRank respectively).
///
/// - `tags`: JSON array string (e.g. `'["rust","memory"]'`), default empty
/// - `importance`: float [0.0, 1.0], default 0.5 per RETRIEVE_PLUS spec
///
/// Existing files get defaults until re-indexed with `brain scan --force`.
pub fn migrate_v40_to_v41(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "BEGIN;
         ALTER TABLE files ADD COLUMN tags TEXT NOT NULL DEFAULT '';
         ALTER TABLE files ADD COLUMN importance REAL NOT NULL DEFAULT 0.5;
         PRAGMA user_version = 41;
         COMMIT;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::*;

    fn setup_v40() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        for v in 0..40 {
            match v {
                0 => migrate_v0_to_v1(&conn).unwrap(),
                1 => migrate_v1_to_v2(&conn).unwrap(),
                2 => migrate_v2_to_v3(&conn).unwrap(),
                3 => migrate_v3_to_v4(&conn).unwrap(),
                4 => migrate_v4_to_v5(&conn).unwrap(),
                5 => migrate_v5_to_v6(&conn).unwrap(),
                6 => migrate_v6_to_v7(&conn).unwrap(),
                7 => migrate_v7_to_v8(&conn).unwrap(),
                8 => migrate_v8_to_v9(&conn).unwrap(),
                9 => migrate_v9_to_v10(&conn).unwrap(),
                10 => migrate_v10_to_v11(&conn).unwrap(),
                11 => migrate_v11_to_v12(&conn).unwrap(),
                12 => migrate_v12_to_v13(&conn).unwrap(),
                13 => migrate_v13_to_v14(&conn).unwrap(),
                14 => migrate_v14_to_v15(&conn).unwrap(),
                15 => migrate_v15_to_v16(&conn).unwrap(),
                16 => migrate_v16_to_v17(&conn).unwrap(),
                17 => migrate_v17_to_v18(&conn).unwrap(),
                18 => migrate_v18_to_v19(&conn).unwrap(),
                19 => migrate_v19_to_v20(&conn).unwrap(),
                20 => migrate_v20_to_v21(&conn).unwrap(),
                21 => migrate_v21_to_v22(&conn).unwrap(),
                22 => migrate_v22_to_v23(&conn).unwrap(),
                23 => migrate_v23_to_v24(&conn).unwrap(),
                24 => migrate_v24_to_v25(&conn).unwrap(),
                25 => migrate_v25_to_v26(&conn).unwrap(),
                26 => migrate_v26_to_v27(&conn).unwrap(),
                27 => migrate_v27_to_v28(&conn).unwrap(),
                28 => migrate_v28_to_v29(&conn).unwrap(),
                29 => migrate_v29_to_v30(&conn).unwrap(),
                30 => migrate_v30_to_v31(&conn).unwrap(),
                31 => migrate_v31_to_v32(&conn).unwrap(),
                32 => migrate_v32_to_v33(&conn).unwrap(),
                33 => migrate_v33_to_v34(&conn).unwrap(),
                34 => migrate_v34_to_v35(&conn).unwrap(),
                35 => migrate_v35_to_v36(&conn).unwrap(),
                36 => migrate_v36_to_v37(&conn).unwrap(),
                37 => migrate_v37_to_v38(&conn).unwrap(),
                38 => migrate_v38_to_v39(&conn).unwrap(),
                39 => migrate_v39_to_v40(&conn).unwrap(),
                _ => panic!("unexpected version {v}"),
            }
        }
        conn
    }

    #[test]
    fn test_version_stamp() {
        let conn = setup_v40();
        migrate_v40_to_v41(&conn).unwrap();
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 41);
    }

    #[test]
    fn test_columns_exist() {
        let conn = setup_v40();
        migrate_v40_to_v41(&conn).unwrap();

        // Insert a file and read back tags + importance
        conn.execute(
            "INSERT INTO files (file_id, path, content_hash, indexing_state)
             VALUES ('f-test', '/test.md', 'abc', 'indexed')",
            [],
        )
        .unwrap();

        let (tags, importance): (String, f64) = conn
            .query_row(
                "SELECT tags, importance FROM files WHERE file_id = 'f-test'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(tags, "");
        assert!((importance - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_existing_files_get_defaults() {
        let conn = setup_v40();

        // Insert a file at v40 (before migration)
        conn.execute(
            "INSERT INTO files (file_id, path, content_hash, indexing_state)
             VALUES ('f-old', '/old.md', 'xyz', 'indexed')",
            [],
        )
        .unwrap();

        migrate_v40_to_v41(&conn).unwrap();

        let (tags, importance): (String, f64) = conn
            .query_row(
                "SELECT tags, importance FROM files WHERE file_id = 'f-old'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(tags, "");
        assert!((importance - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tags_and_importance_writable() {
        let conn = setup_v40();
        migrate_v40_to_v41(&conn).unwrap();

        conn.execute(
            "INSERT INTO files (file_id, path, content_hash, indexing_state, tags, importance)
             VALUES ('f-fm', '/frontmatter.md', 'h1', 'indexed', '[\"rust\",\"memory\"]', 0.8)",
            [],
        )
        .unwrap();

        let (tags, importance): (String, f64) = conn
            .query_row(
                "SELECT tags, importance FROM files WHERE file_id = 'f-fm'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(tags, r#"["rust","memory"]"#);
        assert!((importance - 0.8).abs() < f64::EPSILON);
    }
}
