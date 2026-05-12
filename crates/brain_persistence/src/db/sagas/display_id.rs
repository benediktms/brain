//! Saga short-ID generation and resolution.
//!
//! Wraps the shared [`crate::db::short_id`] module (BLAKE3 + collision-extend)
//! with saga-specific concerns: the `saga-` prefix on the user-facing form,
//! and a resolver that accepts either the bare 26-char ULID or the short
//! `saga-<hex>` form.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::error::{BrainCoreError, Result};

/// Format a saga `display_id` for user-facing emission as `saga-<hex>`.
///
/// Unlike tasks, the saga MCP wire format retains the field NAME `saga_id`
/// even though the value form changes from a bare 26-char ULID to `saga-<hex>`.
/// Tasks renamed `display_id` → `id` on the wire via serde; sagas do not,
/// because saga IDs are global (no brain prefix) and the field name
/// `saga_id` is already type-clarifying enough. Future maintainers: do
/// not unify the two patterns blindly.
pub fn compact_saga_id(display_id: &str) -> String {
    format!("saga-{display_id}")
}

/// Strip the `saga-` prefix from `input` if present; return `None` otherwise.
pub fn parse_short_form(input: &str) -> Option<&str> {
    input.strip_prefix("saga-")
}

/// Resolve any saga reference (bare ULID, `saga-<hex>` short form, or hex
/// prefix of a stored `display_id`) to its canonical 26-char ULID saga_id.
///
/// Precedence:
/// 1. Exact ULID match on `saga_id`.
/// 2. If input starts with `saga-`: exact match on `display_id`, then
///    prefix range scan on `display_id`.
/// 3. ULID prefix fallback (back-compat for tooling that holds historical
///    bare-ULID references).
///
/// Returns `BrainCoreError::Parse("ambiguous short id: …")` when a prefix
/// matches multiple rows.
pub fn resolve_saga_id(conn: &Connection, input: &str) -> Result<String> {
    if input.is_empty() {
        return Err(BrainCoreError::Parse("empty saga id".into()));
    }

    // 1. Exact ULID match.
    let exact: Option<String> = conn
        .query_row(
            "SELECT saga_id FROM sagas WHERE saga_id = ?1",
            [input],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = exact {
        return Ok(id);
    }

    // 2. Short-form resolution if input has the `saga-` prefix.
    if let Some(stripped) = parse_short_form(input) {
        return resolve_via_display_id(conn, stripped);
    }

    // 3. ULID-prefix fallback (back-compat).
    resolve_via_ulid_prefix(conn, input)
}

fn resolve_via_display_id(conn: &Connection, hex_prefix: &str) -> Result<String> {
    if hex_prefix.is_empty()
        || !hex_prefix
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        return Err(BrainCoreError::Parse(format!(
            "saga short id must be `saga-<lowercase hex>`, got `saga-{hex_prefix}`"
        )));
    }

    // Exact match first.
    let exact: Vec<String> = {
        let mut stmt = conn.prepare("SELECT saga_id FROM sagas WHERE display_id = ?1 LIMIT 2")?;
        stmt.query_map([hex_prefix], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if exact.len() == 1 {
        return Ok(exact.into_iter().next().unwrap());
    }

    // Prefix range scan: user supplied a short prefix of a longer stored display_id.
    let upper_bound = increment_string(hex_prefix);
    let candidates: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT saga_id, display_id FROM sagas \
             WHERE display_id >= ?1 AND display_id < ?2 LIMIT 5",
        )?;
        stmt.query_map([hex_prefix, &upper_bound], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };
    match candidates.len() {
        0 => Err(BrainCoreError::SagaNotFound(format!("saga-{hex_prefix}"))),
        1 => Ok(candidates.into_iter().next().unwrap().0),
        _ => Err(BrainCoreError::Parse(format!(
            "ambiguous short id `saga-{hex_prefix}` matches: {}",
            candidates
                .iter()
                .map(|(_, d)| format!("saga-{d}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

fn resolve_via_ulid_prefix(conn: &Connection, input: &str) -> Result<String> {
    let upper_bound = increment_string(input);
    let candidates: Vec<String> = {
        let mut stmt =
            conn.prepare("SELECT saga_id FROM sagas WHERE saga_id >= ?1 AND saga_id < ?2 LIMIT 5")?;
        stmt.query_map([input, &upper_bound], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    match candidates.len() {
        0 => Err(BrainCoreError::SagaNotFound(input.to_string())),
        1 => Ok(candidates.into_iter().next().unwrap()),
        _ => Err(BrainCoreError::Parse(format!(
            "ambiguous saga id prefix `{input}` matches: {}",
            candidates.join(", ")
        ))),
    }
}

/// Batch-load short IDs for all sagas. Returns a `saga_id (ULID) → saga-<hex>`
/// map suitable for CLI table rendering where many rows need their short
/// form without a round-trip per row.
pub fn compact_saga_ids(conn: &Connection) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT saga_id, display_id FROM sagas")?;
    let mut out = HashMap::new();
    for row in stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })? {
        let (saga_id, display_id) = row?;
        out.insert(saga_id, compact_saga_id(&display_id));
    }
    Ok(out)
}

/// Increment a string lexicographically (upper bound of a half-open range scan).
///
/// Mirrors the helpers in `tasks/queries/resolve.rs` and `records/queries.rs`
/// — a small ASCII-safe inc-and-carry. Two existing copies plus this one are
/// candidates for a future shared-utility extraction.
fn increment_string(s: &str) -> String {
    debug_assert!(s.is_ascii(), "increment_string expects ASCII input");
    let mut bytes = s.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            return String::from_utf8(bytes).unwrap_or_else(|_| format!("{s}\u{FFFF}"));
        }
        bytes[i] = 0;
    }
    format!("{s}\u{FFFF}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::{migrate_v52_to_v53, migrate_v53_to_v54};

    fn fresh_v54(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA user_version = 52;",
        )
        .unwrap();
        migrate_v52_to_v53(conn).unwrap();
        migrate_v53_to_v54(conn).unwrap();
    }

    fn insert_saga_with_display_id(conn: &Connection, saga_id: &str, display_id: &str) {
        conn.execute(
            "INSERT INTO sagas (saga_id, title, status, created_at, updated_at, display_id)
             VALUES (?1, ?2, 'planning', 1000, 1000, ?3)",
            rusqlite::params![saga_id, format!("title for {saga_id}"), display_id],
        )
        .unwrap();
    }

    #[test]
    fn compact_formats_with_saga_prefix() {
        assert_eq!(compact_saga_id("abc"), "saga-abc");
        assert_eq!(compact_saga_id("123def"), "saga-123def");
    }

    #[test]
    fn parse_short_form_strips_prefix() {
        assert_eq!(parse_short_form("saga-abc"), Some("abc"));
        assert_eq!(parse_short_form("01KR16ZJRDVNF5D463QMVD9PH0"), None);
        assert_eq!(parse_short_form(""), None);
    }

    #[test]
    fn resolve_accepts_bare_ulid() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abc");

        let resolved = resolve_saga_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0").unwrap();
        assert_eq!(resolved, "01KR16ZJRDVNF5D463QMVD9PH0");
    }

    #[test]
    fn resolve_accepts_short_form() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abc");

        let resolved = resolve_saga_id(&conn, "saga-abc").unwrap();
        assert_eq!(resolved, "01KR16ZJRDVNF5D463QMVD9PH0");
    }

    #[test]
    fn resolve_short_form_prefix_match() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        // Stored display_id is longer than the user's input prefix.
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abcd");

        let resolved = resolve_saga_id(&conn, "saga-abc").unwrap();
        assert_eq!(resolved, "01KR16ZJRDVNF5D463QMVD9PH0");
    }

    #[test]
    fn resolve_short_form_ambiguous_prefix_errors() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abcd");
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH1", "abce");

        let err = resolve_saga_id(&conn, "saga-abc").unwrap_err();
        match err {
            BrainCoreError::Parse(msg) => assert!(msg.contains("ambiguous")),
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_unknown_returns_saga_not_found() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);

        let err = resolve_saga_id(&conn, "saga-deadbeef").unwrap_err();
        assert!(matches!(err, BrainCoreError::SagaNotFound(_)));
    }

    #[test]
    fn resolve_malformed_short_form_errors() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);

        // Uppercase is not valid hex per our convention.
        let err = resolve_saga_id(&conn, "saga-ABC").unwrap_err();
        assert!(matches!(err, BrainCoreError::Parse(_)));

        // Non-hex characters rejected.
        let err = resolve_saga_id(&conn, "saga-xyz").unwrap_err();
        assert!(matches!(err, BrainCoreError::Parse(_)));

        // Empty after prefix.
        let err = resolve_saga_id(&conn, "saga-").unwrap_err();
        assert!(matches!(err, BrainCoreError::Parse(_)));
    }

    #[test]
    fn compact_saga_ids_returns_short_form_map() {
        let conn = Connection::open_in_memory().unwrap();
        fresh_v54(&conn);
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH0", "abc");
        insert_saga_with_display_id(&conn, "01KR16ZJRDVNF5D463QMVD9PH1", "def");

        let map = compact_saga_ids(&conn).unwrap();
        assert_eq!(
            map.get("01KR16ZJRDVNF5D463QMVD9PH0"),
            Some(&"saga-abc".to_string())
        );
        assert_eq!(
            map.get("01KR16ZJRDVNF5D463QMVD9PH1"),
            Some(&"saga-def".to_string())
        );
    }
}
