use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::db::meta;
use crate::error::{BrainCoreError, Result};

/// Minimum ULID prefix length (after project prefix + separator).
const MIN_ULID_PREFIX_LEN: usize = 4;

/// Minimum display prefix: "BRN-" (4) + 4 ULID chars = 8.
pub(crate) const MIN_DISPLAY_PREFIX_LEN: usize = 8;

// -- Row types --

/// A row from the records projection table.
#[derive(Debug, Clone)]
pub struct RecordRow {
    pub record_id: String,
    pub title: String,
    pub kind: String,
    pub status: String,
    pub description: Option<String>,
    pub content_hash: String,
    pub content_size: i64,
    pub media_type: Option<String>,
    pub task_id: Option<String>,
    pub actor: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A row from the record_links projection table.
#[derive(Debug, Clone)]
pub struct RecordLink {
    pub record_id: String,
    pub task_id: Option<String>,
    pub chunk_id: Option<String>,
    pub created_at: i64,
}

/// Filters for listing records.
pub struct RecordFilter {
    pub kind: Option<String>,
    pub status: Option<String>,
    pub tag: Option<String>,
    pub task_id: Option<String>,
    pub limit: Option<usize>,
}

// -- Column constant --

const RECORD_COLUMNS: &str =
    "record_id, title, kind, status, description, content_hash, content_size, \
     media_type, task_id, actor, created_at, updated_at";

fn row_to_record(row: &rusqlite::Row) -> rusqlite::Result<RecordRow> {
    Ok(RecordRow {
        record_id: row.get(0)?,
        title: row.get(1)?,
        kind: row.get(2)?,
        status: row.get(3)?,
        description: row.get(4)?,
        content_hash: row.get(5)?,
        content_size: row.get(6)?,
        media_type: row.get(7)?,
        task_id: row.get(8)?,
        actor: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

// -- Query functions --

/// Get a single record by exact ID.
pub fn get_record(conn: &Connection, record_id: &str) -> Result<Option<RecordRow>> {
    let sql = format!("SELECT {RECORD_COLUMNS} FROM records WHERE record_id = ?1");
    let result = conn.query_row(&sql, [record_id], row_to_record).optional()?;
    Ok(result)
}

/// List records with optional filters.
pub fn list_records(conn: &Connection, filter: &RecordFilter) -> Result<Vec<RecordRow>> {
    let mut conditions: Vec<&str> = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if filter.kind.is_some() {
        conditions.push("r.kind = ?");
        params.push(filter.kind.clone().unwrap());
    }
    if filter.status.is_some() {
        conditions.push("r.status = ?");
        params.push(filter.status.clone().unwrap());
    }
    if filter.task_id.is_some() {
        conditions.push("r.task_id = ?");
        params.push(filter.task_id.clone().unwrap());
    }

    // Tag filter requires a JOIN
    let tag_join = if filter.tag.is_some() {
        conditions.push("rt.tag = ?");
        params.push(filter.tag.clone().unwrap());
        "JOIN record_tags rt ON rt.record_id = r.record_id"
    } else {
        ""
    };

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        // Rebuild with proper numbered placeholders
        let mut numbered = Vec::new();
        for (i, cond) in conditions.iter().enumerate() {
            numbered.push(cond.replacen('?', &format!("?{}", i + 1), 1));
        }
        format!("WHERE {}", numbered.join(" AND "))
    };

    let limit_clause = filter
        .limit
        .map(|l| format!("LIMIT {l}"))
        .unwrap_or_default();

    let sql = format!(
        "SELECT r.{RECORD_COLUMNS_PREFIXED} FROM records r {tag_join} {where_clause} \
         ORDER BY r.updated_at DESC, r.record_id ASC {limit_clause}",
        RECORD_COLUMNS_PREFIXED = "record_id, r.title, r.kind, r.status, r.description, \
             r.content_hash, r.content_size, r.media_type, r.task_id, \
             r.actor, r.created_at, r.updated_at",
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), row_to_record)?;
    crate::db::collect_rows(rows)
}

/// Get all tags for a record.
pub fn get_record_tags(conn: &Connection, record_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT tag FROM record_tags WHERE record_id = ?1 ORDER BY tag ASC")?;
    let rows = stmt.query_map([record_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}

/// Get all links for a record.
pub fn get_record_links(conn: &Connection, record_id: &str) -> Result<Vec<RecordLink>> {
    let mut stmt = conn.prepare(
        "SELECT record_id, task_id, chunk_id, created_at \
         FROM record_links WHERE record_id = ?1 ORDER BY created_at ASC",
    )?;
    let rows = stmt.query_map([record_id], |row| {
        Ok(RecordLink {
            record_id: row.get(0)?,
            task_id: row.get(1)?,
            chunk_id: row.get(2)?,
            created_at: row.get(3)?,
        })
    })?;
    crate::db::collect_rows(rows)
}

/// Check if a record exists in the projection.
pub fn record_exists(conn: &Connection, record_id: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM records WHERE record_id = ?1",
        [record_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Resolve a record ID from a prefix (or exact match).
///
/// - Fast path: exact match
/// - Normalize to uppercase
/// - Handle prefixed IDs (e.g. "BRN-01JPH...")
/// - Handle bare ULID prefix (auto-prepend project prefix from brain_meta)
/// - Range scan on PRIMARY KEY
/// - Error on ambiguous/not-found
pub fn resolve_record_id(conn: &Connection, input: &str) -> Result<String> {
    // Fast path: exact match
    if record_exists(conn, input)? {
        return Ok(input.to_string());
    }

    let normalized = input.to_ascii_uppercase();

    let search_prefix = match normalized.find('-') {
        Some(dash_pos) if dash_pos <= 4 => {
            // Looks like a project prefix (1-4 chars before dash), e.g. "BRN-01JPH..."
            let ulid_part = &normalized[dash_pos + 1..];
            if ulid_part.len() < MIN_ULID_PREFIX_LEN {
                return Err(BrainCoreError::RecordEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters after '{}'",
                    &normalized[..=dash_pos]
                )));
            }
            normalized
        }
        Some(_) => {
            // Some other format with a dash — search as-is
            normalized
        }
        None => {
            // No dash — bare ULID prefix, auto-prepend project prefix
            if normalized.len() < MIN_ULID_PREFIX_LEN {
                return Err(BrainCoreError::RecordEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters, got {}",
                    normalized.len()
                )));
            }
            let prefix =
                meta::get_meta(conn, "project_prefix")?.unwrap_or_else(|| "BRN".to_string());
            format!("{prefix}-{normalized}")
        }
    };

    // Range scan on PRIMARY KEY B-tree
    let upper_bound = increment_string(&search_prefix);
    let mut stmt = conn
        .prepare("SELECT record_id FROM records WHERE record_id >= ?1 AND record_id < ?2")?;
    let matches: Vec<String> = stmt
        .query_map(rusqlite::params![search_prefix, upper_bound], |row| {
            row.get(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    match matches.len() {
        0 => Err(BrainCoreError::RecordEvent(format!(
            "no record found matching prefix: {input}"
        ))),
        1 => Ok(matches.into_iter().next().unwrap()),
        n => Err(BrainCoreError::RecordEvent(format!(
            "ambiguous prefix '{input}': matches {n} records"
        ))),
    }
}

/// Compute the shortest unique prefix for a single record ID.
pub fn compact_record_id(conn: &Connection, record_id: &str) -> Result<String> {
    let prev: Option<String> = conn
        .query_row(
            "SELECT record_id FROM records WHERE record_id < ?1 ORDER BY record_id DESC LIMIT 1",
            [record_id],
            |row| row.get(0),
        )
        .optional()?;
    let next: Option<String> = conn
        .query_row(
            "SELECT record_id FROM records WHERE record_id > ?1 ORDER BY record_id ASC LIMIT 1",
            [record_id],
            |row| row.get(0),
        )
        .optional()?;

    let min_prev = prev
        .as_deref()
        .map(|p| common_prefix_len(record_id, p) + 1)
        .unwrap_or(1);
    let min_next = next
        .as_deref()
        .map(|n| common_prefix_len(record_id, n) + 1)
        .unwrap_or(1);

    let min_len = min_prev
        .max(min_next)
        .max(MIN_DISPLAY_PREFIX_LEN)
        .min(record_id.len());

    Ok(record_id[..min_len].to_string())
}

/// Compute compact display IDs for all records (batch, for list display).
pub fn compact_record_ids(conn: &Connection) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT record_id FROM records ORDER BY record_id")?;
    let ids: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut result = HashMap::new();
    let n = ids.len();

    for i in 0..n {
        let id = &ids[i];
        let prev = if i > 0 { Some(ids[i - 1].as_str()) } else { None };
        let next = if i + 1 < n {
            Some(ids[i + 1].as_str())
        } else {
            None
        };

        let min_len_prev = prev.map(|p| common_prefix_len(id, p) + 1).unwrap_or(1);
        let min_len_next = next.map(|nx| common_prefix_len(id, nx) + 1).unwrap_or(1);

        let min_len = min_len_prev.max(min_len_next).max(MIN_DISPLAY_PREFIX_LEN);
        let prefix_len = min_len.min(id.len());

        result.insert(id.clone(), id[..prefix_len].to_string());
    }

    Ok(result)
}

// -- Internal helpers --

/// Increment the last byte of a string for exclusive upper bounds in range scans.
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

/// Length of the common byte prefix between two strings.
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes()
        .zip(b.bytes())
        .take_while(|(ba, bb)| ba == bb)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use crate::records::events::*;
    use crate::records::projections::apply_event;
    use rusqlite::Connection;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn create_record(conn: &Connection, record_id: &str, title: &str, kind: &str) {
        let ev = RecordEvent::from_payload(
            record_id,
            "test-agent",
            RecordCreatedPayload {
                title: title.to_string(),
                kind: kind.to_string(),
                content_ref: ContentRefPayload {
                    hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
                    size: 42,
                    media_type: Some("application/json".to_string()),
                },
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        apply_event(conn, &ev).unwrap();
    }

    fn create_record_with_tag(
        conn: &Connection,
        record_id: &str,
        title: &str,
        kind: &str,
        tag: &str,
    ) {
        let ev = RecordEvent::from_payload(
            record_id,
            "test-agent",
            RecordCreatedPayload {
                title: title.to_string(),
                kind: kind.to_string(),
                content_ref: ContentRefPayload {
                    hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
                    size: 42,
                    media_type: None,
                },
                description: None,
                task_id: None,
                tags: vec![tag.to_string()],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        apply_event(conn, &ev).unwrap();
    }

    // -- get_record tests --

    #[test]
    fn test_get_record() {
        let conn = setup();
        create_record(&conn, "r1", "My Report", "report");

        let row = get_record(&conn, "r1").unwrap().unwrap();
        assert_eq!(row.record_id, "r1");
        assert_eq!(row.title, "My Report");
        assert_eq!(row.kind, "report");
        assert_eq!(row.status, "active");
        assert_eq!(
            row.content_hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(row.content_size, 42);
        assert_eq!(row.media_type.as_deref(), Some("application/json"));
        assert_eq!(row.actor, "test-agent");
    }

    #[test]
    fn test_get_record_not_found() {
        let conn = setup();
        let result = get_record(&conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    // -- list_records tests --

    #[test]
    fn test_list_records_all() {
        let conn = setup();
        create_record(&conn, "r1", "Report One", "report");
        create_record(&conn, "r2", "Diff Two", "diff");
        create_record(&conn, "r3", "Doc Three", "document");

        let filter = RecordFilter {
            kind: None,
            status: None,
            tag: None,
            task_id: None,
            limit: None,
        };
        let rows = list_records(&conn, &filter).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_list_records_by_kind() {
        let conn = setup();
        create_record(&conn, "r1", "Report One", "report");
        create_record(&conn, "r2", "Diff Two", "diff");
        create_record(&conn, "r3", "Report Three", "report");

        let filter = RecordFilter {
            kind: Some("report".to_string()),
            status: None,
            tag: None,
            task_id: None,
            limit: None,
        };
        let rows = list_records(&conn, &filter).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.kind == "report"));
    }

    #[test]
    fn test_list_records_by_status() {
        let conn = setup();
        create_record(&conn, "r1", "Active Record", "report");
        create_record(&conn, "r2", "To Archive", "diff");

        // Archive r2
        let archive_ev = RecordEvent::from_payload(
            "r2",
            "agent",
            RecordArchivedPayload { reason: None },
        );
        apply_event(&conn, &archive_ev).unwrap();

        let filter = RecordFilter {
            kind: None,
            status: Some("active".to_string()),
            tag: None,
            task_id: None,
            limit: None,
        };
        let rows = list_records(&conn, &filter).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].record_id, "r1");

        let filter_archived = RecordFilter {
            kind: None,
            status: Some("archived".to_string()),
            tag: None,
            task_id: None,
            limit: None,
        };
        let archived = list_records(&conn, &filter_archived).unwrap();
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].record_id, "r2");
    }

    #[test]
    fn test_list_records_by_tag() {
        let conn = setup();
        create_record_with_tag(&conn, "r1", "Tagged", "report", "important");
        create_record(&conn, "r2", "Untagged", "diff");
        create_record_with_tag(&conn, "r3", "Also Tagged", "document", "important");

        let filter = RecordFilter {
            kind: None,
            status: None,
            tag: Some("important".to_string()),
            task_id: None,
            limit: None,
        };
        let rows = list_records(&conn, &filter).unwrap();
        assert_eq!(rows.len(), 2);
        let ids: Vec<&str> = rows.iter().map(|r| r.record_id.as_str()).collect();
        assert!(ids.contains(&"r1"));
        assert!(ids.contains(&"r3"));
    }

    #[test]
    fn test_list_records_with_limit() {
        let conn = setup();
        create_record(&conn, "r1", "One", "report");
        create_record(&conn, "r2", "Two", "report");
        create_record(&conn, "r3", "Three", "report");
        create_record(&conn, "r4", "Four", "report");

        let filter = RecordFilter {
            kind: None,
            status: None,
            tag: None,
            task_id: None,
            limit: Some(2),
        };
        let rows = list_records(&conn, &filter).unwrap();
        assert_eq!(rows.len(), 2);
    }

    // -- get_record_tags tests --

    #[test]
    fn test_get_record_tags() {
        let conn = setup();
        create_record(&conn, "r1", "Record", "report");

        // Add tags via events
        let ev1 = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::TagAdded,
            &TagPayload { tag: "beta".to_string() },
        );
        let ev2 = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::TagAdded,
            &TagPayload { tag: "alpha".to_string() },
        );
        apply_event(&conn, &ev1).unwrap();
        apply_event(&conn, &ev2).unwrap();

        let tags = get_record_tags(&conn, "r1").unwrap();
        assert_eq!(tags, vec!["alpha", "beta"]); // sorted
    }

    #[test]
    fn test_get_record_tags_empty() {
        let conn = setup();
        create_record(&conn, "r1", "Record", "report");
        let tags = get_record_tags(&conn, "r1").unwrap();
        assert!(tags.is_empty());
    }

    // -- get_record_links tests --

    #[test]
    fn test_get_record_links() {
        let conn = setup();
        create_record(&conn, "r1", "Record", "report");

        let ev = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("BRN-01TASK".to_string()),
                chunk_id: None,
            },
        );
        apply_event(&conn, &ev).unwrap();

        let links = get_record_links(&conn, "r1").unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].record_id, "r1");
        assert_eq!(links[0].task_id.as_deref(), Some("BRN-01TASK"));
        assert!(links[0].chunk_id.is_none());
    }

    #[test]
    fn test_get_record_links_empty() {
        let conn = setup();
        create_record(&conn, "r1", "Record", "report");
        let links = get_record_links(&conn, "r1").unwrap();
        assert!(links.is_empty());
    }

    // -- record_exists tests --

    #[test]
    fn test_record_exists() {
        let conn = setup();
        create_record(&conn, "r1", "Record", "report");

        assert!(record_exists(&conn, "r1").unwrap());
        assert!(!record_exists(&conn, "r2").unwrap());
    }

    // -- resolve_record_id tests --

    #[test]
    fn test_resolve_exact_match() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Record", "report");
        let resolved =
            resolve_record_id(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_prefix() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Record", "report");
        let resolved = resolve_record_id(&conn, "BRN-01JPHZ").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_case_insensitive() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Record", "report");
        let resolved = resolve_record_id(&conn, "brn-01jphz").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_bare_ulid_prefix() {
        let conn = setup();
        crate::db::meta::set_meta(&conn, "project_prefix", "BRN").unwrap();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Record", "report");
        let resolved = resolve_record_id(&conn, "01JPHZ").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_not_found() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Record", "report");
        let result = resolve_record_id(&conn, "BRN-99ZZZZ");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no record found"));
    }

    #[test]
    fn test_resolve_ambiguous() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZAAAA", "Record A", "report");
        create_record(&conn, "BRN-01JPHZAAAB", "Record B", "report");
        let result = resolve_record_id(&conn, "BRN-01JPHZAAA");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ambiguous"));
    }

    #[test]
    fn test_resolve_too_short() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Record", "report");
        let result = resolve_record_id(&conn, "BRN-01J");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    // -- compact_record_id tests --

    #[test]
    fn test_compact_record_id() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Only record", "report");
        let compact = compact_record_id(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M").unwrap();
        assert_eq!(compact.len(), MIN_DISPLAY_PREFIX_LEN);
        assert_eq!(compact, "BRN-01JP");
    }

    #[test]
    fn test_compact_record_id_shared_prefix() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZAAAA", "Record A", "report");
        create_record(&conn, "BRN-01JPHZAAAB", "Record B", "report");

        let batch = compact_record_ids(&conn).unwrap();
        assert_eq!(batch["BRN-01JPHZAAAA"], "BRN-01JPHZAAAA");
        assert_eq!(batch["BRN-01JPHZAAAB"], "BRN-01JPHZAAAB");
    }

    #[test]
    fn test_compact_record_id_singular_matches_batch() {
        let conn = setup();
        create_record(&conn, "BRN-01JPHZAAAA", "Record A", "report");
        create_record(&conn, "BRN-01JPHZAAAB", "Record B", "report");
        create_record(&conn, "BRN-01JPHZ9999", "Record C", "report");

        let batch = compact_record_ids(&conn).unwrap();
        for (id, expected) in &batch {
            let single = compact_record_id(&conn, id).unwrap();
            assert_eq!(&single, expected, "mismatch for {id}");
        }
    }
}
