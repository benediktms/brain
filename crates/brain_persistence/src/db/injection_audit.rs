//! injection_audit table — append-only audit log for hook sanitization passes.
//!
//! One row is written per `sanitize_hook_input` call. The table is intentionally
//! append-only at the Rust API level — no update or delete paths are exposed.

use rusqlite::Connection;

use crate::error::Result;

/// Parameters for one injection-audit row.
pub struct InjectionAuditEntry<'a> {
    /// Unix seconds (UTC).
    pub ts: i64,
    /// Hook name, e.g. `"PreToolUse:Edit"`.
    pub hook_event: &'a str,
    /// Claude Code session ID, if available.
    pub session_id: Option<&'a str>,
    /// Comma-separated record IDs injected, if any.
    pub record_ids: Option<&'a str>,
    /// Byte length of text before sanitization.
    pub input_len: i64,
    /// Byte length of text after sanitization and length cap.
    pub output_len: i64,
    /// JSON: per-category strip counts.
    pub stripped_counts: &'a str,
    /// `true` if the output was length-capped.
    pub was_truncated: bool,
    /// Where the opt-in originated, e.g. `"brain.toml"`.
    pub opt_in_source: &'a str,
}

/// Insert one row into the `injection_audit` table.
///
/// The table must already exist (created by migration v46→v47). This function
/// is called from `Db::log_injection_audit` — callers should use that method
/// rather than this free function directly.
pub fn insert(conn: &Connection, entry: &InjectionAuditEntry<'_>) -> Result<()> {
    conn.execute(
        "INSERT INTO injection_audit
             (ts, hook_event, session_id, record_ids,
              input_len, output_len, stripped_counts,
              was_truncated, opt_in_source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            entry.ts,
            entry.hook_event,
            entry.session_id,
            entry.record_ids,
            entry.input_len,
            entry.output_len,
            entry.stripped_counts,
            entry.was_truncated as i32,
            entry.opt_in_source,
        ],
    )?;
    Ok(())
}
