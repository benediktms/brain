//! Trust/provenance types for hook-ingestable records.
//!
//! ## Trust bands
//!
//! `Trust` represents the provenance confidence of a record or memory row.
//! It is stored as `TEXT NOT NULL DEFAULT 'untrusted'` in both the `records`
//! and `summaries` tables (schema v46+).
//!
//! | Variant     | SQL text      | Semantics                                           |
//! |-------------|---------------|-----------------------------------------------------|
//! | `Untrusted` | `untrusted`   | Hook-injected, attacker-controlled, opaque origin.  |
//! | `Vetted`    | `vetted`      | Tool-derived but reviewed/curated via a human path. |
//! | `Trusted`   | `trusted`     | User-authored or explicitly marked safe.            |
//!
//! ### Safe-by-default contract
//!
//! The SQL DEFAULT is `'untrusted'`. Any INSERT that omits the `trust` column
//! lands as `untrusted`. Hook-driven importers rely on this default; they do
//! not need to pass `trust` explicitly. Callers writing trusted records MUST
//! pass `Trust::Trusted` explicitly — the default never promotes.
//!
//! ### Retrieval-side filtering
//!
//! The `--frame=safety` flag (`.4`) applies:
//! ```sql
//! WHERE trust IN ('trusted', 'vetted')
//!   AND (source_tool IS NULL OR source_tool = 'user')
//! ```
//! Default hook-injection paths filter to `trusted` only until vetted paths
//! are established.
//!
//! ## Source tool
//!
//! [`SourceTool`] records the originating tool for a row. `None` means
//! system-internal (e.g. consolidation jobs). Hook paths MUST set a value.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{BrainCoreError, Result};

// ── Trust ────────────────────────────────────────────────────────────────────

/// Provenance confidence level for a record or memory row.
///
/// Stored as TEXT in SQLite. Three bands are defined; extension requires a
/// schema migration (add new variant + SQL CHECK update).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trust {
    /// Hook-injected, attacker-controlled. Default for all hook-driven paths.
    Untrusted,
    /// Tool-derived but curated via a human-in-the-loop review step.
    Vetted,
    /// User-authored or explicitly verified. Required for retrieval by default.
    Trusted,
}

impl Trust {
    /// The SQL text representation stored in the database.
    pub fn as_str(&self) -> &'static str {
        match self {
            Trust::Untrusted => "untrusted",
            Trust::Vetted => "vetted",
            Trust::Trusted => "trusted",
        }
    }

    /// Default for any INSERT that omits the trust column.
    ///
    /// Mirrors the SQL `DEFAULT 'untrusted'` — callers that do not set trust
    /// explicitly receive this value.
    pub const fn default_for_hook() -> Self {
        Trust::Untrusted
    }

    /// Default for user-invoked write paths (CLI `brain memory write`).
    pub const fn default_for_user() -> Self {
        Trust::Trusted
    }
}

impl fmt::Display for Trust {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Trust {
    type Err = BrainCoreError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "untrusted" => Ok(Trust::Untrusted),
            "vetted" => Ok(Trust::Vetted),
            "trusted" => Ok(Trust::Trusted),
            other => Err(BrainCoreError::Database(format!(
                "unknown trust value: '{other}'; expected untrusted | vetted | trusted"
            ))),
        }
    }
}

impl rusqlite::types::ToSql for Trust {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Borrowed(
            rusqlite::types::ValueRef::Text(self.as_str().as_bytes()),
        ))
    }
}

impl rusqlite::types::FromSql for Trust {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str()?;
        s.parse::<Trust>()
            .map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(e)))
    }
}

// ── SourceTool ───────────────────────────────────────────────────────────────

/// The originating tool for a record or memory row.
///
/// Stored as nullable TEXT in SQLite. `None` means system-internal (e.g. a
/// consolidation job). Hook-driven importers MUST set a specific variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTool {
    /// Stop-hook transcript JSONL parser.
    Transcript,
    /// PostToolUse:Bash git commit capture.
    Git,
    /// WebFetch tool output.
    WebFetch,
    /// Bash tool output.
    Bash,
    /// Read tool output.
    Read,
    /// User-invoked CLI path (e.g. `brain memory write`).
    User,
}

impl SourceTool {
    /// The SQL text representation stored in the database.
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceTool::Transcript => "transcript",
            SourceTool::Git => "git",
            SourceTool::WebFetch => "web_fetch",
            SourceTool::Bash => "bash",
            SourceTool::Read => "read",
            SourceTool::User => "user",
        }
    }
}

impl fmt::Display for SourceTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceTool {
    type Err = BrainCoreError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "transcript" => Ok(SourceTool::Transcript),
            "git" => Ok(SourceTool::Git),
            "web_fetch" => Ok(SourceTool::WebFetch),
            "bash" => Ok(SourceTool::Bash),
            "read" => Ok(SourceTool::Read),
            "user" => Ok(SourceTool::User),
            other => Err(BrainCoreError::Database(format!(
                "unknown source_tool value: '{other}'; \
                 expected transcript | git | web_fetch | bash | read | user"
            ))),
        }
    }
}

impl rusqlite::types::ToSql for SourceTool {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Borrowed(
            rusqlite::types::ValueRef::Text(self.as_str().as_bytes()),
        ))
    }
}

impl rusqlite::types::FromSql for SourceTool {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str()?;
        s.parse::<SourceTool>()
            .map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trust_round_trip() {
        for (trust, expected) in [
            (Trust::Untrusted, "untrusted"),
            (Trust::Vetted, "vetted"),
            (Trust::Trusted, "trusted"),
        ] {
            assert_eq!(trust.as_str(), expected);
            assert_eq!(expected.parse::<Trust>().unwrap(), trust);
        }
    }

    #[test]
    fn test_trust_ordering() {
        assert!(Trust::Untrusted < Trust::Vetted);
        assert!(Trust::Vetted < Trust::Trusted);
    }

    #[test]
    fn test_trust_unknown_value_errors() {
        let result = "unknown".parse::<Trust>();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown trust value")
        );
    }

    #[test]
    fn test_source_tool_round_trip() {
        for (tool, expected) in [
            (SourceTool::Transcript, "transcript"),
            (SourceTool::Git, "git"),
            (SourceTool::WebFetch, "web_fetch"),
            (SourceTool::Bash, "bash"),
            (SourceTool::Read, "read"),
            (SourceTool::User, "user"),
        ] {
            assert_eq!(tool.as_str(), expected);
            assert_eq!(expected.parse::<SourceTool>().unwrap(), tool);
        }
    }

    #[test]
    fn test_default_for_hook_is_untrusted() {
        assert_eq!(Trust::default_for_hook(), Trust::Untrusted);
    }

    #[test]
    fn test_default_for_user_is_trusted() {
        assert_eq!(Trust::default_for_user(), Trust::Trusted);
    }
}
