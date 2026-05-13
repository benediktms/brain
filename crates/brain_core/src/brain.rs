//! Value types describing a registered brain at the workspace layer boundary.
//!
//! The `Brain` DTO is the framework-free projection of a brain entry that
//! `brain_core` exposes to its consumers (notably the `BrainRegistry` port
//! trait). Persistence-layer row types live in `brain_persistence` and are
//! mapped to this DTO at the trait boundary so that core abstractions do not
//! leak storage details.
//!
//! The field set is the minimum sufficient surface for current
//! `BrainRegistry::list_brains` consumers: brain identity (`brain_id`,
//! `name`), task prefix, root + alias JSON payloads (kept as raw strings
//! because callers parse them with brain-specific schemas), and the
//! `archived` flag. Fields that no consumer reads through the trait (e.g.
//! `notes_json`, `projected`) are intentionally omitted; callers that need
//! the richer representation should use the persistence-layer row type
//! directly.

/// Projection of a registered brain returned across the workspace layer
/// boundary by [`crate::ports::BrainRegistry::list_brains`].
#[derive(Debug, Clone)]
pub struct Brain {
    /// Stable identifier (ULID) of the brain.
    pub brain_id: String,
    /// Human-readable name. Unique across the registry.
    pub name: String,
    /// Task-ID prefix (e.g. `BRN`, `WRK`). `None` when not yet set.
    pub prefix: Option<String>,
    /// JSON-encoded array of root paths. `None` when unset.
    ///
    /// Kept as a raw JSON string rather than `Vec<PathBuf>` because callers
    /// already parse it with brain-specific schemas and tolerate malformed
    /// entries individually.
    pub roots_json: Option<String>,
    /// JSON-encoded array of alias names. `None` when unset.
    pub aliases_json: Option<String>,
    /// Whether the brain has been archived.
    pub archived: bool,
}
