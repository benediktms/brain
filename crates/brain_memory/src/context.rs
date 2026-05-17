//! Shared semantic context for memory operations that need search +
//! embedder access.
//!
//! Composes the SQLite-backed `Db` with the optional LanceDB
//! [`StoreReader`] and embedder. When the search layer is absent
//! (`store == None` or `embedder == None`), the read-side memory
//! operations (`retrieve`, `reflect.prepare`) return a clear error
//! asking the user to download the embedding model.
//!
//! Used by the heavier memory tools (`reflect`, `retrieve`) that
//! compose multiple stores. The simpler tools (`walk_thread`,
//! `write_episode`, `write_procedure`, `consolidate`,
//! `summarize_scope`) take primitives directly — they don't need
//! the full context.

use std::sync::Arc;

use brain_core::metrics::Metrics;
use brain_core::ports::Embed;
use brain_persistence::db::Db;
use brain_persistence::store::StoreReader;

/// Borrowed view over the resources a semantic memory operation
/// needs.
///
/// Lifetimes: all fields are `&'a` so a single borrow of the
/// composing daemon/MCP context can build one without cloning. The
/// `Option<&...>` fields are `Some` only when the search layer was
/// successfully bootstrapped at process start.
pub struct SemanticContext<'a> {
    pub db: &'a Db,
    pub brain_id: &'a str,
    pub brain_name: &'a str,
    pub store: Option<&'a StoreReader>,
    pub embedder: Option<&'a Arc<dyn Embed>>,
    pub metrics: &'a Arc<Metrics>,
}
