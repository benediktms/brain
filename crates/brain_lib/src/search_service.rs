//! Search service — groups read-side search components.
//!
//! Packages `StoreReader` (LanceDB vector store) and `Embedder` into a single
//! unit, separating search concerns from the core repo layer (`BrainStores`).

use std::sync::Arc;

use crate::embedder::Embed;
use brain_persistence::store::StoreReader;

/// Read-side search components.
///
/// Optional in any context — when absent, memory/search tools are unavailable
/// but task and record operations still work.
pub struct SearchService {
    pub store: StoreReader,
    pub embedder: Arc<dyn Embed>,
}
