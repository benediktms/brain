//! Tag-domain logic — synonym clustering, alias resolution, and the
//! per-brain recluster job (parent task `brn-83a.7.2`).
//!
//! - [`clustering`] is the pure algorithm core (IO-free, fixture-tested).
//! - [`recluster`] wires that algorithm into the v43 schema as a runnable
//!   per-brain job (`brn-83a.7.2.3`).

// `clustering` and `recluster` carry `#[allow(dead_code)]` while
// `brn-83a.7.2.3` is being implemented incrementally. Both annotations
// come off in the final commit of that task once `run_recluster` calls
// `cluster_tags` and exercises the helper types.
#[allow(dead_code)]
pub(crate) mod clustering;
#[allow(dead_code)]
pub mod recluster;

// `pub use` re-exports trigger `unused_imports` until sibling
// `brn-83a.7.2.5` (MCP/CLI surface) consumes them. The allow comes off
// when that task lands.
#[allow(unused_imports)]
pub use clustering::ClusterParams;
#[allow(unused_imports)]
pub use recluster::{ReclusterReport, run_recluster};
