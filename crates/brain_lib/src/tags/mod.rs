//! Tag-domain logic — synonym clustering, alias resolution, and the
//! per-brain recluster job (parent task `brn-83a.7.2`).
//!
//! - [`clustering`] is the pure algorithm core (IO-free, fixture-tested).
//! - [`recluster`] wires that algorithm into the v43 schema as a runnable
//!   per-brain job (`brn-83a.7.2.3`).

// `clustering` and `recluster` are reachable only from the test mod inside
// `recluster.rs` until sibling task `brn-83a.7.2.5` (MCP/CLI surface)
// wires `run_recluster` into a non-test caller. Until then the dead-code
// lint would cascade across every private helper inside `recluster.rs`.
// Both allows lift in `brn-83a.7.2.5`.
#[allow(dead_code)]
pub(crate) mod clustering;
#[allow(dead_code)]
pub(crate) mod recluster;

#[allow(unused_imports)]
pub use clustering::ClusterParams;
#[allow(unused_imports)]
pub use recluster::{ReclusterReport, run_recluster};
