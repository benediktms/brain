//! Tag-domain pure logic — clustering, alias resolution, canonical-pick.
//!
//! This module is the algorithm core for synonym clustering & label alias
//! resolution (parent task brn-83a.7.2). It is deliberately IO-free so it can
//! be fixture-tested in isolation; sibling tasks plug it into the persistence
//! and embedder layers.

// `clustering` has no production callers until brn-83a.7.2.3 (job
// orchestration) wires it up; only the inline tests consume it for now.
#[allow(dead_code)]
pub(crate) mod clustering;
