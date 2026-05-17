//! Cross-crate abstractions over `brain_persistence` types.
//!
//! brn-2fe.15 will extend this module with the `VectorStore` trait that lets
//! `brain_persistence::store::Store` be swapped (or replaced with a remote
//! backend). For now, `VectorSearchStrategy` is the stable enum that survives
//! that swap — callers depend on it instead of `VectorSearchMode` directly.

use brain_persistence::store::VectorSearchMode;

/// Vector-search strategy abstracted from the underlying store.
///
/// Variants mirror `brain_persistence::store::VectorSearchMode` 1-for-1.
/// brn-2fe.15 will keep this enum stable when the underlying store type
/// is swapped for a trait, so callers' `with_mode(...)` calls don't shift.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchStrategy {
    /// Brute-force scan — bypasses any ANN index. O(n), 100% accurate.
    /// Use for golden tests and debugging.
    Exact,
    /// ANN search with refinement — index finds candidates, full uncompressed
    /// vectors rescore. Best balance of speed and fidelity.
    #[default]
    AnnRefined,
    /// Pure ANN search — only the compressed (quantized) vectors. Fastest,
    /// but distances are approximate.
    AnnFast,
}

impl From<VectorSearchStrategy> for VectorSearchMode {
    fn from(s: VectorSearchStrategy) -> Self {
        match s {
            VectorSearchStrategy::Exact => VectorSearchMode::Exact,
            VectorSearchStrategy::AnnRefined => VectorSearchMode::AnnRefined,
            VectorSearchStrategy::AnnFast => VectorSearchMode::AnnFast,
        }
    }
}

impl From<VectorSearchMode> for VectorSearchStrategy {
    fn from(m: VectorSearchMode) -> Self {
        match m {
            VectorSearchMode::Exact => VectorSearchStrategy::Exact,
            VectorSearchMode::AnnRefined => VectorSearchStrategy::AnnRefined,
            VectorSearchMode::AnnFast => VectorSearchStrategy::AnnFast,
        }
    }
}

impl std::fmt::Display for VectorSearchStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact => write!(f, "exact"),
            Self::AnnRefined => write!(f, "ann_refined"),
            Self::AnnFast => write!(f, "ann_fast"),
        }
    }
}

impl std::str::FromStr for VectorSearchStrategy {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "exact" => Ok(Self::Exact),
            "ann_refined" => Ok(Self::AnnRefined),
            "ann_fast" => Ok(Self::AnnFast),
            other => Err(format!(
                "unknown vector_search_strategy '{other}'; expected exact, ann_refined, or ann_fast"
            )),
        }
    }
}
