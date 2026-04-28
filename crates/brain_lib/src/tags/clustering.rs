//! Synonym clustering for raw tag strings.
//!
//! Given a set of `(tag, embedding, reference_count)` candidates, group near-
//! duplicate tags together via cosine-similarity connected components and pick
//! a canonical label for each cluster. The function is deterministic and pure:
//! no DB, no embedder, no IO. It is the algorithm core that
//! `brn-83a.7.2.3` (job orchestration) plugs into.
//!
//! # Inputs
//!
//! Embeddings are expected to be **L2-normalized** (unit length). The brain
//! embedder produces unit vectors, so callers in this codebase do not need to
//! normalize. With unit-length inputs, cosine similarity reduces to a dot
//! product (see [`crate::dedup`] for the same precondition).
//!
//! # Algorithm
//!
//! 1. Build a similarity graph: O(n²) cosine-similarity loop, edge when
//!    `sim ≥ params.cosine_threshold`. Singletons (no edges) form their own
//!    one-member cluster.
//! 2. Connected components via a small in-module union-find.
//! 3. Canonical pick per cluster, in this order (each step breaks ties left
//!    over from the previous):
//!     1. highest `reference_count` wins
//!     2. shortest `tag.len()` (in bytes — sufficient since tag-string content
//!        is unconstrained and we only need a deterministic tiebreak)
//!     3. lexicographic (`tag.cmp`)
//! 4. `cluster_id` is the blake3 hash of `members.sort().join('\0')`,
//!    truncated to 16 hex chars. The ticket text says "SHA-256" but the
//!    property required is "stable hash of sorted member set" — blake3 is
//!    already a workspace dependency (see `crate::utils::content_hash`), so
//!    we avoid pulling in `sha2` for one call site.

/// A single tag candidate fed into [`cluster_tags`].
#[derive(Debug, Clone)]
pub struct TagCandidate {
    pub tag: String,
    /// L2-normalized embedding (see module docs).
    pub embedding: Vec<f32>,
    /// Number of records or tasks referencing this tag. Used as the primary
    /// canonical-pick signal; pass `0` if unknown.
    pub reference_count: i64,
}

/// One cluster produced by [`cluster_tags`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagCluster {
    /// Stable, content-addressable id: blake3 of sorted member set, 16 hex.
    pub cluster_id: String,
    pub canonical: String,
    /// Raw tags in this cluster, including the canonical one. Sorted
    /// lexicographically for output stability.
    pub members: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct ClusterParams {
    pub cosine_threshold: f32,
}

impl Default for ClusterParams {
    fn default() -> Self {
        Self {
            cosine_threshold: 0.85,
        }
    }
}

/// Cosine similarity for L2-normalized vectors (a · b). Returns 0.0 if the
/// vectors are different lengths — defensive for malformed input rather than a
/// panic, since the clustering caller can't recover from a panic mid-loop.
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Minimal union-find. Path compression on `find` keeps the per-call cost
/// near-amortized constant; we skip union-by-rank since tag counts are tiny.
struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // halving
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent[ra] = rb;
        }
    }
}

/// Pick the canonical tag for a cluster using the documented tiebreak chain.
/// `members` is a slice of indices into `candidates`.
fn pick_canonical(candidates: &[TagCandidate], members: &[usize]) -> String {
    let winner = members
        .iter()
        .min_by(|&&a, &&b| {
            let ca = &candidates[a];
            let cb = &candidates[b];
            // Higher reference_count wins → reverse comparison so larger sorts "smaller".
            cb.reference_count
                .cmp(&ca.reference_count)
                .then_with(|| ca.tag.len().cmp(&cb.tag.len()))
                .then_with(|| ca.tag.cmp(&cb.tag))
        })
        .copied()
        .expect("cluster always has at least one member");
    candidates[winner].tag.clone()
}

/// Stable, content-addressable cluster id: blake3 of sorted members joined by
/// NUL, truncated to 16 hex chars. The NUL separator avoids collisions between
/// e.g. `["ab", "cd"]` and `["a", "bcd"]`.
fn cluster_id_for(sorted_members: &[String]) -> String {
    let joined = sorted_members.join("\0");
    let hash = blake3::hash(joined.as_bytes()).to_hex().to_string();
    hash[..16].to_string()
}

/// Cluster tag candidates by embedding similarity and pick a canonical label
/// per cluster. See module docs for algorithm and tiebreak rules.
pub fn cluster_tags(candidates: Vec<TagCandidate>, params: ClusterParams) -> Vec<TagCluster> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let n = candidates.len();
    let mut uf = UnionFind::new(n);

    // O(n²) similarity loop — singletons fall out naturally by never joining.
    for i in 0..n {
        for j in (i + 1)..n {
            if cosine(&candidates[i].embedding, &candidates[j].embedding) >= params.cosine_threshold
            {
                uf.union(i, j);
            }
        }
    }

    // Bucket members by their union-find root.
    let mut buckets: std::collections::BTreeMap<usize, Vec<usize>> =
        std::collections::BTreeMap::new();
    for i in 0..n {
        let root = uf.find(i);
        buckets.entry(root).or_default().push(i);
    }

    let mut out: Vec<TagCluster> = buckets
        .into_values()
        .map(|members| {
            let canonical = pick_canonical(&candidates, &members);
            let mut tags: Vec<String> =
                members.iter().map(|&i| candidates[i].tag.clone()).collect();
            tags.sort();
            let cluster_id = cluster_id_for(&tags);
            TagCluster {
                cluster_id,
                canonical,
                members: tags,
            }
        })
        .collect();

    // Sort output by cluster_id for fully deterministic ordering across runs.
    out.sort_by(|a, b| a.cluster_id.cmp(&b.cluster_id));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L2-normalize a vector so test fixtures can be written semantically.
    fn n(v: Vec<f32>) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / norm).collect()
    }

    fn cand(tag: &str, embedding: Vec<f32>, count: i64) -> TagCandidate {
        TagCandidate {
            tag: tag.to_string(),
            embedding,
            reference_count: count,
        }
    }

    /// Test fixture for the bug/perf cluster scenario.
    ///
    /// Hand-tuned so that:
    /// - At threshold 0.85: bug/bugs/defect form one cluster, perf/performance
    ///   form another.
    /// - At threshold 0.95: bug/bugs link, defect splits off as its own.
    fn bug_perf_fixture() -> Vec<TagCandidate> {
        vec![
            cand("bug", n(vec![1.0, 0.0, 0.0]), 5),
            cand("bugs", n(vec![1.0, 0.3, 0.0]), 1),
            cand("defect", n(vec![1.0, 0.7, 0.0]), 2),
            cand("performance", n(vec![0.0, 0.0, 1.0]), 3),
            cand("perf", n(vec![0.0, 0.3, 1.0]), 1),
        ]
    }

    #[test]
    fn empty_input_returns_empty() {
        let out = cluster_tags(vec![], ClusterParams::default());
        assert!(out.is_empty());
    }

    #[test]
    fn singletons_become_one_member_clusters() {
        // Three orthogonal vectors — no pairwise edges — so each is its own cluster.
        let input = vec![
            cand("alpha", n(vec![1.0, 0.0, 0.0]), 1),
            cand("beta", n(vec![0.0, 1.0, 0.0]), 1),
            cand("gamma", n(vec![0.0, 0.0, 1.0]), 1),
        ];
        let out = cluster_tags(input, ClusterParams::default());
        assert_eq!(out.len(), 3, "expected 3 singleton clusters, got {out:?}");
        for c in &out {
            assert_eq!(c.members.len(), 1);
            assert_eq!(c.canonical, c.members[0]);
        }
    }

    #[test]
    fn dense_group_forms_single_cluster_at_default_threshold() {
        let out = cluster_tags(bug_perf_fixture(), ClusterParams::default());
        assert_eq!(
            out.len(),
            2,
            "expected 2 clusters at threshold 0.85, got {out:?}"
        );

        let bug_cluster = out
            .iter()
            .find(|c| c.members.contains(&"bug".to_string()))
            .unwrap();
        let perf_cluster = out
            .iter()
            .find(|c| c.members.contains(&"perf".to_string()))
            .unwrap();

        let mut bug_members = bug_cluster.members.clone();
        bug_members.sort();
        assert_eq!(bug_members, vec!["bug", "bugs", "defect"]);
        // bug has the highest reference_count (5) → canonical.
        assert_eq!(bug_cluster.canonical, "bug");

        let mut perf_members = perf_cluster.members.clone();
        perf_members.sort();
        assert_eq!(perf_members, vec!["perf", "performance"]);
        // performance has refcount 3 vs perf's 1 → canonical, despite being longer.
        assert_eq!(perf_cluster.canonical, "performance");
    }

    #[test]
    fn tighter_threshold_splits_dense_group() {
        let params = ClusterParams {
            cosine_threshold: 0.95,
        };
        let out = cluster_tags(bug_perf_fixture(), params);
        // bug~bugs (0.958 ≥ 0.95) stays linked.
        // bug~defect (0.819) and bugs~defect (0.949) both fall below 0.95 → defect splits.
        // perf~performance (0.958) stays linked.
        // So we expect 3 clusters: {bug,bugs}, {defect}, {perf,performance}.
        assert_eq!(
            out.len(),
            3,
            "expected 3 clusters at threshold 0.95, got {out:?}"
        );

        let defect_cluster = out.iter().find(|c| c.canonical == "defect").unwrap();
        assert_eq!(defect_cluster.members, vec!["defect"]);
    }

    #[test]
    fn canonical_pick_prefers_highest_reference_count() {
        // Three near-identical tags with distinct counts — count wins outright.
        let v = n(vec![1.0, 0.0, 0.0]);
        let input = vec![
            cand("low", v.clone(), 1),
            cand("mid", v.clone(), 5),
            cand("high", v.clone(), 9),
        ];
        let out = cluster_tags(input, ClusterParams::default());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].canonical, "high");
    }

    #[test]
    fn canonical_pick_breaks_count_tie_by_shortest_tag() {
        // Tied refcount (= 1) → length wins. "ab" (2) beats "abcd" (4) and "abcde" (5).
        let v = n(vec![1.0, 0.0, 0.0]);
        let input = vec![
            cand("abcde", v.clone(), 1),
            cand("ab", v.clone(), 1),
            cand("abcd", v.clone(), 1),
        ];
        let out = cluster_tags(input, ClusterParams::default());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].canonical, "ab");
    }

    #[test]
    fn canonical_pick_breaks_length_tie_lexicographically() {
        // Same refcount, same length (5 chars each) — lex picks "alpha" over "gamma".
        let v = n(vec![1.0, 0.0, 0.0]);
        let input = vec![cand("gamma", v.clone(), 1), cand("alpha", v.clone(), 1)];
        let out = cluster_tags(input, ClusterParams::default());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].canonical, "alpha");
    }

    #[test]
    fn cluster_id_is_deterministic_across_runs() {
        let a = cluster_tags(bug_perf_fixture(), ClusterParams::default());
        let b = cluster_tags(bug_perf_fixture(), ClusterParams::default());

        let mut ids_a: Vec<&str> = a.iter().map(|c| c.cluster_id.as_str()).collect();
        let mut ids_b: Vec<&str> = b.iter().map(|c| c.cluster_id.as_str()).collect();
        ids_a.sort();
        ids_b.sort();
        assert_eq!(
            ids_a, ids_b,
            "cluster_ids must be deterministic across calls"
        );

        for c in &a {
            assert_eq!(c.cluster_id.len(), 16, "cluster_id must be 16 hex chars");
            assert!(c.cluster_id.chars().all(|ch| ch.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn cluster_id_is_independent_of_input_order() {
        let mut shuffled = bug_perf_fixture();
        shuffled.reverse();

        let a = cluster_tags(bug_perf_fixture(), ClusterParams::default());
        let b = cluster_tags(shuffled, ClusterParams::default());

        // Same membership → same cluster_ids regardless of input order.
        let mut ids_a: Vec<&str> = a.iter().map(|c| c.cluster_id.as_str()).collect();
        let mut ids_b: Vec<&str> = b.iter().map(|c| c.cluster_id.as_str()).collect();
        ids_a.sort();
        ids_b.sort();
        assert_eq!(ids_a, ids_b);
    }
}
