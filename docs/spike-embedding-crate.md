# Spike: Evaluate Embedding Crate Extraction

**Decision: Do not extract at this time.**

Recommendation: defer extraction. The existing `Embed` trait abstraction already
delivers the primary benefit of crate separation. The compile-time and
maintenance gains from extraction are marginal given the dominant cost drivers.

---

## Background

This spike follows the `brain_persistence` extraction (BRN-01KKSP1 epic). The
question is whether `Embed`, `Embedder`, `MockEmbedder`, and their
candle/tokenizers deps should move to a new `brain_embedding` crate.

Machine specs: Apple M3 Max, 36 GB RAM, rustc 1.93.1, macOS Sequoia arm64.

---

## Q1: Compile-Time Impact

### Baseline

From `docs/compile-time-baseline.md` (cold build, full workspace):

| Scenario | Time |
|---|---|
| `cargo build --workspace` (cold) | 2m 02s |
| `cargo build -p brain-lib` (cold) | 1m 56s |
| `cargo build -p brain-lib` (warm) | 0.61s |

The baseline document explicitly notes: **LanceDB and its transitive deps
(`lance`, `lance-namespace-impls`) are the dominant compile-time contributors**.
Candle crates are significant but secondary.

### rlib Sizes (Compile-Weight Proxy)

rlib size correlates with codegen time. Measured against
`target/debug/deps/`:

| Crate | rlib size |
|---|---|
| `candle-core` | 88 MB |
| `candle-transformers` | 86 MB |
| `tokenizers` | 38 MB |
| `candle-nn` | 12 MB |
| **candle+tokenizers total** | **~224 MB** |
| `brain-lib` | 58 MB |

For comparison, LanceDB and lance-related crates collectively exceed the candle
footprint. Moving candle/tokenizers to a separate crate would not meaningfully
shorten a cold build of any binary that depends on `brain-lib`, because:

1. The binary still links `brain-lib`, which still depends on `brain_embedding`.
2. Cold build time is bounded by the LanceDB compilation chain, not candle.
3. The workspace already applies `opt-level = 1` to candle crates in dev profile
   (`Cargo.toml` `[profile.dev.package.candle-*]`), mitigating the debug-build
   performance penalty without adding a crate.

**Estimated savings from extraction:** 0s cold (LanceDB remains the bottleneck),
0s warm (artifacts already cached). No measurable improvement.

---

## Q2: Dependency Weight

| Metric | Count |
|---|---|
| Unique transitive deps in candle+tokenizers closure | 159 |
| Those already shared with `brain-persistence` or other `brain-lib` deps | 95 |
| Deps exclusive to candle+tokenizers closure | **64** |

64 unique deps would move to `brain_embedding`. This is meaningful in isolation,
but every consumer that needs embedding functionality still pulls all 64 — they
are not avoided, just relocated. Crate separation only helps users of `brain-lib`
who do not need embedding; there are currently zero such users (the CLI, MCP
server, and IPC router all require the embedder at startup).

Notable heavy deps in the exclusive set: `gemm`, `gemm-f32`, `gemm-f64`,
`gemm-c32`, `gemm-c64` (BLAS kernels), `accelerate-src`, `onig`/`onig_sys`
(regex in tokenizers), `esaxx-rs`. These inflate link time but are not avoidable
by crate extraction since all current consumers need them.

---

## Q3: Trait Boundary Clarity

The `Embed` trait in `crates/brain_lib/src/embedder.rs:77-80` already provides
clean abstraction:

```rust
pub trait Embed: Send + Sync {
    fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>>;
    fn hidden_size(&self) -> usize;
}
```

`MockEmbedder` is already in the same file and requires no model weights.
`embed_batch_async` wraps any `Arc<dyn Embed>` transparently.

Usage pattern across `brain-lib`: 17 files reference the embedder module.
Integration points:

- `pipeline/mod.rs` — `IndexPipeline` holds `Arc<dyn Embed>`
- `query_pipeline.rs` — `QueryPipeline` and `FederatedPipeline` take `&Arc<dyn Embed>`
- `mcp/mod.rs` — `McpContext` holds `Option<Arc<dyn Embed>>`
- `ipc/router.rs` — accesses `ctx.embedder` as `Option<Arc<dyn Embed>>`
- `ports/mock.rs` — uses `MockEmbedder` directly in test helpers

All call sites are already programmed against the `Embed` trait, not `Embedder`
concretely (except `Embedder::load()` call sites in `pipeline/mod.rs` and
`mcp/mod.rs`). The abstraction boundary is already enforced. Crate separation
would not change how callers interact with the embedding layer.

**The primary benefit of crate extraction — forcing callers to use the trait
interface — is already achieved.** The existing design is sufficient.

---

## Q4: Maintenance Overhead

Adding `brain_embedding` would introduce:

- A new `Cargo.toml` with 4 dependencies (candle-core, candle-nn,
  candle-transformers, tokenizers) + workspace re-exports
- A new `lib.rs` re-exporting `Embed`, `Embedder`, `MockEmbedder`,
  `embed_batch_async`, `MODEL_DOWNLOAD_HINT`
- Updated imports in 17 files (`use brain_lib::embedder::*` → `use brain_embedding::*`)
- Updated `brain-lib/Cargo.toml` to depend on `brain_embedding`
- CI must now compile a third crate in the workspace

The `blake3` dependency (used by `MockEmbedder`) would also move, but it is
already a workspace dep shared with `brain-persistence`.

The `error` type (`BrainCoreError::Embedding`) originates in `brain-persistence`.
`brain_embedding` would depend on `brain-persistence` for the error type, making
the dependency graph: `brain-persistence` ← `brain_embedding` ← `brain-lib`.
This is structurally sound but adds a dependency edge and increases the chance of
circular dep accidents during future refactors.

**Maintenance overhead is low-to-moderate, but the benefit does not justify it
at this stage.**

---

## Summary

| Factor | Finding |
|---|---|
| Compile-time savings | Negligible — LanceDB dominates cold builds; candle is cached on warm |
| Dependency isolation | 64 unique deps, but all current consumers need them anyway |
| Trait boundary | Already clean via `Embed` trait; extraction adds no new abstraction |
| Maintenance cost | Low-moderate: 17 import sites, new crate, new dep edge |

---

## Recommendation: Do Not Extract

The `Embed` trait abstraction already provides the decoupling value. Extraction
would reorganize code without improving compile times, without enabling any
consumer to shed the ML dependencies, and without clarifying interfaces. The
benefit-to-cost ratio is negative at this point in the codebase.

### What Would Change This Decision

Extraction becomes worth doing if any of these conditions arise:

1. **A consumer needs `brain-lib` without ML deps.** For example, a lightweight
   CLI subcommand or a WASM target that handles only task management. Currently
   all consumers load the embedder at startup.

2. **LanceDB is removed or replaced.** If LanceDB exits the dependency tree,
   candle/tokenizers become the dominant compile-time contributors (~224 MB rlib
   vs ~58 MB brain-lib). At that point extraction would produce measurable cold
   build savings.

3. **A second embedding backend is added.** If the project adopts OpenAI
   embeddings or a different local model (e.g., fastembed-rs), a dedicated
   `brain_embedding` crate becomes a natural home for the backend registry
   and the trait. Adding a second backend in a single file (`embedder.rs`)
   would become unwieldy.

4. **Compilation profiling reveals candle as bottleneck.** If a fresh
   `cargo build --timings` run (with LanceDB present) shows candle crates
   taking >30s on the critical path, the analysis should be revisited.
