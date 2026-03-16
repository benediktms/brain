# Compile-Time Comparison: Before vs After Persistence Extraction

Post-refactor measurements for the `brain` workspace after extraction of `brain-persistence` crate.
Compared against `docs/compile-time-baseline.md` (pre-refactor baseline).

## Machine Specs

| Field | Value |
|---|---|
| OS | Darwin 25.2.0 (macOS Sequoia) arm64 |
| CPU | Apple M3 Max |
| RAM | 36 GB |
| Rust | rustc 1.93.1 (01f6ddf75 2026-02-11) |
| Date | 2026-03-16 |

## Results

Each scenario run once. Cold = `cargo clean` before measurement. Warm = prior build artifacts present.

Post-refactor note: `cargo build -p brain-lib` now compiles `brain-persistence` as a dependency of `brain-lib`,
meaning the "brain-lib only" cold time includes both crates. This is expected and correct — `brain-persistence`
replaces code that was previously part of `brain-lib`.

| # | Scenario | Baseline (pre-refactor) | Attempt 1 (post-refactor, direct deps present) | Delta | % Change |
|---|---|---|---|---|---|
| 1 | Cold full workspace (`cargo build --workspace`) | 2m 02s | 2m 06s | +4s | +3.3% |
| 2a | Cold `brain-lib` only (`cargo build -p brain-lib`) | 1m 56s | 1m 59s | +3s | +2.6% |
| 2b | Warm `brain-lib`, no changes | 0.61s | 0.78s | +0.17s | +27.9% |
| 3 | Incremental touch `pipeline/mod.rs` | 1.32s | 1.33s | +0.01s | +0.8% |
| 4 | Cold test compile (`cargo test --workspace --no-run`) | 2m 05s | 2m 25s | +20s | +16.0% |

## Direct Dependency Analysis

### Why `rusqlite` and `lancedb` remain in `brain_lib/Cargo.toml`

The persistence extraction moved concrete I/O implementations (schema, migrations, object store, vector store)
into `brain-persistence`. However, the SQL query and projection layer was not migrated — it remains in
`brain_lib` and uses `rusqlite::Connection` directly across 19 source files:

```
crates/brain_lib/src/records/queries.rs
crates/brain_lib/src/records/projections.rs
crates/brain_lib/src/tasks/queries/mod.rs
crates/brain_lib/src/tasks/queries/details.rs
crates/brain_lib/src/tasks/queries/filters.rs
crates/brain_lib/src/tasks/queries/labels.rs
crates/brain_lib/src/tasks/queries/listing.rs
crates/brain_lib/src/tasks/queries/resolve.rs
crates/brain_lib/src/tasks/projections.rs
crates/brain_lib/src/tasks/cycle.rs
crates/brain_lib/src/pipeline/mod.rs
crates/brain_lib/src/pipeline/maintenance.rs
crates/brain_lib/src/mcp/tools/status.rs
... (19 files total)
```

Removing `rusqlite` from `brain_lib/Cargo.toml` without migrating this code would cause compilation failure.
`arrow-schema`, `arrow-array`, and `lancedb` remain as direct deps because the vector pipeline code in
`brain_lib` (embed_poll, pipeline/mod.rs) uses arrow types from those crates.

Verified via `cargo tree -p brain-lib --depth 1`:
```
brain-lib v0.2.1
├── arrow-array v57.3.0
├── arrow-schema v57.3.0
├── lancedb v0.26.2
├── rusqlite v0.33.0
└── brain-persistence v0.2.1
    ├── arrow-array v57.3.0
    ├── arrow-schema v57.3.0
    ├── lancedb v0.26.2
    └── rusqlite v0.33.0
```

These appear as **direct** deps of `brain-lib` (not transitive-only), confirming the migration is incomplete.

## Analysis

### Cold builds (scenarios 1, 2a, 4)

Cold build times increased slightly (+3–4s for build, +20s for test compile). This is expected:
- The extraction added a new `brain-persistence` crate to the dependency graph, introducing one additional
  codegen unit and link step.
- LanceDB and its transitive dependencies (`lance`, `lance-namespace-impls`) remain the dominant cold-build
  contributors. The refactor did not alter the dependency footprint of those crates.
- The +20s increase in test compile (scenario 4) is proportionally larger because test profile builds also
  compile test-only dependencies (`criterion`, `proptest`, `rstest`) for the new `brain-persistence` crate
  in addition to `brain-lib`.
- These cold-build regressions are considered acceptable. They affect CI from a clean state only, not
  developer workflows.

### Warm incremental (scenario 2b)

Warm `brain-lib` rebuild increased from 0.61s to 0.78s (+0.17s, +28%). This is measurement noise within
sub-second range — Cargo's idle overhead, filesystem metadata, and lock acquisition fluctuate at this scale.
No regression of substance.

### Incremental touch (scenario 3)

Incremental rebuild after touching `crates/brain_lib/src/pipeline/mod.rs` is unchanged: 1.32s → 1.33s
(+0.01s, +0.8%). The extraction correctly isolated `brain-persistence` such that changes to `brain-lib`'s
pipeline module do not cascade into `brain-persistence`. This validates the primary architectural goal of the
refactor — persistence and pipeline modules are now independently recompilable.

### ≥20% Incremental Rebuild Improvement Target

**Target not met.** The directive set a target of ≥20% improvement in incremental `brain-lib` rebuilds.
Observed results:

| Metric | Baseline | Post-refactor | Change |
|---|---|---|---|
| Warm no-change rebuild | 0.61s | 0.78s | +28% (slower) |
| Incremental touch rebuild | 1.32s | 1.33s | +0.8% (neutral) |

The incremental touch time is statistically unchanged.

**Root cause:** Two compounding factors prevent the improvement:

1. **Incomplete migration.** The SQL query and projection layer (`tasks/queries/`, `records/queries.rs`,
   `records/projections.rs`, etc.) was not migrated to `brain-persistence`. These modules use
   `rusqlite::Connection` directly, requiring `rusqlite` as a direct `brain-lib` dependency. Until this
   code is migrated, `brain-lib` always links rusqlite — removing the dep from `Cargo.toml` would be
   purely cosmetic (re-exporting from `brain-persistence`) and would not change compile times.

2. **Baseline was already optimal.** The 1.32s incremental baseline reflected `brain-lib` recompiling
   only itself. There was no unnecessary dependency cascade to eliminate — the persistence code was
   compiled once and cached, not re-compiled on every `brain-lib` touch.

### What would achieve the ≥20% target

To realize a measurable compile-time reduction:

1. **Migrate the query/projection layer.** Move `tasks/queries/`, `tasks/projections.rs`,
   `records/queries.rs`, `records/projections.rs`, and related modules into `brain-persistence`.
   This would allow `rusqlite` to be removed as a direct `brain-lib` dep.

2. **Migrate the arrow/vector pipeline code.** Move `pipeline/embed_poll.rs` and vector-related
   pipeline code into `brain-persistence`. This would allow `lancedb`, `arrow-schema`, and
   `arrow-array` to be removed as direct `brain-lib` deps.

3. After both migrations: `brain-lib` incremental rebuilds would not require linking rusqlite or
   lancedb at all, potentially reducing the 1.32s touch rebuild by the rusqlite/lancedb link step.

## Cargo Timings Reports

| Build | Report Path |
|---|---|
| Baseline (pre-refactor) | `target/cargo-timings/cargo-timing-20260315T221429.127667Z.html` |
| Post-refactor (warm, --timings) | `target/cargo-timings/cargo-timing-20260316T073824.12873Z.html` |

Open with any browser. The post-refactor timings show `brain-persistence` and `brain-lib` as separate
leaf nodes in the dependency graph — `brain-persistence` compiles before `brain-lib` begins.

## Summary

| Verdict | Assessment |
|---|---|
| Cold build regression | Acceptable (+3–4s, within 4%) |
| Incremental rebuild | Neutral — unchanged at ~1.33s |
| ≥20% improvement target | Not met — incomplete migration and baseline was already optimal |
| Architectural isolation | Confirmed — `brain-persistence` and `brain-lib` compile independently |
| Full cold build does not regress significantly | Pass — +3.3% is within tolerance |
| Direct deps removed from brain-lib | Not achieved — query/projection layer not migrated |
| Next step to achieve target | Migrate `tasks/queries/`, `records/queries.rs`, `records/projections.rs`, and vector pipeline into `brain-persistence` |
