# ADR-003: Persistence Crate Extraction — Compile-Time Impact

**Decision: Accept extraction despite not meeting the 20% incremental rebuild target.**

The `brain-persistence` extraction achieved architectural isolation (persistence and pipeline modules compile independently) but did not measurably improve incremental build times. Cold builds regressed by ~3% (acceptable). The incomplete migration of the query/projection layer leaves `rusqlite` and `lancedb` as direct `brain-lib` deps.

---

## Baseline (Pre-Refactor) — 2026-03-15

Machine: Apple M3 Max, 36 GB RAM, rustc 1.93.1, macOS Sequoia arm64.

| # | Scenario | Cold | Warm |
|---|---|---|---|
| 1 | Full workspace build | 2m 02s | — |
| 2 | `brain-lib` only | 1m 56s | 0.61s |
| 3 | Incremental touch (`pipeline/mod.rs`) | — | 1.32s |
| 4 | Test compile | 2m 05s | — |

LanceDB and transitive deps (`lance`, `lance-namespace-impls`) dominate cold build time.

## Post-Refactor — 2026-03-16

| # | Scenario | Baseline | Post-refactor | Delta | % |
|---|---|---|---|---|---|
| 1 | Cold full workspace | 2m 02s | 2m 06s | +4s | +3.3% |
| 2a | Cold `brain-lib` only | 1m 56s | 1m 59s | +3s | +2.6% |
| 2b | Warm `brain-lib` | 0.61s | 0.78s | +0.17s | noise |
| 3 | Incremental touch | 1.32s | 1.33s | +0.01s | +0.8% |
| 4 | Cold test compile | 2m 05s | 2m 25s | +20s | +16.0% |

## Why the 20% Target Was Not Met

1. **Incomplete migration.** Query/projection layer (`tasks/queries/`, `records/queries.rs`) still uses `rusqlite::Connection` directly in 19 files, keeping `rusqlite` as a direct `brain-lib` dep.
2. **Baseline was already optimal.** The 1.32s incremental rebuild reflected `brain-lib` recompiling only itself. No unnecessary dependency cascade existed to eliminate.

## What Would Achieve the Target

1. Migrate query/projection layer to `brain-persistence` (remove `rusqlite` from `brain-lib`)
2. Migrate arrow/vector pipeline code to `brain-persistence` (remove `lancedb`, `arrow-*` from `brain-lib`)

## Verdict

| Factor | Assessment |
|---|---|
| Cold build regression | Acceptable (+3-4s, within 4%) |
| Incremental rebuild | Unchanged at ~1.33s |
| 20% improvement target | Not met |
| Architectural isolation | Achieved |
