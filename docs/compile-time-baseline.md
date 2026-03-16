# Compile-Time Baseline

Pre-refactor compile-time measurements for the `brain` workspace.
These figures will be compared against post-refactor times after persistence-layer extraction.

## Machine Specs

| Field | Value |
|---|---|
| OS | Darwin 25.2.0 (macOS Sequoia) arm64 |
| CPU | Apple M3 Max |
| RAM | 36 GB |
| Rust | rustc 1.93.1 (01f6ddf75 2026-02-11) |
| Date | 2026-03-15 |

## Measurements

Each scenario was run once. Cold = `cargo clean` before measurement. Warm = prior build artifacts present.

| # | Scenario | Cold | Warm / Incremental |
|---|---|---|---|
| 1 | Full workspace build (`cargo build --workspace`) | 2m 02s | — |
| 2 | `brain-lib` only (`cargo build -p brain-lib`) | 1m 56s | 0.61s |
| 3 | Incremental touch (`touch crates/brain_lib/src/pipeline/mod.rs` + `cargo build -p brain-lib`) | — | 1.32s |
| 4 | Test compile (`cargo test --workspace --no-run`) | 2m 05s | — |

> **Note:** Warm build for scenario 1 and cold scenario 3 were not measured. The cold full-workspace and `brain-lib`-only times are nearly identical (6s delta), indicating the `cli` crate adds minimal compile time — the bulk is in `brain-lib` and its dependencies.

## Cargo Timings Report

Generated via `cargo build --workspace --timings` (cold build, 1m 57s).

Report location:
```
target/cargo-timings/cargo-timing-20260315T221429.127667Z.html
```

Open with any browser to inspect per-crate compile times and parallelism.

## Anomalies

- The `target` directory was absent at the start of measurement, confirming a fully cold state for all cold runs.
- `cargo clean` on the first pass removed 0 files (no prior artifacts), confirming clean state.
- `brain-lib` warm rebuild (0.61s) indicates no changes detected; incremental touch of `pipeline/mod.rs` triggered a 1.32s recompile — only `brain-lib` itself was recompiled, no dependency cascade.
- LanceDB and its transitive dependencies (`lance`, `lance-namespace-impls`) appear as the dominant compile-time contributors based on the timings report.
