# Architecture Review — 2026-03-15

**Scope**: Full codebase review of `brain` (v0.2.1, ~57k lines Rust across `brain_lib` + `cli`).

---

## Executive Summary

This is a well-engineered, thoughtfully designed system. The architecture documentation is excellent — unusually detailed for a project at this stage, with mermaid sequence diagrams, storage role separation tables, mathematical foundations, and numerical stability notes. The implementation faithfully follows the documented architecture with very few deviations.

**Overall assessment**: Production-quality for a local-first developer tool. The codebase demonstrates strong Rust fundamentals, clear separation of concerns, and disciplined handling of the hard problems (dual-store consistency, event sourcing, hybrid ranking, token budgeting).

**Verdict**: Ship-ready for the current phase. The issues identified below are refinements and forward-looking concerns, not blockers.

---

## Strengths

### 1. Architecture Design (Excellent)

- **Dual-store with clear role separation**: SQLite as authoritative runtime state, LanceDB as derived vector index. The decision to make LanceDB failures non-fatal while SQLite failures are hard errors is exactly right for a local-first system.
- **Indexing state machine** (idle → indexing_started → sqlite_written → indexed) provides crash-safe consistency. If the process dies mid-index, the hash gate catches it on the next scan.
- **Progressive retrieval** (search_minimal → expand) is the killer feature. Token budgeting as a first-class API constraint is a genuinely novel design choice among personal knowledge tools.
- **Multi-brain architecture**: The unified SQLite with `brain_id` partitioning is cleaner than per-brain databases. Per-brain LanceDB instances for independent semantic spaces is the right call.

### 2. Implementation Quality (Strong)

- **Embedding pipeline**: BLAKE3 checksum verification on model load, L2 normalization with epsilon clamping (`1e-12`), debug assertions on unit vector output, `spawn_blocking` for CPU-bound inference. All the right safety measures.
- **SQLite connection pool**: Separate writer mutex + round-robin reader pool with `SQLITE_OPEN_READ_ONLY` + `query_only` pragma. WAL mode enables concurrent reads during writes. The `busy_timeout` of 5s prevents immediate failures under contention.
- **Hash gate**: Content-hash-based change detection with BLAKE3 avoids redundant re-indexing. The chunker version tracking enables lazy re-chunking when the algorithm changes.
- **Work queue**: Bounded queue with last-write-wins dedup and oldest-entry eviction under backpressure. Simple, correct, and well-tested.
- **OptimizeScheduler**: Dual-trigger (row count OR elapsed time) with atomic counters for lock-free mutation tracking. `try_lock` for non-blocking maybe_optimize, blocking `lock` for force_optimize. Clean design.

### 3. Test Infrastructure (Comprehensive)

- **~7k lines of integration tests** across 11 suites, plus unit tests in nearly every module.
- **MockEmbedder** using BLAKE3-based deterministic vectors enables CI without model weights — a pragmatic solution.
- **Golden file tests** for chunker output, link extraction, hash determinism, and intent ranking.
- **Property-based testing** with proptest for randomized input validation.
- **Crash recovery tests** in the records domain (partial writes, projection rebuild).
- **Concurrency tests** for batch equivalence, backpressure, and concurrent R/W.
- **Criterion benchmarks** for indexing, querying, embedding, and IVF-PQ recall.

### 4. Error Handling (Good)

- `BrainCoreError` enum with `thiserror` provides typed errors for each domain (Embedding, VectorDb, Database, TaskEvent, TaskCycle, RecordEvent, ObjectStore).
- SQLite-first write semantics: tasks and records write to SQLite, then emit to JSONL audit logs as best-effort. JSONL failures are warnings, not errors.
- FTS search failures in `QueryPipeline::search_ranked` gracefully degrade to vector-only results.
- LanceDB `merge_insert` failures in `OptimizeScheduler` don't subtract from the pending counter, ensuring retry on the next trigger.

### 5. Security (Adequate)

- `validate_file_id` prevents SQL injection in LanceDB filter expressions by whitelisting `[a-zA-Z0-9-:]`.
- Model files are verified via BLAKE3 checksums before mmap loading.
- `fs_permissions` module checks `~/.brain` directory permissions.

---

## Issues and Recommendations

### Critical (Fix Before Next Major Release)

**None identified.** The system is sound for its intended use case.

### High Priority

#### H1. Error type erases source context

`BrainCoreError` variants like `Database(String)` and `VectorDb(String)` convert errors to strings, losing the original error type and backtrace. This makes debugging harder in production.

**Recommendation**: For the SQLite path, consider preserving the `rusqlite::Error` source:
```rust
#[error("database error: {0}")]
Database(#[from] rusqlite::Error),
```
Or at minimum, use `#[source]` with a boxed error for domains where multiple error sources exist.

#### H2. LanceDB filter expression injection surface

`validate_file_id` whitelists `[a-zA-Z0-9-:]`, which is safe today. However, `update_file_path` in `store.rs:461` constructs a filter with `new_path.replace('\'', "''")` — a manual SQL-escaping approach. If LanceDB's filter language ever changes, this becomes a vulnerability.

**Recommendation**: Audit all `only_if` / `delete` filter string constructions in `store.rs` and verify LanceDB's API doesn't offer parameterized filters. If not, add a `validate_file_path` function with the same defensive approach.

#### H3. MockEmbedder produces low-entropy vectors

The mock embedding cycles 32 BLAKE3 bytes across 384 dimensions (`bytes[i % 32]`). This means dimensions 0 and 32 are identical, as are 1 and 33, etc. This creates artificial structure that real embeddings don't have.

**Impact**: Test results for ranking and retrieval may not reflect production behavior. The golden tests partially mitigate this, but any test relying on semantic similarity properties will be unreliable.

**Recommendation**: Use `blake3::Hasher::new_keyed` with the dimension index as additional entropy, or hash `(text, dimension_bucket)` to reduce periodicity.

### Medium Priority

#### M1. Schema migration lacks transactional guarantees per-step

Each `migrate_vN_to_vN+1` function runs inside the shared connection but there's no explicit transaction wrapping individual migrations in `run_migrations`. If a migration partially succeeds (e.g., creates a table but fails to add an index), the database is left in an inconsistent state with the version stamp not yet advanced.

**Recommendation**: Wrap each migration step in a transaction:
```rust
conn.execute_batch("BEGIN")?;
migrate_fn(conn)?;
conn.pragma_update(None, "user_version", version + 1)?;
conn.execute_batch("COMMIT")?;
```
This is partially mitigated by the fact that most migrations are DDL (which in SQLite auto-commits), but DML-heavy migrations (like data backfills) would benefit.

#### M2. Federated search re-embeds the query per-brain

`FederatedPipeline::search` creates a `QueryPipeline` per brain and calls `search_ranked`, which calls `embed_batch_async` for each brain. Since all brains share the same embedder and the query vector is identical, this is redundant work.

**Recommendation**: Embed the query once before the fan-out loop and pass the pre-computed vector into a variant of `search_ranked` that accepts a vector directly. This is a straightforward optimization that eliminates `N-1` redundant embedding calls for N brains.

#### M3. Records module is large and could benefit from further decomposition

`records/mod.rs` is substantial, containing `RecordStore`, domain types, all the `RecordStore` methods, and rewrite logic. The events/projections/queries split is good, but the top-level module file is doing a lot.

**Recommendation**: Consider extracting domain types (`RecordId`, `RecordKind`, `RecordDomain`, `RetentionClass`, etc.) into a `records/types.rs` module.

#### M4. `TaskStore` list methods have repetitive brain_id scoping

Every `list_*` method in `TaskStore` follows the same pattern:
```rust
let brain_id = self.brain_id.clone();
self.db.with_read_conn(move |conn| {
    let filter = if brain_id.is_empty() { None } else { Some(brain_id.as_str()) };
    queries::list_xyz(conn, filter)
})
```

**Recommendation**: Extract a helper:
```rust
fn with_brain_filter<T>(&self, f: impl FnOnce(&Connection, Option<&str>) -> Result<T>) -> Result<T>
```

#### M5. `from_stores` takes too many arguments

`McpContext::from_stores` has 11 parameters. This is a code smell even with `#[allow(clippy::too_many_arguments)]`.

**Recommendation**: Introduce a builder or a `McpContextConfig` struct to group the initialization parameters.

### Low Priority / Future Considerations

#### L1. No rate limiting on MCP tool calls

The MCP server processes requests synchronously on stdin. A misbehaving or compromised agent could flood the server with requests. For a local-first tool this is low risk, but worth noting.

#### L2. PageRank computation runs synchronously in optimize

`compute_and_store_pagerank` runs inside `run_optimize` via `db.with_write_conn`. For large vaults, this could block the writer mutex during compaction.

**Recommendation**: Consider running PageRank asynchronously via `spawn_blocking` and writing results in a separate transaction.

#### L3. Event log can grow unbounded

The JSONL audit trails (`.brain/tasks/events.jsonl` and per-brain record event logs) are append-only with no compaction or rotation.

**Recommendation**: Add optional log rotation or compaction (snapshot + truncate) as the event count grows, perhaps tied to the `vacuum` command.

#### L4. Cross-encoder reranker trait exists but has no implementation

The `Reranker` trait and `RerankerPolicy` are well-designed but there's no concrete implementation in the codebase yet. The ARCHITECTURE.md mentions ONNX Runtime via `ort` crate as post-v1.

**Recommendation**: Track this as a Phase 5 item. The trait interface is clean and ready for implementation.

#### L5. `Db::open_in_memory` for tests doesn't match production read pool behavior

In-memory databases have 0 readers and fall back to the writer connection for reads. This means tests don't exercise the reader pool code path. The `test_reader_pool_round_robins` test uses an on-disk DB which helps, but most integration tests use in-memory.

---

## Architecture Documentation Accuracy

The ARCHITECTURE.md is remarkably accurate relative to the implementation. Verified claims:

| Claim | Status |
|-------|--------|
| Dual-store (SQLite + LanceDB) | Confirmed |
| Hash gate with BLAKE3 | Confirmed |
| CLS pooling with L2 normalization | Confirmed, including epsilon clamp |
| Six-signal hybrid scoring | Confirmed, all signals implemented |
| Intent-driven weight profiles (5 profiles) | Confirmed, weights sum to 1.0 |
| Token-budgeted progressive retrieval | Confirmed |
| WAL mode with concurrent readers | Confirmed, 4-reader pool |
| Event-sourced tasks with JSONL audit | Confirmed, SQLite-first writes |
| Bounded work queue with backpressure | Confirmed, 1024 capacity default |
| IVF-PQ auto-index at 256+ rows | Confirmed |
| Graceful shutdown with queue drain | Described in docs, implementation in watch.rs |

**Minor discrepancy**: The ARCHITECTURE.md mermaid diagram still shows "Task Event Log (Source of Truth)" in the storage diagram, but the text correctly states SQLite is the source of truth and the event log is a best-effort audit trail. The diagram label should be updated to "Task Event Log (Audit Trail)" for consistency.

---

## Performance Design Review

The performance design is well-reasoned:

- **Embedding**: Sub-batching at 32 chunks bounds memory, `spawn_blocking` avoids blocking tokio runtime. Candle SIMD opt-level in dev profile is a smart dev-experience optimization.
- **LanceDB compaction**: Dual-trigger (200 rows OR 5 minutes) prevents both unbounded fragment accumulation and excessive compaction during quiet periods.
- **Debounce**: 250ms file watcher debounce is a good balance between responsiveness and event storm mitigation.
- **Capsule generation**: Deterministic, zero-ML-cost capsules at ingest time (heading hierarchy + first sentence) is the right trade-off. ML summarization is deferred to consolidation.
- **Token estimation**: Simple `chars / 4` heuristic (`tokens.rs`) is appropriate for budget estimation — precise tokenization would be too slow for the hot path.

**Target latencies** from ARCHITECTURE.md appear achievable given the implementation choices.

---

## Recommendations Summary

| Priority | ID | Issue | Effort |
|----------|----|-------|--------|
| High | H1 | Preserve error source context | S |
| High | H2 | Audit LanceDB filter construction | S |
| High | H3 | Improve MockEmbedder entropy | S |
| Medium | M1 | Transactional migration steps | M |
| Medium | M2 | Embed query once in federated search | S |
| Medium | M3 | Decompose records module | S |
| Medium | M4 | Extract brain_id scoping helper | S |
| Medium | M5 | Builder pattern for McpContext | M |
| Low | L1 | MCP rate limiting | L |
| Low | L2 | Async PageRank computation | M |
| Low | L3 | Event log rotation | M |
| Low | L4 | Implement cross-encoder reranker | L |
| Low | L5 | Test reader pool coverage | S |

**S** = Small (< 1 hour), **M** = Medium (1–4 hours), **L** = Large (> 4 hours)

---

## Conclusion

`brain` is a mature, well-architected system that solves a real problem (token-budgeted retrieval for AI agents) with a sound technical approach. The Rust implementation is clean, safe, and well-tested. The documentation is among the best I've seen for a project at this stage.

The main areas for improvement are incremental: error context preservation, a few API ergonomic cleanups, and the federated query embedding optimization. The architectural foundations are solid and the planned roadmap (episodic memory completion, semantic search for records, consolidation) builds naturally on the existing design.
