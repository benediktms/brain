# ADR 001: Persistence Crate Extraction

**Status:** Accepted

**Date:** 2026-03-15

---

## Context

`brain_lib` accumulated too many responsibilities over time. A single crate owned:

- SQLite connection pooling, schema definitions, and migrations
- LanceDB vector store and optimize scheduler
- Arrow-format chunk indexing
- Pipeline orchestration (indexing, querying, retrieval)
- MCP server and IPC transport
- PageRank scoring and parser utilities

This created several problems:

1. **Untestable persistence** — the concrete SQLite and LanceDB implementations were entangled with application logic, making it impossible to substitute fakes in unit tests.
2. **Heavy compile-time dependency surface** — `rusqlite` (bundled), `lancedb`, and `arrow` all compiled in a single crate, increasing build friction and preventing independent versioning.
3. **Coupling** — changes to schema or migration code required recompiling all application logic, and vice versa.

---

## Decision

### 1. Create `brain_persistence` crate

A new `crates/brain_persistence/` crate was created to own all concrete persistence code:

- `db/` — SQLite connection pool, schema, migrations, meta, FTS helpers
- `store.rs` — `Store` and `StoreReader` (LanceDB + SQLite unified access)
- `error.rs` — persistence-specific error types
- `links.rs` — record link storage
- `pagerank.rs` — PageRank computation over the link graph

Heavy dependencies (`rusqlite` with bundled feature, `lancedb`, `arrow`) now live exclusively in `brain_persistence`. `brain_lib` no longer carries them.

### 2. Define six use-case-oriented port traits in `brain_lib::ports`

Six traits express what the application layer needs from the persistence layer:

| Trait | Purpose |
|---|---|
| `ChunkIndexWriter` | Write embedding vectors and chunk metadata |
| `SchemaMeta` | Read schema version and migration metadata |
| `ChunkSearcher` | Vector similarity search over chunks |
| `ChunkMetaReader` | Read chunk metadata by ID or hash |
| `FileMetaReader` | Read file-level metadata |
| `FtsSearcher` | Full-text search over notes and tasks |

Trait implementations (impl blocks on the concrete `Store`/`StoreReader` types from `brain_persistence`) live in `brain_lib::ports`, keeping the trait definitions and their concrete backing in one visible place.

### 3. Generic pipelines with default type parameters

The three main pipelines were made generic over their store type:

- `IndexPipeline<S = Store>`
- `QueryPipeline<'a, S = StoreReader>`
- `FederatedPipeline<'a, S = StoreReader>`

Generics with defaults were chosen over trait objects because all six port traits use RPITIT (return-position impl Trait in trait, stabilized in Rust 2024 edition). Trait objects cannot be constructed for traits with RPITIT methods; generics are the correct mechanism.

The `Store` and `StoreReader` types remain as the defaults, so all existing call sites compile unchanged.

### 4. Use RPITIT (native Rust async traits)

Port traits use native `async fn` in trait definitions enabled by the Rust 2024 edition. The `async_trait` procedural macro crate was not added. This avoids unnecessary indirection and keeps the trait definitions readable.

### 5. Keep `db: Db` concrete in pipelines

The raw SQLite closure pattern (`Db::with_read_conn`, `Db::with_write_conn`) is used directly in pipeline code for task and record queries that do not go through the port traits. Abstracting `Db` into a trait was deferred — the closure-based API does not compose cleanly with trait object dispatch, and the cost-benefit did not justify the complexity at this stage.

---

## Consequences

### Positive

- **Clean crate boundary** — `brain_persistence` owns all concrete persistence code. `brain_lib` owns application logic. Neither leaks into the other.
- **LanceDB layer fully mockable** — The six port traits can be implemented by test doubles, enabling unit tests for pipeline logic without a real database.
- **Independent versioning possible** — `brain_persistence` can be versioned and released separately from `brain_lib` once the boundary stabilizes.
- **Dependency isolation** — `rusqlite` (bundled), `lancedb`, and `arrow` are confined to `brain_persistence`. Crates depending only on `brain_lib` do not transitively pull in these heavy deps.

### Neutral

- **Compile times unchanged** — This refactor was evaluated as an architectural win, not a compile-time win. Incremental build times were already optimal before the extraction. Full clean-build times are similar because `brain_persistence` and `brain_lib` still compile together in the workspace.

### Negative

- **Slight complexity increase** — Generic type parameters on pipelines add surface area. Authors implementing new pipeline features must understand the port trait system and the generic bounds.

---

## Deferred

The following items were evaluated and explicitly deferred:

| Item | Reason |
|---|---|
| `TaskPersistence` / `RecordPersistence` traits | Task and record queries use complex multi-join SQL that does not map cleanly to a thin trait interface. Deferred until a clear use case for mocking emerges. |
| Full `Db` abstraction | The closure-based `with_read_conn`/`with_write_conn` pattern does not compose with trait object dispatch. Deferred pending a cleaner design. |
| Embedding crate extraction | A spike (`docs/spike-embedding-crate.md`) concluded that extracting the embedder into a separate crate provides no meaningful benefit at current scale. Rejected. |
