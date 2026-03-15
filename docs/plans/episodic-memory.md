---
status: proposed
complexity: large
effort: ~2 weeks
priority: high
---

# Episodic Memory Foundation

**Goal**: Close the episode write → retrieve → reflect → retrieve loop.

## Context

The episodic memory system is **partially implemented**. Episodes go in (`memory.write_episode`) but never come back out through the main retrieval path (`memory.search_minimal`). Reflections can't be stored via MCP at all — `store_reflection()` exists in `db/summaries.rs` but is unreachable from any tool.

This blocks later roadmap phases (URIs, hierarchy summaries, explainability, consolidation, playbooks) because they depend on summaries being first-class retrievable objects.

---

## Architecture Inconsistencies to Fix

1. **`summaries` is the only domain table without `brain_id`** — tasks, records, record_events all have it. This breaks multi-brain isolation.
2. **`store_episode()` and `store_reflection()` have no `brain_id` parameter** — they write unscoped rows.
3. **`list_episodes()` has no `brain_id` filter** — returns episodes from ALL brains.
4. **`reflect` pipeline hardcodes `list_episodes(conn, 10)`** — should be brain-scoped by default but support cross-brain reflection.
5. **`derive_kind()` in `retrieval.rs` only knows "task:", "task-outcome:", "note"** — no "episode" or "reflection" kinds.
6. **`expand` only handles chunk IDs** — `get_chunks_by_ids()` doesn't know about summaries.

---

## Cross-Brain Reflection Design

Reflections often synthesize knowledge from cross-brain tasks (e.g. a reflection about "deployment patterns" referencing episodes from both the infra brain and the app brain). The design must support this.

### Principles

1. **Episodes are written to the current brain** — `memory.write_episode` uses `ctx.brain_id`, because episodes are records of work done in a specific brain context.
2. **Reflections are stored in the current brain** — the reflection itself belongs to the brain where the agent is working. Its `brain_id` is the current context.
3. **Source IDs can reference episodes from any brain** — `reflection_sources` links are by `summary_id` (globally unique ULIDs), not scoped by brain_id. An episode in brain A can be a source for a reflection in brain B.
4. **Prepare mode gathers cross-brain material** — `memory.reflect(mode="prepare")` should accept an optional `brains` parameter (same pattern as `search_minimal`) to pull episodes from multiple brains.
5. **Search finds reflections in the current brain** — `search_minimal` with `brains=["all"]` already handles cross-brain search. Reflections are found via their own brain_id, not their sources' brain_ids.

### Implementation Implications

- **`store_reflection()` source_id validation**: Do NOT filter by brain_id when validating `source_ids`. Use `get_summary(conn, id)` which is a PK lookup — it returns the row regardless of brain_id. This allows cross-brain references.
- **`list_episodes()` in prepare mode**: Accept an optional `brain_id` filter. When `brains` parameter is passed in prepare mode, query with `brain_id IN (...)` or no filter (for "all"). When omitted, scope to current brain (default behavior).
- **`reflection_sources` table**: No schema change needed — it already stores `(reflection_id, source_id)` without brain scoping. The FK references `summaries(summary_id)` which is globally unique.
- **Search/retrieval**: Summary candidates from LanceDB are enriched from SQLite. Brain_id filtering happens at enrichment time for single-brain search, but the `brains` parameter on `search_minimal` already handles cross-brain fan-out.

### `memory.reflect` prepare mode with cross-brain support

```rust
// In prepare mode Params:
brains: Option<Vec<String>>,  // optional, same as search_minimal

// In prepare execution:
let episodes = if let Some(ref brains) = params.brains {
    // Cross-brain: gather episodes from specified brains
    if brains.iter().any(|b| b == "all") {
        list_episodes(conn, limit, "")  // empty brain_id = all brains
    } else {
        list_episodes_multi_brain(conn, limit, &brain_ids)
    }
} else {
    // Default: current brain only
    list_episodes(conn, limit, &ctx.brain_id)
};
```

### `list_episodes()` API evolution

```rust
// Current brain only (default for most callers):
pub fn list_episodes(conn: &Connection, limit: usize, brain_id: &str) -> Result<Vec<SummaryRow>>

// Cross-brain (for reflect prepare with brains param):
pub fn list_episodes_multi_brain(conn: &Connection, limit: usize, brain_ids: &[String]) -> Result<Vec<SummaryRow>>
// SQL: WHERE brain_id IN (...) or WHERE 1=1 for "all"
```

---

## Implementation Steps (6 issues, strict dependency order)

### Issue 0.1: Add `brain_id` to `summaries` table

**Schema migration v21→v22.**

#### Files to modify:
- `crates/brain_lib/src/db/migrations/v21_to_v22.rs` — **new file**
- `crates/brain_lib/src/db/migrations/mod.rs` — add module + re-export
- `crates/brain_lib/src/db/schema.rs` — bump `SCHEMA_VERSION` to 22, add match arm, import
- `crates/brain_lib/src/db/summaries.rs` — add `brain_id` param to all write/query functions
- `crates/brain_lib/src/mcp/mod.rs` — add summaries to `backfill_brain_id()`
- `crates/brain_lib/src/mcp/tools/mem_write_episode.rs` — pass `ctx.brain_id`
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs` — pass `ctx.brain_id` to `list_episodes()`
- `crates/brain_lib/src/query_pipeline.rs` — pass `brain_id` to `list_episodes()` in `reflect()`

#### Migration DDL:
```sql
PRAGMA foreign_keys = OFF;
BEGIN;
ALTER TABLE summaries ADD COLUMN brain_id TEXT NOT NULL DEFAULT '';
PRAGMA user_version = 22;
COMMIT;
PRAGMA foreign_keys = ON;
```

#### Changes to `summaries.rs`:
- `store_episode(conn, episode, brain_id: &str)` — add `brain_id` to INSERT
- `store_reflection(conn, ..., brain_id: &str)` — add `brain_id` to INSERT
- `list_episodes(conn, limit, brain_id: &str)` — add `WHERE brain_id = ?` when brain_id is non-empty; empty string = all brains (backward compat)
- `list_episodes_multi_brain(conn, limit, brain_ids: &[String])` — **new** — `WHERE brain_id IN (...)` for cross-brain reflection prepare
- `get_summary(conn, summary_id)` — **no brain_id filter** (lookup by PK). This is intentional: summaries can be referenced cross-brain via `reflection_sources`
- `store_ml_summary()` — no change needed (these are chunk-scoped, not brain-scoped)

#### Changes to `backfill_brain_id()` in `mcp/mod.rs`:
Add after the existing record_events backfill:
```rust
let summaries_updated = conn.execute(
    "UPDATE summaries SET brain_id = ?1 WHERE brain_id = ''",
    rusqlite::params![brain_id],
)?;
```

#### Tests:
- Migration test: setup v21 schema, run migration, verify column exists
- Backfill test: insert episodes with empty brain_id, backfill, verify
- Multi-brain isolation test: two brain_ids, verify list_episodes filters correctly

---

### Issue 0.2: Complete `memory.reflect` storage phase

**Decision: Add `mode` parameter to existing `memory.reflect` tool** (not a separate tool). This keeps tool count at 25 and matches the two-phase workflow described in the tool's existing description.

#### Files to modify:
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs` — add commit mode + cross-brain prepare support
- `crates/brain_lib/src/db/summaries.rs` — `store_reflection()` already exists, needs brain_id (from 0.1); add `list_episodes_multi_brain()`
- `crates/brain_lib/src/query_pipeline.rs` — update `reflect()` to accept brain_id + optional brains list

#### Changes to `mem_reflect.rs`:

Update `Params` struct:
```rust
#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_mode")]
    mode: String,         // "prepare" or "commit"
    // prepare mode fields:
    topic: Option<String>,
    budget_tokens: Option<u64>,
    #[serde(default)]
    brains: Vec<String>,  // cross-brain support (same pattern as search_minimal)
    // commit mode fields:
    title: Option<String>,
    content: Option<String>,
    source_ids: Option<Vec<String>>,  // may reference episodes from ANY brain
    tags: Option<Vec<String>>,
    importance: Option<f64>,
}
fn default_mode() -> String { "prepare".into() }
```

In `call()`, dispatch on `mode`:
- `"prepare"` — existing behavior (require `topic`). If `brains` is non-empty, gather episodes from specified brains (uses `list_episodes_multi_brain()`). If empty, scope to `ctx.brain_id` (default). The hybrid search half already supports cross-brain via the existing federated pipeline.
- `"commit"` — require `title`, `content`, `source_ids`; call `store_reflection()` with `ctx.brain_id` (reflection stored in current brain); validate source_ids exist via `get_summary()` (PK lookup, no brain_id filter — allows cross-brain references); return `{ "status": "stored", "summary_id": "..." }`

**Cross-brain source validation**: `get_summary(conn, id)` looks up by primary key without brain_id filtering. This is intentional — a reflection in brain B must be able to reference an episode in brain A. The `reflection_sources` junction table stores `(reflection_id, source_id)` as globally unique ULID pairs.

Update tool definition `input_schema` to document both modes and the `brains` parameter.

#### Tests:
- Commit mode: prepare reflection, then commit it, verify stored in summaries with kind='reflection'
- Source validation: commit with invalid source_ids → error
- Cross-brain commit: create episode in brain A, commit reflection in brain B referencing it → succeeds, reflection_sources populated
- Prepare mode: existing test continues to pass unchanged
- Prepare mode with brains: verify episodes from multiple brains returned

---

### Issue 0.3: FTS5 indexing for summaries

**Follow the existing `fts_chunks` / `fts_tasks` pattern exactly.**

#### Files to modify:
- `crates/brain_lib/src/db/schema.rs` — add `fts_summaries` + triggers in `ensure_fts5()`
- `crates/brain_lib/src/db/fts.rs` — add `search_summaries_fts()` function

#### Add to `ensure_fts5()`:
```sql
CREATE VIRTUAL TABLE IF NOT EXISTS fts_summaries USING fts5(
    title, content,
    content=summaries,
    content_rowid=rowid,
    tokenize='porter unicode61'
);

-- INSERT trigger
CREATE TRIGGER IF NOT EXISTS summaries_fts_insert AFTER INSERT ON summaries BEGIN
    INSERT INTO fts_summaries(rowid, title, content)
    VALUES (new.rowid, COALESCE(new.title, ''), new.content);
END;

-- DELETE trigger
CREATE TRIGGER IF NOT EXISTS summaries_fts_delete AFTER DELETE ON summaries BEGIN
    INSERT INTO fts_summaries(fts_summaries, rowid, title, content)
    VALUES ('delete', old.rowid, COALESCE(old.title, ''), old.content);
END;

-- UPDATE trigger (on title or content change)
CREATE TRIGGER IF NOT EXISTS summaries_fts_update AFTER UPDATE OF title, content ON summaries BEGIN
    INSERT INTO fts_summaries(fts_summaries, rowid, title, content)
    VALUES ('delete', old.rowid, COALESCE(old.title, ''), old.content);
    INSERT INTO fts_summaries(rowid, title, content)
    VALUES (new.rowid, COALESCE(new.title, ''), new.content);
END;
```

Using `tokenize='porter unicode61'` for stemming. The existing `fts_chunks` specifies no `tokenize` clause (SQLite defaults to `unicode61` without stemming). Summaries benefit from porter stemming since episode content is prose.

#### Add `search_summaries_fts()` to `fts.rs`:
```rust
pub fn search_summaries_fts(conn: &Connection, query: &str, limit: usize) -> Result<Vec<FtsSummaryResult>> {
    // Same pattern as search_fts() but joins summaries instead of chunks
    // Returns summary_id + normalized BM25 score
    // SQL: SELECT s.summary_id, -bm25(fts_summaries) AS score
    //      FROM fts_summaries JOIN summaries s ON s.rowid = fts_summaries.rowid
    //      WHERE fts_summaries MATCH ?1 ORDER BY score DESC LIMIT ?2
}
```

New struct:
```rust
pub struct FtsSummaryResult {
    pub summary_id: String,
    pub score: f64,
}
```

Also add `reindex_summaries_fts()` for doctor/repair (mirrors `reindex_fts()`).

#### Tests:
- Insert episode → FTS trigger fires → search finds it
- Delete summary → FTS cleaned up
- BM25 scores normalized to [0,1]

---

### Issue 0.4: LanceDB vector indexing for summaries

**Decision: Add a new `Store::upsert_summary()` method** rather than reusing `upsert_chunks()`. Reason: `upsert_chunks()` uses file_id-scoped orphan cleanup (`when_not_matched_by_source_delete` with file_id filter) which doesn't apply to summaries. Summaries use `sum:{summary_id}` as chunk_id with a synthetic file_id of `"__summaries__"`.

#### Files to modify:
- `crates/brain_lib/src/store.rs` — add `upsert_summary()` and `delete_summary()`
- `crates/brain_lib/src/mcp/tools/mem_write_episode.rs` — embed + upsert after SQLite store
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs` — embed + upsert in commit mode

#### New methods on `Store`:

```rust
/// Upsert a single summary embedding into LanceDB.
/// Uses chunk_id = "sum:{summary_id}", file_id = "__summaries__".
pub async fn upsert_summary(
    &self,
    summary_id: &str,
    content: &str,
    embedding: &[f32],
) -> Result<()> {
    let chunk_id = format!("sum:{summary_id}");
    let schema = chunks_schema();
    let batch = make_summary_record_batch(&schema, &chunk_id, content, embedding)?;
    let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));

    let mut builder = self.table.merge_insert(&["chunk_id"]);
    builder.when_matched_update_all(None).when_not_matched_insert_all();
    builder.execute(Box::new(batches)).await?;

    self.optimize_scheduler.record_mutation(1);
    Ok(())
}

/// Delete a summary's embedding from LanceDB.
pub async fn delete_summary(&self, summary_id: &str) -> Result<()> {
    let chunk_id = format!("sum:{summary_id}");
    self.table.delete(&format!("chunk_id = '{chunk_id}'")).await?;
    self.optimize_scheduler.record_mutation(1);
    Ok(())
}
```

Uses file_id = `"__summaries__"`, file_path = `""`, chunk_ord = `0`.

#### Changes to `mem_write_episode.rs`:

After `store_episode()` succeeds, embed + upsert (best-effort — failure logged, not returned):
```rust
// Embed and index in LanceDB (best-effort)
if let Some(ref embedder) = ctx.embedder {
    if let Some(ref store) = ctx.writable_store {
        let text = format!("{}\n\n{}", params.goal, content);
        match crate::embedder::embed_batch_async(embedder, vec![text]).await {
            Ok(vecs) if !vecs.is_empty() => {
                if let Err(e) = store.upsert_summary(&summary_id, &content, &vecs[0]).await {
                    warn!(error = %e, "failed to index episode in LanceDB");
                }
            }
            _ => {}
        }
    }
}
```

#### Changes to `mem_reflect.rs` (commit mode):

Same pattern — embed the reflection content after storing.

#### Tests:
- Write episode → verify `sum:{id}` exists in LanceDB via vector search
- Write reflection → verify indexed
- MockEmbedder works for all tests (deterministic embeddings)

---

### Issue 0.5: Integrate summaries into `search_minimal` hybrid pipeline

**This is the largest change.** Summary candidates must enter the unified candidate pool alongside note chunks.

#### Files to modify:
- `crates/brain_lib/src/query_pipeline.rs` — merge summary candidates into search_ranked()
- `crates/brain_lib/src/retrieval.rs` — update `derive_kind()`, add summary expand support
- `crates/brain_lib/src/db/fts.rs` — already done in 0.3
- `crates/brain_lib/src/mcp/tools/mem_expand.rs` — handle `sum:` prefix IDs
- `crates/brain_lib/src/mcp/tools/mem_search_minimal.rs` — update tool description to mention episode/reflection kinds

#### Changes to `query_pipeline.rs` — `search_ranked()`:

After the existing vector + FTS union for chunks, add a parallel path for summaries:

```rust
// 3b. FTS search over summaries (best-effort, parallel to chunk FTS)
let summary_fts_results = match self.db.with_read_conn(|conn| {
    crate::db::fts::search_summaries_fts(conn, query, CANDIDATE_LIMIT)
}) {
    Ok(r) => r,
    Err(e) => {
        warn!(error = %e, "Summary FTS search failed");
        Vec::new()
    }
};
```

Vector search results with `sum:` prefix are already returned by LanceDB (since we upsert with that prefix). They need to be enriched differently:

```rust
// 5b. Enrich summary candidates from summaries table (not chunks)
let summary_ids: Vec<String> = candidates.keys()
    .filter(|id| id.starts_with("sum:"))
    .cloned()
    .collect();

if !summary_ids.is_empty() {
    let enrichment = self.db.with_read_conn(|conn| {
        get_summaries_by_ids(conn, &summary_ids)  // new function in summaries.rs
    });
    if let Ok(rows) = enrichment {
        for row in &rows {
            let key = format!("sum:{}", row.summary_id);
            if let Some(candidate) = candidates.get_mut(&key) {
                candidate.file_path = String::new(); // summaries have no file
                candidate.heading_path = row.title.clone().unwrap_or_default();
                candidate.content = row.content.clone();
                candidate.token_estimate = estimate_tokens(&row.content);
                candidate.importance = row.importance;
                candidate.tags = row.tags.clone();
                candidate.age_seconds = (now - row.created_at).max(0) as f64;
                // Note: pagerank_score defaults to 0.0 for summaries.
                // The existing signal #4 is a pre-normalized PageRank from
                // the files table — summaries are not files and have no
                // PageRank. See the semantic-search-records plan for a
                // discussion of options (default 0.0, derive from
                // reflection_sources, or extend PageRank graph).
            }
        }
    }
}
```

Add summary FTS candidates to the union (same pattern as chunk FTS):
```rust
for fr in &summary_fts_results {
    let key = format!("sum:{}", fr.summary_id);
    if let Some(existing) = candidates.get_mut(&key) {
        existing.bm25 = fr.score;
    } else {
        candidates.insert(key.clone(), CandidateSignals {
            chunk_id: key,
            bm25: fr.score,
            // ... zero/default for other signals
        });
    }
}
```

**New function in `summaries.rs`:**
```rust
/// Batch-load summaries by their prefixed IDs (e.g. "sum:01ABC").
/// Strips the "sum:" prefix before querying.
pub fn get_summaries_by_prefixed_ids(conn: &Connection, prefixed_ids: &[String]) -> Result<Vec<SummaryRow>> {
    // Strip "sum:" prefix, SELECT WHERE summary_id IN (...)
}
```

#### Changes to `retrieval.rs`:

Update `derive_kind()`:
```rust
fn derive_kind(chunk_id: &str) -> String {
    if chunk_id.starts_with("sum:") {
        // Need to distinguish episode vs reflection — check against a lookup
        // or encode kind in the chunk_id prefix
        "episode".to_string()  // default; refined below
    } else if chunk_id.starts_with("task-outcome:") {
        "task-outcome".to_string()
    } else if chunk_id.starts_with("task:") {
        "task".to_string()
    } else {
        "note".to_string()
    }
}
```

**Design decision for kind detection**: We need to know if a `sum:` ID is an episode or reflection for the stub kind. Two options:
1. Use different prefixes: `ep:` for episodes, `ref:` for reflections
2. Keep `sum:` and look up the kind during enrichment

**Recommendation: Use `sum:` prefix uniformly** and pass the kind through enrichment. Add a `kind` field to `CandidateSignals` (or derive it during stub creation from the enriched data). The enrichment step already loads the `SummaryRow` which has `kind`. Store it on the candidate and propagate to `make_stub()`.

This requires adding an optional `summary_kind: Option<String>` to `CandidateSignals` and using it in `make_stub()` when the chunk_id starts with `sum:`.

#### Changes to `mem_expand.rs` — expand for summary IDs:

In `QueryPipeline::expand()`, before looking up chunks, partition IDs:

```rust
pub async fn expand(&self, memory_ids: &[String], budget_tokens: usize) -> Result<ExpandResult> {
    let (summary_ids, chunk_ids): (Vec<_>, Vec<_>) = memory_ids
        .iter()
        .partition(|id| id.starts_with("sum:"));

    // Fetch chunks as before
    let chunk_rows = self.db.with_read_conn(|conn| get_chunks_by_ids(conn, &chunk_ids))?;

    // Fetch summaries
    let summary_rows = if !summary_ids.is_empty() {
        self.db.with_read_conn(|conn| get_summaries_by_prefixed_ids(conn, &summary_ids))?
    } else {
        vec![]
    };

    // Convert both to ExpandableChunk, merge preserving original order
    // ...
}
```

#### Changes to `mem_search_minimal.rs`:

Update tool description to mention "episode" and "reflection" as possible `kind` values:
```
"Results include note chunks, task capsules, episodes, and reflections. Each result carries a `kind` field: \"note\", \"task\", \"task-outcome\", \"episode\", or \"reflection\"."
```

#### Tests:
- Write episode → search_minimal finds it with kind="episode"
- Write reflection → search_minimal finds it with kind="reflection"
- Expand with `sum:` ID returns full summary content
- Mixed results (chunks + episodes) correctly ranked and packed

---

### Issue 0.6: Backfill, migration, and test coverage

#### Files to modify:
- `crates/brain_lib/src/pipeline/maintenance.rs` or new `db/summary_backfill.rs` — backfill logic
- `crates/cli/src/commands/reindex.rs` — add `--summaries` flag or auto-detect
- Integration test files

#### Backfill strategy:

**CLI-only** via `brain reindex --summaries` (no auto-startup cost):

The backfill function (invoked from reindex command):
1. `SELECT summary_id, title, content FROM summaries WHERE kind IN ('episode','reflection')`
2. For each, check if `sum:{summary_id}` exists in LanceDB (batch query by file_id = `__summaries__`)
3. Batch-embed all unindexed content
4. Upsert into LanceDB
5. Log count: `info!(count, "backfilled summary embeddings")`

FTS backfill happens automatically — `ensure_fts5()` creates the table/triggers on every startup (idempotent), and a one-time `INSERT INTO fts_summaries(fts_summaries) VALUES('rebuild')` in the reindex command repopulates from the summaries table.

New episodes/reflections written after this plan's deployment get embedded at write time (Issue 0.4). The reindex command is only needed for episodes that existed before the migration.

#### Integration tests to add:

1. **Episode round-trip**: `write_episode` → `search_minimal("goal text")` → result has kind="episode" and matching summary_id
2. **Reflect prepare→commit→search**: `reflect(mode="prepare")` → `reflect(mode="commit")` → `search_minimal` finds the reflection
3. **Reflection source linking**: commit reflection with source_ids → verify `reflection_sources` populated
4. **Multi-brain isolation**: two brain_ids → episodes from brain A invisible in brain B's default search
5. **Cross-brain reflection**: episode in brain A → reflection in brain B with source_id referencing brain A's episode → `reflection_sources` populated, both discoverable via respective brain searches
6. **Cross-brain prepare**: `reflect(mode="prepare", brains=["all"])` → returns episodes from multiple brains
7. **Expand for summaries**: `expand(memory_ids=["sum:..."])` → returns full episode content
8. **FTS for summaries**: keyword search finds episode by keyword in content
9. **Mixed ranking**: episodes ranked alongside note chunks by hybrid score

---

## Files Changed (complete list)

| File | Change | Issue |
|------|--------|-------|
| `crates/brain_lib/src/db/migrations/v21_to_v22.rs` | **New** — migration adding brain_id to summaries | 0.1 |
| `crates/brain_lib/src/db/migrations/mod.rs` | Add module + re-export | 0.1 |
| `crates/brain_lib/src/db/schema.rs` | Bump SCHEMA_VERSION=22, add match arm, add fts_summaries to ensure_fts5() | 0.1, 0.3 |
| `crates/brain_lib/src/db/summaries.rs` | Add brain_id params, add `get_summaries_by_prefixed_ids()` | 0.1, 0.5 |
| `crates/brain_lib/src/db/fts.rs` | Add `search_summaries_fts()`, `FtsSummaryResult`, `reindex_summaries_fts()` | 0.3 |
| `crates/brain_lib/src/store.rs` | Add `upsert_summary()`, `delete_summary()` | 0.4 |
| `crates/brain_lib/src/mcp/mod.rs` | Add summaries to backfill_brain_id() | 0.1 |
| `crates/brain_lib/src/mcp/tools/mem_write_episode.rs` | Pass brain_id, embed+upsert after store | 0.1, 0.4 |
| `crates/brain_lib/src/mcp/tools/mem_reflect.rs` | Add commit mode, pass brain_id, embed+upsert | 0.1, 0.2, 0.4 |
| `crates/brain_lib/src/mcp/tools/mem_expand.rs` | Handle `sum:` prefix IDs | 0.5 |
| `crates/brain_lib/src/mcp/tools/mem_search_minimal.rs` | Update tool description | 0.5 |
| `crates/brain_lib/src/query_pipeline.rs` | Merge summary candidates, pass brain_id to list_episodes | 0.1, 0.5 |
| `crates/brain_lib/src/retrieval.rs` | Update derive_kind() for sum: prefix | 0.5 |
| `crates/brain_lib/src/ranking.rs` | Add summary_kind to CandidateSignals (optional) | 0.5 |

---

## Key Design Decisions

1. **Mode parameter on reflect, not separate tool** — keeps tool count at 25, matches existing description
2. **`sum:` prefix for all summaries in LanceDB** — episode vs reflection kind determined from SQLite enrichment, not from prefix
3. **`__summaries__` as synthetic file_id** — prevents orphan cleanup from deleting summary embeddings
4. **Best-effort embedding on write** — failure to embed doesn't fail the episode/reflection write; embeddings backfilled via CLI `brain reindex`
5. **FTS5 with porter stemming** — episodes are prose, benefit from stemming; existing chunks FTS does not use porter (maintains backward compat)
6. **CLI-only backfill** — existing summary embeddings populated via `brain reindex --summaries`; no auto-startup cost
7. **Brain_id filtering via SQLite enrichment** — LanceDB doesn't store brain_id; after vector search, summary candidates are enriched from SQLite where brain_id filtering applies
8. **Cross-brain reflection support** — reflections stored in the current brain, but `source_ids` can reference episodes from any brain. `get_summary()` is a PK lookup with no brain_id filter. `reflect(mode="prepare")` accepts `brains` parameter to gather episodes across brains (same pattern as `search_minimal`). This enables cross-brain tasks where knowledge from multiple projects feeds into a single reflection.

---

## Verification

### Build
```bash
just check    # or cargo check --workspace
just test     # or cargo test --workspace
```

### Manual testing (with a brain set up)
```bash
# 1. Write an episode
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory.write_episode","arguments":{"goal":"Debug auth","actions":"Traced token flow","outcome":"Found cache bug","tags":["auth","debugging"]}}}' | brain mcp

# 2. Search for it
echo '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"memory.search_minimal","arguments":{"query":"auth debugging","intent":"reflection"}}}' | brain mcp
# → Should return result with kind="episode"

# 3. Reflect on it
echo '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"memory.reflect","arguments":{"mode":"prepare","topic":"authentication patterns"}}}' | brain mcp

# 4. Commit reflection
echo '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"memory.reflect","arguments":{"mode":"commit","title":"Auth cache invalidation pattern","content":"Always invalidate cache after token refresh...","source_ids":["<episode_id>"],"tags":["auth","pattern"],"importance":0.9}}}' | brain mcp

# 5. Search finds reflection
echo '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"memory.search_minimal","arguments":{"query":"cache invalidation auth"}}}' | brain mcp
# → Should return result with kind="reflection"

# 6. Expand works for summary ID
echo '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"memory.expand","arguments":{"memory_ids":["sum:<reflection_id>"]}}}' | brain mcp
```

### Automated tests
All existing tests must pass. New tests added for each issue as described above. Run with:
```bash
cargo test --workspace
```
