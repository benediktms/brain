# Plan: Semantic Search Over Records

**Status:** Proposed
**Priority:** High
**Estimated complexity:** Medium — touches embedding, storage, query pipeline, and MCP layers but follows established patterns

## Problem

Records (drone checkpoints, implementation artifacts, reviews, analyses) are only retrievable by ID or metadata filters via `records.get` and `records.list`. Meanwhile, notes go through a full hybrid retrieval pipeline (`memory.search_minimal`) combining vector similarity, BM25, recency, link density, tag overlap, and importance scoring.

This means the most operationally rich data in Brain — what agents actually did — is dark to meaning-based queries. You can't ask "what did we discover last time we touched the auth module?" without already knowing the record ID.

## Goal

Bring records into the same hybrid retrieval pipeline as notes so that `memory.search_minimal` returns both note chunks and record content, ranked by the same unified scoring formula.

## Design Principles

- **Reuse, don't duplicate.** The hybrid scoring pipeline, embedder, LanceDB store, and token-budgeted packing already work well. Records should flow through the same infrastructure with minimal branching.
- **Records are first-class retrieval citizens.** A record result and a note chunk result should be interchangeable at the stub level — callers shouldn't need to care about provenance to get value.
- **Respect the payload lifecycle.** Records can be archived or have their payload evicted. Search must degrade gracefully: archived records can still appear (metadata is useful context), evicted records should be flagged as non-expandable.

## Implementation

### Phase 1: Record Embedding & Indexing

**Goal:** Generate and store embeddings for record content alongside note chunks in LanceDB.

#### 1a. Embed record content on create/update

In the `records.create_artifact` and `records.save_snapshot` code paths, after writing content to the object store:

1. Read back the content bytes and decode to text (skip non-text `media_type` — e.g. binary blobs, images).
2. Build an embeddable document from structured fields:
   ```
   {title}\n{description}\n\n{content_text}
   ```
3. If the document exceeds the embedder's token window, chunk it using the same chunking strategy as notes (heading-aware splitting). Most records are <4K tokens so this is the uncommon path.
4. Embed via the shared `Embedder` instance (already held in `McpContext`).
5. Upsert into the per-brain LanceDB table with a namespaced chunk ID: `rec:{record_id}` (or `rec:{record_id}:{chunk_index}` if chunked).

**Key file:** `crates/brain_lib/src/records/mod.rs` or a new `crates/brain_lib/src/records/indexing.rs`

#### 1b. Add FTS5 coverage for records

Create a new FTS5 virtual table for record text search:

```sql
CREATE VIRTUAL TABLE fts_records USING fts5(
    record_id,
    content,
    content=records,        -- or a dedicated records_text table
    content_rowid=rowid,
    tokenize='porter unicode61'
);
```

Populate on record create/update with the same `{title} {description} {content_text}` document used for embedding. Add triggers to keep it in sync (mirror the pattern in `db/fts.rs`).

**Key file:** `crates/brain_lib/src/db/schema.rs` (migration), `crates/brain_lib/src/db/fts.rs`

#### 1c. Handle payload lifecycle events

- **Archive:** Keep the embedding and FTS entry. Archived records are still useful for retrieval — they just won't have expandable content. Mark with a flag in results.
- **Payload eviction (`PayloadEvicted`):** Keep the embedding (it was computed from content that existed). Flag as non-expandable. The embedding still points to the right semantic neighbourhood even after content is gone.
- **Delete (if ever added):** Remove from LanceDB and FTS.

#### 1d. Backfill existing records

Add a one-time migration or CLI command (`brain reindex-records`) that iterates active records with `payload_available = 1`, reads content from the object store, and generates embeddings. This follows the same pattern as the existing `brain reindex` for notes.

**Key file:** New command in `crates/brain/src/cli/` or extend existing reindex.

### Phase 2: Query Pipeline Integration

**Goal:** `search_ranked()` returns both note chunks and record results, scored by the same hybrid formula.

#### 2a. Extend vector search to include records

No changes needed to LanceDB queries — record embeddings live in the same table with `rec:` prefixed chunk IDs. The vector search path already returns all results regardless of prefix.

After vector search, partition results by prefix:
- `rec:*` → record candidates
- everything else → note chunk candidates (existing path)

#### 2b. Extend FTS search to include records

Run a parallel BM25 query against `fts_records` in addition to `fts_chunks`. Merge results into the unified candidate pool with their source tagged.

**Key file:** `crates/brain_lib/src/query_pipeline.rs`

#### 2c. Enrich record candidates from SQLite

For candidates with `rec:` IDs, load metadata from the `records` table instead of `chunks`:
- `title` → heading equivalent
- `description` → summary
- `kind`, `tags`, `task_id` → metadata for display
- `created_at` → recency signal
- `status`, `payload_available` → for lifecycle flags

**Key file:** `crates/brain_lib/src/retrieval.rs`

#### 2d. Adapt scoring signals for records

Map the six hybrid signals to record equivalents:

| Signal | Notes (existing) | Records (new) |
|--------|-----------------|---------------|
| Vector similarity | dot product | Same — shared embedding space |
| BM25 keyword | `fts_chunks` score | `fts_records` score |
| Recency | `exp(-age/tau)` on chunk timestamp | Same formula on `records.created_at` |
| Link density | backlink count from `links` table | Count from `record_links` table |
| Tag match | Jaccard on chunk tags | Jaccard on `record_tags` |
| Importance | Per-chunk importance score | Default 1.0, or derive from `retention_class` (permanent=1.0, standard=0.7, ephemeral=0.4) |

The scoring formula itself (`ranking.rs`) doesn't change — it operates on `CandidateSignals` which is source-agnostic.

#### 2e. Extend stub packing

Add a `"record"` variant to the stub kind enum (alongside `"note"` etc.) so callers can distinguish provenance. Include `record_id`, `kind`, `status`, and `payload_available` in the stub metadata.

For `memory.expand` calls on record stubs: fetch content from the object store via `records::objects::read_object()` if `payload_available`, otherwise return a "content evicted" message.

**Key file:** `crates/brain_lib/src/retrieval.rs`

### Phase 3: MCP Tool Surface

**Goal:** Expose record search through existing and new tools.

#### 3a. `memory.search_minimal` — no parameter changes needed

Records automatically appear in results since they flow through the same pipeline. The response format already uses generic stubs with a `kind` field. Callers that understand `kind: "record"` can act on it; callers that don't will treat it like any other result.

#### 3b. `records.search` — new dedicated tool (optional)

For callers that want records specifically:

```json
{
  "name": "records.search",
  "params": {
    "query": "string (required)",
    "kind": "string (optional — filter to specific record kind)",
    "status": "active|archived|all (default: active)",
    "tags": "string[] (optional)",
    "task_id": "string (optional — scope to a specific task)",
    "limit": "integer (default: 10)",
    "brain": "string (optional)"
  }
}
```

This runs the same hybrid pipeline but filters candidates to records only before scoring. Useful when you know you want implementation artifacts, not notes.

#### 3c. `memory.expand` — extend to support record IDs

When `expand` receives a `rec:` prefixed ID, fetch from the object store instead of the chunks table.

### Phase 4: Testing & Validation

1. **Unit tests:** Embedding generation for records, FTS insert/query, lifecycle flag propagation.
2. **Integration tests:** End-to-end `search_ranked()` returning mixed note + record results with correct scoring.
3. **Backfill test:** Verify `reindex-records` correctly processes existing records.
4. **Lifecycle tests:** Archive a record, verify it still appears in search. Evict payload, verify it appears but is flagged non-expandable.

## Migration & Rollout

- **Schema migration:** v22 adds `fts_records` table and triggers.
- **Backfill:** Run automatically on first startup after migration (or via explicit CLI command for large stores).
- **No breaking changes:** `memory.search_minimal` response format gains a new `kind` value but is otherwise backward-compatible. Existing callers won't break.

## Out of Scope (for now)

- **Code-aware embeddings:** BGE-small handles prose well; a code-specific model (e.g. nomic-embed-code) would improve retrieval for code-heavy artifacts but is a separate concern (pluggable embedding model).
- **Cross-encoder reranking for records:** The existing cross-encoder trigger (fusion confidence < 0.3) will naturally apply to mixed result sets. No record-specific reranking needed.
- **Automatic retention/cleanup:** Addressed by the separate "Snapshot Lifecycle" gap. This plan focuses purely on making records searchable.

## Files Likely Modified

| File | Change |
|------|--------|
| `crates/brain_lib/src/db/schema.rs` | Migration v22: `fts_records` table + triggers |
| `crates/brain_lib/src/db/fts.rs` | FTS5 query function for records |
| `crates/brain_lib/src/records/indexing.rs` | **New** — embed + index record content |
| `crates/brain_lib/src/records/mod.rs` | Call indexing on create/update |
| `crates/brain_lib/src/query_pipeline.rs` | Add records to vector + FTS candidate pools |
| `crates/brain_lib/src/retrieval.rs` | Enrich + pack record stubs, extend expand |
| `crates/brain_lib/src/store.rs` | No changes needed (records reuse existing table) |
| `crates/brain_lib/src/ranking.rs` | No changes needed (scoring is source-agnostic) |
| `crates/brain_lib/src/mcp/tools/record_search.rs` | **New** — `records.search` tool |
| `crates/brain_lib/src/mcp/tools/mem_search_minimal.rs` | Minor: handle `rec:` IDs in expand |
