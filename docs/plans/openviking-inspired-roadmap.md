---
status: in-progress
complexity: epic
effort: ~3 months
priority: high
---

# OpenViking-Inspired Development Roadmap for Brain

**Scope:** 6-phase roadmap, blocked on [Episodic Memory Foundation](./episodic-memory.md).

---

## 0. Episodic Memory Blocker

### Current Status

**Phase 0 is complete.** The episodic memory loop is closed: episodes and reflections are written, indexed, retrieved, and searchable via the main hybrid pipeline.

#### What Was Delivered

| Component | Status | Detail |
|-----------|--------|--------|
| `memory.write_episode` MCP tool | Complete | Stores structured episodes (goal/actions/outcome) with tags/importance to `summaries` table |
| `summaries` table schema | Complete | Supports `episode`, `reflection`, `summary` kinds with timestamps, importance, validity windows |
| `reflection_sources` table | Complete | Junction table linking reflections → source episodes; FK constraints in place |
| `store_episode()` DB function | Complete | Writes to `summaries` with `kind='episode'`, returns ULID |
| `store_reflection()` DB function | Complete | Writes reflection + source links; called by `memory.reflect` commit mode |
| `list_episodes()` DB function | Complete | Recency-ordered retrieval, limit-based |
| `memory.reflect` two-phase tool | Complete | `prepare` mode returns recent episodes + related chunks; `commit` mode stores the synthesized reflection |
| Episode/reflection vector indexing (LanceDB) | Complete | Summaries embedded at write time with `sum:{id}` prefix; semantic search works |
| Episode/reflection FTS5 indexing (`fts_summaries`) | Complete | BM25 keyword search over episode/reflection content |
| Episodes + reflections in `search_minimal` results | Complete | Hybrid pipeline merges summary candidates into the result pool; `episode` and `reflection` stub kinds returned |
| `brain_id` on `summaries` table | Complete | Schema v25 migration; all summary queries are brain-scoped |
| Cross-brain `memory.reflect` prepare | Complete | `brains` parameter allows gathering episodes across registered brains |
| Unit + integration tests | Complete | Episode write/read, reflection storage, FTS reindex, and pipeline integration all tested |

#### Why This Unblocks Later Phases

- **Phase 1 (URIs):** Episodes/reflections are brain-scoped first-class objects. URI assignment is now straightforward.
- **Phase 2 (Hierarchy summaries):** The summary storage and retrieval pattern is proven and reusable.
- **Phase 3 (Explainability):** All memory types are in the candidate pool — score breakdowns are now meaningful across all object types.
- **Phase 4 (Consolidation):** Episodes are retrievable. The reflect commit mode provides the write path for consolidated memories.
- **Phase 5 (Playbooks):** The `playbook` kind can follow the same pattern proven here.

### Concrete Issues

#### Issue 0.1: Add `brain_id` to `summaries` table

**Problem:** `summaries` is the only domain table without `brain_id`, breaking multi-brain isolation. All queries over summaries are unpartitioned.

**Proposed change:**
- Schema migration v22: `ALTER TABLE summaries ADD COLUMN brain_id TEXT`
- Backfill existing rows from the default brain
- Add `brain_id` to `reflection_sources` queries
- Update `store_episode()`, `store_reflection()`, `list_episodes()` to accept and filter by `brain_id`

**Files:**
- `crates/brain_lib/src/db/schema.rs` — migration
- `crates/brain_lib/src/db/summaries.rs` — all query functions
- `crates/brain_lib/src/mcp/tools/mem_write_episode.rs` — pass brain context
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs` — pass brain context

**Dependencies:** None
**Acceptance criteria:** All summary queries are brain-scoped; existing data migrated
**Size:** S

#### Issue 0.2: Complete `memory.reflect` two-phase tool

**Problem:** `memory.reflect` only implements retrieval (phase 1). Agents receive source material but have no MCP path to store the synthesized reflection. `store_reflection()` exists in `db/summaries.rs` but is unreachable.

**Proposed change:**
- Add a `mode` parameter to `memory.reflect`: `"prepare"` (default, current behavior) and `"commit"`
- In `"commit"` mode, accept: `title`, `content`, `source_ids` (array of summary_ids), `tags`, `importance`
- Validate source_ids exist in `summaries`
- Call `store_reflection()` to persist with source linkage
- Return the new `summary_id`
- Alternatively: create a separate `memory.commit_reflection` tool if overloading `reflect` is too complex

**Files:**
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs` — add commit mode
- `crates/brain_lib/src/db/summaries.rs` — may need minor adjustments for brain_id

**Dependencies:** Issue 0.1 (brain_id)
**Acceptance criteria:** Agent can prepare sources, generate a reflection, and store it via MCP; reflection appears in `summaries` with `kind='reflection'` and source links populated
**Size:** M

#### Issue 0.3: FTS5 indexing for summaries

**Problem:** Episodes and reflections are invisible to BM25 keyword search. `fts_chunks` and `fts_tasks` exist, but there is no `fts_summaries`.

**Proposed change:**
- Create `fts_summaries` virtual table:
  ```sql
  CREATE VIRTUAL TABLE fts_summaries USING fts5(
      content,
      content=summaries,
      content_rowid=rowid,
      tokenize='porter unicode61'
  );
  ```
- Add INSERT/UPDATE/DELETE triggers to keep in sync (mirror pattern in `db/fts.rs`)
- Populate at write time in `store_episode()` and `store_reflection()`
- Backfill existing summaries in migration

**Files:**
- `crates/brain_lib/src/db/schema.rs` — migration
- `crates/brain_lib/src/db/fts.rs` — add `search_summaries_fts()` query function

**Dependencies:** None (can run in parallel with 0.1)
**Acceptance criteria:** BM25 query for episode content returns matching summaries with scores
**Size:** S

#### Issue 0.4: LanceDB vector indexing for summaries

**Problem:** Episodes/reflections have no embeddings. They can't be retrieved by semantic similarity.

**Proposed change:**
- At `store_episode()` and `store_reflection()` time, embed the content via the shared `Embedder`
- Upsert into the per-brain LanceDB table with `sum:{summary_id}` prefixed chunk IDs
- Content to embed: `"{title}\n\n{content}"` (for episodes: `"Goal: {goal}\nActions: {actions}\nOutcome: {outcome}"`)
- Add `delete_summary_vectors()` for cleanup if summaries are ever removed

**Files:**
- `crates/brain_lib/src/records/indexing.rs` or new `crates/brain_lib/src/db/summary_indexing.rs`
- `crates/brain_lib/src/mcp/tools/mem_write_episode.rs` — call embedder after store
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs` — call embedder after commit

**Dependencies:** Issue 0.1 (brain_id for correct LanceDB table)
**Acceptance criteria:** Vector search for "auth module debugging" returns an episode about auth module debugging
**Size:** M

#### Issue 0.5: Integrate summaries into `search_minimal` hybrid pipeline

**Problem:** `search_minimal` only returns note chunks. Episodes and reflections never appear as candidates.

**Proposed change:**
- In `query_pipeline.rs`, after vector search, partition results by prefix (`sum:*` → summary candidates)
- Run parallel BM25 query against `fts_summaries`
- Merge summary candidates into the unified candidate pool
- Enrich from `summaries` table: `title`, `content`, `kind`, `tags`, `importance`, `created_at`
- Map scoring signals:
  - Vector similarity: same (shared embedding space)
  - BM25: from `fts_summaries`
  - Recency: `exp(-age/tau)` on `created_at`
  - PageRank: defaults to 0.0 for summaries (the existing signal is a pre-normalized PageRank from the `files` table — summaries are not files). See `semantic-search-records.md` §2d for a discussion of options.
  - Tag match: Jaccard on `tags`
  - Importance: stored value
- Add `"episode"` and `"reflection"` as stub kinds in retrieval results
- For `memory.expand`: if ID starts with `sum:`, fetch from `summaries` table

**Files:**
- `crates/brain_lib/src/query_pipeline.rs` — candidate pool merging
- `crates/brain_lib/src/retrieval.rs` — stub packing, expand handling
- `crates/brain_lib/src/db/fts.rs` — summary FTS query

**Dependencies:** Issues 0.3, 0.4
**Acceptance criteria:** `search_minimal` returns episodes alongside note chunks, correctly ranked; expand works for summary IDs
**Size:** L

#### Issue 0.6: Backfill, migration, and test coverage

**Problem:** Existing episodes have no embeddings or FTS entries. Test coverage has gaps.

**Proposed change:**
- CLI command or startup migration that:
  1. Iterates all `summaries` rows
  2. Generates embeddings and upserts into LanceDB
  3. Populates `fts_summaries`
- Add integration tests:
  - Episode → search_minimal round-trip
  - Reflect prepare → commit → search round-trip
  - Reflection source linking verification
  - Multi-brain isolation for summaries
  - Expand on summary IDs
- Add observability: log count of episodes/reflections indexed at startup

**Files:**
- `crates/brain/src/cli/` — reindex command extension
- `tests/retrieval_tests.rs` — integration tests
- `crates/brain_lib/src/db/summaries.rs` — test additions

**Dependencies:** Issues 0.1–0.5
**Acceptance criteria:** Fresh install and existing-data upgrade both result in fully searchable episodes; all new tests pass
**Size:** M

---

## 1. Current-State Summary of Brain

Brain is a local-first Rust knowledge daemon exposing 25 MCP tools across four domains:

**Storage layers:**
- **SQLite** (v25 schema): chunks, files, links, tasks, records, summaries, record_tags, record_links, reflection_sources. FTS5 virtual tables for chunks, tasks, and summaries (`fts_summaries`).
- **LanceDB** (per-brain): 384-dim BGE-small embeddings for note chunks and episodic memory (`sum:{id}` prefix). IVF-PQ indexing with automatic optimization.
- **Object store** (content-addressed): BLAKE3-hashed blobs for record payloads. Optional zstd compression.
- **Event log** (per-brain JSONL): Append-only audit trail for records (best-effort, not authoritative).

**Retrieval:**
- Hybrid 6-signal scoring: vector similarity, BM25, recency (exponential decay, 30d tau), PageRank (pre-normalized, from files table), tag Jaccard, importance
- Intent-driven weight profiles: lookup, planning, reflection, synthesis (unrecognized intents fall back to a default profile)
- Fusion confidence triggers cross-encoder reranking when vector/FTS disagree (<0.3)
- Token-budgeted progressive packing: minimal stubs → expand on demand
- Episode and reflection stubs returned as first-class results alongside note chunks

**Domains:**
- **Notes/memory:** Markdown files → chunks → embeddings. Hybrid search via `memory.search_minimal`/`memory.expand`.
- **Tasks:** CRUD + event sourcing, dependency graph, `tasks.next` prioritization.
- **Records:** Content-addressed artifacts with metadata, tags, links. No semantic search (addressed by the sibling plan).
- **Episodic memory:** Fully implemented (see [Episodic Memory Foundation](./episodic-memory.md)). Episodes and reflections are written, embedded, FTS-indexed, and retrievable via `search_minimal`. `memory.reflect` supports prepare and commit modes. Cross-brain episode gathering supported.

**Key boundaries:**
- Brain-scoped: each brain is a directory with its own LanceDB, event log, and content path. SQLite is shared with `brain_id` partitioning on all domain tables including `summaries`.
- MCP is the only external interface. No REST API, no gRPC.
- Embedding is local-only (BGE-small-en-v1.5, 384-dim, via candle/safetensors). No remote model dependency on the hot path.

---

## 2. Transferable Ideas from OpenViking

### 2.1 Unified URI / Object Addressing (`viking://`)

**What:** OpenViking uses `viking://{scope}/{path}` URIs to address all context objects — resources, memories, skills, sessions, temp objects. Every object has a canonical address.

**Why it fits Brain:** Brain already has multiple ID namespaces (`chunk_id`, `file_id`, `task_id`, `record_id`, `summary_id`) but no unified addressing scheme. Cross-domain references (e.g. record → task, reflection → episode) use raw foreign keys. A `synapse://{brain}/{domain}/{id}` URI would unify this.

**What NOT to copy:** OpenViking's filesystem metaphor (AGFS virtual filesystem with directories, tree traversal) is over-engineered for Brain's domain model. Brain's entities are typed, not hierarchical — a flat URI resolver is sufficient.

### 2.2 Tiered Content Representations (L0/L1/L2)

**What:** OpenViking stores three representations per object:
- L0 (~100 tokens): ultra-concise abstract for vector search
- L1 (~2000 tokens): overview summary with navigation pointers
- L2 (unlimited): full original content

**Why it fits Brain:** Brain's progressive retrieval already approximates this (stubs → expanded chunks), but it's implicit. Explicitly generating and storing L0 abstracts for notes and records would improve vector search precision (BGE-small performs better on short, focused text than on long documents). L1 summaries would reduce expand costs.

**What NOT to copy:** Making all three tiers mandatory or requiring LLM generation for every object. Brain should generate L0/L1 lazily or on-demand, not as a blocking write-path requirement. Avoid introducing remote LLM dependency.

### 2.3 Retrieval Observability

**What:** OpenViking provides debug/visualization of retrieval trajectories — which nodes were visited, how scores were computed, why results were ranked as they were.

**Why it fits Brain:** Brain's hybrid scoring is sophisticated (6 signals, intent-driven weights, fusion confidence) but opaque. When retrieval quality is poor, there's no way to diagnose whether the issue is vector similarity, BM25, recency bias, or weight selection. An explain mode would make the system debuggable.

**What NOT to copy:** OpenViking's tree-traversal visualization is specific to its filesystem metaphor. Brain needs score breakdowns per candidate, not path traces.

### 2.4 Session-to-Memory Consolidation

**What:** OpenViking automatically compresses session transcripts and extracts durable memories in six categories: profile, preferences, entities, events, cases, patterns. Includes deduplication against existing memories.

**Why it fits Brain:** Brain's `memory.write_episode` is agent-initiated, meaning durable memory only happens when the agent explicitly calls it. Automated extraction from session artifacts (e.g. drone checkpoints, task completion events) would capture knowledge that agents forget to record. The six-category taxonomy is a useful starting point.

**What NOT to copy:** Automatic LLM-driven extraction on every session. Brain should offer this as an opt-in tool or post-session hook, not a mandatory pipeline. Deduplication should use the existing hybrid search to find near-duplicates rather than adding a separate dedup system.

### 2.5 Procedural Memory / Skills

**What:** OpenViking treats skills as first-class context objects — callable procedures stored alongside memories and resources, searchable and versionable.

**Why it fits Brain:** Brain has no representation for "how to do X" knowledge. Debugging recipes, deployment runbooks, review checklists, and coding patterns are currently either in notes (unstructured) or not captured at all. A `playbook` kind in `summaries` (or a dedicated table) would make procedural knowledge retrievable.

**What NOT to copy:** OpenViking's skill execution engine. Brain is a knowledge store, not a runtime. Playbooks should be retrieved and presented to agents, not executed by Brain.

### 2.6 Hierarchical Summaries

**What:** OpenViking generates directory-level summaries that aggregate child content, enabling top-down navigation.

**Why it fits Brain:** Brain indexes individual chunks but has no folder, project, or tag-level summaries. Asking "what's in the architecture notes?" requires scanning all chunks. Derived summary nodes for directories, tags, or task groups would enable higher-level retrieval.

**What NOT to copy:** Deep recursive tree structures. Brain's hierarchy is shallow (brain → domain → object). One level of derived summaries is sufficient.

---

## 3. Recommended Roadmap

### Phase 0 — [Episodic Memory Foundation](./episodic-memory.md) ✓ Complete

**Goal:** Close the episode write → retrieve → reflect → retrieve loop.
**Why first:** Every later phase depends on summaries being first-class retrievable objects.
**Benefit:** Agents can build on their own history. Cold starts become warm starts.
**Delivered:** Schema v25 (`brain_id` on summaries, `fts_summaries`), LanceDB embedding for episodes/reflections, `search_minimal` integration, `memory.reflect` commit mode, cross-brain prepare.
**Plan:** See [`episodic-memory.md`](./episodic-memory.md) for full implementation details.

### Phase 1 — Unified `synapse://` URI / Object Addressing

**Goal:** Every object in Brain has a canonical URI. Cross-domain references use URIs. MCP tools return URIs.
**Why second:** Once episodes join the retrieval pool, there are 5 object types (chunks, tasks, records, episodes, reflections) with ad-hoc ID formats. URIs prevent a combinatorial explosion of ID-handling code.
**Benefit:** Simpler cross-referencing, stable external identifiers, foundation for Phase 2 drill-down.
**Risks:** Low. URIs are additive — existing IDs continue to work. Migration is a matter of adding a computed field.

### Phase 2 — Derived Hierarchy Summaries

**Goal:** Auto-generate summary nodes for directories, tags, and task groups. Make them searchable.
**Why third:** Requires URI addressing (Phase 1) for parent→child links. Requires the summary storage and retrieval pattern proven in [Episodic Memory Foundation](./episodic-memory.md).
**Benefit:** Higher-level retrieval without scanning all chunks. "What's in the architecture docs?" returns a summary, not 50 stubs.
**Risks:** LLM dependency for summary generation (can be mitigated with rule-based extractive summaries as fallback). Staleness — summaries must be invalidated when children change.

### Phase 3 — Retrieval Explainability / Observability

**Goal:** Add an explain mode to `search_minimal` that returns per-candidate score breakdowns.
**Why fourth:** By now the retrieval pool includes notes, records (sibling plan), episodes, reflections, and hierarchy summaries. Debugging ranking across 5+ object types requires observability.
**Benefit:** Diagnosable retrieval. Tunable weights with empirical feedback.
**Risks:** Low. Purely additive — explain mode is an optional parameter.

### Phase 4 — Optional Session/Episode Consolidation

**Goal:** Tool or hook that reviews recent episodes and promotes durable memories (preferences, patterns, entities).
**Why fifth:** Requires episodes to be retrievable ([Episodic Memory](./episodic-memory.md)) and deduplication to work well (proven retrieval pipeline). Benefits from hierarchy summaries (Phase 2) for grouping related episodes.
**Benefit:** Knowledge accumulates automatically instead of depending on agent discipline.
**Risks:** False positive memories. Mitigation: reviewable suggestions before commit. Avoid making this automatic without user opt-in.

### Phase 5 — Procedural Memory / Playbooks

**Goal:** New `playbook` entity type for reusable procedures, searchable alongside other memory.
**Why sixth:** Requires the full retrieval stack ([Episodic Memory](./episodic-memory.md)), URIs for linking (Phase 1), and ideally consolidation (Phase 4) to auto-extract recurring patterns into playbooks.
**Benefit:** "How do I deploy this?" returns a structured playbook, not scattered notes.
**Risks:** Scope creep into execution. Keep playbooks as knowledge artifacts, not runnable scripts.

### Phase 6 — Artifact Enrichment for Records

**Goal:** Generate searchable surrogate text and metadata for record payloads (covered in sibling plan `semantic-search-records.md`). Extend with L0/L1 derived representations.
**Why last:** Record search is the sibling plan's scope. This phase adds OpenViking-inspired tiered representations on top.
**Benefit:** Better retrieval precision for records via focused L0 abstracts.
**Risks:** LLM dependency for L0/L1 generation. Mitigate with extractive methods first.

---

## 4. Concrete Tasks / Issues

### Phase 0 — Episodic Memory Foundation ✓ Complete

See [`episodic-memory.md`](./episodic-memory.md) for the full standalone plan (Issues 0.1–0.6). All issues implemented.

---

### Phase 1 — Unified `synapse://` URI / Object Addressing

#### Issue 1.1: Define `synapse://` URI scheme and parser

**Problem:** Five object types use different ID formats with no unified addressing. Cross-references require knowing the target domain.

**Proposed change:**
- Define URI format: `synapse://{brain_name}/{domain}/{id}` where domain ∈ {`note`, `chunk`, `task`, `record`, `episode`, `reflection`}
- Implement `SynapseUri` type in a new `crates/brain_lib/src/uri.rs`:
  ```rust
  pub struct SynapseUri {
      pub brain: String,
      pub domain: UriDomain,
      pub id: String,
  }
  ```
- Implement `Display` (serialize) and `FromStr` (parse) with validation
- Implement `resolve(conn, uri) -> Option<UriTarget>` that fetches the referenced object

**Files:**
- `crates/brain_lib/src/uri.rs` — **new**
- `crates/brain_lib/src/lib.rs` — module declaration

**Dependencies:** None
**Acceptance criteria:** `"synapse://default/episode/01ABC123".parse::<SynapseUri>()` works; resolve returns the correct row
**Size:** M

#### Issue 1.2: Return URIs from MCP tool responses

**Problem:** MCP tools return raw IDs (`summary_id`, `task_id`, `record_id`). Callers must know the domain to construct references.

**Proposed change:**
- Add a `uri` field to all MCP tool responses that return object identifiers
- Compute URI at response time from brain context + domain + ID
- Backward-compatible: existing fields remain, URI is additive

**Files:**
- `crates/brain_lib/src/mcp/tools/mem_write_episode.rs`
- `crates/brain_lib/src/mcp/tools/mem_reflect.rs`
- `crates/brain_lib/src/mcp/tools/task_*.rs`
- `crates/brain_lib/src/mcp/tools/record_*.rs`
- `crates/brain_lib/src/retrieval.rs` — stubs in search results

**Dependencies:** Issue 1.1
**Acceptance criteria:** Every MCP tool that creates or returns an object includes a `uri` field
**Size:** M

#### Issue 1.3: Accept URIs in MCP tools as input identifiers

**Problem:** Tools like `records.get`, `tasks.get`, `memory.expand` accept only domain-specific IDs.

**Proposed change:**
- Accept both raw IDs and `synapse://` URIs in ID parameters
- Parse URI, extract domain and ID, dispatch to correct handler
- For cross-brain URIs, resolve against the target brain's database

**Files:**
- `crates/brain_lib/src/mcp/tools/` — all tools accepting IDs
- `crates/brain_lib/src/uri.rs` — resolver

**Dependencies:** Issue 1.1
**Acceptance criteria:** `memory.expand(id="synapse://default/episode/01ABC")` works identically to `memory.expand(id="sum:01ABC")`
**Size:** M

#### Issue 1.4: URI-based cross-domain linking

**Problem:** `record_links` has separate `task_id` and `chunk_id` columns. `reflection_sources` only links to summaries. There's no generic "this object references that object" mechanism.

**Proposed change:**
- Add a `links` table (or extend existing):
  ```sql
  CREATE TABLE object_links (
      source_uri TEXT NOT NULL,
      target_uri TEXT NOT NULL,
      relation TEXT NOT NULL DEFAULT 'references',
      created_at INTEGER NOT NULL,
      PRIMARY KEY (source_uri, target_uri, relation)
  );
  ```
- Migrate existing `record_links` and `reflection_sources` data
- Expose via `links.add` / `links.list` MCP tools or integrate into existing domain tools

**Files:**
- `crates/brain_lib/src/db/schema.rs` — migration
- `crates/brain_lib/src/db/links.rs` — new or extended
- `crates/brain_lib/src/uri.rs`

**Dependencies:** Issues 1.1, 0.1
**Acceptance criteria:** Any object can link to any other object via URIs; a future PageRank extension or link-count signal could leverage `object_links` for cross-domain scoring (out of scope for this issue — the existing PageRank signal only covers files)
**Size:** L
**Notes:** This is the most invasive change in Phase 1. Consider keeping existing tables and adding `object_links` as a supplementary table rather than replacing.

---

### Phase 2 — Derived Hierarchy Summaries

#### Issue 2.1: Directory-level summary generation

**Problem:** No way to ask "what's in the architecture docs?" without scanning all chunks from files in that directory.

**Proposed change:**
- Add a `derived_summaries` table:
  ```sql
  CREATE TABLE derived_summaries (
      summary_id TEXT PRIMARY KEY,
      scope_type TEXT NOT NULL,  -- 'directory', 'tag', 'task_group'
      scope_key TEXT NOT NULL,   -- directory path, tag name, or task_id
      brain_id TEXT NOT NULL,
      level TEXT NOT NULL,       -- 'abstract' (L0) or 'overview' (L1)
      content TEXT NOT NULL,
      token_estimate INTEGER,
      source_hash TEXT NOT NULL, -- BLAKE3 of sorted child IDs; used for staleness check
      created_at INTEGER NOT NULL,
      UNIQUE(scope_type, scope_key, brain_id, level)
  );
  ```
- Generation: rule-based extractive summarization (first sentence of each child chunk, concatenated). LLM-based generation as optional upgrade.
- Trigger: on reindex, or via explicit `memory.summarize_scope` tool
- Staleness: `source_hash` changes when children change → regenerate on next access

**Files:**
- `crates/brain_lib/src/db/schema.rs` — migration
- `crates/brain_lib/src/db/derived_summaries.rs` — **new**
- `crates/brain_lib/src/mcp/tools/mem_summarize_scope.rs` — **new**

**Dependencies:** [Episodic Memory](./episodic-memory.md) (summary retrieval pattern), Phase 1 (URIs for child references)
**Acceptance criteria:** `memory.search_minimal("architecture overview")` can return a directory summary for `docs/` alongside individual chunks
**Size:** L

#### Issue 2.2: Integrate derived summaries into retrieval

**Problem:** Derived summaries exist but aren't searchable.

**Proposed change:**
- Embed derived summaries into LanceDB with `ds:{summary_id}` prefix
- Add to FTS5 (either `fts_summaries` or a dedicated `fts_derived_summaries`)
- Include in `search_minimal` candidate pool
- Stubs include `scope_type` and `scope_key` for drill-down
- Expand returns the summary content + list of child URIs for navigation

**Files:**
- `crates/brain_lib/src/query_pipeline.rs`
- `crates/brain_lib/src/retrieval.rs`

**Dependencies:** Issue 2.1
**Acceptance criteria:** Derived summaries appear in search results with correct scoring; expand includes child URIs
**Size:** M

#### Issue 2.3: Tag-level and task-group summaries

**Problem:** Tags and task groups aggregate related content but have no summary representation.

**Proposed change:**
- Extend Issue 2.1's mechanism to support `scope_type='tag'` and `scope_type='task_group'`
- For tags: aggregate all chunks/records/episodes with a given tag
- For task groups: aggregate all records and episodes linked to a task
- Same staleness detection via `source_hash`

**Files:** Same as 2.1 (extended, not new)
**Dependencies:** Issue 2.1
**Acceptance criteria:** `memory.search_minimal("performance work")` returns a tag summary for `#performance` if one exists
**Size:** M

---

### Phase 3 — Retrieval Explainability / Observability

#### Issue 3.1: Explain mode for `search_minimal`

**Problem:** When retrieval quality is poor, there's no way to understand why. The hybrid scoring formula is opaque.

**Proposed change:**
- Add `explain: bool` parameter to `memory.search_minimal` (default false)
- When true, include per-candidate score breakdown in response:
  ```json
  {
    "id": "sum:01ABC",
    "score": 0.82,
    "explain": {
      "vector_sim": 0.91,
      "bm25": 0.45,
      "recency": 0.78,
      "links": 0.12,
      "tag_match": 1.0,
      "importance": 0.8,
      "weights": "reflection",
      "fusion_confidence": 0.35
    }
  }
  ```
- Preserve existing response format when `explain=false`

**Files:**
- `crates/brain_lib/src/mcp/tools/mem_search_minimal.rs` — parameter + response
- `crates/brain_lib/src/query_pipeline.rs` — carry scores through pipeline
- `crates/brain_lib/src/ranking.rs` — expose individual signal values
- `crates/brain_lib/src/retrieval.rs` — include in stubs

**Dependencies:** None (can be done in parallel with [Episodic Memory](./episodic-memory.md), but more useful after all object types are in the pool)
**Acceptance criteria:** `explain=true` returns per-signal scores for every result; scores sum correctly to final score
**Size:** M

#### Issue 3.2: Query diagnostics / stats

**Problem:** No visibility into how many candidates came from each source (vector, FTS, which domain), or how many were filtered/reranked.

**Proposed change:**
- Add optional `diagnostics` object to search response:
  ```json
  {
    "diagnostics": {
      "vector_candidates": 47,
      "fts_candidates": 23,
      "union_size": 58,
      "fusion_confidence": 0.28,
      "reranked": true,
      "by_domain": { "chunk": 38, "episode": 12, "record": 8 },
      "query_time_ms": 45
    }
  }
  ```
- Gated behind `explain=true` to avoid overhead in normal queries

**Files:**
- `crates/brain_lib/src/query_pipeline.rs` — collect stats during execution
- `crates/brain_lib/src/retrieval.rs` — include in response

**Dependencies:** None
**Acceptance criteria:** Diagnostics accurately reflect pipeline execution; timing is measured
**Size:** S

---

### Phase 4 — Optional Session/Episode Consolidation

#### Issue 4.1: `memory.consolidate` MCP tool

**Problem:** Durable memory depends on agents calling `write_episode` explicitly. Valuable patterns and preferences accumulate in episodes but are never synthesized into higher-order knowledge.

**Proposed change:**
- New `memory.consolidate` tool that:
  1. Retrieves recent episodes (configurable window, default 20)
  2. Groups by semantic similarity using existing vector search
  3. Returns consolidation candidates as structured suggestions:
     ```json
     {
       "candidates": [
         {
           "category": "pattern",
           "title": "Auth module requires cache invalidation after token refresh",
           "source_episodes": ["01ABC", "01DEF"],
           "suggested_importance": 0.9
         }
       ]
     }
     ```
  4. Agent reviews and approves/edits candidates
  5. Approved candidates stored as reflections (via `memory.reflect` commit mode)

**Categories** (inspired by OpenViking's six-category framework, adapted):
- `preference` — user/project preferences observed across episodes
- `entity` — key entities, modules, services mentioned repeatedly
- `pattern` — recurring patterns, gotchas, debugging approaches
- `event` — significant one-time events worth remembering
- `case` — complete problem→solution case studies

**Files:**
- `crates/brain_lib/src/mcp/tools/mem_consolidate.rs` — **new**
- `crates/brain_lib/src/db/summaries.rs` — add `category` field or use tags

**Dependencies:** [Episodic Memory](./episodic-memory.md) (complete episode loop), Issue 0.2 (reflect commit)
**Acceptance criteria:** Tool returns structured candidates; approved candidates become reflections with source links; deduplication prevents storing near-identical memories
**Size:** L
**Notes:** This is the tool that benefits most from being agent-driven rather than automatic. Keep it as a tool the agent invokes, not a background job.

#### Issue 4.2: Deduplication for consolidated memories

**Problem:** Repeated consolidation runs could produce redundant memories.

**Proposed change:**
- Before committing a consolidation candidate, vector-search existing reflections
- If cosine similarity > 0.92 with an existing reflection, flag as duplicate
- Return duplicates to the agent for merge/skip decision
- Optionally: merge by appending new source_ids to existing reflection

**Files:**
- `crates/brain_lib/src/mcp/tools/mem_consolidate.rs`
- `crates/brain_lib/src/db/summaries.rs` — update source links

**Dependencies:** Issue 4.1, Issue 0.4 (LanceDB for summaries)
**Acceptance criteria:** Repeated consolidation of similar episodes produces at most one reflection, not duplicates
**Size:** M

---

### Phase 5 — Procedural Memory / Playbooks

#### Issue 5.1: Playbook entity type

**Problem:** "How to debug X" or "deployment checklist for Y" knowledge has no structured home. It lives in unstructured notes or isn't captured.

**Proposed change:**
- Add `playbook` as a new `kind` in the `summaries` table (already supports extensible kinds)
- Schema: title, steps (structured JSON or markdown), tags, linked tasks/records
- MCP tool `memory.write_playbook`:
  ```json
  {
    "title": "Debug auth token refresh",
    "steps": "1. Check cache TTL...\n2. Verify token endpoint...",
    "tags": ["auth", "debugging"],
    "linked_tasks": ["BRN-01KK..."],
    "importance": 0.9
  }
  ```
- Playbooks are embedded and indexed like episodes/reflections
- Searchable via `search_minimal` with `kind: "playbook"` in stubs

**Files:**
- `crates/brain_lib/src/db/summaries.rs` — add `playbook` kind, store/get functions
- `crates/brain_lib/src/mcp/tools/mem_write_playbook.rs` — **new**
- `crates/brain_lib/src/query_pipeline.rs` — already handles `sum:` prefix
- `crates/brain_lib/src/retrieval.rs` — playbook stub format

**Dependencies:** [Episodic Memory](./episodic-memory.md) (summary retrieval pattern)
**Acceptance criteria:** `search_minimal("how to debug auth")` returns a playbook; expand shows full steps
**Size:** M

#### Issue 5.2: Playbook versioning

**Problem:** Playbooks evolve as understanding improves. Overwriting loses history.

**Proposed change:**
- Add optional `parent_id` to `summaries` for version chains
- `memory.update_playbook` creates a new version linked to the previous one
- Latest version is returned by default; history accessible via `memory.expand` with `versions=true`

**Files:**
- `crates/brain_lib/src/db/summaries.rs` — add `parent_id` column
- `crates/brain_lib/src/db/schema.rs` — migration
- `crates/brain_lib/src/mcp/tools/mem_write_playbook.rs`

**Dependencies:** Issue 5.1
**Acceptance criteria:** Updating a playbook creates a new version; old versions retrievable
**Size:** S

---

### Phase 6 — Artifact Enrichment for Records

**Note:** This phase extends the sibling plan (`semantic-search-records.md`). The sibling plan covers basic record embedding. This phase adds OpenViking-inspired tiered representations.

#### Issue 6.1: L0 abstract generation for records

**Problem:** Record payloads can be large. Embedding the full content with BGE-small (512-token window) loses information from long artifacts.

**Proposed change:**
- On record creation, generate an L0 abstract (~100 tokens): extractive summary using first N sentences + title + tags
- Store in a `record_abstracts` table or as a column on `records`
- Use L0 abstract (not full content) as the primary embedding source
- Full content remains in object store for expand

**Files:**
- `crates/brain_lib/src/records/indexing.rs`
- `crates/brain_lib/src/db/schema.rs` — migration for abstract column/table
- `crates/brain_lib/src/records/queries.rs`

**Dependencies:** Sibling plan (records in retrieval pipeline)
**Acceptance criteria:** Vector search over records uses focused abstracts; retrieval precision improves for long artifacts
**Size:** M
**Notes:** Start with rule-based extractive summarization. LLM-based abstraction is a future upgrade.

#### Issue 6.2: Derived metadata extraction for records

**Problem:** Records store raw payloads but don't extract searchable metadata (mentioned files, modules, error patterns, etc.).

**Proposed change:**
- On record creation, run lightweight pattern extraction:
  - File paths mentioned
  - Error messages / stack traces
  - Code identifiers (function names, class names)
- Store as structured tags or in a `record_metadata` JSON column
- Include in FTS and tag matching signals

**Files:**
- `crates/brain_lib/src/records/indexing.rs` — extraction logic
- `crates/brain_lib/src/records/queries.rs` — metadata storage/query

**Dependencies:** Sibling plan
**Acceptance criteria:** A record containing "error in auth_handler.rs: TokenExpired" is findable via `search_minimal("TokenExpired")` even if the full payload isn't FTS-indexed
**Size:** M

---

## 5. API / MCP Implications

### New MCP tools

| Tool | Phase | Breaking? |
|------|-------|-----------|
| `memory.commit_reflection` (or `memory.reflect` commit mode) | 0 | No — additive |
| `memory.summarize_scope` | 2 | No — new tool |
| `memory.consolidate` | 4 | No — new tool |
| `memory.write_playbook` | 5 | No — new tool |
| `memory.update_playbook` | 5 | No — new tool |

### Modified MCP tools

| Tool | Change | Phase | Breaking? |
|------|--------|-------|-----------|
| `memory.search_minimal` | Returns episodes, reflections, derived summaries, playbooks as new stub kinds | 0, 2, 5 | No — additive kinds |
| `memory.search_minimal` | `explain` parameter | 3 | No — optional param |
| `memory.expand` | Handles `sum:` and `ds:` prefixed IDs | 0, 2 | No — additive |
| All tools returning IDs | Add `uri` field | 1 | No — additive field |
| All tools accepting IDs | Accept `synapse://` URIs | 1 | No — additive input format |
| `memory.reflect` | Add `mode` parameter (prepare/commit) | 0 | No — default is current behavior |

### Backward Compatibility

All changes are additive. No existing parameters change meaning. No existing response fields are removed. Clients that ignore new fields continue to work.

### Migration Strategy

- [Episodic Memory](./episodic-memory.md) — **delivered** as schema migration v25 (`brain_id` on summaries, `fts_summaries`)
- Phase 1 requires schema migration v26 (`object_links` table)
- Phase 2 requires schema migration v27 (`derived_summaries` table)
- Phase 5 requires schema migration v28 (`parent_id` on summaries)
- Phase 6 extends whichever migration the sibling plan uses

Each migration is independent and backward-compatible. Failed migrations should be resumable.

---

## 6. Data Model / Indexing Implications

### New Tables

| Table | Phase | Purpose |
|-------|-------|---------|
| `fts_summaries` (FTS5) | 0 ✓ | Keyword search over episodes/reflections |
| `object_links` | 1 | URI-based cross-domain linking |
| `derived_summaries` | 2 | Directory/tag/task-group summaries |

### Modified Tables

| Table | Change | Phase |
|-------|--------|-------|
| `summaries` | Add `brain_id` column | 0 ✓ |
| `summaries` | Add `parent_id` column | 5 |
| `records` | Add abstract/metadata columns | 6 |

### LanceDB Index Changes

| Prefix | Object Type | Phase |
|--------|-------------|-------|
| `sum:{id}` | Episodes and reflections | 0 ✓ |
| `ds:{id}` | Derived hierarchy summaries | 2 |
| `rec:{id}` | Records (sibling plan) | Sibling |

All use the same 384-dim BGE-small embeddings in the same per-brain LanceDB table. The `chunk_id` prefix distinguishes types during enrichment.

### Rebuild / Reindex Costs

- [Episodic Memory](./episodic-memory.md) backfill: embed all existing episodes (typically <1000; fast)
- Phase 2 generation: one-time summary generation for all directories/tags (potentially slow for large brains; should be incremental)
- Phase 6 abstraction: one-time for existing records (bounded by record count)

All derived data is regenerable from source. Dropping and rebuilding the LanceDB table + FTS tables is always safe.

---

## 7. What to Defer or Reject

### Defer

| Idea | Why Defer |
|------|-----------|
| **Pluggable embedding models** | BGE-small is adequate for prose. Code-aware models (nomic-embed-code) are worth evaluating but not until the current pipeline handles all object types. |
| **Automatic consolidation** (background job) | Start with agent-invoked `memory.consolidate`. Automatic extraction risks false positives and introduces LLM dependency on a background path. Revisit after the tool proves useful. |
| **Cross-brain dependency edges** | Useful for multi-repo work but adds significant complexity to task graph resolution. Current within-brain deps are sufficient. |
| **Snapshot diffing** (`records.diff`) | Nice-to-have for review cycles but low impact vs other work. |
| **L1 overview generation** | L0 abstracts (extractive) are sufficient for now. L1 overviews require LLM generation and are a Phase 6+ upgrade. |

### Reject

| Idea | Why Reject |
|------|-----------|
| **AGFS virtual filesystem** | Brain's entity model is typed, not hierarchical. A filesystem abstraction adds complexity without matching Brain's domain. Flat URIs are sufficient. |
| **Multi-tenant / cloud storage** | Brain is local-first by design. S3 backends, multi-tenant isolation, and cloud-native features from OpenViking don't fit. |
| **LLM-required hot paths** | OpenViking uses LLM for intent analysis in retrieval. Brain's intent profiles are rule-based and deterministic — this is a feature, not a limitation. Keep the hot path LLM-free. |
| **Skill execution engine** | Brain stores knowledge; it doesn't execute procedures. Playbooks are retrieved and presented to agents, not run by Brain. |
| **Session transcript storage** | OpenViking stores full session transcripts. Brain stores structured episodes. Full transcripts are the agent framework's responsibility, not the knowledge store's. |
| **Priority queue-based recursive retrieval** | OpenViking's tree-traversal retrieval is tied to its filesystem metaphor. Brain's flat hybrid retrieval with intent-driven weights is simpler and effective. |
