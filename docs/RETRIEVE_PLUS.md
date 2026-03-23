---
layout: ADR
number: 001
title: Retrieve+ — Unified Memory Retrieval Surface
status: proposed
date: 2026-03-22
authors: brain-collective
deciders: []
technical-reviewers: []
related: BRN-01KM5Z5 (LLM job queue), BRN-01KM99PC (Retrieve+ epic)
supersedes: []
superseeded-by: []
challenged-date: 2026-06-22
challenged-by: []
tags: [area:memory, type:feature, phase:design]
---

## Table of Contents

- [Context](#context)
  - [Problem Statement](#problem-statement)
  - [Design Goals](#design-goals)
  - [Non-Goals](#non-goals)
- [Decision](#decision)
- [Consequences](#consequences)
  - [What Becomes Easier](#what-becomes-easier)
  - [What Becomes Harder](#what-becomes-harder)
  - [Risks](#risks)
- [Detailed Specification](#detailed-specification)
  - [1. Retrieval Modes](#1-retrieval-modes)
    - [1.1 Query Mode](#11-query-mode)
    - [1.2 URI Mode](#12-uri-mode)
  - [2. LOD System](#2-lod-system)
    - [2.1 Level Definitions](#21-level-definitions)
    - [2.2 Storage Schema](#22-storage-schema)
    - [2.3 Generation Strategy](#23-generation-strategy)
    - [2.4 TTL and Refresh Policy](#24-ttl-and-refresh-policy)
  - [3. Explainability](#3-explainability)
    - [3.1 Per-Result Explainability Fields](#31-per-result-explainability-fields)
    - [3.2 Response-Level Diagnostics](#32-response-level-diagnostics)
    - [3.3 Annotated Example Response](#33-annotated-example-response)
  - [4. Embedding Strategy](#4-embedding-strategy)
    - [4.1 Current: BGE-small (384d)](#41-current-bge-small-384d)
    - [4.2 Proposal: Dual Encoder Profiles](#42-proposal-dual-encoder-profiles)
    - [4.3 Time-Based Recency Filtering](#43-time-based-recency-filtering)
  - [5. Metadata & Labeling](#5-metadata--labeling)
    - [5.1 Canonical Facets](#51-canonical-facets)
    - [5.2 Soft Label System](#52-soft-label-system)
    - [5.3 Synonym Clustering](#53-synonym-clustering)
    - [5.4 Metadata Filters in `retrieve`](#54-metadata-filters-in-retrieve)
  - [6. Job Queue Integration](#6-job-queue-integration)
    - [6.1 Job Table Reference](#61-job-table-reference)
    - [6.2 Summarization Job Flow](#62-summarization-job-flow)
    - [6.3 Consolidation Job Flow](#63-consolidation-job-flow)
    - [6.4 Error Handling and Retry](#64-error-handling-and-retry)
  - [7. Tool Schema](#7-tool-schema)
  - [8. Dependency Graph](#8-dependency-graph)
- [Alternatives Considered](#alternatives-considered)
- [Open Questions](#open-questions)

---

## Context

### Problem Statement

Brain's current retrieval surface is split across two MCP tools:

| Tool | Responsibility | Limitation |
|------|---------------|------------|
| `memory.search_minimal` | Returns compact stubs via hybrid scoring | Token-optimised but opaque; no LOD control |
| `memory.expand` | Returns full chunk content by ID | Requires a separate round-trip; expands one object at a time |

This split creates compounding problems for agents:

1. **Two-call overhead.** Every useful retrieval requires at minimum two calls: search, then expand. For multi-object retrieval, the expand fan-out is proportional to result count.
2. **No LOD control.** Agents cannot request intermediate-fidelity summaries. They receive either raw stubs or full content — nothing in between.
3. **No explainability.** Agents cannot see why a result ranked where it did. Debugging retrieval quality is impossible without reading source code.
4. **No URI-addressed direct access.** There is no way to fetch a specific object by its canonical identifier without knowing which domain-specific tool handles that type.
5. **Ephemeral-only responses.** The system generates nothing; it retrieves raw stored content. Higher-quality representations (summaries, abstractions) require the agent to build them manually and store them elsewhere.

### Design Goals

| ID | Goal |
|----|------|
| G1 | **Unified surface.** A single `retrieve` tool handles all retrieval modes — semantic query, direct URI, and cross-brain lookup. |
| G2 | **LOD control.** Agents declare the fidelity they need (L0/L1/L2). The system returns the appropriate representation, generating it if necessary. |
| G3 | **Explainability on every response.** Every result carries provenance: which strategy was used, what LOD level was applied, when the representation was generated. |
| G4 | **Persistent representations.** L0/L1/L2 summaries are stored durably in the `lod_chunks` table, not regenerated per request. |
| G5 | **Async generation without blocking retrieval.** LOD generation is enqueued as a job. The tool never blocks on generation; it returns the best available representation and signals freshness. |
| G6 | **Soft metadata labeling.** Server-side synonym clustering normalizes tag vocabulary across brains. |
| G7 | **Dual embedding profiles.** BGE-small (384d) remains the default. A richer encoder profile is evaluated in a parallel spike (BRN-01KM99PC.9). |
| G8 | **Time-scope filtering.** Metadata-based recency filters without re-embedding. |
| G9 | **Backward compatibility.** `search_minimal` and `expand` continue to function. `retrieve` is an additive surface. |

### Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Replace `search_minimal`/`expand` immediately | Migration is phased; existing tools remain until `retrieve` reaches parity |
| LLM-required hot paths | LOD generation is async and off the retrieval critical path |
| Remote embedding APIs | Brain is local-first; all embedding is done by the on-device BGE-small model |
| Skill execution | Brain stores knowledge; it does not run procedures |
| Full-text transcription of audio/video | Out of scope; records are text artifacts |
| Cross-brain write operations | `retrieve` is read-only; writes remain in domain-specific tools |

---

## Decision

We introduce **Retrieve+**: a new `retrieve` MCP tool that unifies all memory retrieval into a single surface with three capabilities not present in the existing `search_minimal`/`expand` pair:

1. **Level-of-Detail (LOD) control** — callers specify L0 (compact abstract), L1 (async LLM-generated summary), or L2 (full passthrough). The system returns the best available representation and enqueues generation for missing levels.
2. **Canonical URI addressing** — every retrievable object is addressable via a `synapse://{brain}/{domain}/{id}` URI, decoupling caller code from domain-specific ID routing.
3. **Inline explainability** — every result carries provenance fields (`lod_fresh`, `strategy_used`, `generated_at`, per-signal `explain` block) so agents can evaluate result quality without inspecting source code.

LOD generation is decoupled from the retrieval hot path via the jobs table (schema v31, BRN-01KM5Z5). The `retrieve` tool never waits on LLM inference — it returns the best available representation immediately and signals freshness via `lod_fresh`. Persistent LOD representations are stored in a new `lod_chunks` table keyed on `(object_uri, lod_level)`.

The existing `search_minimal` and `expand` tools are not removed. They are soft-deprecated: their descriptions are annotated with `deprecated: true` and `retrieve` is the documented preference for new code.

---

## Consequences

### What Becomes Easier

- **Agent context loading.** A single `retrieve` call with `lod="L1"` and `count=5` replaces a two-step search-then-expand fan-out. Agents compose richer context in fewer round-trips.
- **Retrieval debugging.** The `explain` block and `diagnostics` object expose the full scoring signal, making retrieval quality visible without source code access.
- **Direct object access.** Any object addressable by a `synapse://` URI can be fetched at any LOD level without knowing which domain table holds it.
- **LOD-aware caching.** L0 and L1 representations are stored durably in `lod_chunks`. Repeated retrieval of the same object at the same LOD level is a cache hit after the first miss.
- **Vocabulary normalization.** Synonym clustering in `tag_clusters` means agents do not need to know all variant spellings of a concept to retrieve related content.

### What Becomes Harder

- **Schema complexity increases.** The new `lod_chunks` and `tag_clusters` tables add surface area to the database schema. Migrations must be sequenced correctly relative to dependent tasks.
- **LOD staleness is implicit.** Callers must inspect `lod_fresh` to determine whether content is current. Agents that ignore `lod_fresh` may silently operate on stale summaries.
- **First-retrieval quality degradation.** For L1 requests on objects that have never been summarized, the first response is always an L0 fallback. Agents sensitive to this must either check `lod_fresh` and re-query, or use a future `wait_ms` extension (see OQ-2).
- **Job queue dependency.** All L1 generation depends on the jobs table (BRN-01KM5Z5). Retrieve+ cannot proceed to L1 capability without that infrastructure being available.

### Risks

- **Model selection unresolved (OQ-1).** flan-t5 was removed and no L1 generation model has been selected. If no suitable local model is chosen, L1 quality degrades to extractive fallback permanently — diminishing the value of the LOD system.
- **`lod_chunks` scope ambiguity (OQ-3).** The schema uses `object_uri` as the key, which implies support for all object types. Records store payloads in the object store (BLAKE3-keyed), not in SQLite, making source hash computation structurally different. If scope is not decided before BRN-01KM99PC.5.1, the schema may need a breaking revision.
- **Agent migration friction.** Agents (Drone Protocol, Sentinel, Probe) have hardcoded calls to `search_minimal`. Soft deprecation reduces urgency but does not eliminate the migration cost. If `search_minimal` is eventually removed without migration, deployed agents break silently.
- **Synonym clustering at scale.** Embedding all unique tags and clustering them is fast for small brains but potentially expensive for large ones. Without a decided trigger policy (OQ-4), the background job may either run too infrequently (stale clusters) or too aggressively (resource contention).

---

## Detailed Specification

### 1. Retrieval Modes

`retrieve` supports two primary modes, selected by the shape of the input.

#### 1.1 Query Mode

**Purpose:** Natural language semantic retrieval with LOD control and strategy hints.

**Trigger:** Input contains a non-empty `query` string.

##### Input Schema

```json
{
  "query": "how does the auth token refresh flow work?",
  "lod": "L1",
  "count": 5,
  "strategy": "synthesis",
  "brain": "my-project",
  "metadata_filters": {
    "time_scope": "recent_30d",
    "tags": ["auth", "tokens"],
    "kinds": ["chunk", "episode", "reflection"]
  }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `query` | string | Yes (query mode) | — | Natural language query |
| `lod` | enum | No | `"L0"` | Requested level of detail: `L0`, `L1`, or `L2` |
| `count` | integer | No | `10` | Maximum number of results |
| `strategy` | string | No | `"auto"` | Ranking weight profile: `lookup`, `planning`, `reflection`, `synthesis`, `auto` |
| `brain` | string | No | current brain | Target brain name or ID |
| `metadata_filters` | object | No | `{}` | Structured filters; see §5.4 |

##### Output Schema

```json
{
  "results": [
    {
      "uri": "synapse://my-project/chunk/01ABCDEF",
      "kind": "chunk",
      "lod": "L1",
      "lod_fresh": true,
      "title": "Auth token refresh flow",
      "content": "The refresh flow acquires a new access token by...",
      "score": 0.84,
      "strategy_used": "synthesis",
      "generated_at": "2026-03-20T14:22:00Z",
      "source_uri": "synapse://my-project/chunk/01ABCDEF",
      "explain": null
    }
  ],
  "diagnostics": {
    "vector_candidates": 52,
    "fts_candidates": 18,
    "union_size": 61,
    "reranked": true,
    "query_time_ms": 38,
    "lod_hits": 4,
    "lod_misses": 1,
    "lod_generation_enqueued": 1
  }
}
```

| Field | Description |
|-------|-------------|
| `uri` | Canonical `synapse://` URI for this result |
| `kind` | Object type: `chunk`, `episode`, `reflection`, `record`, `derived_summary` |
| `lod` | Actual LOD level returned (may differ from requested if stored representation not yet available) |
| `lod_fresh` | `true` if the returned LOD content was generated within the configured TTL |
| `title` | Human-readable title |
| `content` | Content at the requested LOD level |
| `score` | Final hybrid score (0.0–1.0) |
| `strategy_used` | The ranking weight profile that was applied |
| `generated_at` | ISO 8601 timestamp when this LOD representation was generated |
| `source_uri` | URI of the source object (same as `uri` for L0/L1; the original chunk for L2 passthrough) |
| `explain` | Per-signal score breakdown if `explain=true` was requested; `null` otherwise |

**LOD fallback behavior:** If the requested LOD level (e.g., L1) is not yet available for a result, the system:
1. Returns the highest available level (e.g., L0) instead.
2. Sets `lod_fresh: false`.
3. Enqueues an async `summarize` job for the missing level.
4. Includes the job in `diagnostics.lod_generation_enqueued`.

##### Query Mode Examples

**Example 1: Default L0 lookup**
```json
{ "query": "jobs table schema", "lod": "L0" }
```
Returns raw chunk stubs with scores. Fast. No generation overhead.

**Example 2: L1 synthesis for a planning session**
```json
{
  "query": "Retrieve+ design decisions",
  "lod": "L1",
  "count": 3,
  "strategy": "synthesis"
}
```
Returns generated summaries (~2000 tokens each) of the most relevant chunks. If L1 summaries are stale or missing, returns L0 and enqueues generation.

**Example 3: Filtered recent episodes**
```json
{
  "query": "auth debugging",
  "lod": "L0",
  "metadata_filters": {
    "time_scope": "recent_7d",
    "kinds": ["episode"]
  }
}
```
Returns only episode objects from the last 7 days.

---

#### 1.2 URI Mode

**Purpose:** Direct object access by canonical `synapse://` URI. No semantic ranking — fetch the object at the specified address and return it at the requested LOD level.

**Trigger:** Input contains a non-empty `uri` string and no `query` string.

##### URI Format

```
synapse://{brain_name}/{domain}/{id}
```

| Component | Values | Example |
|-----------|--------|---------|
| `brain_name` | Registered brain name or `_` for current | `my-project` |
| `domain` | `chunk`, `task`, `record`, `episode`, `reflection`, `derived_summary` | `chunk` |
| `id` | Domain-specific ULID or prefixed ID | `01ABCDEF...` |

**Examples:**
```
synapse://my-project/chunk/01ABCDEF
synapse://personal/episode/01GHIJKL
synapse://_/task/BRN-01KMXXX
synapse://work/record/01MNOPQR
```

##### Routing Logic

```
retrieve(uri="synapse://brain/domain/id", lod="L1")
    │
    ├─ Parse URI → (brain_name, domain, id)
    ├─ Resolve brain_name → brain_id (registry lookup)
    ├─ Check lod_chunks for (object_uri, lod="L1")
    │     ├─ HIT: return lod_chunks row (check freshness)
    │     └─ MISS: enqueue summarize job, return L0 from source table
    └─ Return result with lod_fresh flag
```

For `domain=task` or `domain=record`, the system fetches from the respective SQLite tables (tasks, records) — no LanceDB lookup. For `chunk`, `episode`, `reflection`, and `derived_summary`, the system can return LOD representations from `lod_chunks`.

##### Input Schema

```json
{
  "uri": "synapse://my-project/chunk/01ABCDEF",
  "lod": "L1"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `uri` | string | Yes (URI mode) | — | Canonical `synapse://` URI |
| `lod` | enum | No | `"L0"` | Requested LOD level |

##### Output Schema

Same structure as query mode result, but `results` always contains exactly one object (or an error if the URI is invalid or the object does not exist).

---

### 2. LOD System

#### 2.1 Level Definitions

| Level | Token Budget | Description | Generation Method | Use Case |
|-------|-------------|-------------|-------------------|----------|
| **L0** | ~100 tokens | Ultra-concise abstract: key entity, topic, and context. Derived from chunk text via extractive rules (first sentence + top noun phrases). | Deterministic, synchronous, no LLM. | Vector search precision; fast candidate scoring. |
| **L1** | ~2000 tokens | Overview summary: narrative description of the content, key decisions, referenced entities, and navigation pointers to related objects. | Async, LLM-generated (flan-t5 removed; replaced by configurable local model or extractive fallback). | Planning sessions; agent orientation; pre-expanded context. |
| **L2** | Unlimited | Full original content verbatim. No generation required — returns the raw source text from the `chunks` or `summaries` table. | Passthrough — no generation. | Deep reading; exact content recovery; expand replacement. |

**Key invariant:** L0 ⊆ L1 ⊆ L2 in information density, but they are stored and served independently. The system does not derive L0 from L1 or L1 from L2 — each level is generated from the source object directly.

#### 2.2 Storage Schema

**TODO for BRN-01KM99PC.5.1:** Implement the `lod_chunks` table and migration.

##### Proposed `lod_chunks` Table

```sql
CREATE TABLE lod_chunks (
    id          TEXT    NOT NULL,           -- ULID, primary key
    object_uri  TEXT    NOT NULL,           -- synapse:// URI of the source object
    brain_id    TEXT    NOT NULL,           -- owning brain (partitioning key)
    lod_level   TEXT    NOT NULL,           -- 'L0' | 'L1' | 'L2'
    content     TEXT    NOT NULL,           -- generated content at this LOD level
    token_est   INTEGER,                    -- estimated token count
    method      TEXT    NOT NULL,           -- 'extractive' | 'llm' | 'passthrough'
    model_id    TEXT,                       -- model used (NULL for extractive/passthrough)
    source_hash TEXT    NOT NULL,           -- BLAKE3 of source content at generation time
    created_at  TEXT    NOT NULL,           -- ISO 8601
    expires_at  TEXT,                       -- ISO 8601; NULL = no expiry
    job_id      TEXT,                       -- FK → jobs.id (NULL if generated synchronously)
    PRIMARY KEY (id),
    UNIQUE (object_uri, lod_level)          -- one representation per (object, level)
);

CREATE INDEX idx_lod_chunks_uri   ON lod_chunks (object_uri, lod_level);
CREATE INDEX idx_lod_chunks_brain ON lod_chunks (brain_id, created_at DESC);
CREATE INDEX idx_lod_chunks_exp   ON lod_chunks (expires_at) WHERE expires_at IS NOT NULL;
```

**Design notes:**
- `object_uri` is the canonical `synapse://` URI of the source. This decouples LOD storage from domain-specific ID formats.
- `source_hash` is the BLAKE3 hash of the source text at generation time. Freshness is determined by comparing against the current source hash — if they differ, the LOD representation is stale.
- `lod_level='L2'` rows are never written to this table; L2 is served directly from the source table.
- `job_id` links to the `jobs` table (schema v31) for traceability of async generation.

#### 2.3 Generation Strategy

##### L0 — Deterministic Extractive

Generated synchronously at index time (or lazily on first retrieve if missing). No LLM dependency.

**Algorithm:**
1. Take the first sentence of the chunk (split on `.` or `\n`).
2. Extract up to 5 key noun phrases via regex patterns on code identifiers, capitalized terms, and backtick-quoted tokens.
3. Concatenate into a single compact string capped at 100 tokens.
4. Store in `lod_chunks` with `method='extractive'`.

**Trigger:** Every chunk write (index pipeline) generates an L0 representation in the same transaction.

##### L1 — Async LLM-Generated

Generated asynchronously via the jobs queue. Never blocks the retrieval hot path.

**Trigger:** On first L1 miss, the `retrieve` orchestrator enqueues a `summarize` job:
```json
{
  "job_type": "summarize",
  "priority": 3,
  "chunk_id": "01ABCDEF",
  "brain_id": "my-project",
  "payload": {
    "text": "<source content>",
    "level": "L1"
  }
}
```

The job worker generates the L1 summary and writes it to `lod_chunks`. On the next `retrieve` call, the L1 hit is served directly.

**Model:** The summary model is configurable per-brain in `brain.toml` under `[summarizer]`. If no model is configured, an extractive fallback (concatenation of the first 3 sentences plus key entities) is used.

##### L2 — Passthrough

No generation. The source content is returned directly from the `chunks` table (for note chunks) or `summaries` table (for episodes/reflections). `lod_chunks` is not written for L2.

#### 2.4 TTL and Refresh Policy

| LOD Level | Default TTL | Staleness Check |
|-----------|------------|-----------------|
| L0 | None (permanent unless source changes) | `source_hash` comparison at serve time |
| L1 | 30 days (`expires_at` set at creation) | `source_hash` comparison + TTL check |
| L2 | N/A — passthrough | Always fresh (reads live source) |

**Staleness detection:** At serve time, the system computes the current BLAKE3 hash of the source object's content. If it differs from `lod_chunks.source_hash`, the stored LOD representation is stale. The system returns the stale representation with `lod_fresh: false` and enqueues a `summarize` job to regenerate.

**Batch refresh:** A `consolidate` job (job_type='consolidate') can be enqueued to sweep all `lod_chunks` rows with `expires_at < now()` and regenerate them. This is handled by the job queue infrastructure (BRN-01KM5Z5).

---

### 3. Explainability

Every `retrieve` response carries provenance metadata. Agents receive enough context to understand why a result was returned, evaluate its freshness, and decide whether to request a different LOD level.

#### 3.1 Per-Result Explainability Fields

| Field | Type | Always Present | Description |
|-------|------|----------------|-------------|
| `uri` | string | Yes | Canonical `synapse://` URI |
| `kind` | string | Yes | Object domain: `chunk`, `episode`, `reflection`, `record`, `derived_summary` |
| `lod` | string | Yes | Actual LOD level returned (may differ from requested if generation pending) |
| `lod_fresh` | boolean | Yes | `true` if LOD content is within TTL and source hash matches |
| `score` | float | Yes | Final hybrid score (0.0–1.0). `null` for URI mode (no ranking) |
| `strategy_used` | string | Yes | Ranking weight profile applied: `lookup`, `planning`, `reflection`, `synthesis`, `auto` |
| `generated_at` | string | Yes | ISO 8601 timestamp when this LOD representation was generated. `null` for L2 passthrough |
| `source_uri` | string | Yes | URI of the original source object. Identical to `uri` for chunks; differs for derived summaries |

**Optional explain block** (returned when `explain: true` is passed in the request):

| Field | Type | Description |
|-------|------|-------------|
| `explain.vector_sim` | float | Cosine similarity score (0.0–1.0) |
| `explain.bm25` | float | BM25 keyword score (0.0–1.0, normalized) |
| `explain.recency` | float | Exponential decay score on `created_at` (tau=30d) |
| `explain.links` | float | PageRank-derived link authority signal (0.0–1.0) |
| `explain.tag_match` | float | Jaccard similarity between query tags and object tags |
| `explain.importance` | float | Stored importance value (0.0–1.0; 0.5 default for chunks) |
| `explain.weights` | string | Profile name used for signal weighting |
| `explain.fusion_confidence` | float | Agreement between vector and BM25 rankings (0.0–1.0). Values < 0.3 trigger cross-encoder reranking |

#### 3.2 Response-Level Diagnostics

Returned in the `diagnostics` block on every query mode response:

```json
{
  "diagnostics": {
    "vector_candidates": 52,
    "fts_candidates": 18,
    "union_size": 61,
    "reranked": true,
    "query_time_ms": 38,
    "lod_hits": 4,
    "lod_misses": 1,
    "lod_generation_enqueued": 1
  }
}
```

| Field | Description |
|-------|-------------|
| `vector_candidates` | Count of candidates from ANN vector search |
| `fts_candidates` | Count of candidates from FTS5 BM25 search |
| `union_size` | Count after union and deduplication |
| `reranked` | Whether cross-encoder reranking was applied |
| `query_time_ms` | Wall-clock time for the full retrieval pipeline |
| `lod_hits` | Results served from `lod_chunks` at requested level |
| `lod_misses` | Results where requested LOD was unavailable; fallback applied |
| `lod_generation_enqueued` | Count of `summarize` jobs enqueued during this call |

#### 3.3 Annotated Example Response

```json
{
  "results": [
    {
      "uri": "synapse://my-project/chunk/01KMABCDEF",
      "kind": "chunk",
      "lod": "L1",                              // ← Requested L1, returned L1 ✓
      "lod_fresh": true,                         // ← Source hash matches; within TTL
      "title": "Auth token refresh flow",
      "content": "The token refresh flow begins when...",
      "score": 0.84,
      "strategy_used": "synthesis",              // ← Planning-oriented weight profile
      "generated_at": "2026-03-20T14:22:00Z",
      "source_uri": "synapse://my-project/chunk/01KMABCDEF",
      "explain": {
        "vector_sim": 0.91,
        "bm25": 0.62,
        "recency": 0.88,
        "links": 0.15,
        "tag_match": 0.67,
        "importance": 0.50,
        "weights": "synthesis",
        "fusion_confidence": 0.72             // ← High agreement; no reranking needed
      }
    },
    {
      "uri": "synapse://my-project/chunk/01KMGHIJKL",
      "kind": "chunk",
      "lod": "L0",                              // ← Requested L1 but L1 not yet generated
      "lod_fresh": false,                        // ← Stale: L1 generation pending
      "title": "Token expiry handling",
      "content": "Token expiry triggers a refresh...",
      "score": 0.71,
      "strategy_used": "synthesis",
      "generated_at": null,                      // ← L0 is extractive; no generation timestamp
      "source_uri": "synapse://my-project/chunk/01KMGHIJKL",
      "explain": null
    }
  ],
  "diagnostics": {
    "vector_candidates": 52,
    "fts_candidates": 18,
    "union_size": 61,
    "reranked": false,
    "query_time_ms": 34,
    "lod_hits": 1,                              // ← 1 result served from lod_chunks
    "lod_misses": 1,                            // ← 1 result fell back to L0
    "lod_generation_enqueued": 1               // ← 1 summarize job enqueued for L1
  }
}
```

---

### 4. Embedding Strategy

#### 4.1 Current: BGE-small (384d)

Brain's production embedding model is **BAAI/bge-small-en-v1.5**:

| Property | Value |
|----------|-------|
| Dimensions | 384 |
| Max tokens | 512 |
| Pooling | CLS (first token) |
| Normalization | L2 |
| Similarity | Dot product (equivalent to cosine for L2-normalized vectors) |
| Storage | Per-brain LanceDB with IVF-PQ indexing |
| Prefixes | `chunk:{id}`, `sum:{id}`, `ds:{id}`, `rec:{id}` |

All object types (chunks, episodes, reflections, derived summaries, records) share the same 384-dim embedding space within a brain. Inter-brain comparisons are not supported.

**Known limitations:**
- 512-token window truncates long documents; L0 abstracts mitigate this by providing focused short-text inputs.
- BGE-small is general-purpose prose; code-heavy content (function names, error messages) has lower recall.
- 384 dimensions limits representational capacity relative to larger encoders.

#### 4.2 Proposal: Dual Encoder Profiles

**Spike:** BRN-01KM99PC.9 evaluates a richer encoder profile as a parallel embedding provider.

**Architecture (proposed):**

```
brain.toml:
  [embedding]
  primary = "bge-small"        # 384d — default, always-on
  secondary = "nomic-embed"    # TBD — evaluated in spike

LanceDB per-brain:
  table: chunks_bge384         # existing
  table: chunks_nomic768       # NEW — created only if secondary is configured
```

**Retrieval behavior with dual profiles:**
- Query is embedded with both models (parallel, non-blocking).
- Results from each ANN search are merged into the candidate pool.
- Fusion scoring weights primary and secondary scores by a configurable alpha parameter.
- If secondary model is unavailable, retrieval degrades gracefully to primary only.

**Decision authority:** BRN-01KM99PC.9 (dual embedding spike) produces a benchmark report before dual profiles are implemented in production. BRN-01KM99PC.10 (parallel embedding profiles) handles the implementation if the spike recommends proceeding.

#### 4.3 Time-Based Recency Filtering

Recency filtering is **metadata-based** — it does not require re-embedding. It operates as a pre-filter on the candidate pool.

**Mechanism:**
1. `metadata_filters.time_scope` is mapped to a concrete timestamp threshold:
   - `recent_7d` → `created_at > now() - 7 days`
   - `recent_30d` → `created_at > now() - 30 days`
   - `recent_90d` → `created_at > now() - 90 days`
   - `{iso_date}` → `created_at > {iso_date}` (exact threshold)
2. The threshold is applied as a SQLite WHERE clause on the metadata join step in `query_pipeline.rs`.
3. The recency signal in hybrid scoring is still applied as an exponential decay (tau=30d) over the surviving candidates — recency filtering and recency scoring are complementary.

**Why not a vector filter:** Re-embedding queries with temporal context (e.g., "auth flow as of last week") would require training data Brain does not have. Metadata filtering is deterministic and fast.

---

### 5. Metadata & Labeling

#### 5.1 Canonical Facets

Brain's metadata system uses typed facets rather than a flat tag namespace. Facets partition tag vocabulary into orthogonal axes:

| Facet | Prefix | Examples | Object Types |
|-------|--------|----------|--------------|
| Area | `area:` | `area:auth`, `area:indexing`, `area:mcp` | All |
| Type | `type:` | `type:bugfix`, `type:feature`, `type:design` | Tasks, Episodes |
| Phase | `phase:` | `phase:design`, `phase:polish` | Tasks |
| Project | `project:` | `project:brain`, `project:api` | All |
| Status | `status:` | `status:pending`, `status:done` | Tasks |
| Domain | `domain:` | `domain:rust`, `domain:sql` | Chunks, Records |

Agents may also apply arbitrary tags (no prefix). Unprefixed tags participate in synonym clustering (§5.3) but are not facet-typed.

#### 5.2 Soft Label System

**Soft labels** are user-applied tags that the server normalizes without requiring exact vocabulary compliance. The system accepts any tag string and clusters near-synonymous tags into canonical forms via periodic background processing.

**Properties:**
- Tags are stored verbatim in `chunk_tags`, `summary_tags`, and `record_tags` columns.
- The `tag_clusters` table maps variant forms to canonical labels.
- At query time, `metadata_filters.tags` are expanded via the cluster map before matching.
- Tag matching uses Jaccard similarity (intersection / union) on the expanded canonical set.

**Example:**
```
User writes: ["auth", "authentication", "auth-flow"]
Canonical cluster: { canonical: "auth", variants: ["authentication", "auth-flow", "token-auth"] }
Query for "authentication" → expands to ["auth", "authentication", "auth-flow", "token-auth"]
```

#### 5.3 Synonym Clustering

**Spike:** BRN-01KM99PC.7.2 evaluates the embedding-based clustering approach.

**Proposed algorithm:**
1. **Collect** all tag strings across all objects in a brain.
2. **Embed** each unique tag using the primary embedding model (short text; fast).
3. **Cluster** using cosine similarity threshold (default: 0.85). Tags within threshold are grouped.
4. **Select canonical** label: the shortest or most frequent tag in each cluster.
5. **Write** to `tag_clusters` table.
6. **Schedule** re-clustering as a `consolidate` job triggered weekly or on significant vocabulary growth (>100 new unique tags since last run).

**`tag_clusters` Table** (proposed, pending BRN-01KM99PC.7.2):

```sql
CREATE TABLE tag_clusters (
    canonical   TEXT NOT NULL,
    variant     TEXT NOT NULL,
    similarity  REAL NOT NULL,
    brain_id    TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (canonical, variant, brain_id)
);

CREATE INDEX idx_tag_clusters_variant ON tag_clusters (variant, brain_id);
```

#### 5.4 Metadata Filters in `retrieve`

The `metadata_filters` object in `retrieve` input:

```json
{
  "metadata_filters": {
    "time_scope": "recent_30d",
    "tags": ["auth", "tokens"],
    "kinds": ["chunk", "episode"],
    "brain": "my-project"
  }
}
```

| Filter | Type | Description |
|--------|------|-------------|
| `time_scope` | string | Time threshold: `recent_7d`, `recent_30d`, `recent_90d`, or ISO 8601 date |
| `tags` | string[] | Tag filter (AND semantics; expanded via synonym clusters) |
| `kinds` | string[] | Object kinds to include: `chunk`, `episode`, `reflection`, `record`, `derived_summary` |
| `brain` | string | Restrict to a specific brain (overrides top-level `brain` field if both present) |

---

### 6. Job Queue Integration

Retrieve+ uses the jobs table (schema v31, migration `v30_to_v31.rs`) for all async LOD generation work. The job queue is the infrastructure provided by BRN-01KM5Z5.

#### 6.1 Job Table Reference

```
jobs table (v31)
  id          TEXT PRIMARY KEY
  job_type    TEXT  — 'summarize' | 'consolidate'
  status      TEXT  — 'pending' | 'running' | 'done' | 'failed'
  priority    INTEGER  — lower = higher priority
  chunk_id    TEXT  — target chunk (NULL for consolidate)
  brain_id    TEXT  — owning brain
  payload     TEXT  — JSON (see below)
  result      TEXT  — JSON {"summary": "..."} on success
  error       TEXT  — error message on failure
  created_at  TEXT
  updated_at  TEXT
  started_at  TEXT  — set on status → running
  completed_at TEXT — set on status → done/failed
  worker_id   TEXT  — identity of claiming worker
```

Key indexes:
- `idx_jobs_status_priority (status, priority DESC, created_at ASC)` — poll queue
- `idx_jobs_chunk_id (chunk_id) WHERE chunk_id IS NOT NULL` — dedup lookup

#### 6.2 Summarization Job Flow

```
retrieve(query="...", lod="L1")
    │
    ├─ Candidate scoring (vector + BM25 + hybrid)
    ├─ For each result, check lod_chunks (object_uri, lod='L1')
    │     ├─ HIT (fresh): serve from lod_chunks directly
    │     ├─ HIT (stale): serve stale + enqueue summarize job
    │     └─ MISS: serve L0 fallback + enqueue summarize job
    └─ Return results with lod/lod_fresh fields

Enqueue logic:
    INSERT INTO jobs (id, job_type, status, priority, chunk_id, brain_id, payload, created_at, updated_at)
    VALUES (new_ulid(), 'summarize', 'pending', 3, chunk_id, brain_id,
            json('{"text": "<source>", "level": "L1"}'), now(), now())
    ON CONFLICT (chunk_id) WHERE status IN ('pending', 'running') DO NOTHING
```

**Dedup:** The `idx_jobs_chunk_id` partial index enables a check-then-insert pattern. If a summarize job for the same chunk is already pending or running, a new job is not enqueued.

**Worker flow:**
1. Worker polls: `SELECT * FROM jobs WHERE status='pending' ORDER BY priority DESC, created_at ASC LIMIT 1`
2. Worker claims: `UPDATE jobs SET status='running', started_at=now(), worker_id=me WHERE id=... AND status='pending'`
3. Worker generates summary (local model inference or extractive fallback).
4. Worker writes result to `lod_chunks`.
5. Worker closes job: `UPDATE jobs SET status='done', result=json('{"summary":"..."}'), completed_at=now() WHERE id=...`

#### 6.3 Consolidation Job Flow

Consolidation jobs sweep `lod_chunks` for expired or stale entries and trigger regeneration.

**Trigger:** Background daemon runs a consolidation sweep on schedule (default: every 24h) or when `brain consolidate` is invoked.

**Flow:**
```
Enqueue consolidate job:
    INSERT INTO jobs (id, job_type, status, priority, brain_id, created_at, updated_at)
    VALUES (new_ulid(), 'consolidate', 'pending', 4, brain_id, now(), now())

Worker:
    1. Fetch all lod_chunks WHERE expires_at < now() OR source_hash != current_source_hash
    2. For each stale row:
         a. Enqueue a summarize job at priority=4
         b. DO NOT delete the stale row — serve it with lod_fresh=false until replacement ready
    3. Update consolidate job → done
```

**Invariant:** Stale LOD representations are never deleted before their replacement is ready. The `lod_chunks` table always has a servable representation for any object that has been retrieved at least once.

#### 6.4 Error Handling and Retry

| Scenario | Behavior |
|----------|----------|
| Worker crashes mid-job | `status='running'` row with `started_at` older than 5m is reclaimed by next worker (claimed restart). |
| Model inference fails | Job set to `status='failed'`, `error` populated. Retrieve falls back to L0 indefinitely. |
| Consecutive failures | After 3 failures for the same chunk_id, no new summarize jobs are enqueued for that chunk. A `skip_summarize` flag (or tombstone row in `lod_chunks` with `method='failed'`) prevents infinite re-queuing. |
| Consolidate job fails | Logged; next consolidation cycle retries. Stale representations continue to be served. |

**Retry policy:** Failed jobs are not automatically retried. Re-enqueueing requires either: (a) a new `retrieve` call that detects a MISS and re-enqueues, or (b) an explicit `brain jobs retry <chunk_id>` CLI command (future work).

---

### 7. Tool Schema

Full MCP tool definition for `retrieve`:

```json
{
  "name": "retrieve",
  "description": "Unified retrieval tool supporting query mode (semantic search with LOD control) and URI mode (direct object access). Returns results with explainability fields, freshness signals, and optional per-signal score breakdowns. Replaces the search_minimal + expand pattern for new code. Backward-compatible: search_minimal and expand continue to function.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "Natural language query for semantic retrieval. Mutually exclusive with `uri`. If both are provided, `uri` takes precedence."
      },
      "uri": {
        "type": "string",
        "description": "Canonical synapse:// URI for direct object access. Format: synapse://{brain_name}/{domain}/{id}. Mutually exclusive with `query`."
      },
      "lod": {
        "type": "string",
        "enum": ["L0", "L1", "L2"],
        "default": "L0",
        "description": "Requested level of detail. L0: concise abstract (~100 tokens). L1: overview summary (~2000 tokens, async generated). L2: full original content (passthrough). If the requested level is not yet available, the best available level is returned with lod_fresh=false and generation is enqueued."
      },
      "count": {
        "type": "integer",
        "minimum": 1,
        "maximum": 50,
        "default": 10,
        "description": "Maximum number of results to return. Applies to query mode only; URI mode always returns exactly one result."
      },
      "strategy": {
        "type": "string",
        "enum": ["lookup", "planning", "reflection", "synthesis", "auto"],
        "default": "auto",
        "description": "Ranking weight profile. lookup: keyword-heavy. planning: recency + links. reflection: recency-heavy. synthesis: vector-heavy. auto: inferred from query structure."
      },
      "brain": {
        "type": "string",
        "description": "Target brain name or ID. Defaults to the current brain. For URI mode, the brain component of the URI takes precedence."
      },
      "metadata_filters": {
        "type": "object",
        "description": "Structured pre-filters applied before scoring.",
        "properties": {
          "time_scope": {
            "type": "string",
            "description": "Recency filter: 'recent_7d', 'recent_30d', 'recent_90d', or an ISO 8601 date string as a lower bound on created_at."
          },
          "tags": {
            "type": "array",
            "items": { "type": "string" },
            "description": "Tag filter (AND semantics). Each tag is expanded via synonym clusters before matching."
          },
          "kinds": {
            "type": "array",
            "items": {
              "type": "string",
              "enum": ["chunk", "episode", "reflection", "record", "derived_summary"]
            },
            "description": "Restrict results to the specified object kinds."
          }
        },
        "additionalProperties": false
      },
      "explain": {
        "type": "boolean",
        "default": false,
        "description": "When true, include per-signal score breakdowns in each result's explain field, and populate the diagnostics block."
      }
    },
    "additionalProperties": false,
    "oneOf": [
      { "required": ["query"] },
      { "required": ["uri"] }
    ]
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "results": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "uri":            { "type": "string" },
            "kind":           { "type": "string" },
            "lod":            { "type": "string", "enum": ["L0", "L1", "L2"] },
            "lod_fresh":      { "type": "boolean" },
            "title":          { "type": ["string", "null"] },
            "content":        { "type": "string" },
            "score":          { "type": ["number", "null"] },
            "strategy_used":  { "type": "string" },
            "generated_at":   { "type": ["string", "null"] },
            "source_uri":     { "type": "string" },
            "explain": {
              "type": ["object", "null"],
              "properties": {
                "vector_sim":         { "type": "number" },
                "bm25":               { "type": "number" },
                "recency":            { "type": "number" },
                "links":              { "type": "number" },
                "tag_match":          { "type": "number" },
                "importance":         { "type": "number" },
                "weights":            { "type": "string" },
                "fusion_confidence":  { "type": "number" }
              }
            }
          },
          "required": ["uri", "kind", "lod", "lod_fresh", "content", "strategy_used", "source_uri"]
        }
      },
      "diagnostics": {
        "type": ["object", "null"],
        "properties": {
          "vector_candidates":        { "type": "integer" },
          "fts_candidates":           { "type": "integer" },
          "union_size":               { "type": "integer" },
          "reranked":                 { "type": "boolean" },
          "query_time_ms":            { "type": "integer" },
          "lod_hits":                 { "type": "integer" },
          "lod_misses":               { "type": "integer" },
          "lod_generation_enqueued":  { "type": "integer" }
        }
      }
    },
    "required": ["results"]
  }
}
```

---

### 8. Dependency Graph

```
BRN-01KM5Z5.1  Remove flan-t5 dependency          [DONE]
    │
BRN-01KM5Z5.2  Jobs table (v31 migration)          [DONE]
    │                                               (schema at /tmp/wave1-jobs-schema.md)
    │
    ├──────────────────────────────────────────────────────────────────────────┐
    │                                                                          │
BRN-01KM99PC.4  Retrieve+ architecture spec         [THIS TASK]               │
    │                                                                          │
    ├── BRN-01KM99PC.5.1  lod_chunks table schema        ◄────────────────────┘
    │       └── BRN-01KM99PC.5.2  L0 extractive generation
    │               └── BRN-01KM99PC.5.3  L1 async generation (jobs integration)
    │                       └── BRN-01KM99PC.5.4  TTL + staleness detection
    │
    ├── BRN-01KM99PC.6.1  retrieve() orchestrator (routing + LOD dispatch)
    │       └── BRN-01KM99PC.6.2  query mode (hybrid search + LOD overlay)
    │               └── BRN-01KM99PC.6.3  URI mode (direct access + LOD)
    │                       └── BRN-01KM99PC.6.4  MCP tool registration + integration tests
    │
    ├── BRN-01KM99PC.7.1  Canonical metadata facets implementation
    │       └── BRN-01KM99PC.7.2  Synonym clustering spike (tag_clusters table)
    │
    ├── BRN-01KM99PC.9   Dual embedding spike (evaluate richer encoder)
    │       └── BRN-01KM99PC.10  Parallel embedding profiles (if spike recommends)
    │
    └── BRN-01KM99PC.11  Benchmark: retrieve vs. search_minimal+expand
```

**Legend:**
- `[DONE]` — task complete, output available
- `[THIS TASK]` — current document
- Arrows indicate dependency direction (← blocked by)

**Critical path:** `5.1 → 5.2 → 5.3 → 5.4 → 6.1 → 6.2 → 6.3 → 6.4`  
**Parallel tracks:** Metadata (7.1, 7.2), embedding spike (9, 10), and benchmark (11) can proceed concurrently with LOD storage.

**Note on naming:** The brain task IDs use the suffix pattern `BRN-01KM99PC.N` throughout this document. The resolved full task IDs are available in the brain task store.

---

## Alternatives Considered

### A. Extend `search_minimal` with LOD parameters

**Approach:** Add `lod` and `explain` fields to the existing `search_minimal` tool rather than introducing a new tool.

**Rejected because:** `search_minimal` was designed as a lightweight stub-return tool. Embedding LOD generation, URI routing, and async job dispatch into its semantics would violate its contract and break existing callers relying on its current response shape. A new surface avoids backward-compatibility hazards.

---

### B. Remove `search_minimal` and `expand` immediately

**Approach:** Deprecate and remove the existing tools once `retrieve` is implemented.

**Rejected because:** Agents (Drone Protocol, Sentinel, Probe) have hardcoded calls to `search_minimal`. Forced removal without a migration window would break deployed agents. Soft deprecation is the minimum viable compatibility strategy.

---

### C. Synchronous L1 generation on the retrieval hot path

**Approach:** Generate L1 summaries inline during retrieval, blocking the response until generation completes.

**Rejected because:** Local LLM inference takes 2–10 seconds per chunk depending on model size. Blocking the retrieval hot path on model inference is unacceptable for interactive agent workflows. The async-enqueue + L0-fallback pattern ensures the tool always responds in under 100ms.

---

### D. Always generate L0 and L1 at index time

**Approach:** Pre-compute both L0 and L1 representations for every chunk at indexing time, eliminating on-demand generation entirely.

**Rejected because:** L1 generation requires an LLM and is expensive per-chunk at scale. Pre-generating L1 for every indexed chunk — including low-priority or rarely-retrieved content — wastes compute. The lazy on-first-miss pattern generates L1 only for content agents actually retrieve, concentrating cost where value exists.

---

### E. Use remote embedding APIs

**Approach:** Replace BGE-small with a cloud-hosted embedding API (e.g., OpenAI `text-embedding-3-small`).

**Rejected because:** Brain is explicitly local-first (Non-Goal in §1.3). Remote APIs introduce latency, cost, and privacy concerns incompatible with the design constraints. The dual encoder spike (BRN-01KM99PC.9) evaluates richer local models only.

---

### F. Flat tag namespace without synonym clustering

**Approach:** Require agents to use exact tag strings; no normalization or clustering.

**Rejected because:** Agents and humans naturally use synonyms (`auth`, `authentication`, `token-auth`). Without clustering, retrieval precision degrades as vocabulary drifts. The soft label system normalizes this without requiring strict vocabulary enforcement.

---

## Open Questions

The following questions require resolution before implementation begins. They are ordered by blocking impact.

### OQ-1: L1 Summary Model Selection

**Question:** What local LLM or extractive method should generate L1 summaries?

**Context:** flan-t5 was removed (BRN-01KM5Z5.1). No replacement has been decided. Options:
- Rule-based extractive (first N sentences + key entities): no LLM dependency, lower quality.
- Small local model (e.g., Phi-3-mini, Mistral-7B-GGUF): higher quality, requires 4–8 GB RAM.
- Configurable per-brain: extractive default, LLM opt-in.

**Blocking:** BRN-01KM99PC.5.3 (L1 generation). Must be resolved before that task starts.

**Recommended resolution:** Configurable per-brain; extractive as default to keep Brain local-first.

---

### OQ-2: LOD Generation on the Critical Path

**Question:** Is it acceptable to return L0 fallback on L1 miss, or should some callers block on generation?

**Context:** The current design always enqueues async and returns L0. This means the first retrieval of a chunk at L1 always gets stale/fallback content. Some agent workflows (planning sessions, long-context synthesis) may prefer to wait 2–5 seconds for L1 rather than receive L0 and re-query.

**Options:**
- A: Always async. Return L0 + enqueue. Agent re-queries. (Current design.)
- B: Add a `wait_ms` parameter. If `wait_ms > 0`, block up to that duration for L1 generation.
- C: Pre-generate L0 and L1 at index time for all chunks. No on-demand generation.

**Blocking:** Affects the `retrieve` tool schema and orchestrator design (BRN-01KM99PC.6.1).

---

### OQ-3: `lod_chunks` Table Scope

**Question:** Should `lod_chunks` store LOD representations for all object types (chunks, episodes, reflections, records) or only for note chunks?

**Context:** The spec proposes `object_uri` as the key, which supports all types. However, records have payloads in the object store (BLAKE3-keyed), not in SQLite, making source hash computation different. Episodes and reflections are already summary-like — their "L1" may be trivially identical to their source content.

**Blocking:** BRN-01KM99PC.5.1 (schema design). The schema must handle this before storage is built.

---

### OQ-4: Synonym Cluster Update Frequency

**Question:** How often should tag synonym clustering run, and what triggers it?

**Context:** Embedding all tags and clustering is fast for small brains (<10k unique tags) but could be expensive for large ones. Options: weekly cron job, trigger on N new unique tags, or manual-only via CLI.

**Blocking:** BRN-01KM99PC.7.2 (clustering spike). Low blocking impact — clustering is a background concern.

---

### OQ-5: `retrieve` Tool Backward Compatibility Window

**Question:** What is the deprecation timeline for `search_minimal` and `expand`?

**Context:** This ADR positions `retrieve` as the replacement surface. However, agents (Drone Protocol, Sentinel, Probe) have hardcoded calls to `search_minimal`. Removing `search_minimal` without notice breaks deployed agents. The options are:
- Parallel forever (both tools remain, `search_minimal` never deprecated).
- Soft deprecation: `search_minimal` remains but `retrieve` is documented as preferred; agents migrate at their own pace.
- Hard deprecation: `search_minimal` removed in a major version bump after `retrieve` reaches parity.

**Blocking:** Does not block implementation, but should be decided before BRN-01KM99PC.6.4 (MCP tool registration) to determine whether `search_minimal` should be marked deprecated in its description string.

**Recommended resolution:** Soft deprecation. `search_minimal` and `expand` remain with a `deprecated: true` annotation in their tool descriptions. No removal until benchmark (BRN-01KM99PC.11) confirms parity.

---

*Blocks: BRN-01KM99PC.5.1, BRN-01KM99PC.6.1, BRN-01KM99PC.7.1, BRN-01KM99PC.7.2, BRN-01KM99PC.9, BRN-01KM99PC.11.*
