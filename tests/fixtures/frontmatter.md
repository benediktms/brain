---
title: "Decision: Choosing LanceDB as the Vector Store"
tags: [architecture, vector-database, lancedb, decision]
date: 2026-01-15
aliases: [vector-store-decision, lancedb-choice]
status: accepted
---

# Choosing LanceDB Over Alternatives

We evaluated three vector storage options for the personal knowledge base: LanceDB, Qdrant, and SQLite with a custom vector extension. The decision came down to operational simplicity, Rust compatibility, and the upsert story.

## Why LanceDB

LanceDB is an embedded, Arrow-native vector database written in Rust. It stores data on local disk with no server process, which aligns with our local-first constraint. The key differentiator is its `merge_insert` API, which provides true upsert semantics: matched rows are updated, unmatched source rows are inserted, and unmatched target rows scoped to a file can be deleted. This maps directly to our per-file re-indexing pattern.

## Why Not Qdrant

Qdrant is a full-featured vector database but runs as a separate server process. For a single-user laptop tool, managing a background Qdrant instance adds operational complexity with no benefit. Qdrant's Rust client is also an HTTP client, which introduces network overhead for what should be a local function call.

## Why Not SQLite with Vector Extensions

SQLite-vec and similar extensions keep everything in a single database file, which is appealing. However, the vector search performance degrades without careful tuning, and the extension ecosystem is still maturing. We already use SQLite for metadata and full-text search, so separating the vector concern into LanceDB gives us a cleaner architecture with purpose-built indexing.

## Risks and Mitigations

LanceDB is relatively young. The main risk is API instability between releases. We mitigate this by pinning the version in Cargo.toml and wrapping all LanceDB calls behind a thin store abstraction so that swapping the backend later is feasible without rewriting the indexing pipeline.
