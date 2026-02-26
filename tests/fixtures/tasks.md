# Project Roadmap

Current priorities and open work items for the knowledge base project.

## Phase 0: Proof of Concept

- [x] Set up Cargo workspace with core dependencies #infrastructure
- [x] Implement vault scanner to find Markdown files #scanner
- [x] Build double-newline and heading chunker #chunker
- [x] Load BGE-small embedding model via Candle #embeddings
- [x] Create LanceDB store with insert and query #storage
- [ ] Write smoke tests for the POC pipeline #testing @due(2026-03-01)
- [ ] Set up CI with cargo check, test, and clippy #infrastructure @due(2026-03-05)

## Phase 1: Incremental Updates

- [ ] Add SQLite for file identity and content hashing #storage @due(2026-03-15)
- [ ] Implement BLAKE3 hash gate to skip unchanged files #performance
- [ ] Build file watcher with debounced event handling #watcher @due(2026-03-20)
- [ ] Wire the full incremental sync pipeline #integration @due(2026-03-30)
- [ ] Write integration tests for upsert correctness #testing

## Backlog

- [ ] Investigate cross-encoder reranking for improved precision #research
- [ ] Add support for multiple brain registrations #feature
- [ ] Explore MCP stdio server for agent integration #mcp
