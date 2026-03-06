# Changelog

All notable changes to this project will be documented in this file.

## [0.1.2] - 2026-03-06

### Features

- Improve init setup and MCP server resilience (#28)

## [0.1.1] - 2026-03-06

### Bug Fixes

- Scan blocking and error metrics (#19)
- Eager delete orphaned chunks
- TOCTOU race condition for eager and redundant file reads
- Fall-through case for 0 row tables in index store
- Make rename_file async so it updates both SQLite an LanceDB
- Return proper error to avoid daemon panics
- Added content hash check to the resurrect indexing

### Documentation

- Update readme (#18)
- Update retrieval policy graph in architecture reference
- Document decision to decouple notes and tasks architecture
- Add math and compsci references
- Describe inital considerations for performance strategies
- Add initial project scope documentation

### Features

- Resolve config path from cwd (#27)
- Add brain inti command (#26)
- Dogfooding prep (#25)
- Implement lanceDB schema versioning (#22)
- Add reindex, vacuum, and doctor CLI commands
- Wire cli hybrid ranking (#21)
- Add bounded work queue with file_id coalescing (#20)
- Add runtime observability metrics and structured logging (#16)
- Separate read/write SQLite connections and add StoreReader
- Lancedb dual trigger optimize scheduler (#13)
- Migrate task id to prefixed ulid (#12)
- Implement get and list tools for tasks (#8)
- Task related cli tooling (#7)
- Implement batched embedding with backpressure (#6)
- Add initial MCP task tooling (#4)
- Event sourced task system (#2)
- Hybrid retrieval for agentic memory (#1)
- Process file modification events
- Add proper file ID validation for store operations
- Dual-store-arch-impl
- Workspace-setup

### Performance

- Use candle accelerate for macos (#15)
- Check for debug target when invoking query command from justfile

### Refactoring

- Codebase cleanup and reconsolidating (#11)

### Testing

- Add byte-offset correctness tests for chunker and parser (#24)
- Add migration test harness with schema snapshots (#23)
- Perf and concurrency test (#17)
- Add smoke tests for current chunk, embed and hash-gate pipeline
- Add-testing-fixtures

