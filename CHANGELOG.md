# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Bug Fixes

- Eager delete orphaned chunks
- TOCTOU race condition for eager and redundant file reads
- Fall-through case for 0 row tables in index store
- Make rename_file async so it updates both SQLite an LanceDB
- Return proper error to avoid daemon panics
- Added content hash check to the resurrect indexing

### Documentation

- Document decision to decouple notes and tasks architecture
- Add math and compsci references
- Describe inital considerations for performance strategies
- Add initial project scope documentation

### Features

- Hybrid retrieval for agentic memory (#1)
- Process file modification events
- Add proper file ID validation for store operations
- Dual-store-arch-impl
- Workspace-setup

### Performance

- Check for debug target when invoking query command from justfile

### Testing

- Add smoke tests for current chunk, embed and hash-gate pipeline
- Add-testing-fixtures

