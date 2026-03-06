export BRAIN_MODEL_DIR := env("BRAIN_MODEL_DIR", env("HOME", "") + "/.brain/models/bge-small-en-v1.5")
export BRAIN_DB := env("BRAIN_DB", "./.brain/lancedb")
export BRAIN_SQLITE_DB := env("BRAIN_SQLITE_DB", "./.brain/brain.db")

bin := "./target/debug/brain"

default:
    @just --list

[group('setup')]
setup-model:
    ./scripts/setup-model.sh

[group('dev')]
build:
    cargo build

[group('dev')]
check:
    cargo check

[group('dev')]
test *args:
    cargo test {{args}}

[group('dev')]
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace -- -D warnings

[group('dev')]
fmt:
    cargo fmt --all

[group('dev')]
fmt-check:
    cargo fmt --all -- --check

[private]
ensure-binary:
    @test -f {{bin}} || cargo build --bin brain

[group('app')]
index notes_path=".": ensure-binary
    {{bin}} index {{notes_path}}

[group('app')]
query query_text top_k="5" intent="auto" *args: ensure-binary
    {{bin}} query "{{query_text}}" -k {{top_k}} -i {{intent}} {{args}}

# Query with per-signal score breakdown
[group('app')]
query-verbose query_text top_k="5" intent="auto": ensure-binary
    {{bin}} query "{{query_text}}" -k {{top_k}} -i {{intent}} --verbose

# Force re-index all files (clears content hashes, re-embeds everything)
[group('maintenance')]
reindex notes_path=".": ensure-binary
    {{bin}} reindex --full {{notes_path}}

# Re-index a single file
[group('maintenance')]
reindex-file path: ensure-binary
    {{bin}} reindex --file {{path}}

# Run health checks on the index
[group('maintenance')]
doctor notes_path=".": ensure-binary
    {{bin}} doctor {{notes_path}}

# Compact and reclaim space (SQLite VACUUM + LanceDB optimize + purge deleted)
[group('maintenance')]
vacuum *args: ensure-binary
    {{bin}} vacuum {{args}}

[group('maintenance')]
clean:
    cargo clean

[group('app')]
watch notes_path=".": ensure-binary
    {{bin}} watch {{notes_path}}

# Watch with JSON structured logs written to .brain/brain.log
[group('app')]
watch-log notes_path=".": ensure-binary
    @mkdir -p .brain
    BRAIN_LOG_FORMAT=json {{bin}} watch {{notes_path}} 2>.brain/brain.log

# Available actions: start, stop, status. e.g. "just daemon start ./notes"
[group('app')]
daemon +args: ensure-binary
    {{bin}} daemon {{args}}

[group('app')]
import-beads *args: ensure-binary
    {{bin}} import-beads {{args}}

[group('app')]
tasks-export *args: ensure-binary
    {{bin}} tasks export {{args}}

# Full pipeline: beads → brain (import) → markdown export
[group('app')]
import-all *args: ensure-binary
    {{bin}} import-beads
    {{bin}} tasks export

[group('maintenance')]
clean-db:
    rm -rf .brain/lancedb .brain/brain.db .brain/brain.db-shm .brain/brain.db-wal

# Preview changelog output (stdout only)
[group('release')]
changelog:
    git-cliff

# Update CHANGELOG.md in-place
[group('release')]
changelog-update:
    git-cliff -o CHANGELOG.md

# Bump version, update changelog, commit and tag (patch|minor|major)
[group('release')]
tag level:
    ./scripts/tag-release.sh {{level}}

# Show what cargo-dist will build: lists target platforms, installers,
# and which binaries get included in the release. Useful for verifying
# your dist-workspace.toml config without actually building anything.
[group('release')]
dist-plan:
    cargo dist plan

# Build release artifacts (tarballs, installers) for your current machine.
# This is a local dry run — it produces the same archives that CI would
# upload to a GitHub Release, but only for your native platform.
[group('release')]
dist-build:
    cargo dist build

# Build release artifacts as if you were releasing a specific version tag.
# Useful to test what a tagged release (e.g. v0.2.0) would produce
# without actually creating the git tag.
[group('release')]
dist-build-tag tag:
    cargo dist build --tag {{tag}}

# Build then inspect dynamic library linkage of the resulting binary.
# Shows which .dylib/.so files the binary depends on at runtime —
# important for verifying the binary is portable across machines.
[group('release')]
dist-linkage:
    cargo dist build && cargo dist linkage --print-output

# Regenerate .github/workflows/release.yml from dist-workspace.toml.
# Run this after changing dist-workspace.toml to keep the CI workflow
# in sync with your distribution config.
[group('release')]
dist-generate:
    cargo dist generate
