export BRAIN_MODEL_DIR := env("BRAIN_MODEL_DIR", "./.brain/models/bge-small-en-v1.5")
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
query query_text top_k="5": ensure-binary
    {{bin}} query "{{query_text}}" -k {{top_k}}

[group('maintenance')]
clean:
    cargo clean

[group('app')]
watch notes_path=".": ensure-binary
    {{bin}} watch {{notes_path}}

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
