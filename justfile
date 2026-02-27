export BRAIN_MODEL_DIR := env("BRAIN_MODEL_DIR", "./.brain/models/bge-small-en-v1.5")
export BRAIN_DB := env("BRAIN_DB", "./.brain/lancedb")
export BRAIN_SQLITE_DB := env("BRAIN_SQLITE_DB", "./.brain/brain.db")

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
    cargo clippy --workspace -- -D warnings

[group('dev')]
fmt:
    cargo fmt --all

[group('dev')]
fmt-check:
    cargo fmt --all -- --check

[group('app')]
index notes_path=".":
    cargo run --bin brain -- index {{notes_path}}

[group('app')]
query query_text top_k="5":
    @test -f ./target/debug/brain || cargo build --bin brain
    ./target/debug/brain query "{{query_text}}" -k {{top_k}}

[group('maintenance')]
clean:
    cargo clean

[group('app')]
watch notes_path=".":
    cargo run --bin brain -- watch {{notes_path}}

# Available actions: start, stop, status. e.g. "just daemon start ./notes"
[group('app')]
daemon +args:
    cargo run --bin brain -- daemon {{args}}

[group('maintenance')]
clean-db:
    rm -rf .brain/lancedb .brain/brain.db
