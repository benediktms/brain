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
    cargo run --bin brain -- query {{query_text}} -k {{top_k}}

[group('maintenance')]
clean:
    cargo clean

[group('maintenance')]
clean-db:
    rm -rf brain_lancedb
