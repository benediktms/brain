brain_name := env("BRAIN_NAME", file_name(justfile_directory()))
brain_home := env("BRAIN_HOME", env("HOME", "") + "/.brain")
brain_data := brain_home / "brains" / brain_name

export BRAIN_MODEL_DIR := env("BRAIN_MODEL_DIR", brain_home / "models/bge-small-en-v1.5")
export BRAIN_DB := env("BRAIN_DB", brain_data / "lancedb")
export BRAIN_SQLITE_DB := env("BRAIN_SQLITE_DB", brain_data / "brain.db")

bin := "./target/release/brain"

default:
    @just --list

# ── Setup ─────────────────────────────────────────────────────────────────

# Full first-run: build, symlink, download models, install daemon
[group('setup')]
setup: install setup-models
    @echo "==> Setup complete. Run 'brain daemon start' or 'just watch' to begin."

# Build release binary, symlink to ~/bin, install daemon service
[group('setup')]
install:
    cargo build --release
    @mkdir -p ~/bin
    @ln -sf "{{justfile_directory()}}/target/release/brain" ~/bin/brain
    {{bin}} daemon install
    @echo "==> Installed brain to ~/bin/brain with daemon service"

# Download all ML models (embedder + summarizer)
[group('setup')]
setup-models:
    ./scripts/setup-model.sh

# Remove symlink, uninstall daemon, clean build artifacts
[group('setup')]
uninstall:
    {{bin}} daemon uninstall || true
    @rm -f ~/bin/brain
    cargo clean
    @echo "==> Uninstalled brain"

# ── Dev ───────────────────────────────────────────────────────────────────

# Compile debug build
[group('dev')]
build:
    cargo build

alias b := build

# Type-check without codegen
[group('dev')]
check:
    cargo check

alias c := check

# Run tests (e.g. just t -p brain-lib, just t -- --nocapture)
[group('dev')]
test *args:
    cargo test {{args}}

alias t := test

# Check formatting + run clippy
[group('dev')]
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace -- -D warnings

alias l := lint

# Auto-format all code
[group('dev')]
fmt:
    cargo fmt --all

# ── App ───────────────────────────────────────────────────────────────────

[private]
ensure-binary:
    @test -f {{bin}} || cargo build --release --bin brain

# Index markdown files into the vector store
[group('app')]
index notes_path=".": ensure-binary
    {{bin}} index {{notes_path}}

alias idx := index

# Search current brain (intent: auto|lookup|planning|reflection|synthesis, --verbose)
[group('app')]
query query_text top_k="5" intent="auto" *args: ensure-binary
    {{bin}} query "{{query_text}}" -k {{top_k}} -i {{intent}} {{args}}

alias q := query
alias search := query

# Search all registered brains (intent: auto|lookup|planning|reflection|synthesis, --verbose)
[group('app')]
query-all query_text top_k="5" intent="auto" *args: ensure-binary
    {{bin}} query "{{query_text}}" -k {{top_k}} -i {{intent}} --brain all {{args}}

alias qa := query-all

# Watch directory for changes and re-index incrementally
[group('app')]
watch notes_path=".": ensure-binary
    {{bin}} watch {{notes_path}}

alias w := watch

# Manage background daemon: start | stop | status | install | uninstall
[group('app')]
daemon +args: ensure-binary
    {{bin}} daemon {{args}}

alias d := daemon

# ── Maintenance ───────────────────────────────────────────────────────────

# Run health checks on the index
[group('maintenance')]
doctor notes_path=".": ensure-binary
    {{bin}} doctor {{notes_path}}

# Compact and reclaim space (--older-than <days>, default: 30)
[group('maintenance')]
vacuum *args: ensure-binary
    {{bin}} vacuum {{args}}

# Force re-index all files
[group('maintenance')]
reindex notes_path=".": ensure-binary
    {{bin}} reindex --full {{notes_path}}

# Re-index a single file
[group('maintenance')]
reindex-file path: ensure-binary
    {{bin}} reindex --file {{path}}

# ── Release ───────────────────────────────────────────────────────────────

# Bump version, update changelog, commit and tag (patch|minor|major)
[group('release')]
tag level:
    ./scripts/tag-release.sh {{level}}
