brain_name := env("BRAIN_NAME", file_name(justfile_directory()))
brain_home := env("BRAIN_HOME", env("HOME", "") + "/.brain")
brain_data := brain_home / "brains" / brain_name
log_dir := brain_home / "logs"

export BRAIN_MODEL_DIR := env("BRAIN_MODEL_DIR", brain_home / "models/bge-small-en-v1.5")
export BRAIN_DB := env("BRAIN_DB", brain_data / "lancedb")
export BRAIN_SQLITE_DB := env("BRAIN_SQLITE_DB", brain_data / "brain.db")

bin := "./target/release/brain"

default:
    @just --list

# ── Setup ─────────────────────────────────────────────────────────────────

# Full first-run: build, symlink, download models, install daemon
[group('setup')]
setup: install setup-models install-hooks
    @echo "==> Setup complete. Run 'brain daemon start' or 'just watch' to begin."

# Build release binary, symlink to ~/bin, install daemon service
[group('setup')]
install:
    BRAIN_FROM_SOURCE=true cargo build --release
    @mkdir -p ~/bin
    @ln -sf "{{justfile_directory()}}/target/release/brain" ~/bin/brain
    # BRAIN_COMPARATOR=1 arms the polymorphic-link-graph soak comparator on
    # the local daemon — your own brain is production for the soak window.
    # See docs/OPERATIONS.md § Polymorphic Link Graph Soak Comparator.
    BRAIN_COMPARATOR=1 {{bin}} daemon start
    @echo "==> Started brain daemon and symlinked binary to ~/bin/brain"

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

# Point git at the versioned hooks/ directory (works across worktrees)
[group('setup')]
install-hooks:
    git config core.hooksPath hooks
    @echo "==> Git hooks installed (core.hooksPath = hooks/)"

# Regenerate AGENTS.md + bridge CLAUDE.md from template
[group('dev')]
docs: ensure-binary
    {{bin}} docs

# ── Dev ───────────────────────────────────────────────────────────────────

# Compile debug build
[group('dev')]
build:
    cargo build

alias b := build

# Type-check without codegen. Covers `--workspace --all-features --all-targets`
# so #[cfg(test)] blocks inside production source files are also compile-checked
# — bare `cargo check` (default targets) silently skips them.
[group('dev')]
check:
    cargo check --workspace --all-features --all-targets

alias c := check

# Run tests via nextest (e.g. just t -p brain-lib, just t --no-fail-fast).
# Doctests are not executed by nextest — use `just test-doc` for those.
# Requires cargo-nextest: `cargo install --locked cargo-nextest`.

test *args:
    cargo nextest run {{args}}

alias t := test

# Run doctests (nextest cannot execute these)
[group('dev')]
test-doc *args:
    cargo test --doc {{args}}

# Run fd-heavy perf tests serially with raised fd limit
[group('dev')]
test-perf:
    ulimit -n 4096 && cargo nextest run -p brain-lib --test perf_tests --run-ignored only --test-threads 1

# Check formatting + run clippy
[group('dev')]
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-features -- -D warnings

alias l := lint

# Auto-format all code
[group('dev')]
fmt:
    cargo fmt --all

# Architectural ratchet for brain_rpc (saga-5df / brn-2fe.25).
#
# Verifies that brain_rpc remains a wire-contract crate with zero leakage
# from DB / persistence / domain crates, and that the "inside" of the hex
# (domain.rs, transport.rs, client.rs, testing.rs) contains zero raw I/O
# imports. Adapters (unix.rs, the StdProcessSpawner in spawner.rs) are
# *allowed* to use std::io / std::os — they live at the edge of the hex.
#
# Run before every brain_rpc commit and as a CI gate. See
# .omc/prd.json for the architectural ratchet's full rule set.
[group('dev')]
audit-rpc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> brain_rpc architectural gates"
    forbidden_re='use (rusqlite|lancedb|candle|brain_persistence|brain_lib|brain_tasks|brain_sagas|brain_records|brain_tags|brain_retrieval|brain_embedder)::'
    # architecture.rs is the programmatic version of this very gate — it
    # contains the forbidden crate names as data (string literals and
    # rustdoc examples) but never imports them. The Rust test skips
    # itself in its walk; the bash recipe does the same here via
    # --exclude. If you ever rename architecture.rs, update both.
    if grep -rE "$forbidden_re" crates/brain_rpc/src/ crates/brain_rpc/tests/ --exclude=architecture.rs 2>/dev/null; then
        echo "FAIL: forbidden import found in brain_rpc — see lines above"
        exit 1
    fi
    echo "  ok: no forbidden imports"
    io_re='use (std::io|std::os|UnixStream|TcpStream|BufRead|BufWriter|std::net|std::process)'
    port_files=(
        crates/brain_rpc/src/domain.rs
        crates/brain_rpc/src/transport.rs
        crates/brain_rpc/src/client.rs
        crates/brain_rpc/src/testing.rs
    )
    for f in "${port_files[@]}"; do
        if [ -f "$f" ] && grep -E "$io_re" "$f" 2>/dev/null; then
            echo "FAIL: port-layer file $f leaks I/O imports — see lines above"
            exit 1
        fi
    done
    echo "  ok: port-layer files (domain/transport/client/testing) have no I/O imports"
    forbidden_dep_re='^(rusqlite|lancedb|candle|brain[_-](persistence|lib|tasks|sagas|records|tags|retrieval|embedder))'
    if cargo tree -p brain-rpc -e normal --prefix none 2>/dev/null | grep -E "$forbidden_dep_re"; then
        echo "FAIL: forbidden transitive dep on brain-rpc — see lines above"
        exit 1
    fi
    echo "  ok: cargo tree shows no forbidden transitive deps"
    echo "==> All brain_rpc architectural gates passed."

# Architectural ratchet for brain_daemon (saga-5df / brn-2fe.26).
#
# Verifies that brain_daemon stays a thin RPC-server crate with zero
# leakage from DB / persistence / domain crates, and that the "inside"
# of the hex (config.rs, dispatcher.rs) contains zero raw I/O imports.
# Adapters (server.rs, main.rs, future services/*) are *allowed* to
# use std::io / std::os / std::process — they live at the edge of the
# hex.
#
# Run before every brain_daemon commit and as a CI gate. The list of
# forbidden upstream crates intentionally mirrors audit-rpc so a future
# crate split (brain-mcp etc.) inherits the same discipline.
[group('dev')]
audit-daemon:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> brain_daemon architectural gates"
    forbidden_re='use (rusqlite|lancedb|candle|brain_persistence|brain_lib|brain_tasks|brain_sagas|brain_records|brain_tags|brain_retrieval|brain_embedder)::'
    # architecture.rs is the programmatic version of this very gate — it
    # contains the forbidden crate names as data but never imports them.
    # The Rust test skips itself in its walk; the bash recipe does the
    # same here via --exclude. If you ever rename architecture.rs,
    # update both.
    if grep -rE "$forbidden_re" crates/brain_daemon/src/ crates/brain_daemon/tests/ --exclude=architecture.rs 2>/dev/null; then
        echo "FAIL: forbidden import found in brain_daemon — see lines above"
        exit 1
    fi
    echo "  ok: no forbidden imports"
    io_re='use (std::io|std::os|UnixStream|TcpStream|BufRead|BufWriter|std::net|std::process)'
    port_files=(
        crates/brain_daemon/src/config.rs
        crates/brain_daemon/src/dispatcher.rs
    )
    for f in "${port_files[@]}"; do
        if [ -f "$f" ] && grep -E "$io_re" "$f" 2>/dev/null; then
            echo "FAIL: port-layer file $f leaks I/O imports — see lines above"
            exit 1
        fi
    done
    echo "  ok: port-layer files (config/dispatcher) have no I/O imports"
    forbidden_dep_re='^(rusqlite|lancedb|candle|brain[_-](persistence|lib|tasks|sagas|records|tags|retrieval|embedder))'
    if cargo tree -p brain-daemon -e normal --prefix none 2>/dev/null | grep -E "$forbidden_dep_re"; then
        echo "FAIL: forbidden transitive dep on brain-daemon — see lines above"
        exit 1
    fi
    echo "  ok: cargo tree shows no forbidden transitive deps"
    echo "==> All brain_daemon architectural gates passed."

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
    BRAIN_COMPARATOR=1 {{bin}} daemon {{args}}

alias d := daemon

# Manage agent plugins: install | uninstall [--target claude|codex] [--dry-run]
[group('app')]
plugin +args:
    cargo run --bin brain -- plugin {{args}}

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

[group('maintenance')]
log-audit top_n="20":
    @if ! ls {{log_dir}}/brain.*.log >/dev/null 2>&1; then \
        echo "No logs found at {{log_dir}}/brain.*.log"; exit 1; \
    fi
    @echo "==> Auditing {{log_dir}}/brain.*.log"
    @head -1 {{log_dir}}/brain.*.log 2>/dev/null | grep -q '^[0-9]' || { \
        echo "Logs appear non-plain-text (JSON?). Audit only supports the default text format."; exit 1; \
    }
    @echo
    @echo "── Lines per level ─────────────────────────"
    @awk '/^[0-9]/ { count[$2]++ } END { for (l in count) printf "%-6s %8d\n", l, count[l] }' {{log_dir}}/brain.*.log | sort -k2 -rn
    @echo
    @echo "── Top {{top_n}} emitters (level, target) ──"
    @awk '/^[0-9]/ { sub(/:$/, "", $3); print $2, $3 }' {{log_dir}}/brain.*.log | sort | uniq -c | sort -rn | head -n {{top_n}}
    @echo
    @echo "Demotion candidates: INFO/DEBUG targets with high counts."
    @echo "Update DEFAULT_LOG_FILTER in crates/brain_lib/src/config/mod.rs."

# ── Architecture ──────────────────────────────────────────────────────────

# Verify brain-lib has no direct rusqlite/lancedb deps (persistence boundary check)
[group('dev')]
check-deps:
    @cargo tree -p brain-lib --depth 1 2>/dev/null | grep -qE 'lancedb|arrow-schema|arrow-array' \
        && echo 'FAIL: brain-lib has direct lancedb/arrow deps' && exit 1 \
        || echo 'OK: brain-lib persistence boundary intact (lancedb/arrow removed; rusqlite deferred)'

# ── Release ───────────────────────────────────────────────────────────────

# Bump version, update changelog, commit, tag, and push (patch|minor|major)
[group('release')]
tag level:
    ./scripts/tag-release.sh {{level}}
    git push origin master --tags
