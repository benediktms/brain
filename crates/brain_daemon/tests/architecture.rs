//! Programmatic architecture gate for `brain_daemon`. Runs with
//! `cargo test`, so it executes in CI without anyone having to
//! remember `just audit-daemon`.
//!
//! Two rules:
//!
//! 1. No file under `crates/brain_daemon/src/` or `tests/` imports
//!    rusqlite, lancedb, candle, or any brain_* domain crate. The MVP
//!    daemon must stay decoupled from internal storage shapes.
//!
//! 2. Port-layer files (`config.rs`, `dispatcher.rs`) contain no
//!    concrete I/O imports (`std::io`, `std::os`, `std::process`,
//!    `std::net`). Adapter files (`server.rs`, `main.rs`) are exempt
//!    by design — they live at the edge of the hexagon.
//!
//! Mirrors `crates/brain_rpc/tests/architecture.rs` — same parser,
//! same gate shape, different file list and dependency list.

use std::fs;
use std::path::{Path, PathBuf};

const CRATE_ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// Crates whose types may never appear in brain_daemon's import surface.
///
/// As of the BrainStoresDispatcher landing, brain_lib / brain_persistence
/// / brain_tasks are ALLOWED — they're used inside handlers.rs to map
/// requests onto real domain calls. brain_records joined the allowed
/// set when the records / analyses / artifacts / documents / plans /
/// snapshots wire surface landed (the dispatcher needs the integrity
/// API + typed Record domain types for anti-corruption mapping).
/// Direct rusqlite / lancedb / candle imports remain forbidden (those
/// belong inside brain_persistence and brain_embedder), as do the
/// not-yet-extracted domain crates whose types haven't earned a place
/// on the daemon's request surface yet.
const FORBIDDEN_CRATES: &[&str] = &[
    "rusqlite",
    "lancedb",
    "candle",
    "brain_sagas",
    "brain_tags",
    "brain_retrieval",
    "brain_embedder",
];

/// Concrete-I/O modules disallowed in port-layer files only.
const FORBIDDEN_IO_PREFIXES: &[&str] = &["std::io", "std::os", "std::process", "std::net"];

/// The "inside" of the hexagon — files that must stay free of
/// concrete I/O. server.rs and main.rs are adapters and are excluded.
const PORT_LAYER_FILES: &[&str] = &["config.rs", "dispatcher.rs"];

fn rust_files_in(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return out,
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            out.extend(rust_files_in(&p));
        } else if p.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(p);
        }
    }
    out
}

/// Same parser as brain_rpc's architecture test — keeps both gates in
/// sync on how they identify `use` lines (skipping comments and false
/// positives like `let x = use_count();`).
fn first_use_segment(line: &str) -> Option<&str> {
    let rest = line.trim_start().strip_prefix("use ")?;
    let end = rest
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    if end == 0 { None } else { Some(&rest[..end]) }
}

#[test]
fn no_forbidden_crate_imports_in_src_or_tests() {
    let crate_root = Path::new(CRATE_ROOT);
    let mut violations = Vec::new();

    for subdir in ["src", "tests"] {
        let dir = crate_root.join(subdir);
        for file in rust_files_in(&dir) {
            // Skip this file itself — it lists the forbidden crate
            // names as data (in `&[&str]` constants) but never imports
            // them.
            if file.file_name().and_then(|n| n.to_str()) == Some("architecture.rs") {
                continue;
            }
            let content = match fs::read_to_string(&file) {
                Ok(c) => c,
                Err(_) => continue,
            };
            for (idx, line) in content.lines().enumerate() {
                if let Some(first) = first_use_segment(line) {
                    if FORBIDDEN_CRATES.contains(&first) {
                        violations.push(format!("{}:{}: {}", file.display(), idx + 1, line.trim()));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "\n\nForbidden crate imports found in brain_daemon:\n  {}\n\nDuring the MVP, brain_daemon must stay decoupled from internal storage. Real handlers backed by brain_lib / BrainStores land in a follow-up ticket — see crate-level rustdoc.\n",
        violations.join("\n  ")
    );
}

#[test]
fn port_layer_files_have_no_io_imports() {
    let src = Path::new(CRATE_ROOT).join("src");
    let mut violations = Vec::new();
    let mut files_checked = 0;

    for file_name in PORT_LAYER_FILES {
        let file = src.join(file_name);
        if !file.exists() {
            // Story not landed yet — skip. The architecture gate
            // catches accidental violations, not missing files.
            continue;
        }
        files_checked += 1;
        let content = fs::read_to_string(&file).expect("read port-layer file");
        for (idx, line) in content.lines().enumerate() {
            let trimmed = line.trim_start();
            for prefix in FORBIDDEN_IO_PREFIXES {
                let needle = format!("use {prefix}");
                if trimmed.starts_with(&needle) {
                    violations.push(format!("{}:{}: {}", file.display(), idx + 1, line.trim()));
                }
            }
        }
    }

    assert!(
        files_checked > 0,
        "no port-layer files found under {} — test scope misconfigured",
        src.display()
    );
    assert!(
        violations.is_empty(),
        "\n\nI/O imports found in port-layer files:\n  {}\n\nPort-layer files ({}) MUST stay free of concrete I/O. I/O lives in server.rs / main.rs (adapter files), which are exempt from this rule by design.\n",
        violations.join("\n  "),
        PORT_LAYER_FILES.join(", ")
    );
}

#[test]
fn cargo_toml_has_no_forbidden_direct_dependencies() {
    // Companion gate to the source-grep check above. brain_daemon's
    // audit-daemon recipe deliberately dropped its cargo-tree gate
    // when brain-lib became a dep (transitives include rusqlite /
    // lancedb / candle), so the architectural ratchet relies on the
    // source grep PLUS this gate to catch a regression where someone
    // adds `rusqlite = "..."` directly to [dependencies] without
    // ever `use rusqlite::...`-ing it. Dev-deps / build-deps are
    // excluded by design.
    let manifest_path = Path::new(CRATE_ROOT).join("Cargo.toml");
    let raw = fs::read_to_string(&manifest_path).expect("read Cargo.toml");
    let parsed: toml::Value = raw.parse().expect("Cargo.toml is valid TOML");

    let deps = parsed
        .get("dependencies")
        .and_then(|v| v.as_table())
        .expect("Cargo.toml has [dependencies] table");

    let mut violations = Vec::new();
    for (key, val) in deps {
        // A renamed dep (`alias = { package = "real-name", ... }`) puts
        // the real crate name in the `package` field. Check BOTH so an
        // alias can't smuggle a forbidden crate past the gate.
        let normalized_key = key.replace('-', "_");
        let normalized_package = val
            .as_table()
            .and_then(|t| t.get("package"))
            .and_then(|p| p.as_str())
            .map(|p| p.replace('-', "_"));

        let forbidden = FORBIDDEN_CRATES.contains(&normalized_key.as_str())
            || normalized_package
                .as_deref()
                .is_some_and(|p| FORBIDDEN_CRATES.contains(&p));
        if forbidden {
            violations.push(key.clone());
        }
    }

    assert!(
        violations.is_empty(),
        "\n\nForbidden DIRECT dependencies in brain_daemon/Cargo.toml [dependencies]: {:?}\n\nbrain_daemon allows brain-lib / brain-persistence / brain-tasks (for real handlers backed by BrainStores), but rusqlite / lancedb / candle and the not-yet-allowed brain_* domain crates remain forbidden directly. If a forbidden crate is genuinely needed, relax FORBIDDEN_CRATES in this file AND in the audit-daemon recipe at the same time.\n",
        violations
    );
}

#[test]
fn first_use_segment_handles_expected_forms() {
    // Mirror brain_rpc's parser tests — both gates share the same parser
    // logic and break together if it regresses.
    assert_eq!(
        first_use_segment("use rusqlite::Connection;"),
        Some("rusqlite")
    );
    assert_eq!(first_use_segment("use rusqlite;"), Some("rusqlite"));
    assert_eq!(first_use_segment("use rusqlite as r;"), Some("rusqlite"));
    assert_eq!(first_use_segment("    use std::io::Read;"), Some("std"));
    assert_eq!(first_use_segment("// use rusqlite::Connection;"), None);
    assert_eq!(first_use_segment("//! use rusqlite::Connection;"), None);
    assert_eq!(first_use_segment("let x = use_count();"), None);
    assert_eq!(first_use_segment(""), None);
}
