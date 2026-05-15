//! Programmatic architecture gate. Runs as part of `cargo test`, which
//! means it runs in CI without anyone needing to remember to invoke
//! `just audit-rpc` separately.
//!
//! Two enforced rules:
//!
//! 1. No file under `crates/brain_rpc/src/` or `crates/brain_rpc/tests/`
//!    imports from rusqlite, lancedb, candle, or any brain_* domain crate.
//!    `brain_rpc` is the wire-protocol contract crate — coupling it to
//!    internal storage shapes breaks the protocol on every refactor.
//!
//! 2. Port-layer files (`domain.rs`, `transport.rs`, `client.rs`,
//!    `testing.rs`) contain no concrete I/O imports
//!    (`std::io`, `std::os`, `std::process`, `std::net`). Adapter
//!    files (`unix.rs`, `spawner.rs`) are allowed to use these by
//!    design — they live at the edge of the hexagon.
//!
//! If you ever need to relax these rules, update the test and the
//! crate-level rustdoc together, and bump PROTOCOL_VERSION if the
//! relaxation would change wire shape.

use std::fs;
use std::path::{Path, PathBuf};

const CRATE_ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// Crates whose types may never appear in brain_rpc's import surface.
const FORBIDDEN_CRATES: &[&str] = &[
    "rusqlite",
    "lancedb",
    "candle",
    "brain_persistence",
    "brain_lib",
    "brain_tasks",
    "brain_sagas",
    "brain_records",
    "brain_tags",
    "brain_retrieval",
    "brain_embedder",
];

/// Concrete-I/O modules disallowed in port-layer files only. Adapters
/// (unix.rs, spawner.rs) are I/O-heavy by intent.
const FORBIDDEN_IO_PREFIXES: &[&str] = &[
    "std::io",
    "std::os",
    "std::process",
    "std::net",
];

/// The hexagon's "inside" — files that must stay free of concrete I/O.
const PORT_LAYER_FILES: &[&str] = &["domain.rs", "transport.rs", "client.rs", "testing.rs"];

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

/// Return the first segment after `use ` on a line, if the line is a
/// `use` statement. Examples:
/// - `use rusqlite::Connection;` -> Some("rusqlite")
/// - `use rusqlite as r;` -> Some("rusqlite")
/// - `// use rusqlite::...` (comment) -> None (trim_start doesn't match "use ")
/// - `let x = use_count();` -> None (doesn't start with "use ")
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
            // Skip this file itself — it lists the forbidden crates as
            // identifiers, but never imports them.
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
                        violations.push(format!(
                            "{}:{}: {}",
                            file.display(),
                            idx + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "\n\nForbidden crate imports found in brain_rpc:\n  {}\n\nbrain_rpc is the wire-protocol contract crate. It must never import from rusqlite, lancedb, candle, or any brain_* domain crate — coupling the protocol to internal storage shapes would break compatibility on every refactor. See crate-level rustdoc.\n",
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
            // Story may not have landed yet (test runs at every iteration).
            // Don't fail — fail-on-presence is more useful than fail-on-absence.
            continue;
        }
        files_checked += 1;
        let content = fs::read_to_string(&file).expect("read port-layer file");
        for (idx, line) in content.lines().enumerate() {
            let trimmed = line.trim_start();
            for prefix in FORBIDDEN_IO_PREFIXES {
                let needle = format!("use {prefix}");
                if trimmed.starts_with(&needle) {
                    violations.push(format!(
                        "{}:{}: {}",
                        file.display(),
                        idx + 1,
                        line.trim()
                    ));
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
        "\n\nI/O imports found in port-layer files:\n  {}\n\nPort-layer files ({}) MUST stay free of concrete I/O. I/O lives in adapters (unix.rs, spawner.rs) by design — see crate-level rustdoc for the hexagonal-architecture rationale.\n",
        violations.join("\n  "),
        PORT_LAYER_FILES.join(", ")
    );
}

#[test]
fn first_use_segment_handles_expected_forms() {
    assert_eq!(first_use_segment("use rusqlite::Connection;"), Some("rusqlite"));
    assert_eq!(first_use_segment("use rusqlite;"), Some("rusqlite"));
    assert_eq!(first_use_segment("use rusqlite as r;"), Some("rusqlite"));
    assert_eq!(first_use_segment("    use std::io::Read;"), Some("std"));
    assert_eq!(first_use_segment("// use rusqlite::Connection;"), None);
    assert_eq!(first_use_segment("//! use rusqlite::Connection;"), None);
    assert_eq!(first_use_segment("let x = use_count();"), None);
    assert_eq!(first_use_segment(""), None);
}
