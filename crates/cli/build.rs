use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let version = if env::var("BRAIN_FROM_SOURCE")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        // Local install (just install): embed git SHA so developers can identify
        // which commit the binary was built from.
        git_sha()
    } else {
        // crates.io release or plain cargo build: use the published semver.
        env!("CARGO_PKG_VERSION").to_string()
    };

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let dest_path = Path::new(&out_dir).join("version.rs");

    fs::write(
        dest_path,
        format!("pub const VERSION: &str = \"{version}\";\n"),
    )
    .expect("Failed to write version.rs");

    println!("cargo:rerun-if-env-changed=BRAIN_FROM_SOURCE");
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
