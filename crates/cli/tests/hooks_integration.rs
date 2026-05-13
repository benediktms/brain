#![allow(clippy::disallowed_macros, clippy::disallowed_types)]

//! Subprocess integration tests for the `brain hooks` command surface.
//!
//! These tests spawn the compiled `brain` binary via `assert_cmd` and assert
//! the hook envelope JSON shape. They are the only tests that exercise the
//! end-to-end path Claude Code uses on a real machine — process spawn,
//! stdin/stdout plumbing, and envelope serialization.

use assert_cmd::prelude::*;
use std::process::Command;
use tempfile::TempDir;

/// Return a [`Command`] for the `brain` binary with env vars cleaned up so
/// that the justfile-exported `BRAIN_SQLITE_DB` / `BRAIN_DB` /
/// `BRAIN_MODEL_DIR` / `BRAIN_HOME` do not bleed into the subprocess.
fn brain_cmd() -> Command {
    let mut cmd = Command::cargo_bin("brain").unwrap();
    cmd.env_remove("BRAIN_SQLITE_DB")
        .env_remove("BRAIN_DB")
        .env_remove("BRAIN_MODEL_DIR")
        .env_remove("BRAIN_HOME");
    cmd
}

#[test]
fn brain_hooks_session_start_emits_valid_envelope() {
    // Use an isolated BRAIN_HOME so the host machine's registry can't
    // affect the test. We don't pre-populate it — the hook should still
    // emit a valid (potentially empty) envelope when no brain is registered.
    let home = TempDir::new().unwrap();

    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["hooks", "session-start"])
        .output()
        .expect("failed to run brain hooks session-start");

    assert!(
        output.status.success(),
        "brain hooks session-start exited with non-zero status: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let envelope: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should parse as JSON");

    assert_eq!(envelope["suppressOutput"], true);
    assert_eq!(
        envelope["hookSpecificOutput"]["hookEventName"],
        "SessionStart"
    );
    assert!(
        envelope["hookSpecificOutput"]["additionalContext"].is_string(),
        "additionalContext must be a string"
    );
}

#[test]
fn brain_hooks_user_prompt_submit_emits_valid_envelope() {
    let home = TempDir::new().unwrap();

    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["hooks", "user-prompt-submit"])
        .output()
        .expect("failed to run brain hooks user-prompt-submit");

    assert!(
        output.status.success(),
        "brain hooks user-prompt-submit exited with non-zero status: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let envelope: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should parse as JSON");

    assert_eq!(envelope["suppressOutput"], true);
    assert_eq!(
        envelope["hookSpecificOutput"]["hookEventName"],
        "UserPromptSubmit"
    );
    let ctx = envelope["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .expect("additionalContext is string");
    assert!(
        ctx.contains("memory_write_episode"),
        "user-prompt-submit nudge must reference memory_write_episode, got: {ctx}"
    );
}

#[test]
fn brain_hooks_help_hides_internal_subcommands() {
    // The internal-only hook subcommands (session-start, user-prompt-submit,
    // pre-compact, stop, pre-tool-use) must not be advertised in
    // `brain hooks --help` — only `install` and `status` are user-facing.
    let output = brain_cmd()
        .args(["hooks", "--help"])
        .output()
        .expect("failed to run brain hooks --help");
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    // Internal subcommands must be hidden. We allow them to appear in
    // descriptive text (none currently), but they must not appear as
    // standalone usage tokens. Checking the `Commands:` block is the
    // cleanest signal — clap puts visible subcommands there.
    for hidden in [
        "session-start",
        "user-prompt-submit",
        "pre-compact",
        "pre-tool-use",
    ] {
        assert!(
            !stdout.contains(hidden),
            "internal subcommand {hidden:?} must not appear in `brain hooks --help`, got:\n{stdout}"
        );
    }
    // "stop" is short enough to collide with other text — check it as a
    // line-starting token instead.
    assert!(
        !stdout
            .lines()
            .any(|l| l.trim_start().starts_with("stop ") || l.trim_start() == "stop"),
        "internal subcommand `stop` must not appear in `brain hooks --help`, got:\n{stdout}"
    );

    // Sanity: the two visible subcommands ARE present.
    assert!(
        stdout.contains("install"),
        "`install` subcommand should be visible in help, got:\n{stdout}"
    );
    assert!(
        stdout.contains("status"),
        "`status` subcommand should be visible in help, got:\n{stdout}"
    );
}
