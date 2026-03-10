/// CLI integration tests.
///
/// These tests exercise the compiled `brain` binary through `assert_cmd`.
/// Each test gets its own isolated `BRAIN_HOME` via a `TempDir` so that
/// global state (the registry in `~/.brain/config.toml`) is never touched.
///
/// Tests that require model weights (index, query, watch, doctor) are
/// explicitly excluded — they would fail in CI without the BGE model.
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return a [`Command`] for the `brain` binary with env vars cleaned up so
/// that the justfile-exported `BRAIN_SQLITE_DB` / `BRAIN_DB` /
/// `BRAIN_MODEL_DIR` do not bleed into the subprocess.
fn brain_cmd() -> Command {
    let mut cmd = Command::cargo_bin("brain").unwrap();
    // Strip any env vars that the justfile might have exported.
    cmd.env_remove("BRAIN_SQLITE_DB")
        .env_remove("BRAIN_DB")
        .env_remove("BRAIN_MODEL_DIR")
        .env_remove("BRAIN_HOME");
    cmd
}

/// Build a `brain init` command in an isolated temp directory.
///
/// Sets:
/// - `current_dir` to `project_dir` (where `.brain/` will be created)
/// - `BRAIN_HOME` to `brain_home` (so global config goes there, not `~/.brain`)
fn init_cmd(project_dir: &std::path::Path, brain_home: &std::path::Path) -> Command {
    let mut cmd = brain_cmd();
    cmd.current_dir(project_dir).env("BRAIN_HOME", brain_home);
    cmd.arg("init").arg("--name").arg("test-brain");
    cmd
}

/// Run `brain init` in a fresh temp environment and return both directories.
///
/// Returns `(project_dir, brain_home_dir)`.
fn setup_brain() -> (TempDir, TempDir) {
    let project = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();
    init_cmd(project.path(), home.path())
        .assert()
        .success();
    (project, home)
}

/// Path to the sqlite DB that `brain init` creates inside `brain_home`.
fn sqlite_db_path(brain_home: &std::path::Path) -> std::path::PathBuf {
    brain_home.join("brains").join("test-brain").join("brain.db")
}

// ---------------------------------------------------------------------------
// --help / --version
// ---------------------------------------------------------------------------

#[test]
fn help_flag_shows_usage() {
    brain_cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("brain"))
        .stdout(predicate::str::contains("Usage"));
}

#[test]
fn subcommand_help_shows_usage() {
    brain_cmd()
        .args(["tasks", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage"));
}

#[test]
fn version_flag_shows_version() {
    brain_cmd()
        .arg("-v")
        .assert()
        .stdout(predicate::str::contains("brain"));
}

// ---------------------------------------------------------------------------
// brain list (no brains registered)
// ---------------------------------------------------------------------------

#[test]
fn list_with_empty_registry_prints_message() {
    let home = TempDir::new().unwrap();
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("No brains registered"));
}

// ---------------------------------------------------------------------------
// brain init
// ---------------------------------------------------------------------------

#[test]
fn init_creates_brain_dir() {
    let project = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    init_cmd(project.path(), home.path())
        .assert()
        .success();

    // .brain/brain.toml should exist in the project dir
    assert!(project.path().join(".brain").join("brain.toml").is_file());
    // .brain/.gitignore should exist
    assert!(project.path().join(".brain").join(".gitignore").is_file());
}

#[test]
fn init_registers_brain_in_global_config() {
    let project = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    init_cmd(project.path(), home.path())
        .assert()
        .success();

    let config_path = home.path().join("config.toml");
    assert!(config_path.is_file(), "global config should be created");
    let config_text = std::fs::read_to_string(config_path).unwrap();
    assert!(
        config_text.contains("test-brain"),
        "global config should contain brain name"
    );
}

#[test]
fn init_creates_sqlite_db() {
    let (project, home) = setup_brain();
    let _ = project; // keep alive
    let db = sqlite_db_path(home.path());
    assert!(db.is_file(), "sqlite db should be created at {}", db.display());
}

#[test]
fn init_fails_if_already_initialized() {
    let project = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // First init succeeds
    init_cmd(project.path(), home.path())
        .assert()
        .success();

    // Second init fails with a clear message
    init_cmd(project.path(), home.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("already initialized"));
}

// ---------------------------------------------------------------------------
// brain list (after init)
// ---------------------------------------------------------------------------

#[test]
fn list_shows_registered_brain() {
    let (project, home) = setup_brain();
    let _ = project; // keep alive

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("test-brain"));
}

// ---------------------------------------------------------------------------
// brain config get / set
// ---------------------------------------------------------------------------

#[test]
fn config_get_prefix_succeeds_after_init() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["config", "get", "prefix"])
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^[A-Z]{3}\n$").unwrap());
}

#[test]
fn config_set_prefix_succeeds() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["config", "set", "prefix", "ABC"])
        .assert()
        .success();

    // Verify the new prefix is readable
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["config", "get", "prefix"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ABC"));
}

#[test]
fn config_set_prefix_rejects_non_three_letter_value() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["config", "set", "prefix", "TOOLONG"])
        .assert()
        .failure();
}

#[test]
fn config_get_unknown_key_fails() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["config", "get", "nonexistent_key"])
        .assert()
        .failure();
}

// ---------------------------------------------------------------------------
// brain tasks (list / create / stats)
// ---------------------------------------------------------------------------

#[test]
fn tasks_list_empty_succeeds() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "list"])
        .assert()
        .success();
}

#[test]
fn tasks_create_and_list() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    // Create a task
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "create", "--title", "My test task"])
        .assert()
        .success();

    // Verify it shows up in the list
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("My test task"));
}

#[test]
fn tasks_stats_after_init_succeeds() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "stats"])
        .assert()
        .success();
}

#[test]
fn tasks_json_output_is_valid() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    // Create a task first
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "create", "--title", "JSON task"])
        .assert()
        .success();

    // List with --json and verify output is valid JSON object
    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "--json", "list"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("output should be valid JSON");
    assert!(parsed.is_object(), "JSON output should be an object");
    assert!(
        parsed["tasks"].is_array(),
        "JSON output should have a 'tasks' array"
    );
    assert!(
        parsed["count"].is_number(),
        "JSON output should have a 'count' field"
    );
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[test]
fn missing_subcommand_exits_with_error() {
    brain_cmd().assert().failure();
}

#[test]
fn unknown_subcommand_exits_with_error() {
    brain_cmd()
        .arg("this-command-does-not-exist")
        .assert()
        .failure();
}

#[test]
fn config_get_without_db_fails_gracefully() {
    let home = TempDir::new().unwrap();

    // Point to a nonexistent db path — should fail with an error message, not panic
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(home.path().join("nonexistent").join("brain.db"))
        .args(["config", "get", "prefix"])
        .assert()
        .failure();
}

#[test]
fn tasks_list_without_db_fails_gracefully() {
    let home = TempDir::new().unwrap();

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(home.path().join("nonexistent").join("brain.db"))
        .args(["tasks", "list"])
        .assert()
        .failure();
}
