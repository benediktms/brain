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
    init_cmd(project.path(), home.path()).assert().success();
    (project, home)
}

/// Path to the sqlite DB that `brain init` creates inside `brain_home`.
fn sqlite_db_path(brain_home: &std::path::Path) -> std::path::PathBuf {
    brain_home
        .join("brains")
        .join("test-brain")
        .join("brain.db")
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

    init_cmd(project.path(), home.path()).assert().success();

    // .brain/brain.toml should exist in the project dir
    assert!(project.path().join(".brain").join("brain.toml").is_file());
    // .brain/.gitignore should exist
    assert!(project.path().join(".brain").join(".gitignore").is_file());
}

#[test]
fn init_registers_brain_in_global_config() {
    let project = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    init_cmd(project.path(), home.path()).assert().success();

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
    assert!(
        db.is_file(),
        "sqlite db should be created at {}",
        db.display()
    );
}

#[test]
fn init_detects_existing_brain_and_adds_path() {
    let project = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // First init succeeds
    init_cmd(project.path(), home.path()).assert().success();

    // Second init on same path: detects existing brain, prints info, succeeds
    init_cmd(project.path(), home.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("already registered")
                .or(predicate::str::contains("Path added")),
        );
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
// brain alias (add / remove / list / collision)
// ---------------------------------------------------------------------------

#[test]
fn alias_add_and_list() {
    let (project, home) = setup_brain();
    let _ = project;

    // Add alias
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["alias", "add", "test-brain", "tb"])
        .assert()
        .success()
        .stdout(predicate::str::contains("tb"));

    // list --json shows the alias
    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    let entry = brains.iter().find(|b| b["name"] == "test-brain").unwrap();
    let aliases = entry["aliases"].as_array().unwrap();
    assert!(
        aliases.iter().any(|a| a == "tb"),
        "aliases should contain 'tb': {aliases:?}"
    );
}

#[test]
fn alias_remove() {
    let (project, home) = setup_brain();
    let _ = project;

    // Add then remove
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["alias", "add", "test-brain", "tb"])
        .assert()
        .success();

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["alias", "remove", "test-brain", "tb"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed"));

    // Verify alias is gone from list --json
    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    let entry = brains.iter().find(|b| b["name"] == "test-brain").unwrap();
    let aliases = entry["aliases"].as_array().unwrap();
    assert!(
        !aliases.iter().any(|a| a == "tb"),
        "alias 'tb' should have been removed: {aliases:?}"
    );
}

#[test]
fn alias_collision_with_name() {
    let project1 = TempDir::new().unwrap();
    let project2 = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // Init brain1
    brain_cmd()
        .current_dir(project1.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "brain1"])
        .assert()
        .success();

    // Init brain2
    brain_cmd()
        .current_dir(project2.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "brain2"])
        .assert()
        .success();

    // Attempt to alias brain2 with brain1's name — must fail
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["alias", "add", "brain2", "brain1"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("collides").or(predicate::str::contains("exists")));
}

#[test]
fn init_reregister_adds_extra_root() {
    let project_a = TempDir::new().unwrap();
    let project_b = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // Init brain in dir A
    brain_cmd()
        .current_dir(project_a.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "shared-brain"])
        .assert()
        .success();

    // Read the brain ID from the config created in A
    let config_text = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
    let global: serde_json::Value = {
        // Parse via toml first then use brain_id
        let cfg: toml::Value = toml::from_str(&config_text).unwrap();
        let id = cfg["brains"]["shared-brain"]["id"]
            .as_str()
            .unwrap_or("")
            .to_string();
        serde_json::json!({"id": id})
    };
    let brain_id = global["id"].as_str().unwrap().to_string();

    // Create .brain/brain.toml in dir B with the same ID
    std::fs::create_dir_all(project_b.path().join(".brain")).unwrap();
    std::fs::write(
        project_b.path().join(".brain").join("brain.toml"),
        format!("name = \"shared-brain\"\nid = \"{brain_id}\"\n"),
    )
    .unwrap();

    // Run init in dir B — should detect existing brain and add path
    brain_cmd()
        .current_dir(project_b.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "shared-brain"])
        .assert()
        .success();

    // Verify list --json shows project_b in extra_roots
    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    let entry = brains.iter().find(|b| b["name"] == "shared-brain").unwrap();
    let extra_roots = entry["extra_roots"].as_array().unwrap();
    let b_path = project_b.path().to_string_lossy().to_string();
    assert!(
        extra_roots
            .iter()
            .any(|r| r.as_str().unwrap_or("").contains(&b_path)),
        "extra_roots should contain project_b path: {extra_roots:?}"
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

// ---------------------------------------------------------------------------
// Unified DB routing
// ---------------------------------------------------------------------------

/// When a unified DB exists at $BRAIN_HOME/brain.db, task commands should
/// route data through it rather than the per-brain DB.
#[test]
fn tasks_route_through_unified_db() {
    let (project, home) = setup_brain();
    let _ = project;
    let per_brain_db = sqlite_db_path(home.path());
    let unified_db = home.path().join("brain.db");

    // Create the unified DB (brain init may not create it)
    if !unified_db.exists() {
        brain_lib::db::Db::open(&unified_db).unwrap();
    }

    // Create a task — should route to unified DB
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&per_brain_db)
        .args(["tasks", "create", "--title", "Unified routing test"])
        .assert()
        .success();

    // Verify it shows up in the list (also routed through unified DB)
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&per_brain_db)
        .args(["tasks", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Unified routing test"));
}

/// Snapshots list command should work with unified DB routing.
#[test]
fn snapshots_list_routes_through_unified_db() {
    let (project, home) = setup_brain();
    let _ = project;
    let per_brain_db = sqlite_db_path(home.path());
    let unified_db = home.path().join("brain.db");

    if !unified_db.exists() {
        brain_lib::db::Db::open(&unified_db).unwrap();
    }

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&per_brain_db)
        .args(["snapshots", "list"])
        .assert()
        .success();
}

/// Artifacts list command should work with unified DB routing.
#[test]
fn artifacts_list_routes_through_unified_db() {
    let (project, home) = setup_brain();
    let _ = project;
    let per_brain_db = sqlite_db_path(home.path());
    let unified_db = home.path().join("brain.db");

    if !unified_db.exists() {
        brain_lib::db::Db::open(&unified_db).unwrap();
    }

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&per_brain_db)
        .args(["artifacts", "list"])
        .assert()
        .success();
}

// ---------------------------------------------------------------------------
// Prefix routing — per-brain DB prefix must win over unified DB prefix
// ---------------------------------------------------------------------------

/// Helper: set project_prefix in a SQLite database via raw SQL.
fn set_prefix(db_path: &std::path::Path, prefix: &str) {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO brain_meta (key, value) VALUES ('project_prefix', ?1)",
        [prefix],
    )
    .unwrap();
}

/// Helper: read project_prefix from a SQLite database.
fn get_prefix(db_path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.query_row(
        "SELECT value FROM brain_meta WHERE key = 'project_prefix'",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

/// Task creation must use the prefix stored in the single brain DB.
#[test]
fn task_prefix_uses_brain_db() {
    let (project, home) = setup_brain();
    let _ = project;
    let db_path = sqlite_db_path(home.path());

    // Plant a known prefix in the DB
    set_prefix(&db_path, "AAA");
    assert_eq!(get_prefix(&db_path), "AAA");

    // Create a task — should use prefix "AAA"
    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db_path)
        .args(["tasks", "create", "--title", "Prefix test"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        stdout.contains("AAA-"),
        "Task ID should use prefix 'AAA', got: {stdout}"
    );
}

/// Same test but with JSON output — verify the task_id field in JSON uses the brain DB prefix.
#[test]
fn task_prefix_json_uses_brain_db() {
    let (project, home) = setup_brain();
    let _ = project;
    let db_path = sqlite_db_path(home.path());

    set_prefix(&db_path, "COR");

    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db_path)
        .args(["tasks", "--json", "create", "--title", "JSON prefix test"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let task_id = parsed["task"]["task_id"].as_str().unwrap();

    assert!(
        task_id.starts_with("COR-"),
        "task_id should start with 'COR-', got: {task_id}"
    );
}

/// Snapshot save should use the prefix stored in the brain DB.
#[test]
fn snapshot_prefix_uses_brain_db() {
    let (project, home) = setup_brain();
    let _ = project;
    let db_path = sqlite_db_path(home.path());

    set_prefix(&db_path, "SNP");

    // Save a snapshot via a temp file
    let payload_file = home.path().join("test_payload.txt");
    std::fs::write(&payload_file, "test payload").unwrap();

    let output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db_path)
        .args(["snapshots", "save", "--title", "Prefix snap", "--file"])
        .arg(&payload_file)
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        stdout.contains("SNP-"),
        "Snapshot ID should use prefix 'SNP', got: {stdout}"
    );
}
