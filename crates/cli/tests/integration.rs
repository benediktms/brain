/// CLI integration tests.
///
/// These tests exercise the compiled `brain` binary through `assert_cmd`.
/// Each test gets its own isolated `BRAIN_HOME` via a `TempDir` so that
/// global state (the registry in `~/.brain/state_projection.toml`) is never touched.
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

/// Path to the unified sqlite DB at `brain_home/brain.db`.
fn sqlite_db_path(brain_home: &std::path::Path) -> std::path::PathBuf {
    brain_home.join("brain.db")
}

/// Path to the per-brain LanceDB directory.
fn lance_db_path(brain_home: &std::path::Path) -> std::path::PathBuf {
    brain_home.join("brains").join("test-brain").join("lancedb")
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

/// Verify `brain -v` output is a valid short git SHA (7 hex chars).
///
/// This only applies to local-install builds (`just install`). Crates.io releases
/// report semver instead — the test is skipped in that case.
#[test]
fn version_flag_shows_valid_sha() {
    let output = brain_cmd().arg("-v").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();

    // If the version looks like semver (contains '.'), this is a crates.io release.
    // Skip the SHA validation — semver does not match the 7-char hex pattern.
    if trimmed.contains('.') {
        // crates.io release: version is semver, nothing to validate here.
        return;
    }

    // Local install: version should be a 7-char lowercase hex SHA.
    assert!(
        trimmed.starts_with("brain ") && trimmed.len() == 6 + 7,
        "expected 'brain <7-hex-sha>', got: {trimmed}"
    );
    let sha = &trimmed[6..];
    assert!(
        sha.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "version SHA should be lowercase hex, got: {sha}"
    );
}

/// Verify `brain -v` reports the same SHA as `git rev-parse --short HEAD`
/// when built as a local install. Crates.io releases report semver instead —
/// this test is skipped in that case.
#[test]
fn version_flag_matches_git_head() {
    let output = brain_cmd().arg("-v").output().unwrap();
    assert!(output.status.success());
    let brain_version = String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .strip_prefix("brain ")
        .unwrap_or("")
        .to_string();

    // If the version is semver, this is a crates.io release — skip.
    if brain_version.contains('.') {
        return;
    }

    // Local install: version must match git HEAD.
    let git_sha = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();

    assert_eq!(
        brain_version, git_sha,
        "brain --version ({brain_version}) should match git rev-parse --short HEAD ({git_sha})"
    );
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

    let config_path = home.path().join("state_projection.toml");
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

/// Negative case: `brain init` in a non-git directory with no marker must
/// register a fresh brain — Case C must short-circuit gracefully on
/// `git rev-parse`-equivalent failure rather than panic or spuriously match.
#[test]
fn init_in_non_git_directory_creates_new_brain() {
    let project_a = TempDir::new().unwrap();
    let project_b = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // Pre-register an unrelated brain so Case C has something to potentially
    // match against. project_a is a plain tempdir (no git repo).
    brain_cmd()
        .current_dir(project_a.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "first-brain"])
        .assert()
        .success();

    // project_b is also a plain tempdir, no git, no marker, never registered.
    brain_cmd()
        .current_dir(project_b.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "second-brain"])
        .assert()
        .success();

    let list_output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(list_output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&list_output.stdout).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    assert_eq!(
        brains.len(),
        2,
        "non-git tempdir must register as a separate brain, not attach via Case C: {brains:?}"
    );
}

/// Negative case: `brain init` in a worktree of repo X must NOT attach to a
/// brain registered against an unrelated repo Y, even though Case C runs.
#[test]
fn init_does_not_attach_across_unrelated_repos() {
    fn run_git(args: &[&str], cwd: &std::path::Path) {
        let status = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .expect("git not available on PATH");
        assert!(status.success(), "git {args:?} failed in {}", cwd.display());
    }
    fn init_repo(path: &std::path::Path) {
        run_git(&["init", "-q", "-b", "main"], path);
        run_git(&["config", "user.email", "test@test"], path);
        run_git(&["config", "user.name", "test"], path);
        std::fs::write(path.join("readme"), "x").unwrap();
        run_git(&["add", "."], path);
        run_git(&["commit", "-q", "-m", "init"], path);
    }

    let repo_x = TempDir::new().unwrap();
    let repo_y = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // Two independent git repos.
    init_repo(repo_x.path());
    init_repo(repo_y.path());

    // Register brain in repo X only.
    brain_cmd()
        .current_dir(repo_x.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "brain-x"])
        .assert()
        .success();

    // brain init in repo Y must NOT attach to repo X's brain — different .git dirs.
    brain_cmd()
        .current_dir(repo_y.path())
        .env("BRAIN_HOME", home.path())
        .args(["init", "--name", "brain-y"])
        .assert()
        .success();

    let list_output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(list_output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&list_output.stdout).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    assert_eq!(
        brains.len(),
        2,
        "Case C must not match across unrelated git repos: {brains:?}"
    );
}

/// `brain init` in a fresh git worktree of an already-registered brain must
/// auto-attach via the shared `.git` directory (common-dir lookup), even when
/// no `.brain/brain.toml` marker is present in the worktree (e.g. project
/// gitignores `.brain/`, or the marker was never committed).
///
/// This exercises Case C in `init.rs` — the path-independent fallback added in
/// brn-4e9.1.
#[test]
fn init_attaches_via_git_common_dir_when_marker_missing() {
    fn run_git(args: &[&str], cwd: &std::path::Path) {
        let status = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .expect("git not available on PATH");
        assert!(status.success(), "git {args:?} failed in {}", cwd.display());
    }

    let main = TempDir::new().unwrap();
    let wt_parent = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // 1. Initialize a real git repo in `main` with one commit so worktrees can be added.
    run_git(&["init", "-q", "-b", "main"], main.path());
    run_git(&["config", "user.email", "test@test"], main.path());
    run_git(&["config", "user.name", "test"], main.path());
    std::fs::write(main.path().join("readme"), "x").unwrap();
    run_git(&["add", "."], main.path());
    run_git(&["commit", "-q", "-m", "init"], main.path());

    // 2. brain init in `main`. The marker file is created but uncommitted.
    init_cmd(main.path(), home.path()).assert().success();
    assert!(main.path().join(".brain/brain.toml").exists());

    // 3. Create a linked worktree. Uncommitted files (including .brain/brain.toml)
    //    do NOT propagate to the new worktree, so Case A's marker-file detection
    //    must miss.
    let wt_path = wt_parent.path().join("linked");
    run_git(
        &[
            "worktree",
            "add",
            "--detach",
            wt_path.to_str().unwrap(),
            "HEAD",
        ],
        main.path(),
    );
    assert!(
        !wt_path.join(".brain/brain.toml").exists(),
        "worktree must not carry the uncommitted marker file"
    );

    // 4. Run brain init in the worktree. Case A misses (no marker), Case B misses
    //    (path never registered), Case C must match via shared .git.
    init_cmd(&wt_path, home.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Path added to existing brain"));

    // 5. Verify exactly one brain exists, with roots covering both main and worktree paths.
    let list_output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(list_output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&list_output.stdout).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    assert_eq!(
        brains.len(),
        1,
        "Case C must attach to existing brain, not create a new one. Got: {brains:?}"
    );

    let entry = &brains[0];
    let primary_root = entry["root"].as_str().unwrap_or("").to_string();
    let extra_roots: Vec<String> = entry["extra_roots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    let all_roots: Vec<String> = std::iter::once(primary_root).chain(extra_roots).collect();

    let main_str = main.path().to_string_lossy().to_string();
    let wt_str = wt_path.to_string_lossy().to_string();
    assert!(
        all_roots.iter().any(|r| r.contains(&main_str)),
        "main checkout must remain in roots: {all_roots:?}"
    );
    assert!(
        all_roots.iter().any(|r| r.contains(&wt_str)),
        "worktree path must be added to roots: {all_roots:?}"
    );

    // 6. Marker file should now exist in the worktree (helper drops it post-attach).
    assert!(
        wt_path.join(".brain/brain.toml").exists(),
        "attach helper must write a marker file for fast-path on subsequent inits"
    );
}

/// Regression: `brain init` must not destroy the parent's registered roots
/// when `state_projection.toml` has drifted out of sync with the DB.
///
/// Conditions:
/// 1. Brain X is registered: DB row has roots = [A], projection has the entry.
/// 2. Projection is wiped (simulates drift — projection deleted, manually edited,
///    or regenerated incompletely). DB row remains.
/// 3. `brain init` runs in dir B with `.brain/brain.toml` carrying brain X's id.
///
/// Before fix (brn-4e9.2): the upsert at init.rs:184-201 would replace `roots_json`
/// with `vec![B]`, silently losing A.
/// After fix: roots become [A, B] (additive merge).
#[test]
fn init_preserves_existing_db_roots_when_projection_is_stale() {
    let project_a = TempDir::new().unwrap();
    let project_b = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();

    // Step 1: register brain in dir A. DB and projection both have it.
    init_cmd(project_a.path(), home.path()).assert().success();

    // Capture brain_id from list --json.
    let list_output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(list_output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&list_output.stdout).unwrap();
    let brain_id = parsed["brains"][0]["id"].as_str().unwrap().to_string();
    assert!(!brain_id.is_empty());

    // Step 2: wipe state_projection.toml. DB row is untouched.
    let projection_path = home.path().join("state_projection.toml");
    assert!(projection_path.exists());
    std::fs::write(&projection_path, "[brains]\n").unwrap();

    // Step 3: create marker file in dir B referencing the same brain_id.
    let brain_dir_b = project_b.path().join(".brain");
    std::fs::create_dir_all(&brain_dir_b).unwrap();
    std::fs::write(
        brain_dir_b.join("brain.toml"),
        format!("name = \"test-brain\"\nid = \"{brain_id}\"\nnotes = []\n"),
    )
    .unwrap();

    // Step 4: run init in B. With the bug, this overwrites the DB's roots_json.
    init_cmd(project_b.path(), home.path()).assert().success();

    // Step 5: verify DB row's roots are additive (contain both A and B).
    let list_output = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(list_output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&list_output.stdout).unwrap();
    let brains = parsed["brains"].as_array().unwrap();
    assert_eq!(
        brains.len(),
        1,
        "expected exactly one brain (additive merge under same id), got {brains:?}"
    );
    let entry = &brains[0];
    assert_eq!(entry["id"].as_str().unwrap(), brain_id);

    // Collect all roots: `root` plus `extra_roots`.
    let primary_root = entry["root"].as_str().unwrap_or("").to_string();
    let extra_roots: Vec<String> = entry["extra_roots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    let mut all_roots: Vec<String> = std::iter::once(primary_root).chain(extra_roots).collect();
    all_roots.sort();

    let path_a = project_a.path().to_string_lossy().to_string();
    let path_b = project_b.path().to_string_lossy().to_string();
    assert!(
        all_roots.iter().any(|r| r.contains(&path_a)),
        "DB roots must still contain dir A after init in B: {all_roots:?}"
    );
    assert!(
        all_roots.iter().any(|r| r.contains(&path_b)),
        "DB roots must contain dir B after init in B: {all_roots:?}"
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
    let lance = lance_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .arg("--lance-db")
        .arg(&lance)
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
    let lance = lance_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .arg("--lance-db")
        .arg(&lance)
        .args(["config", "set", "prefix", "ABC"])
        .assert()
        .success();

    // Verify the new prefix is readable
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .arg("--lance-db")
        .arg(&lance)
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
    let lance = lance_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .arg("--lance-db")
        .arg(&lance)
        .args(["config", "set", "prefix", "TOOLONG"])
        .assert()
        .failure();
}

#[test]
fn config_get_unknown_key_fails() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());
    let lance = lance_db_path(home.path());

    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .arg("--lance-db")
        .arg(&lance)
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
    let config_text = std::fs::read_to_string(home.path().join("state_projection.toml")).unwrap();
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
        brain_persistence::db::Db::open(&unified_db).unwrap();
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
        brain_persistence::db::Db::open(&unified_db).unwrap();
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
        brain_persistence::db::Db::open(&unified_db).unwrap();
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
///
/// Writes to `brain_meta.project_prefix` — which `ensure_brain_registered`
/// reads as the authoritative prefix when registering a brain for the first
/// time. Also updates `brains.prefix` for any already-registered brains.
fn set_prefix(db_path: &std::path::Path, prefix: &str) {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO brain_meta (key, value) VALUES ('project_prefix', ?1)",
        [prefix],
    )
    .unwrap();
    conn.execute("UPDATE brains SET prefix = ?1", [prefix])
        .unwrap();
}

/// Helper: read project_prefix from a SQLite database.
/// Reads from `brains.prefix` (primary), falling back to `brain_meta`.
fn get_prefix(db_path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    // Try brains.prefix first
    let result: Option<String> = conn
        .query_row(
            "SELECT prefix FROM brains WHERE brain_id != '' AND prefix IS NOT NULL LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();
    if let Some(prefix) = result {
        return prefix;
    }
    // Fallback to brain_meta
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
        task_id.starts_with("cor-"),
        "task_id should use compact form with lowercase prefix 'cor-', got: {task_id}"
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

// ---------------------------------------------------------------------------
// Orphan-dep readiness (brn-6f4)
// ---------------------------------------------------------------------------
//
// A task whose only dep references a nonexistent task_id (orphaned dep) must:
//   - NOT appear in `brain tasks ready`
//   - NOT appear in `brain tasks next`
//
// We inject the orphan dep directly via rusqlite after creating the task via
// the CLI, because the event layer correctly rejects deps on missing tasks.

#[test]
fn tasks_orphan_dep_not_in_ready_list() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    // Create a task via CLI.
    let create_out = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "create", "--title", "Orphan dep task"])
        .output()
        .unwrap();
    assert!(create_out.status.success(), "task create should succeed");
    let create_stdout = String::from_utf8(create_out.stdout).unwrap();

    // Extract the task_id from the create output (format: "Created task <id>").
    let task_id = create_stdout
        .lines()
        .find_map(|line| {
            let line = line.trim();
            if line.starts_with("Created") || line.contains("Created task") {
                line.split_whitespace().last().map(str::to_string)
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            // Fallback: query the DB directly for the task_id.
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.query_row(
                "SELECT task_id FROM tasks WHERE title = 'Orphan dep task' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        });

    // Inject an orphan dep directly into the DB (FK checks off during insert).
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO task_deps (task_id, depends_on) VALUES (?1, 'ghost-nonexistent-task')",
            rusqlite::params![task_id],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    }

    // `brain tasks ready` must NOT show "Orphan dep task".
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "ready"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Orphan dep task").not());

    // `brain tasks next` must NOT show "Orphan dep task".
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "next"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Orphan dep task").not());
}

// Verifies the brn-3a93 contract end-to-end through the CLI binary:
// a task with an unresolved external blocker (`task_external_ids` row with
// `blocking=1, resolved_at IS NULL`) must be excluded from `brain tasks ready`
// and surfaced in `brain tasks show`. There is no CLI subcommand for the
// external_blocker_added event yet (tracked separately), so we inject the row
// directly via rusqlite — same pattern as the orphan-dep test above.

#[test]
fn tasks_external_blocker_excluded_from_ready_and_shown_on_get() {
    let (project, home) = setup_brain();
    let _ = project;
    let db = sqlite_db_path(home.path());

    let create_out = brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "create", "--title", "Awaits external sign-off"])
        .output()
        .unwrap();
    assert!(create_out.status.success(), "task create should succeed");
    let create_stdout = String::from_utf8(create_out.stdout).unwrap();

    let task_id = create_stdout
        .lines()
        .find_map(|line| {
            let line = line.trim();
            if line.starts_with("Created") || line.contains("Created task") {
                line.split_whitespace().last().map(str::to_string)
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.query_row(
                "SELECT task_id FROM tasks WHERE title = 'Awaits external sign-off' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        });

    // Inject an unresolved external blocker directly.
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute(
            "INSERT INTO task_external_ids
                (task_id, source, external_id, external_url, blocking, resolved_at, imported_at)
             VALUES (?1, 'jira', 'PLAT-42', 'https://example/PLAT-42', 1, NULL, strftime('%s','now'))",
            rusqlite::params![task_id],
        )
        .unwrap();
    }

    // `brain tasks ready` must exclude the task.
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "ready"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Awaits external sign-off").not());

    // `brain tasks show <id>` must surface the blocker (source + external_id).
    brain_cmd()
        .env("BRAIN_HOME", home.path())
        .arg("--sqlite-db")
        .arg(&db)
        .args(["tasks", "show", &task_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("PLAT-42"))
        .stdout(predicate::str::contains("jira"));
}
