//! Golden tests: deterministic output verification against JSON fixtures.
//!
//! Run `cargo test -p brain-lib -- golden_generate --ignored --nocapture` to regenerate
//! fixtures after intentional algorithm changes.

use std::collections::HashMap;
use std::path::PathBuf;

use brain_lib::chunker::{CHUNKER_VERSION, Chunk, chunk_text};
use brain_lib::links::{Link, extract_links};
use brain_lib::ranking::{CandidateSignals, Weights, rank_candidates, resolve_intent};
use brain_lib::tasks::events::*;
use brain_lib::tasks::projections::{apply_event, rebuild};
use brain_lib::tasks::queries::{list_all_deps, list_all_labels, list_blocked, list_ready};
use brain_lib::utils::content_hash;
use brain_persistence::db::Db;
use serde::{Deserialize, Serialize};

// ─── Helpers ─────────────────────────────────────────────────────

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .join(name)
}

fn fixture_text(name: &str) -> String {
    std::fs::read_to_string(fixture_path(name)).expect("fixture should exist")
}

fn load_golden<T: for<'de> Deserialize<'de>>(name: &str) -> T {
    let text = std::fs::read_to_string(fixture_path(name))
        .unwrap_or_else(|e| panic!("golden fixture {name} missing: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("golden fixture {name} parse error: {e}"))
}

fn save_golden<T: Serialize>(name: &str, value: &T) {
    let json = serde_json::to_string_pretty(value).unwrap();
    std::fs::write(fixture_path(name), &json).unwrap();
    println!("  wrote {name}");
}

// ─── Golden fixture types ────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct GoldenChunker {
    input_file: String,
    chunker_version: u32,
    chunks: Vec<Chunk>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenLinks {
    input_file: String,
    links: Vec<Link>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenHashCase {
    input: String,
    expected_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenHash {
    cases: Vec<GoldenHashCase>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenRankingCandidate {
    chunk_id: String,
    sim_vector: f64,
    bm25: f64,
    age_seconds: f64,
    pagerank_score: f64,
    importance: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenIntentRanking {
    candidates: Vec<GoldenRankingCandidate>,
    expected_orders: HashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct GoldenTask {
    task_id: String,
    title: String,
    status: String,
    priority: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct GoldenDep {
    task_id: String,
    depends_on: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenTaskExpected {
    tasks: Vec<GoldenTask>,
    deps: Vec<GoldenDep>,
    ready_ids: Vec<String>,
    blocked_ids: Vec<String>,
    labels: HashMap<String, Vec<String>>,
}

// ─── Task event scenario builder ─────────────────────────────────

fn make_task_events() -> Vec<TaskEvent> {
    let mut events = Vec::new();
    let mut ts = 1700000000i64;
    let mut evt_num = 0u32;

    let mut next_event =
        |task_id: &str, event_type: EventType, payload: serde_json::Value| -> TaskEvent {
            evt_num += 1;
            ts += 1;
            TaskEvent {
                event_id: format!("EVT-{evt_num:04}"),
                task_id: task_id.to_string(),
                timestamp: ts,
                actor: "user".to_string(),
                event_type,
                event_version: CURRENT_EVENT_VERSION,
                payload,
            }
        };

    // Create 3 tasks
    events.push(next_event(
        "T1",
        EventType::TaskCreated,
        serde_json::to_value(TaskCreatedPayload {
            title: "Implement auth module".into(),
            description: Some("Core authentication logic".into()),
            priority: 1,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: Some(TaskType::Feature),
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            display_id: None,
        })
        .unwrap(),
    ));

    events.push(next_event(
        "T2",
        EventType::TaskCreated,
        serde_json::to_value(TaskCreatedPayload {
            title: "Write integration tests".into(),
            description: None,
            priority: 2,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: Some(TaskType::Task),
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            display_id: None,
        })
        .unwrap(),
    ));

    events.push(next_event(
        "T3",
        EventType::TaskCreated,
        serde_json::to_value(TaskCreatedPayload {
            title: "Deploy to staging".into(),
            description: None,
            priority: 3,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: Some(TaskType::Task),
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            display_id: None,
        })
        .unwrap(),
    ));

    // Dependencies: T2 depends on T1, T3 depends on T2
    events.push(next_event(
        "T2",
        EventType::DependencyAdded,
        serde_json::to_value(DependencyPayload {
            depends_on_task_id: "T1".into(),
        })
        .unwrap(),
    ));

    events.push(next_event(
        "T3",
        EventType::DependencyAdded,
        serde_json::to_value(DependencyPayload {
            depends_on_task_id: "T2".into(),
        })
        .unwrap(),
    ));

    // Labels
    events.push(next_event(
        "T1",
        EventType::LabelAdded,
        serde_json::to_value(LabelPayload {
            label: "core".into(),
        })
        .unwrap(),
    ));

    events.push(next_event(
        "T2",
        EventType::LabelAdded,
        serde_json::to_value(LabelPayload {
            label: "testing".into(),
        })
        .unwrap(),
    ));

    // Comment
    events.push(next_event(
        "T2",
        EventType::CommentAdded,
        serde_json::to_value(CommentPayload {
            body: "Waiting for auth to be done".into(),
        })
        .unwrap(),
    ));

    // Status changes: T1 → in_progress → done
    events.push(next_event(
        "T1",
        EventType::StatusChanged,
        serde_json::to_value(StatusChangedPayload {
            new_status: TaskStatus::InProgress,
        })
        .unwrap(),
    ));

    events.push(next_event(
        "T1",
        EventType::StatusChanged,
        serde_json::to_value(StatusChangedPayload {
            new_status: TaskStatus::Done,
        })
        .unwrap(),
    ));

    events
}

fn make_ranking_candidates() -> Vec<GoldenRankingCandidate> {
    vec![
        GoldenRankingCandidate {
            chunk_id: "vector_king".into(),
            sim_vector: 0.95,
            bm25: 0.10,
            age_seconds: 3600.0,
            pagerank_score: 0.289,
            importance: 0.5,
        },
        GoldenRankingCandidate {
            chunk_id: "keyword_king".into(),
            sim_vector: 0.10,
            bm25: 0.95,
            age_seconds: 86400.0,
            pagerank_score: 0.458,
            importance: 0.5,
        },
        GoldenRankingCandidate {
            chunk_id: "link_hub".into(),
            sim_vector: 0.40,
            bm25: 0.30,
            age_seconds: 172800.0,
            pagerank_score: 1.0,
            importance: 0.5,
        },
        GoldenRankingCandidate {
            chunk_id: "recent_gem".into(),
            sim_vector: 0.20,
            bm25: 0.20,
            age_seconds: 60.0,
            pagerank_score: 0.0,
            importance: 2.0,
        },
    ]
}

fn golden_to_signals(candidates: &[GoldenRankingCandidate]) -> Vec<CandidateSignals> {
    candidates
        .iter()
        .map(|c| CandidateSignals {
            chunk_id: c.chunk_id.clone(),
            sim_vector: c.sim_vector,
            bm25: c.bm25,
            age_seconds: c.age_seconds,
            pagerank_score: c.pagerank_score,
            tags: vec![],
            importance: c.importance,
            file_path: format!("/notes/{}.md", c.chunk_id),
            heading_path: String::new(),
            content: format!("content of {}", c.chunk_id),
            token_estimate: 20,
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
        })
        .collect()
}

// ─── Fixture generator (run with --ignored) ──────────────────────

#[test]
#[ignore]
fn golden_generate() {
    println!("Generating golden fixtures...");

    // 1. Chunker fixtures
    for (input_file, output_file) in [
        ("simple.md", "golden/chunker_simple.json"),
        ("headings.md", "golden/chunker_headings.json"),
    ] {
        let text = fixture_text(input_file);
        let chunks = chunk_text(&text);
        save_golden(
            output_file,
            &GoldenChunker {
                input_file: input_file.into(),
                chunker_version: CHUNKER_VERSION,
                chunks,
            },
        );
    }

    // 2. Links fixture
    let text = fixture_text("wikilinks.md");
    let links = extract_links(&text);
    save_golden(
        "golden/links_wikilinks.json",
        &GoldenLinks {
            input_file: "wikilinks.md".into(),
            links,
        },
    );

    // 3. Hash determinism fixture
    let hash_inputs = [
        "",
        "hello world",
        "hello   \nworld\n",
        "hello\r\nworld\r\n",
        "\n\n\n",
        "  ",
        "日本語テスト",
    ];
    save_golden(
        "golden/hash_determinism.json",
        &GoldenHash {
            cases: hash_inputs
                .iter()
                .map(|&input| GoldenHashCase {
                    input: input.to_string(),
                    expected_hash: content_hash(input),
                })
                .collect(),
        },
    );

    // 4. Task event replay
    let events = make_task_events();

    let jsonl: String = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(fixture_path("golden/task_events.jsonl"), &jsonl).unwrap();
    println!("  wrote golden/task_events.jsonl");

    let db = Db::open_in_memory().unwrap();
    let (tasks_out, dep_list, ready_ids, blocked_ids, label_map) = db
        .with_write_conn(|conn| {
            for event in &events {
                apply_event(conn, event, "")?;
            }

            let ready = list_ready(conn, None)?;
            let blocked = list_blocked(conn, None)?;
            let deps = list_all_deps(conn)?;
            let labels_raw = list_all_labels(conn)?;

            let mut tasks_out = Vec::new();
            let mut stmt = conn
                .prepare("SELECT task_id, title, status, priority FROM tasks ORDER BY task_id")?;
            let rows = stmt.query_map([], |row| {
                Ok(GoldenTask {
                    task_id: row.get(0)?,
                    title: row.get(1)?,
                    status: row.get(2)?,
                    priority: row.get(3)?,
                })
            })?;
            for row in rows {
                tasks_out.push(row?);
            }

            let mut label_map: HashMap<String, Vec<String>> = HashMap::new();
            for (task_id, label) in &labels_raw {
                label_map
                    .entry(task_id.clone())
                    .or_default()
                    .push(label.clone());
            }

            let dep_list: Vec<GoldenDep> = deps
                .iter()
                .map(|d| GoldenDep {
                    task_id: d.task_id.clone(),
                    depends_on: d.depends_on.clone(),
                })
                .collect();
            let ready_ids: Vec<String> = ready.iter().map(|r| r.task_id.clone()).collect();
            let blocked_ids: Vec<String> = blocked.iter().map(|r| r.task_id.clone()).collect();

            Ok((tasks_out, dep_list, ready_ids, blocked_ids, label_map))
        })
        .unwrap();

    save_golden(
        "golden/task_events_expected.json",
        &GoldenTaskExpected {
            tasks: tasks_out,
            deps: dep_list,
            ready_ids,
            blocked_ids,
            labels: label_map,
        },
    );

    // 5. Intent ranking
    let candidates = make_ranking_candidates();
    let signals = golden_to_signals(&candidates);

    let mut expected_orders = HashMap::new();
    for intent in ["lookup", "planning", "reflection", "synthesis"] {
        let profile = resolve_intent(intent);
        let weights = Weights::from_profile(profile);
        let results = rank_candidates(&signals, &weights, &[], &HashMap::new());
        let order: Vec<String> = results.iter().map(|r| r.chunk_id.clone()).collect();
        expected_orders.insert(intent.to_string(), order);
    }

    save_golden(
        "golden/intent_ranking.json",
        &GoldenIntentRanking {
            candidates,
            expected_orders,
        },
    );

    println!("All golden fixtures generated!");
}

// ─── Golden #1: Chunker ─────────────────────────────────────────

#[test]
fn golden_chunker_simple() {
    let input = fixture_text("simple.md");
    let expected: GoldenChunker = load_golden("golden/chunker_simple.json");
    assert_eq!(
        CHUNKER_VERSION, expected.chunker_version,
        "chunker version changed — regenerate: cargo test -p brain-lib -- golden_generate --ignored --nocapture"
    );
    let actual = chunk_text(&input);
    assert_eq!(actual, expected.chunks);
}

#[test]
fn golden_chunker_headings() {
    let input = fixture_text("headings.md");
    let expected: GoldenChunker = load_golden("golden/chunker_headings.json");
    assert_eq!(
        CHUNKER_VERSION, expected.chunker_version,
        "chunker version changed — regenerate fixtures"
    );
    let actual = chunk_text(&input);
    assert_eq!(actual, expected.chunks);
}

// ─── Golden #2: Links ───────────────────────────────────────────

#[test]
fn golden_links_wikilinks() {
    let input = fixture_text("wikilinks.md");
    let expected: GoldenLinks = load_golden("golden/links_wikilinks.json");
    let actual = extract_links(&input);
    assert_eq!(actual, expected.links);
}

// ─── Golden #3: Hash determinism ─────────────────────────────────

#[test]
fn golden_hash_determinism() {
    let expected: GoldenHash = load_golden("golden/hash_determinism.json");
    for case in &expected.cases {
        let actual = content_hash(&case.input);
        assert_eq!(
            actual, case.expected_hash,
            "hash mismatch for input: {:?}",
            case.input
        );
    }
}

// ─── Golden #4: Task event replay ───────────────────────────────

#[test]
fn golden_task_replay() {
    let jsonl = fixture_text("golden/task_events.jsonl");
    let expected: GoldenTaskExpected = load_golden("golden/task_events_expected.json");

    let events: Vec<TaskEvent> = jsonl
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("parse event"))
        .collect();

    let db = Db::open_in_memory().unwrap();
    let (actual_tasks, actual_deps, actual_ready, actual_blocked, actual_labels) = db
        .with_write_conn(|conn| {
            for event in &events {
                apply_event(conn, event, "")?;
            }

            // Query tasks
            let mut stmt = conn
                .prepare("SELECT task_id, title, status, priority FROM tasks ORDER BY task_id")?;
            let tasks: Vec<GoldenTask> = stmt
                .query_map([], |row| {
                    Ok(GoldenTask {
                        task_id: row.get(0)?,
                        title: row.get(1)?,
                        status: row.get(2)?,
                        priority: row.get(3)?,
                    })
                })?
                .map(|r| r.unwrap())
                .collect();

            // Query deps
            let deps = list_all_deps(conn)?;
            let mut dep_list: Vec<GoldenDep> = deps
                .iter()
                .map(|d| GoldenDep {
                    task_id: d.task_id.clone(),
                    depends_on: d.depends_on.clone(),
                })
                .collect();
            dep_list.sort_by(|a, b| (&a.task_id, &a.depends_on).cmp(&(&b.task_id, &b.depends_on)));

            // Query ready/blocked
            let ready = list_ready(conn, None)?;
            let mut ready_ids: Vec<String> = ready.iter().map(|r| r.task_id.clone()).collect();
            ready_ids.sort();

            let blocked = list_blocked(conn, None)?;
            let mut blocked_ids: Vec<String> = blocked.iter().map(|r| r.task_id.clone()).collect();
            blocked_ids.sort();

            // Query labels
            let labels_raw = list_all_labels(conn)?;
            let mut label_map: HashMap<String, Vec<String>> = HashMap::new();
            for (task_id, label) in &labels_raw {
                label_map
                    .entry(task_id.clone())
                    .or_default()
                    .push(label.clone());
            }

            Ok((tasks, dep_list, ready_ids, blocked_ids, label_map))
        })
        .unwrap();

    assert_eq!(actual_tasks, expected.tasks);

    let mut expected_deps = expected.deps.clone();
    expected_deps.sort_by(|a, b| (&a.task_id, &a.depends_on).cmp(&(&b.task_id, &b.depends_on)));
    assert_eq!(actual_deps, expected_deps);

    let mut expected_ready = expected.ready_ids.clone();
    expected_ready.sort();
    assert_eq!(actual_ready, expected_ready);

    let mut expected_blocked = expected.blocked_ids.clone();
    expected_blocked.sort();
    assert_eq!(actual_blocked, expected_blocked);

    assert_eq!(actual_labels, expected.labels);
}

#[test]
fn golden_task_replay_idempotent() {
    let jsonl = fixture_text("golden/task_events.jsonl");
    let events: Vec<TaskEvent> = jsonl
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("parse event"))
        .collect();

    let db = Db::open_in_memory().unwrap();

    // Apply once, then rebuild, verify identical state
    let (count1, ready1, deps1_len, labels1) = db
        .with_write_conn(|conn| {
            for event in &events {
                apply_event(conn, event, "")?;
            }
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
            let ready: Vec<String> = list_ready(conn, None)?
                .iter()
                .map(|r| r.task_id.clone())
                .collect();
            let deps_len = list_all_deps(conn)?.len();
            let labels = list_all_labels(conn)?;
            Ok((count, ready, deps_len, labels))
        })
        .unwrap();

    let (count2, ready2, deps2_len, labels2) = db
        .with_write_conn(|conn| {
            rebuild(conn, &events)?;
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
            let ready: Vec<String> = list_ready(conn, None)?
                .iter()
                .map(|r| r.task_id.clone())
                .collect();
            let deps_len = list_all_deps(conn)?.len();
            let labels = list_all_labels(conn)?;
            Ok((count, ready, deps_len, labels))
        })
        .unwrap();

    assert_eq!(count1, count2);
    assert_eq!(ready1, ready2);
    assert_eq!(deps1_len, deps2_len);
    assert_eq!(labels1, labels2);
}

// ─── Golden #5: Intent ranking ──────────────────────────────────

#[test]
fn golden_intent_ranking() {
    let expected: GoldenIntentRanking = load_golden("golden/intent_ranking.json");
    let signals = golden_to_signals(&expected.candidates);

    for (intent, expected_order) in &expected.expected_orders {
        let profile = resolve_intent(intent);
        let weights = Weights::from_profile(profile);
        let results = rank_candidates(&signals, &weights, &[], &HashMap::new());
        let actual_order: Vec<String> = results.iter().map(|r| r.chunk_id.clone()).collect();
        assert_eq!(
            &actual_order, expected_order,
            "ranking mismatch for intent '{intent}'"
        );
    }
}
