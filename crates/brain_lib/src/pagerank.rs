use std::collections::HashMap;

use rusqlite::Connection;

use crate::error::Result;

const DAMPING: f64 = 0.85;
const MAX_ITER: usize = 50;
const CONVERGENCE_THRESHOLD: f64 = 1e-6;

/// Compute PageRank scores over the file link graph.
///
/// Returns a map of `file_id -> normalized_score` (min-max normalized to [0.0, 1.0]).
/// Files not present in the link graph receive the base score `(1 - d) / N`.
/// Returns an empty map if there are no nodes.
pub fn compute_pagerank(conn: &Connection) -> Result<HashMap<String, f64>> {
    // Build adjacency list from links with resolved target_file_id
    let mut out_edges: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_edges: HashMap<String, Vec<String>> = HashMap::new();
    let mut nodes: std::collections::HashSet<String> = std::collections::HashSet::new();

    let mut stmt = conn.prepare(
        "SELECT source_file_id, target_file_id FROM links WHERE target_file_id IS NOT NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        let source: String = row.get(0)?;
        let target: String = row.get(1)?;
        Ok((source, target))
    })?;

    for row in rows {
        let (source, target) = row?;
        nodes.insert(source.clone());
        nodes.insert(target.clone());
        out_edges.entry(source.clone()).or_default().push(target.clone());
        in_edges.entry(target).or_default().push(source);
    }

    // Also include all files from the files table so dangling files are scored
    let mut file_stmt = conn.prepare("SELECT file_id FROM files")?;
    let file_ids = file_stmt.query_map([], |row| row.get::<_, String>(0))?;
    for fid in file_ids {
        nodes.insert(fid?);
    }

    let n = nodes.len();
    if n == 0 {
        return Ok(HashMap::new());
    }

    let node_vec: Vec<String> = nodes.into_iter().collect();
    let n_f = n as f64;
    let base = (1.0 - DAMPING) / n_f;

    // Initialize scores uniformly
    let mut scores: HashMap<String, f64> = node_vec.iter().map(|id| (id.clone(), 1.0 / n_f)).collect();

    for _ in 0..MAX_ITER {
        let mut new_scores: HashMap<String, f64> = HashMap::with_capacity(n);

        // Sum dangling node scores (nodes with no outgoing edges)
        let dangling_sum: f64 = node_vec
            .iter()
            .filter(|id| !out_edges.contains_key(*id))
            .map(|id| scores[id])
            .sum();

        let dangling_contrib = DAMPING * dangling_sum / n_f;

        for v in &node_vec {
            // Contributions from nodes pointing to v
            let link_contrib: f64 = in_edges
                .get(v)
                .map(|sources| {
                    sources
                        .iter()
                        .map(|u| {
                            let out_deg = out_edges[u].len() as f64;
                            scores[u] / out_deg
                        })
                        .sum()
                })
                .unwrap_or(0.0);

            new_scores.insert(v.clone(), base + dangling_contrib + DAMPING * link_contrib);
        }

        // Check convergence
        let max_delta = node_vec
            .iter()
            .map(|id| (new_scores[id] - scores[id]).abs())
            .fold(0.0_f64, f64::max);

        scores = new_scores;

        if max_delta < CONVERGENCE_THRESHOLD {
            break;
        }
    }

    // Min-max normalize to [0.0, 1.0]
    let min_score = scores.values().cloned().fold(f64::INFINITY, f64::min);
    let max_score = scores.values().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = max_score - min_score;

    if range > 0.0 {
        for v in scores.values_mut() {
            *v = (*v - min_score) / range;
        }
    } else {
        // All scores equal — set to 0.5
        for v in scores.values_mut() {
            *v = 0.5;
        }
    }

    Ok(scores)
}

/// Compute PageRank and write scores to `files.pagerank_score`.
///
/// Files not in the link graph are assigned the median of computed scores (or 0.5 for an empty
/// graph). Scores are written in a single transaction.
pub fn compute_and_store_pagerank(conn: &Connection) -> Result<()> {
    let scores = compute_pagerank(conn)?;

    if scores.is_empty() {
        return Ok(());
    }

    // Compute median for files that may be absent from the scores map (shouldn't happen since we
    // include all files in compute_pagerank, but kept as defensive fallback).
    let mut sorted_vals: Vec<f64> = scores.values().cloned().collect();
    sorted_vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = if sorted_vals.len() % 2 == 0 {
        (sorted_vals[sorted_vals.len() / 2 - 1] + sorted_vals[sorted_vals.len() / 2]) / 2.0
    } else {
        sorted_vals[sorted_vals.len() / 2]
    };

    let tx = conn.unchecked_transaction()?;
    {
        let mut update_stmt =
            tx.prepare("UPDATE files SET pagerank_score = ?1 WHERE file_id = ?2")?;

        for (file_id, score) in &scores {
            update_stmt.execute(rusqlite::params![score, file_id])?;
        }

        // Files not in the scores map get the median (defensive)
        let mut missing_stmt = tx.prepare(
            "UPDATE files SET pagerank_score = ?1 WHERE pagerank_score IS NULL",
        )?;
        missing_stmt.execute(rusqlite::params![median])?;
    }
    tx.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_db(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE files (
                file_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                pagerank_score REAL
            );
            CREATE TABLE links (
                link_id TEXT PRIMARY KEY,
                source_file_id TEXT NOT NULL,
                target_file_id TEXT
            );",
        )
        .unwrap();
    }

    fn insert_file(conn: &Connection, id: &str) {
        conn.execute(
            "INSERT INTO files (file_id, path) VALUES (?1, ?2)",
            rusqlite::params![id, format!("{}.md", id)],
        )
        .unwrap();
    }

    fn insert_link(conn: &Connection, source: &str, target: &str) {
        let link_id = format!("{}_{}", source, target);
        conn.execute(
            "INSERT INTO links (link_id, source_file_id, target_file_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![link_id, source, target],
        )
        .unwrap();
    }

    #[test]
    fn test_empty_graph() {
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);
        // No files, no links
        let scores = compute_pagerank(&conn).unwrap();
        assert!(scores.is_empty());

        // compute_and_store_pagerank should also succeed
        compute_and_store_pagerank(&conn).unwrap();
    }

    #[test]
    fn test_star_topology() {
        // Hub (h) linked FROM 4 leaves (a, b, c, d): a->h, b->h, c->h, d->h
        // Hub should have the highest score.
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        let ids = ["h", "a", "b", "c", "d"];
        for id in &ids {
            insert_file(&conn, id);
        }
        insert_link(&conn, "a", "h");
        insert_link(&conn, "b", "h");
        insert_link(&conn, "c", "h");
        insert_link(&conn, "d", "h");

        let scores = compute_pagerank(&conn).unwrap();

        // All scores normalized to [0, 1]
        for (_, &s) in scores.iter() {
            assert!(s >= 0.0 && s <= 1.0, "score out of range: {s}");
        }

        let hub_score = scores["h"];
        for leaf in ["a", "b", "c", "d"] {
            assert!(
                hub_score > scores[leaf],
                "hub score {hub_score} should be > leaf score {} for {leaf}",
                scores[leaf]
            );
        }

        // Hub should be normalized to 1.0 (maximum)
        assert!(
            (hub_score - 1.0).abs() < 1e-9,
            "hub should be normalized to 1.0, got {hub_score}"
        );
    }

    #[test]
    fn test_cycle_equal_scores() {
        // A -> B -> C -> D -> A
        // All nodes should have approximately equal scores.
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        for id in ["a", "b", "c", "d"] {
            insert_file(&conn, id);
        }
        insert_link(&conn, "a", "b");
        insert_link(&conn, "b", "c");
        insert_link(&conn, "c", "d");
        insert_link(&conn, "d", "a");

        let scores = compute_pagerank(&conn).unwrap();

        let vals: Vec<f64> = ["a", "b", "c", "d"].iter().map(|id| scores[*id]).collect();
        let first = vals[0];
        for &v in &vals {
            assert!(
                (v - first).abs() < 1e-4,
                "cycle node scores should be approximately equal: {vals:?}"
            );
        }

        // All scores normalized to [0, 1]
        for v in &vals {
            assert!(*v >= 0.0 && *v <= 1.0);
        }
    }

    #[test]
    fn test_disconnected_nodes() {
        // nodes x, y have no links; node a -> b
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        for id in ["a", "b", "x", "y"] {
            insert_file(&conn, id);
        }
        insert_link(&conn, "a", "b");

        let scores = compute_pagerank(&conn).unwrap();

        // All 4 nodes present
        assert_eq!(scores.len(), 4);

        // All scores in [0, 1]
        for (_, &s) in &scores {
            assert!(s >= 0.0 && s <= 1.0, "score out of range: {s}");
        }

        // b (has inbound link) should score higher than x and y (no links at all)
        assert!(
            scores["b"] >= scores["x"],
            "b={} should be >= x={}",
            scores["b"],
            scores["x"]
        );
        assert!(
            scores["b"] >= scores["y"],
            "b={} should be >= y={}",
            scores["b"],
            scores["y"]
        );
    }

    #[test]
    fn test_normalization_range() {
        // Any non-trivial graph should produce scores in [0.0, 1.0].
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        for id in ["a", "b", "c"] {
            insert_file(&conn, id);
        }
        insert_link(&conn, "a", "b");
        insert_link(&conn, "b", "c");
        insert_link(&conn, "a", "c");

        let scores = compute_pagerank(&conn).unwrap();

        let min = scores.values().cloned().fold(f64::INFINITY, f64::min);
        let max = scores.values().cloned().fold(f64::NEG_INFINITY, f64::max);
        assert!((min - 0.0).abs() < 1e-9, "min should be 0.0, got {min}");
        assert!((max - 1.0).abs() < 1e-9, "max should be 1.0, got {max}");
    }

    #[test]
    fn test_compute_and_store_pagerank() {
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        for id in ["hub", "leaf1", "leaf2"] {
            insert_file(&conn, id);
        }
        insert_link(&conn, "leaf1", "hub");
        insert_link(&conn, "leaf2", "hub");

        compute_and_store_pagerank(&conn).unwrap();

        let hub_score: f64 = conn
            .query_row(
                "SELECT pagerank_score FROM files WHERE file_id = 'hub'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let leaf_score: f64 = conn
            .query_row(
                "SELECT pagerank_score FROM files WHERE file_id = 'leaf1'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(
            hub_score > leaf_score,
            "hub {hub_score} should exceed leaf {leaf_score}"
        );
        assert!(hub_score >= 0.0 && hub_score <= 1.0);
        assert!(leaf_score >= 0.0 && leaf_score <= 1.0);
    }
}
