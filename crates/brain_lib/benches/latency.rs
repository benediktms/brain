//! Criterion benchmarks for indexing, querying, and embedding latency.
//!
//! Uses MockEmbedder for CI compatibility — measures pipeline overhead, not
//! real inference. Run with: `cargo bench -p brain-lib`

use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::pipeline::IndexPipeline;
use brain_persistence::db::Db;
use brain_persistence::store::{IvfPqConfig, Store, VectorSearchMode};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tempfile::TempDir;

/// Create a pipeline with mock embedder in a temp directory.
async fn setup() -> (IndexPipeline, TempDir) {
    let tmp = TempDir::new().unwrap();
    let sqlite_path = tmp.path().join("brain.db");
    let lance_path = tmp.path().join("brain_lancedb");

    let db = Db::open(&sqlite_path).unwrap();
    let store = Store::open_or_create(&lance_path).await.unwrap();
    let embedder = Arc::new(MockEmbedder);

    let pipeline = IndexPipeline::with_embedder(db, store, embedder)
        .await
        .unwrap();
    (pipeline, tmp)
}

/// Generate `count` markdown files with 2 heading sections each.
fn generate_brain(dir: &std::path::Path, count: usize) -> Vec<PathBuf> {
    (0..count)
        .map(|i| {
            let path = dir.join(format!("file_{i:04}.md"));
            std::fs::write(
                &path,
                format!(
                    "## Section A of {i}\n\nContent alpha for file {i}.\n\n\
                     ## Section B of {i}\n\nContent beta for file {i}."
                ),
            )
            .unwrap();
            path
        })
        .collect()
}

// ─── Indexing benchmarks ─────────────────────────────────────────

fn bench_indexing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("indexing");

    for count in [1, 10, 50] {
        group.bench_function(format!("index_files_batch/{count}"), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: fresh pipeline + brain per iteration
                    let rt2 = tokio::runtime::Handle::current();
                    rt2.block_on(async {
                        let (pipeline, tmp) = setup().await;
                        let dir = tmp.path().join("notes");
                        std::fs::create_dir_all(&dir).unwrap();
                        let paths = generate_brain(&dir, count);
                        (pipeline, tmp, paths)
                    })
                },
                |(pipeline, _tmp, paths)| async move {
                    black_box(pipeline.index_files_batch(&paths).await.unwrap());
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.bench_function("index_file_single", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let rt2 = tokio::runtime::Handle::current();
                rt2.block_on(async {
                    let (pipeline, tmp) = setup().await;
                    let dir = tmp.path().join("notes");
                    std::fs::create_dir_all(&dir).unwrap();
                    let path = dir.join("single.md");
                    std::fs::write(
                        &path,
                        "## Heading\n\nSome benchmark content for a single file.",
                    )
                    .unwrap();
                    (pipeline, tmp, path)
                })
            },
            |(pipeline, _tmp, path)| async move {
                black_box(pipeline.index_file(&path).await.unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ─── Query benchmarks ────────────────────────────────────────────

fn bench_querying(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("querying");

    for corpus_size in [10, 50, 100] {
        group.bench_function(format!("vector_query/{corpus_size}"), |b| {
            // One-time setup: build corpus + optimize
            let (pipeline, _tmp, query_vec) = rt.block_on(async {
                let (pipeline, tmp) = setup().await;
                let dir = tmp.path().join("notes");
                std::fs::create_dir_all(&dir).unwrap();
                let paths = generate_brain(&dir, corpus_size);
                pipeline.index_files_batch(&paths).await.unwrap();
                pipeline.store().optimizer().force_optimize().await;

                let qv = pipeline
                    .embedder()
                    .embed_batch(&["benchmark search query"])
                    .unwrap()[0]
                    .clone();
                (pipeline, tmp, qv)
            });

            b.to_async(&rt).iter(|| {
                let qv = query_vec.clone();
                let store = pipeline.store();
                async move {
                    black_box(
                        store
                            .query(&qv, 10, 20, Default::default(), None)
                            .await
                            .unwrap(),
                    );
                }
            });
        });
    }

    group.finish();
}

// ─── Embedding benchmarks ────────────────────────────────────────

fn bench_embedding(c: &mut Criterion) {
    let mut group = c.benchmark_group("embedding");
    let embedder = MockEmbedder;

    for batch_size in [1, 10, 32, 64] {
        group.bench_function(format!("mock_embed_batch/{batch_size}"), |b| {
            let texts_owned: Vec<String> = (0..batch_size)
                .map(|i| format!("Chunk text {i} for benchmarking embedding throughput."))
                .collect();
            let texts: Vec<&str> = texts_owned.iter().map(|s| s.as_str()).collect();

            b.iter(|| {
                black_box(embedder.embed_batch(&texts).unwrap());
            });
        });
    }

    group.finish();
}

// ─── IVF-PQ recall benchmarks ───────────────────────────────────

fn bench_ivf_pq_recall(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("ivf_pq_recall");
    // Fewer iterations since index creation is expensive
    group.sample_size(10);

    let corpus_size = 500; // 500 files × 2 chunks = 1000 vectors

    // One-time setup: build corpus, get brute-force ground truth, then create index
    let (pipeline, _tmp, query_vecs, ground_truth) = rt.block_on(async {
        let (pipeline, tmp) = setup().await;
        let dir = tmp.path().join("notes");
        std::fs::create_dir_all(&dir).unwrap();
        let paths = generate_brain(&dir, corpus_size);
        pipeline.index_files_batch(&paths).await.unwrap();
        pipeline.store().optimizer().force_optimize().await;

        // Generate 10 query vectors
        let queries: Vec<String> = (0..10)
            .map(|i| format!("recall benchmark query number {i}"))
            .collect();
        let query_strs: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();
        let query_vecs = pipeline.embedder().embed_batch(&query_strs).unwrap();

        // Brute-force ground truth — Exact mode bypasses any ANN index.
        let mut ground_truth = Vec::new();
        for qv in &query_vecs {
            let results = pipeline
                .store()
                .query(qv, 10, 20, VectorSearchMode::Exact, None)
                .await
                .unwrap();
            let ids: Vec<String> = results.into_iter().map(|r| r.chunk_id).collect();
            ground_truth.push(ids);
        }

        // Create IVF-PQ index
        let config = IvfPqConfig::default();
        pipeline.store().create_vector_index(&config).await.unwrap();

        (pipeline, tmp, query_vecs, ground_truth)
    });

    for nprobes in [5, 10, 20, 40] {
        group.bench_function(format!("query_nprobes_{nprobes}"), |b| {
            b.to_async(&rt).iter(|| {
                let store = pipeline.store();
                let qvs = &query_vecs;
                async move {
                    for qv in qvs {
                        black_box(
                            store
                                .query(qv, 10, nprobes, Default::default(), None)
                                .await
                                .unwrap(),
                        );
                    }
                }
            });
        });
    }

    // After benching, measure recall@10 for each nprobes setting
    let recall_results: Vec<(usize, f64)> = rt.block_on(async {
        let mut results = Vec::new();
        for nprobes in [5, 10, 20, 40] {
            let mut total_recall = 0.0;
            for (i, qv) in query_vecs.iter().enumerate() {
                let ann_results = pipeline
                    .store()
                    .query(qv, 10, nprobes, Default::default(), None)
                    .await
                    .unwrap();
                let ann_ids: std::collections::HashSet<&str> =
                    ann_results.iter().map(|r| r.chunk_id.as_str()).collect();
                let gt_ids: std::collections::HashSet<&str> =
                    ground_truth[i].iter().map(|s| s.as_str()).collect();
                let intersection = ann_ids.intersection(&gt_ids).count();
                total_recall += intersection as f64 / gt_ids.len().max(1) as f64;
            }
            let avg_recall = total_recall / query_vecs.len() as f64;
            results.push((nprobes, avg_recall));
        }
        results
    });

    // Print recall results for reference
    eprintln!(
        "\n── IVF-PQ Recall@10 (corpus={corpus_size} files, {} vectors) ──",
        corpus_size * 2
    );
    for (nprobes, recall) in &recall_results {
        eprintln!("  nprobes={nprobes:>3}: recall@10 = {recall:.4}");
    }
    eprintln!();

    group.finish();
}

criterion_group!(
    benches,
    bench_indexing,
    bench_querying,
    bench_embedding,
    bench_ivf_pq_recall,
);
criterion_main!(benches);
