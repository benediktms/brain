//! Criterion benchmarks for indexing, querying, and embedding latency.
//!
//! Uses MockEmbedder for CI compatibility — measures pipeline overhead, not
//! real inference. Run with: `cargo bench -p brain-lib`

use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::db::Db;
use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::pipeline::IndexPipeline;
use brain_lib::store::Store;

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

    let pipeline = IndexPipeline::with_embedder(db, store, embedder);
    (pipeline, tmp)
}

/// Generate `count` markdown files with 2 heading sections each.
fn generate_vault(dir: &std::path::Path, count: usize) -> Vec<PathBuf> {
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
                    // Setup: fresh pipeline + vault per iteration
                    let rt2 = tokio::runtime::Handle::current();
                    rt2.block_on(async {
                        let (pipeline, tmp) = setup().await;
                        let dir = tmp.path().join("notes");
                        std::fs::create_dir_all(&dir).unwrap();
                        let paths = generate_vault(&dir, count);
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
                let paths = generate_vault(&dir, corpus_size);
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
                    black_box(store.query(&qv, 10).await.unwrap());
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

criterion_group!(benches, bench_indexing, bench_querying, bench_embedding);
criterion_main!(benches);
