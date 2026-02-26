use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    types::Float32Type, FixedSizeListArray, Float32Array, Int32Array, RecordBatch,
    RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lancedb::query::{ExecutableQuery, QueryBase};
use tracing::info;

use crate::error::BrainCoreError;

const EMBEDDING_DIM: i32 = 384;

pub struct Store {
    db: lancedb::Connection,
    table: lancedb::Table,
}

impl Store {
    /// Open or create a LanceDB store at the given directory.
    pub async fn open_or_create(db_path: &Path) -> crate::error::Result<Self> {
        let db = lancedb::connect(db_path.to_str().unwrap_or("brain_lancedb"))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to connect: {e}")))?;

        let table_names = db
            .table_names()
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to list tables: {e}")))?;

        let table = if table_names.iter().any(|n| n == "chunks") {
            db.open_table("chunks")
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to open table: {e}")))?
        } else {
            let schema = chunks_schema();
            let empty_batch = empty_record_batch(&schema);
            let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
            db.create_table("chunks", Box::new(batches))
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to create table: {e}")))?
        };

        info!("LanceDB store ready");
        Ok(Self { db, table })
    }

    /// Insert chunks with their embeddings for a given file.
    pub async fn insert_chunks(
        &self,
        file_path: &str,
        chunks: &[(usize, &str)], // (ord, content)
        embeddings: &[Vec<f32>],
    ) -> crate::error::Result<()> {
        assert_eq!(chunks.len(), embeddings.len());
        if chunks.is_empty() {
            return Ok(());
        }

        let schema = chunks_schema();
        let batch = make_record_batch(&schema, file_path, chunks, embeddings)?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));

        self.table
            .add(Box::new(batches))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("insert failed: {e}")))?;

        info!(file_path, chunk_count = chunks.len(), "chunks inserted");
        Ok(())
    }

    /// Search for the top-k most similar chunks to the given embedding.
    pub async fn query(
        &self,
        embedding: &[f32],
        top_k: usize,
    ) -> crate::error::Result<Vec<QueryResult>> {
        let results = self
            .table
            .vector_search(embedding)
            .map_err(|e| BrainCoreError::VectorDb(format!("search setup failed: {e}")))?
            .distance_type(lancedb::DistanceType::Dot)
            .limit(top_k)
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("search failed: {e}")))?;

        let mut output = Vec::new();
        // Collect record batches from the stream
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = results
            .try_collect()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("result collection failed: {e}")))?;

        for batch in &batches {
            let file_paths = batch
                .column_by_name("file_path")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing file_path column".into()))?;
            let chunk_ords = batch
                .column_by_name("chunk_ord")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing chunk_ord column".into()))?;
            let contents = batch
                .column_by_name("content")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing content column".into()))?;
            let distances = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

            for i in 0..batch.num_rows() {
                output.push(QueryResult {
                    file_path: file_paths.value(i).to_string(),
                    chunk_ord: chunk_ords.value(i) as usize,
                    content: contents.value(i).to_string(),
                    score: distances.map(|d| d.value(i)),
                });
            }
        }

        Ok(output)
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub file_path: String,
    pub chunk_ord: usize,
    pub content: String,
    pub score: Option<f32>,
}

fn chunks_schema() -> Schema {
    Schema::new(vec![
        Field::new("chunk_id", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("chunk_ord", DataType::Int32, false),
        Field::new("content", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                EMBEDDING_DIM,
            ),
            false,
        ),
    ])
}

fn empty_record_batch(schema: &Schema) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    Vec::<Option<Vec<Option<f32>>>>::new(),
                    EMBEDDING_DIM,
                ),
            ),
        ],
    )
    .expect("empty batch should be valid")
}

fn make_record_batch(
    schema: &Schema,
    file_path: &str,
    chunks: &[(usize, &str)],
    embeddings: &[Vec<f32>],
) -> crate::error::Result<RecordBatch> {
    let chunk_ids: Vec<String> = chunks
        .iter()
        .map(|(ord, _)| format!("{file_path}:{ord}"))
        .collect();
    let file_paths: Vec<&str> = vec![file_path; chunks.len()];
    let ords: Vec<i32> = chunks.iter().map(|(ord, _)| *ord as i32).collect();
    let contents: Vec<&str> = chunks.iter().map(|(_, content)| *content).collect();

    let embedding_values: Vec<Option<Vec<Option<f32>>>> = embeddings
        .iter()
        .map(|emb| Some(emb.iter().map(|v| Some(*v)).collect()))
        .collect();

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(chunk_ids)),
            Arc::new(StringArray::from(file_paths)),
            Arc::new(Int32Array::from(ords)),
            Arc::new(StringArray::from(contents)),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    embedding_values,
                    EMBEDDING_DIM,
                ),
            ),
        ],
    )
    .map_err(|e| BrainCoreError::VectorDb(format!("failed to build record batch: {e}")))
}
