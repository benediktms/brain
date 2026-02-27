use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray,
    types::Float32Type,
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
        let db = lancedb::connect(db_path.to_str().unwrap_or(".brain/lancedb"))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to connect: {e}")))?;

        let table_names = db
            .table_names()
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to list tables: {e}")))?;

        let table = if table_names.iter().any(|n| n == "chunks") {
            let t = db
                .open_table("chunks")
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to open table: {e}")))?;

            // POC migration: detect old schema (no file_id column) and recreate
            if Self::needs_migration(&t).await {
                info!("detected old POC schema without file_id column — recreating table");
                db.drop_table("chunks", &[]).await.map_err(|e| {
                    BrainCoreError::VectorDb(format!("failed to drop old table: {e}"))
                })?;
                let schema = chunks_schema();
                let empty_batch = empty_record_batch(&schema);
                let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
                db.create_table("chunks", Box::new(batches))
                    .execute()
                    .await
                    .map_err(|e| BrainCoreError::VectorDb(format!("failed to create table: {e}")))?
            } else {
                t
            }
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

    /// Check if the table uses the old POC schema (no file_id column).
    async fn needs_migration(table: &lancedb::Table) -> bool {
        match table.schema().await {
            Ok(schema) => schema.field_with_name("file_id").is_err(),
            Err(_) => false,
        }
    }

    /// Upsert chunks for a file using merge_insert.
    ///
    /// - Matched chunks (by chunk_id) are updated
    /// - New chunks are inserted
    /// - Orphaned chunks for this file_id are deleted
    pub async fn upsert_chunks(
        &self,
        file_id: &str,
        file_path: &str,
        chunks: &[(usize, &str)],
        embeddings: &[Vec<f32>],
    ) -> crate::error::Result<()> {
        if chunks.len() != embeddings.len() {
            return Err(BrainCoreError::VectorDb(format!(
                "chunk/embedding count mismatch: {} vs {}",
                chunks.len(),
                embeddings.len()
            )));
        }
        if chunks.is_empty() {
            // No chunks — just delete any existing chunks for this file
            self.delete_file_chunks(file_id).await?;
            return Ok(());
        }

        let schema = chunks_schema();
        let batch = make_record_batch(&schema, file_id, file_path, chunks, embeddings)?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));

        let mut builder = self.table.merge_insert(&["chunk_id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(Some(format!(
                "file_id = '{}'",
                validate_file_id(file_id)?
            )));
        builder
            .execute(Box::new(batches))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("upsert failed: {e}")))?;

        info!(
            file_path,
            file_id,
            chunk_count = chunks.len(),
            "chunks upserted"
        );
        Ok(())
    }

    /// Update the file_path column for all chunks belonging to a file_id.
    pub async fn update_file_path(
        &self,
        file_id: &str,
        new_path: &str,
    ) -> crate::error::Result<()> {
        let fid = validate_file_id(file_id)?;
        self.table
            .update()
            .only_if(format!("file_id = '{fid}'"))
            .column("file_path", format!("'{}'", new_path.replace('\'', "''")))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("update file_path failed: {e}")))?;

        info!(file_id, new_path, "file_path updated in LanceDB");
        Ok(())
    }

    /// Delete all chunks for a given file_id.
    pub async fn delete_file_chunks(&self, file_id: &str) -> crate::error::Result<()> {
        self.table
            .delete(&format!("file_id = '{}'", validate_file_id(file_id)?))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("delete failed: {e}")))?;

        info!(file_id, "file chunks deleted from LanceDB");
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

    /// Get a reference to the underlying LanceDB connection.
    pub fn connection(&self) -> &lancedb::Connection {
        &self.db
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
        Field::new("file_id", DataType::Utf8, false),
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

/// Validate that a file_id is a well-formed UUID (hex digits and hyphens only)
/// before interpolating it into a LanceDB filter expression.
fn validate_file_id(file_id: &str) -> crate::error::Result<&str> {
    if !file_id.is_empty() && file_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        Ok(file_id)
    } else {
        Err(BrainCoreError::VectorDb(format!(
            "invalid file_id for filter: {file_id}"
        )))
    }
}

fn make_record_batch(
    schema: &Schema,
    file_id: &str,
    file_path: &str,
    chunks: &[(usize, &str)],
    embeddings: &[Vec<f32>],
) -> crate::error::Result<RecordBatch> {
    let chunk_ids: Vec<String> = chunks
        .iter()
        .map(|(ord, _)| format!("{file_id}:{ord}"))
        .collect();
    let file_ids: Vec<&str> = vec![file_id; chunks.len()];
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
            Arc::new(StringArray::from(file_ids)),
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
