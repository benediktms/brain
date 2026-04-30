use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrainCoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("embedding error: {0}")]
    Embedding(String),

    #[error("vector DB error: {0}")]
    VectorDb(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("schema version error: {0}")]
    SchemaVersion(String),

    #[error("migration error: {0}")]
    Migration(String),

    #[error("task event error: {0}")]
    TaskEvent(String),

    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("task transfer CAS failed — retry: {0}")]
    TaskTransferCasFailed(String),

    #[error("brain not found: {0}")]
    BrainNotFound(String),

    #[error("task dependency cycle: {0}")]
    TaskCycle(String),

    #[error("record event error: {0}")]
    RecordEvent(String),

    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<rusqlite::Error> for BrainCoreError {
    fn from(e: rusqlite::Error) -> Self {
        BrainCoreError::Database(e.to_string())
    }
}

impl From<serde_json::Error> for BrainCoreError {
    fn from(e: serde_json::Error) -> Self {
        BrainCoreError::TaskEvent(format!("payload serialize failed: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, BrainCoreError>;
