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
}

impl From<rusqlite::Error> for BrainCoreError {
    fn from(e: rusqlite::Error) -> Self {
        BrainCoreError::Database(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, BrainCoreError>;
