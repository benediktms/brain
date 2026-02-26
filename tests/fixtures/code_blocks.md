# Useful Patterns: Error Handling Across Languages

Error handling is one of the most important design decisions in any project. Different languages take fundamentally different approaches, and understanding the tradeoffs helps you write more robust code regardless of your primary language.

## Rust: Algebraic Error Types

Rust uses the `Result<T, E>` type to make errors explicit in function signatures. The compiler forces you to handle both the success and failure cases. Combined with `thiserror` for library errors and `anyhow` for application errors, this gives you a layered strategy.

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("vector DB error: {0}")]
    VectorDb(String),

    #[error("serialization failed: {0}")]
    Serde(#[from] serde_json::Error),
}

pub fn open_database(path: &Path) -> Result<Database, StorageError> {
    let file = std::fs::File::open(path)?; // auto-converts via From
    let config: Config = serde_json::from_reader(file)?;
    Database::connect(&config).map_err(|e| StorageError::VectorDb(e.to_string()))
}
```

The `?` operator and `From` trait implementations create an ergonomic error propagation chain without hiding failure modes.

## Python: Exception Hierarchies

Python uses exceptions with inheritance-based hierarchies. This is more flexible at runtime but provides no compile-time guarantees about which errors a function might raise.

```python
class BrainError(Exception):
    """Base error for all brain operations."""
    pass

class EmbeddingError(BrainError):
    """Failed to generate embeddings."""
    pass

def embed_text(text: str) -> list[float]:
    if not text.strip():
        raise EmbeddingError("Cannot embed empty text")
    return model.encode(text).tolist()
```

The convention of catching specific exception types and letting unexpected errors propagate mirrors Rust's distinction between recoverable and unrecoverable errors.

## SQL: Error Codes and Transactions

Database errors require special handling because partial failures can leave data in an inconsistent state. The solution is transactions with explicit rollback on error.

```sql
BEGIN TRANSACTION;

INSERT INTO files (file_id, path, content_hash)
VALUES ('01H5...', '/notes/example.md', 'abc123');

INSERT INTO chunks (chunk_id, file_id, content, ord)
VALUES ('01H5...a', '01H5...', 'First paragraph...', 0);

-- If any statement fails, roll back everything
COMMIT;
```

The key insight is that error handling in SQL is not about catching exceptions but about defining atomic boundaries. Either all inserts succeed or none do. This maps to Rust's approach of using `?` within a transaction closure and rolling back on any error.

## Common Principles

Across all three languages, effective error handling follows the same principles: make errors visible rather than silent, handle them at the appropriate level, and ensure that partial failures do not corrupt shared state. The syntax differs but the intent is universal.
