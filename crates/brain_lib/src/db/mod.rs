pub mod chunks;
pub mod files;
pub mod fts;
pub mod links;
pub mod meta;
mod migrations;
pub(crate) mod schema;
pub mod summaries;

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

/// Collect all rows from a `query_map` result into a `Vec`.
///
/// Replaces the boilerplate:
/// ```ignore
/// let mut result = Vec::new();
/// for row in rows { result.push(row?); }
/// Ok(result)
/// ```
pub(crate) fn collect_rows<T>(
    rows: impl Iterator<Item = std::result::Result<T, rusqlite::Error>>,
) -> Result<Vec<T>> {
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

/// SQLite control-plane database with a thread-safe connection.
///
/// The inner connection is wrapped in `Arc<Mutex<>>` so that `Db` is both
/// `Send` and `Sync`, allowing it to be held across `.await` points in async code.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    /// Open (or create) the SQLite database at the given path and initialize the schema.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        schema::init_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory database (for testing).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        schema::init_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Execute a closure with a reference to the underlying connection.
    ///
    /// This is the primary way to interact with the database. The mutex is held
    /// for the duration of the closure, so keep operations short.
    pub fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let conn = self
            .conn
            .lock()
            .map_err(|e| BrainCoreError::Database(format!("mutex poisoned: {e}")))?;
        f(&conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_open_creates_and_reopens() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        // First open: creates the database
        {
            let _db = Db::open(path).unwrap();
        }

        // Second open: reuses the database without error
        {
            let db = Db::open(path).unwrap();
            // Verify tables exist by querying one
            db.with_conn(|conn| {
                let count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='files'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(count, 1);
                Ok(())
            })
            .unwrap();
        }
    }
}
