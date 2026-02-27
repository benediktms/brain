pub mod chunks;
pub mod files;
pub mod schema;

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

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
    #[cfg(test)]
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
            let db = Db::open(path).unwrap();
            let version = db.with_conn(schema::schema_version).unwrap();
            assert_eq!(version, Some(1));
        }

        // Second open: reuses the database
        {
            let db = Db::open(path).unwrap();
            let version = db.with_conn(schema::schema_version).unwrap();
            assert_eq!(version, Some(1));
        }
    }
}
