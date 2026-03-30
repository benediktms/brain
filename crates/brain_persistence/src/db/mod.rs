pub mod chunks;
pub mod crypto;
pub mod files;
pub mod fts;
pub mod job;
pub mod jobs;
pub mod links;
pub mod meta;
#[cfg(test)]
mod migration_harness;
mod migrations;
pub mod object_links;
pub mod providers;
pub mod records;
pub mod schema;
pub mod summaries;
pub mod tasks;

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub use rusqlite::Connection;
use rusqlite::OpenFlags;

use crate::error::{BrainCoreError, Result};

/// Collect all rows from a `query_map` result into a `Vec`.
///
/// Replaces the boilerplate:
/// ```ignore
/// let mut result = Vec::new();
/// for row in rows { result.push(row?); }
/// Ok(result)
/// ```
pub fn collect_rows<T>(
    rows: impl Iterator<Item = std::result::Result<T, rusqlite::Error>>,
) -> Result<Vec<T>> {
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

/// Number of read-only connections in the pool for on-disk databases.
const READ_POOL_SIZE: usize = 4;

/// SQLite database with separate read/write connections.
///
/// WAL mode (already enabled by schema init) allows concurrent readers alongside
/// a single writer. The write connection is exclusive (`Arc<Mutex<>>`), while
/// read connections are pooled and round-robined to eliminate contention between
/// queries and writes.
#[derive(Clone)]
pub struct Db {
    writer: Arc<Mutex<Connection>>,
    readers: Arc<Vec<Mutex<Connection>>>,
    next_reader: Arc<AtomicUsize>,
}

impl Db {
    /// Open (or create) the SQLite database at the given path and initialize the schema.
    pub fn open(path: &Path) -> Result<Self> {
        let writer = Connection::open(path)?;
        schema::init_schema(&writer)?;

        let read_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        let mut readers = Vec::with_capacity(READ_POOL_SIZE);
        for _ in 0..READ_POOL_SIZE {
            let r = Connection::open_with_flags(path, read_flags)?;
            r.pragma_update(None, "query_only", "ON")
                .map_err(|e| BrainCoreError::Database(format!("set query_only: {e}")))?;
            r.pragma_update(None, "busy_timeout", "5000")
                .map_err(|e| BrainCoreError::Database(format!("set busy_timeout: {e}")))?;
            readers.push(Mutex::new(r));
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: Arc::new(readers),
            next_reader: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Open an in-memory database (for testing).
    ///
    /// Uses 0 readers — `with_read_conn` falls back to the write connection.
    pub fn open_in_memory() -> Result<Self> {
        let writer = Connection::open_in_memory()?;
        schema::init_schema(&writer)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: Arc::new(Vec::new()),
            next_reader: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Execute a closure with a reference to the write connection.
    pub fn with_write_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let conn = self
            .writer
            .lock()
            .map_err(|e| BrainCoreError::Database(format!("writer mutex poisoned: {e}")))?;
        f(&conn)
    }

    /// Execute a closure with a reference to a read-only connection.
    ///
    /// Round-robins across the read pool. Falls back to the write connection
    /// when no readers are available (in-memory databases).
    pub fn with_read_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        if self.readers.is_empty() {
            return self.with_write_conn(f);
        }
        let idx = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let conn = self.readers[idx]
            .lock()
            .map_err(|e| BrainCoreError::Database(format!("reader mutex poisoned: {e}")))?;
        f(&conn)
    }

    /// Ensure a brain is registered in the `brains` table (idempotent).
    ///
    /// Must be called before writing tasks/records with FK-constrained `brain_id`.
    pub fn ensure_brain_registered(&self, brain_id: &str, brain_name: &str) -> Result<()> {
        self.with_write_conn(|conn| schema::ensure_brain_registered(conn, brain_id, brain_name))
    }

    /// Project state_projection.toml brain entries into the brains table.
    ///
    /// Sets projected=1 for all provided brains. Stale projected rows (no longer
    /// in config) are cleared to projected=0. Preserves existing prefix values.
    pub fn project_config_to_brains(&self, brains: &[schema::BrainProjection]) -> Result<()> {
        self.with_write_conn(|conn| schema::project_config_to_brains(conn, brains))
    }

    /// Resolve a brain by name, brain_id, alias, or root path.
    ///
    /// Returns `(brain_id, name)`. Resolution order: name → id → alias → root path.
    pub fn resolve_brain(&self, input: &str) -> Result<(String, String)> {
        self.with_read_conn(|conn| schema::resolve_brain(conn, input))
    }

    /// Upsert a brain entry. Preserves existing prefix via COALESCE.
    pub fn upsert_brain(&self, input: &schema::BrainUpsert<'_>) -> Result<()> {
        self.with_write_conn(|conn| schema::upsert_brain(conn, input))
    }

    /// Check whether a brain has been archived.
    ///
    /// Returns `false` when no matching row exists (brain not yet registered).
    pub fn is_brain_archived(&self, brain_id: &str) -> Result<bool> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| schema::is_brain_archived(conn, &brain_id))
    }

    /// List all brain rows, optionally filtered to active-only.
    pub fn list_brains(&self, active_only: bool) -> Result<Vec<schema::BrainRow>> {
        self.with_read_conn(|conn| schema::list_brains(conn, active_only))
    }

    /// Read prefix for a brain by brain_id.
    pub fn get_brain_prefix(&self, brain_id: &str) -> Result<Option<String>> {
        self.with_read_conn(|conn| schema::get_brain_prefix(conn, brain_id))
    }

    /// Read a full brain row by brain_id.
    pub fn get_brain(&self, brain_id: &str) -> Result<Option<schema::BrainRow>> {
        self.with_read_conn(|conn| schema::get_brain(conn, brain_id))
    }

    /// Read a full brain row by name.
    pub fn get_brain_by_name(&self, name: &str) -> Result<Option<schema::BrainRow>> {
        self.with_read_conn(|conn| schema::get_brain_by_name(conn, name))
    }

    /// Update roots JSON for a brain.
    pub fn update_brain_roots(&self, brain_id: &str, roots_json: &str) -> Result<()> {
        self.with_write_conn(|conn| schema::update_brain_roots(conn, brain_id, roots_json))
    }

    /// Mark a brain as archived.
    pub fn archive_brain(&self, brain_id: &str) -> Result<()> {
        self.with_write_conn(|conn| schema::archive_brain(conn, brain_id))
    }

    /// Atomically archive a brain and clear its roots.
    pub fn archive_and_clear_roots(&self, brain_id: &str) -> Result<()> {
        self.with_write_conn(|conn| schema::archive_and_clear_roots(conn, brain_id))
    }

    /// Delete a brain by name.
    pub fn delete_brain(&self, name: &str) -> Result<bool> {
        self.with_write_conn(|conn| schema::delete_brain(conn, name))
    }

    // ── Provider CRUD ──────────────────────────────────────────────────

    /// Insert a new provider. Returns the generated ID.
    pub fn insert_provider(&self, input: &providers::InsertProvider) -> Result<String> {
        self.with_write_conn(|conn| providers::insert_provider(conn, input))
    }

    /// Get a provider by ID.
    pub fn get_provider(&self, id: &str) -> Result<Option<providers::ProviderRow>> {
        self.with_read_conn(|conn| providers::get_provider(conn, id))
    }

    /// Get the most recently updated provider for a given name.
    pub fn get_provider_by_name(&self, name: &str) -> Result<Option<providers::ProviderRow>> {
        self.with_read_conn(|conn| providers::get_provider_by_name(conn, name))
    }

    /// List all providers.
    pub fn list_providers(&self) -> Result<Vec<providers::ProviderRow>> {
        self.with_read_conn(providers::list_providers)
    }

    /// Delete a provider by ID.
    pub fn delete_provider(&self, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.with_write_conn(move |conn| providers::delete_provider(conn, &id))
    }

    /// Check if a provider with the given name and key hash exists.
    pub fn provider_exists(&self, name: &str, api_key_hash: &str) -> Result<bool> {
        self.with_read_conn(|conn| providers::provider_exists(conn, name, api_key_hash))
    }

    /// Flush the WAL file to the main database and truncate it.
    ///
    /// This ensures all committed transactions are persisted to the main
    /// database file, which is important during graceful shutdown.
    pub fn wal_checkpoint(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use tempfile::TempDir;

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
            db.with_write_conn(|conn| {
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

    #[test]
    fn test_read_conn_rejects_writes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();

        // Read connections should reject INSERT statements
        let result = db.with_read_conn(|conn| {
            conn.execute("CREATE TABLE test_rw (id INTEGER PRIMARY KEY)", [])
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(())
        });
        assert!(result.is_err(), "write via read connection should fail");
    }

    #[test]
    fn test_in_memory_read_falls_back_to_writer() {
        let db = Db::open_in_memory().unwrap();
        assert!(db.readers.is_empty(), "in-memory should have 0 readers");

        // with_read_conn should fall back to writer and succeed
        db.with_write_conn(|conn| {
            conn.execute("CREATE TABLE fallback_test (id INTEGER PRIMARY KEY)", [])
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            conn.execute("INSERT INTO fallback_test (id) VALUES (1)", [])
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(())
        })
        .unwrap();

        // Read via with_read_conn (which falls back to writer for in-memory)
        let count: i64 = db
            .with_read_conn(|conn| {
                let c =
                    conn.query_row("SELECT COUNT(*) FROM fallback_test", [], |row| row.get(0))?;
                Ok(c)
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_concurrent_readers_and_writer() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("concurrent.db");
        let db = Db::open(&db_path).unwrap();

        // Seed a table with some rows
        db.with_write_conn(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS conc (id INTEGER PRIMARY KEY, val TEXT)",
                [],
            )
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            for i in 0..100 {
                conn.execute(
                    "INSERT INTO conc (id, val) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("v{i}")],
                )
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            }
            Ok(())
        })
        .unwrap();

        // Spawn concurrent readers + a writer — verify no deadlock or error
        let db_w = db.clone();
        let writer_handle = std::thread::spawn(move || {
            for i in 100..200 {
                db_w.with_write_conn(|conn| {
                    conn.execute(
                        "INSERT INTO conc (id, val) VALUES (?1, ?2)",
                        rusqlite::params![i, format!("v{i}")],
                    )
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?;
                    Ok(())
                })
                .unwrap();
            }
        });

        let mut reader_handles = Vec::new();
        for _ in 0..4 {
            let db_r = db.clone();
            reader_handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let count: i64 = db_r
                        .with_read_conn(|conn| {
                            let c =
                                conn.query_row("SELECT COUNT(*) FROM conc", [], |row| row.get(0))?;
                            Ok(c)
                        })
                        .unwrap();
                    // Count should be between initial 100 and final 200
                    assert!((100..=200).contains(&count), "unexpected count: {count}");
                }
            }));
        }

        writer_handle.join().expect("writer thread panicked");
        for h in reader_handles {
            h.join().expect("reader thread panicked");
        }

        // Final count should be exactly 200
        let final_count: i64 = db
            .with_read_conn(|conn| {
                let c = conn.query_row("SELECT COUNT(*) FROM conc", [], |row| row.get(0))?;
                Ok(c)
            })
            .unwrap();
        assert_eq!(final_count, 200);
    }

    #[test]
    fn test_wal_checkpoint() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("checkpoint.db");
        let db = Db::open(&db_path).unwrap();

        // Write some data so the WAL has content
        db.with_write_conn(|conn| {
            conn.execute(
                "CREATE TABLE ckpt_test (id INTEGER PRIMARY KEY, val TEXT)",
                [],
            )
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            conn.execute("INSERT INTO ckpt_test (id, val) VALUES (1, 'hello')", [])
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(())
        })
        .unwrap();

        // Checkpoint should succeed and truncate the WAL
        db.wal_checkpoint().unwrap();

        // WAL file should be truncated (0 bytes)
        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            let wal_size = std::fs::metadata(&wal_path).unwrap().len();
            assert_eq!(wal_size, 0, "WAL should be truncated after checkpoint");
        }

        // Data should still be readable
        let count: i64 = db
            .with_read_conn(|conn| {
                let c = conn.query_row("SELECT COUNT(*) FROM ckpt_test", [], |row| row.get(0))?;
                Ok(c)
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_reader_pool_round_robins() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("roundrobin.db");
        let db = Db::open(&db_path).unwrap();

        assert_eq!(db.readers.len(), READ_POOL_SIZE);

        // Make several read calls and verify the counter advances
        let start = db.next_reader.load(Ordering::Relaxed);
        for i in 1..=8 {
            db.with_read_conn(|_conn| Ok(())).unwrap();
            let current = db.next_reader.load(Ordering::Relaxed);
            assert_eq!(current, start + i);
        }
    }
}
