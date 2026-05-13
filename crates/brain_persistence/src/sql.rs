//! SQL result type local to `brain_persistence`.
//!
//! # Design
//!
//! `SqlError` is the error type for all closures passed to `Db::with_*_conn`.
//! It has two infrastructure variants and one domain passthrough:
//!
//! - `SqlError::Rusqlite` — low-level SQLite errors; absorbed natively by `?`
//!   via the `#[from]` impl so closures never need to spell out the conversion.
//! - `SqlError::MutexPoisoned` — the write-connection `Mutex` was poisoned by a
//!   previous panic; classified separately from SQL errors so callers can decide
//!   whether to abort vs. recover.
//! - `SqlError::Domain` — a `BrainCoreError` returned from business logic
//!   running inside a closure. **There is no `From<BrainCoreError>` impl**:
//!   callers must write `.map_err(SqlError::Domain)?` or
//!   `Err(SqlError::Domain(...))` explicitly. This makes the SQL/domain boundary
//!   visible at every call site and prevents accidental smuggling via `?`.
//!
//! At the persistence boundary, [`SqlResultExt::into_brain_core`] translates a
//! `SqlResult<T>` back to `brain_core::error::Result<T>`.

use brain_core::error::BrainCoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("rusqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    #[error("payload serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("mutex poisoned: {0}")]
    MutexPoisoned(String),

    /// A domain error surfaced from business logic running inside a
    /// `with_*_conn` closure. Use `.map_err(SqlError::Domain)?` or
    /// `Err(SqlError::Domain(...))` to lift a `BrainCoreError` into this
    /// variant — there is intentionally no `From` impl.
    #[error("{0}")]
    Domain(BrainCoreError),
}

pub type SqlResult<T> = std::result::Result<T, SqlError>;

/// Convert a `SqlResult<T>` back to `brain_core::error::Result<T>`. Used at
/// the boundary where persistence-internal results escape into framework-
/// agnostic code.
pub trait SqlResultExt<T> {
    fn into_brain_core(self) -> brain_core::error::Result<T>;
}

impl<T> SqlResultExt<T> for SqlResult<T> {
    fn into_brain_core(self) -> brain_core::error::Result<T> {
        self.map_err(|e| match e {
            SqlError::Rusqlite(err) => BrainCoreError::Database(err.to_string()),
            SqlError::Serde(err) => BrainCoreError::TaskEvent(format!(
                "payload serialize failed: {err}"
            )),
            SqlError::Domain(err) => err,
            SqlError::MutexPoisoned(msg) => BrainCoreError::Database(msg),
        })
    }
}
