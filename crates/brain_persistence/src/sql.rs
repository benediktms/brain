//! SQL error type local to brain_persistence.
//!
//! Keeps rusqlite out of brain_core: the `From<rusqlite::Error>` impl lives
//! here, on a wrapper this crate owns. Closures passed to `Db::with_*_conn`
//! return `SqlResult<T>`; the outer wrapper translates back to
//! `brain_core::error::Result<T>` via [`SqlResultExt::into_brain_core`].

use brain_core::error::BrainCoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("rusqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    /// Lets closures inside `with_*_conn` propagate `BrainCoreError` via `?`
    /// without forcing every call site to translate manually.
    #[error("{0}")]
    BrainCore(#[from] BrainCoreError),
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
            SqlError::BrainCore(err) => err,
        })
    }
}
