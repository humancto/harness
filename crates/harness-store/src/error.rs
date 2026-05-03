//! Errors from the `SQLite` store.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    #[error("home directory not found")]
    HomeNotFound,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("migration {version} failed: {reason}")]
    Migration { version: u32, reason: String },

    #[error("schema version mismatch: db reports {found}, expected {expected}")]
    SchemaVersion { found: String, expected: String },

    #[error("cbor encode/decode: {0}")]
    Cbor(String),

    #[error("invalid db path {0:?}")]
    InvalidPath(PathBuf),

    #[error("not found")]
    NotFound,

    #[error("invalid task state transition from {from:?} to {to:?}")]
    InvalidTransition { from: String, to: String },
}
