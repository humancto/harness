//! Errors returned by the vault.

use std::path::PathBuf;

use thiserror::Error;

/// Errors emitted while loading or parsing a credential store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SecretsError {
    #[error("$HOME could not be resolved")]
    NoHome,

    #[error("io error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parse error in {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("invalid tag {tag:?} (must match `secret/[a-z0-9-]+`)")]
    InvalidTag { tag: String },

    #[error("permissions on {path} are too loose ({mode:o}); expected mode 0600 or stricter")]
    PermissionsTooLoose { path: PathBuf, mode: u32 },
}
