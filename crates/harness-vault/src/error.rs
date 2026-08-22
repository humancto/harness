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

    #[error("serialize error for {path}: {source}")]
    Serialize {
        path: PathBuf,
        #[source]
        source: toml::ser::Error,
    },

    /// Envelope is structurally wrong: unsupported `format_version`,
    /// non-hex fields, wrong nonce length, non-UTF-8 payload.
    #[error("malformed encrypted credential file {path}: {reason}")]
    BadFormat { path: PathBuf, reason: String },

    /// AEAD open failed. Deliberately does not distinguish wrong key
    /// from tampered ciphertext — the cipher can't, and the error
    /// message must not help an attacker tell the difference.
    #[error(
        "failed to decrypt {path}: wrong key or tampered file \
         (was the node identity key replaced?)"
    )]
    Decrypt { path: PathBuf },

    #[error("failed to encrypt credential file {path}")]
    Encrypt { path: PathBuf },
}
