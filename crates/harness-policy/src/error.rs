//! Errors surfaced by `harness-policy` — distinct enough that the daemon
//! startup path can decide which kinds are fatal (parse / validate / no-home)
//! vs. "missing file → fall back to deny-all" (`Io` with `NotFound`).

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PolicyError {
    #[error("io error reading policy: {0}")]
    Io(#[from] std::io::Error),

    #[error("toml parse error: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("policy validation failed: {errors:#?}")]
    Validate { errors: Vec<String> },

    #[error("home directory could not be resolved")]
    NoHome,
}
