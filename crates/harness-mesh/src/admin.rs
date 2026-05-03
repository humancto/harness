//! Admin password storage at `~/.harness/admin.toml`. ADR-0007 documents
//! the decision rationale (argon2id, single-user, sliding-TTL bearer
//! sessions). This module owns the *persistence* side; the HTTP middleware
//! lives in `harness-api`.

use std::path::{Path, PathBuf};

use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fs_util::{create_root_dir, enforce_mode_0600, write_atomic};

const ADMIN_TOML: &str = "admin.toml";

/// On-disk format. `format_version` lets us evolve the layout without a
/// silent migration footgun.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminFile {
    pub format_version: u32,
    pub hash: String,
    /// Unix seconds.
    pub created_at: u64,
}

impl AdminFile {
    /// Current format version. Bumping it invalidates older files (the
    /// caller must migrate or refuse to load).
    pub const CURRENT_VERSION: u32 = 1;

    /// Hash a fresh password using argon2id at OWASP-default params.
    ///
    /// # Errors
    /// `AdminError::Hash` on encoding failure (vanishingly rare; the
    /// argon2 crate returns its own typed error).
    pub fn from_password(password: &str) -> Result<Self, AdminError> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AdminError::Hash(e.to_string()))?
            .to_string();
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(Self {
            format_version: Self::CURRENT_VERSION,
            hash,
            created_at,
        })
    }

    /// Verify a candidate password against the stored hash.
    /// Constant-time at the argon2 layer.
    #[must_use]
    pub fn verify(&self, password: &str) -> bool {
        let Ok(parsed) = PasswordHash::new(&self.hash) else {
            return false;
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    }
}

/// Errors from the admin store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AdminError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("toml encode: {0}")]
    TomlEncode(#[from] toml::ser::Error),

    #[error("toml decode: {0}")]
    TomlDecode(#[from] toml::de::Error),

    #[error("hash: {0}")]
    Hash(String),

    #[error("admin file at {path:?} has format_version {found}, expected {expected}")]
    FormatVersion {
        path: PathBuf,
        found: u32,
        expected: u32,
    },

    #[error("admin password not set; run `harness admin set-password`")]
    NotInitialized,
}

/// Path to `<root>/admin.toml`.
#[must_use]
pub fn admin_path(root: &Path) -> PathBuf {
    root.join(ADMIN_TOML)
}

/// Load the admin file, if present.
///
/// # Errors
/// Returns `AdminError::NotInitialized` if the file is missing,
/// `AdminError::Io` on any other read failure, `AdminError::TomlDecode`
/// on malformed contents, `AdminError::FormatVersion` on a mismatch.
pub fn load(root: &Path) -> Result<AdminFile, AdminError> {
    let path = admin_path(root);
    if !path.exists() {
        return Err(AdminError::NotInitialized);
    }
    let bytes = std::fs::read(&path)?;
    let f: AdminFile = toml::from_str(&String::from_utf8_lossy(&bytes))?;
    if f.format_version != AdminFile::CURRENT_VERSION {
        return Err(AdminError::FormatVersion {
            path,
            found: f.format_version,
            expected: AdminFile::CURRENT_VERSION,
        });
    }
    Ok(f)
}

/// Set (or replace) the admin password. Atomic write + 0600 mode.
///
/// # Errors
/// IO / hashing / encoding errors.
pub fn save(root: &Path, file: &AdminFile) -> Result<(), AdminError> {
    create_root_dir(root).map_err(|e| AdminError::Io(std::io::Error::other(e.to_string())))?;
    let path = admin_path(root);
    let tmp = path.with_extension("toml.tmp");
    let toml = toml::to_string_pretty(file)?;
    write_atomic(&tmp, &path, toml.as_bytes())
        .map_err(|e| AdminError::Io(std::io::Error::other(e.to_string())))?;
    enforce_mode_0600(&path).map_err(|e| AdminError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

/// Convenience: hash + persist in one call.
pub fn set_password(root: &Path, password: &str) -> Result<AdminFile, AdminError> {
    let file = AdminFile::from_password(password)?;
    save(root, &file)?;
    Ok(file)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn from_password_then_verify_succeeds() {
        let f = AdminFile::from_password("hunter2").expect("hash");
        assert!(f.verify("hunter2"));
        assert!(!f.verify("hunter3"));
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = TempDir::new().expect("tmp");
        let f = AdminFile::from_password("hunter2").expect("hash");
        save(dir.path(), &f).expect("save");
        let loaded = load(dir.path()).expect("load");
        assert_eq!(loaded, f);
        assert!(loaded.verify("hunter2"));
    }

    #[test]
    fn load_returns_not_initialized_when_absent() {
        let dir = TempDir::new().expect("tmp");
        let err = load(dir.path()).unwrap_err();
        assert!(matches!(err, AdminError::NotInitialized));
    }

    #[test]
    fn save_creates_file_at_admin_path() {
        let dir = TempDir::new().expect("tmp");
        set_password(dir.path(), "pw").expect("set");
        assert!(admin_path(dir.path()).exists());
    }

    #[test]
    fn replacing_password_overwrites_hash() {
        let dir = TempDir::new().expect("tmp");
        let _v1 = set_password(dir.path(), "first").expect("v1");
        let v2 = set_password(dir.path(), "second").expect("v2");
        let loaded = load(dir.path()).expect("load");
        assert_eq!(loaded, v2);
        assert!(!loaded.verify("first"));
        assert!(loaded.verify("second"));
    }

    #[test]
    fn empty_password_is_accepted_at_storage_level() {
        // Storage does not enforce a min-length policy — that's the CLI's
        // job. Confirm the storage is not the place that says "no."
        let f = AdminFile::from_password("").expect("hash empty");
        assert!(f.verify(""));
    }

    #[test]
    fn corrupt_hash_field_makes_verify_false() {
        let mut f = AdminFile::from_password("hunter2").expect("hash");
        f.hash = "not-a-valid-phc-string".into();
        assert!(!f.verify("hunter2"));
    }

    #[test]
    fn format_version_mismatch_is_error() {
        let dir = TempDir::new().expect("tmp");
        let mut f = AdminFile::from_password("hunter2").expect("hash");
        f.format_version = 99;
        save(dir.path(), &f).expect("save bogus");
        let err = load(dir.path()).unwrap_err();
        assert!(matches!(err, AdminError::FormatVersion { .. }));
    }
}
