//! `~/.harness/` identity file layout.
//!
//! On-disk format is **32 raw bytes** of the Ed25519 secret seed (no PEM, no
//! PKCS#8, no CBOR wrapper). Justification:
//!
//! - Smallest possible attack surface — no parser bugs to land on a private
//!   key.
//! - `harness_core::Identity::from_secret_bytes` already takes exactly
//!   `&[u8; 32]`; symmetrical.
//! - File mode `0600` is the access control; armoring adds nothing on top.
//!
//! ```text
//! ~/.harness/                  mode 0700, created if missing
//! ~/.harness/identity.key      mode 0600, the private key seed
//! ```
//!
//! See `phase-1.1-identity.plan.md` §4 for the design notes that drove these
//! choices.
//!
//! FORMAT v0: 32 raw bytes. If we ever switch to a wrapped on-disk format,
//! sniff a leading magic byte (length-32-vs-not is a free discriminator).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::{fs, io};

use harness_core::Identity;

use crate::fs_util::{create_root_dir, enforce_mode_0600, write_atomic, FsError, Mode0600Error};

/// Default seed length, equal to `Identity`'s expected secret-bytes length.
const SEED_LEN: usize = 32;

/// Name of the key file inside the identity root.
const KEY_FILENAME: &str = "identity.key";

/// Errors from filesystem identity operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IdentityError {
    #[error("could not locate user home directory")]
    NoHomeDir,
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("identity.key has wrong length: expected {expected} bytes, got {got}")]
    CorruptKeyFile { expected: usize, got: usize },
    #[error("identity.key has unsafe permissions {actual:o}; expected 0600")]
    UnsafePermissions { actual: u32 },
}

impl IdentityError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

impl From<FsError> for IdentityError {
    fn from(e: FsError) -> Self {
        match e {
            FsError::Io { path, source } => Self::Io { path, source },
        }
    }
}

impl From<Mode0600Error> for IdentityError {
    fn from(e: Mode0600Error) -> Self {
        match e {
            Mode0600Error::Io { path, source } => Self::Io { path, source },
            Mode0600Error::UnsafePermissions { actual } => Self::UnsafePermissions { actual },
        }
    }
}

/// `~/.harness/`, resolved from the user's home directory.
pub fn default_root() -> Result<PathBuf, IdentityError> {
    dirs::home_dir()
        .map(|h| h.join(".harness"))
        .ok_or(IdentityError::NoHomeDir)
}

/// Path to the key file inside `root`.
fn key_path(root: &Path) -> PathBuf {
    root.join(KEY_FILENAME)
}

/// Load an identity from `<root>/identity.key`, or generate + save one if the
/// file is absent.
///
/// This is what daemon startup will call. If the file exists, it is loaded
/// strictly — wrong length, unsafe permissions, or any I/O error are hard
/// errors, not fall-back-to-generate.
pub fn init_or_load(root: &Path) -> Result<Identity, IdentityError> {
    let key = key_path(root);
    match fs::metadata(&key) {
        Ok(_) => load(root),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let id = Identity::generate();
            save(root, &id)?;
            Ok(id)
        }
        Err(e) => Err(IdentityError::io(key, e)),
    }
}

/// Load an identity from `<root>/identity.key`. Errors if the file does not
/// exist (use [`init_or_load`] for the create-if-absent variant).
pub fn load(root: &Path) -> Result<Identity, IdentityError> {
    let key = key_path(root);
    enforce_mode_0600(&key)?;

    let mut file = fs::File::open(&key).map_err(|e| IdentityError::io(&key, e))?;
    let mut buf = Vec::with_capacity(SEED_LEN);
    file.read_to_end(&mut buf)
        .map_err(|e| IdentityError::io(&key, e))?;
    let seed: [u8; SEED_LEN] =
        buf.as_slice()
            .try_into()
            .map_err(|_| IdentityError::CorruptKeyFile {
                expected: SEED_LEN,
                got: buf.len(),
            })?;
    Ok(Identity::from_secret_bytes(&seed))
}

/// Save `id` to `<root>/identity.key` with mode `0600`. Creates `<root>`
/// (mode `0700`) if missing. Refuses to overwrite an existing key file —
/// rotation requires explicit policy that this PR doesn't define.
pub fn save(root: &Path, id: &Identity) -> Result<(), IdentityError> {
    create_root_dir(root)?;
    let final_path = key_path(root);
    if final_path.exists() {
        return Err(IdentityError::io(
            &final_path,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "identity.key already exists; refusing to overwrite",
            ),
        ));
    }
    let tmp = root.join(format!("{KEY_FILENAME}.tmp.{}", std::process::id()));
    let bytes = id.to_secret_bytes();
    write_atomic(&tmp, &final_path, bytes.as_slice())?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn corrupt_key_file_error_carries_lengths() {
        let e = IdentityError::CorruptKeyFile {
            expected: 32,
            got: 16,
        };
        let msg = e.to_string();
        assert!(
            msg.contains("32"),
            "expected msg includes expected len: {msg}"
        );
        assert!(
            msg.contains("16"),
            "expected msg includes actual len: {msg}"
        );
    }
}
