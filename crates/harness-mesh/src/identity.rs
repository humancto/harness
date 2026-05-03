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

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

use harness_core::Identity;

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
    enforce_key_permissions(&key)?;

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
    write_atomic(&tmp, &final_path, bytes.as_slice())
}

/// Create `<root>` mode `0700` if missing. Idempotent.
fn create_root_dir(root: &Path) -> Result<(), IdentityError> {
    if !root.exists() {
        fs::create_dir_all(root).map_err(|e| IdentityError::io(root, e))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o700);
        fs::set_permissions(root, perms).map_err(|e| IdentityError::io(root, e))?;
    }
    Ok(())
}

/// Hard-error if the key file's permissions are not exactly `0600` on Unix.
/// On Windows, log a warning and proceed (no portable mode-bit equivalent).
#[cfg_attr(not(unix), allow(unused_variables))]
fn enforce_key_permissions(path: &Path) -> Result<(), IdentityError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(path).map_err(|e| IdentityError::io(path, e))?;
        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o600 {
            return Err(IdentityError::UnsafePermissions { actual: mode });
        }
    }
    #[cfg(windows)]
    {
        tracing::warn!(
            target: "harness.identity",
            ?path,
            "windows: skipping mode-0600 check on identity.key — Windows ACLs not yet enforced"
        );
    }
    Ok(())
}

/// Write `data` to `tmp` (born `0600` on Unix), `fsync`, then rename to
/// `final_path`. Removes a stale `tmp` if the rename fails so we don't
/// pollute `root`.
fn write_atomic(tmp: &Path, final_path: &Path, data: &[u8]) -> Result<(), IdentityError> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(tmp).map_err(|e| IdentityError::io(tmp, e))?;

    let write_then_rename = || -> Result<(), IdentityError> {
        file.write_all(data)
            .map_err(|e| IdentityError::io(tmp, e))?;
        file.sync_all().map_err(|e| IdentityError::io(tmp, e))?;
        drop(file);
        fs::rename(tmp, final_path).map_err(|e| IdentityError::io(final_path, e))
    };

    if let Err(e) = write_then_rename() {
        let _ = fs::remove_file(tmp);
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn corrupt_key_file_error_carries_lengths() {
        // Just construct the error and round-trip its Display to make sure
        // the variant is wired up. The full filesystem flow is covered in
        // tests/identity_fs.rs.
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
