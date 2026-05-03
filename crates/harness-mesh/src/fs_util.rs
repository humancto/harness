//! Filesystem helpers shared between `harness_mesh::identity` and
//! `harness_mesh::trust`.
//!
//! Both modules need the same atomic-write recipe (born `0600` on Unix, fsync
//! before rename, scrub stale tmp on failure) and the same `~/.harness/`
//! root-directory creation (mode `0700`). Extracting them here ensures the
//! security-critical code paths exist in exactly one place.
//!
//! These helpers are `pub(crate)` — they're an internal contract between
//! sibling modules, not part of `harness-mesh`'s public API.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Errors from filesystem helpers. Convert into the caller's domain error
/// (e.g. `IdentityError`, `TrustError`) via `From`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum FsError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl FsError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// Create `<root>` (mode `0700` on Unix) if missing. Idempotent — calling on
/// an existing directory just re-applies the permission. Errors only on I/O
/// failure.
pub(crate) fn create_root_dir(root: &Path) -> Result<(), FsError> {
    if !root.exists() {
        fs::create_dir_all(root).map_err(|e| FsError::io(root, e))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o700);
        fs::set_permissions(root, perms).map_err(|e| FsError::io(root, e))?;
    }
    Ok(())
}

/// Write `data` to `tmp` (born `0600` on Unix), `fsync`, then rename to
/// `final_path`.
///
/// `OpenOptions::new().mode(0o600).create_new(true).open(tmp)` ensures the
/// file is born `0600`, eliminating any race window between `create` and a
/// would-be `chmod`.
///
/// On any error between `open` and `rename`, the stale `tmp` is removed so we
/// don't pollute `root` across retries.
pub(crate) fn write_atomic(tmp: &Path, final_path: &Path, data: &[u8]) -> Result<(), FsError> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(tmp).map_err(|e| FsError::io(tmp, e))?;

    let write_then_rename = || -> Result<(), FsError> {
        file.write_all(data).map_err(|e| FsError::io(tmp, e))?;
        file.sync_all().map_err(|e| FsError::io(tmp, e))?;
        drop(file);
        fs::rename(tmp, final_path).map_err(|e| FsError::io(final_path, e))
    };

    if let Err(e) = write_then_rename() {
        let _ = fs::remove_file(tmp);
        return Err(e);
    }
    Ok(())
}

/// Hard-error if `path`'s permissions are not exactly `0600` on Unix.
/// On Windows, log a `tracing::warn` and proceed (no portable mode-bit
/// equivalent — see PRD §10.1; `windows-acl` integration is a follow-up).
#[cfg_attr(not(unix), allow(unused_variables))]
pub(crate) fn enforce_mode_0600(path: &Path) -> Result<(), Mode0600Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(path).map_err(|e| Mode0600Error::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o600 {
            return Err(Mode0600Error::UnsafePermissions { actual: mode });
        }
    }
    #[cfg(windows)]
    {
        tracing::warn!(
            target: "harness.fs",
            ?path,
            "windows: skipping mode-0600 check — Windows ACLs not yet enforced"
        );
    }
    Ok(())
}

/// Outcome of `enforce_mode_0600`. Caller decides how to surface the
/// `UnsafePermissions` variant in their domain error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum Mode0600Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("file has unsafe permissions {actual:o}; expected 0600")]
    UnsafePermissions { actual: u32 },
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_root_dir_is_idempotent() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().join("nested").join("root");
        create_root_dir(&root).expect("first call creates");
        assert!(root.exists());
        create_root_dir(&root).expect("second call no-ops");
        assert!(root.exists());
    }

    #[cfg(unix)]
    #[test]
    fn create_root_dir_sets_mode_0700() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().join("root");
        create_root_dir(&root).expect("create");
        let mode = fs::metadata(&root).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn write_atomic_creates_file_and_removes_tmp() {
        let dir = TempDir::new().expect("tempdir");
        let tmp = dir.path().join("foo.tmp");
        let final_path = dir.path().join("foo");
        write_atomic(&tmp, &final_path, b"hello").expect("write");
        assert!(final_path.exists());
        assert!(!tmp.exists(), "tmp must be renamed away");
        assert_eq!(fs::read(&final_path).expect("read"), b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn write_atomic_creates_file_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().expect("tempdir");
        let tmp = dir.path().join("foo.tmp");
        let final_path = dir.path().join("foo");
        write_atomic(&tmp, &final_path, b"x").expect("write");
        let mode = fs::metadata(&final_path)
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn write_atomic_refuses_to_overwrite_existing_tmp() {
        let dir = TempDir::new().expect("tempdir");
        let tmp = dir.path().join("foo.tmp");
        let final_path = dir.path().join("foo");
        fs::write(&tmp, b"stale").expect("seed stale tmp");
        let res = write_atomic(&tmp, &final_path, b"x");
        assert!(res.is_err(), "must error when tmp already exists");
    }
}
