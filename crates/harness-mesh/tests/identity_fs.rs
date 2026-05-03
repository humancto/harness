//! Filesystem-level integration tests for `harness_mesh::identity`.
//!
//! These tests use `tempfile::TempDir`. **No** writes touch the user's real
//! `~/.harness/` directory.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::fs;
use std::io::Write;

use harness_mesh::identity::{init_or_load, load, save, IdentityError};
use tempfile::TempDir;

#[cfg(unix)]
fn mode_of(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path).expect("stat").permissions().mode() & 0o777
}

#[test]
fn init_or_load_creates_dir_and_file_when_absent() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path().join(".harness");
    assert!(!root.exists());

    let id = init_or_load(&root).expect("init_or_load");
    let key = root.join("identity.key");
    assert!(key.exists(), "key file must be created");

    #[cfg(unix)]
    {
        assert_eq!(mode_of(&key), 0o600, "key file must be born 0600");
        assert_eq!(mode_of(&root), 0o700, "root dir must be 0700");
    }

    // The file is exactly 32 bytes — the raw seed format documented in the
    // module doc-comment.
    let bytes = fs::read(&key).expect("read");
    assert_eq!(bytes.len(), 32, "on-disk seed must be 32 bytes");

    // The identity round-trips: a second call returns the same key.
    let again = init_or_load(&root).expect("init_or_load second call");
    assert_eq!(id.public_key(), again.public_key());
    assert_eq!(id.node_id(), again.node_id());
}

#[test]
fn save_then_load_round_trip() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let id = harness_core::Identity::generate();
    save(&root, &id).expect("save");
    let back = load(&root).expect("load");
    assert_eq!(id.public_key(), back.public_key());
    assert_eq!(id.node_id(), back.node_id());
}

#[test]
fn load_errors_when_file_missing() {
    let tmp = TempDir::new().expect("tempdir");
    let err = load(tmp.path()).expect_err("load on empty dir must error");
    match err {
        IdentityError::Io { source, .. } => {
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        }
        other => panic!("expected Io(NotFound), got {other:?}"),
    }
}

#[test]
fn load_errors_on_wrong_length() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let key = root.join("identity.key");

    // Write 16 bytes (half of the expected seed) with the right mode so we
    // exercise the length check, not the permission check.
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&key).expect("create");
    f.write_all(&[0u8; 16]).expect("write");
    drop(f);

    let err = load(root).expect_err("16-byte file must error");
    match err {
        IdentityError::CorruptKeyFile { expected, got } => {
            assert_eq!(expected, 32);
            assert_eq!(got, 16);
        }
        other => panic!("expected CorruptKeyFile, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn load_errors_on_unsafe_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();

    // Save first to create a valid file at 0600, then loosen its mode.
    let id = harness_core::Identity::generate();
    save(root, &id).expect("save");
    let key = root.join("identity.key");
    fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).expect("chmod");

    let err = load(root).expect_err("0644 key file must error");
    match err {
        IdentityError::UnsafePermissions { actual } => {
            assert_eq!(actual, 0o644, "must report the actual mode");
        }
        other => panic!("expected UnsafePermissions, got {other:?}"),
    }
}

#[test]
fn save_refuses_to_overwrite() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let id = harness_core::Identity::generate();
    save(root, &id).expect("first save");
    let err = save(root, &id).expect_err("second save must refuse");
    match err {
        IdentityError::Io { source, .. } => {
            assert_eq!(source.kind(), std::io::ErrorKind::AlreadyExists);
        }
        other => panic!("expected Io(AlreadyExists), got {other:?}"),
    }
}

#[test]
fn init_or_load_is_idempotent_across_many_calls() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path().join(".harness");
    let first = init_or_load(&root).expect("first").node_id();
    for _ in 0..5 {
        let again = init_or_load(&root).expect("repeat").node_id();
        assert_eq!(first, again);
    }
}

#[cfg(unix)]
#[test]
fn save_does_not_leave_temp_files_behind() {
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let id = harness_core::Identity::generate();
    save(root, &id).expect("save");

    let stragglers: Vec<_> = fs::read_dir(root)
        .expect("readdir")
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("identity.key.tmp.")
        })
        .collect();
    assert!(
        stragglers.is_empty(),
        "save must not leave .tmp files behind: {stragglers:?}"
    );
}

#[cfg(windows)]
#[test]
fn windows_skips_permission_check() {
    // On Windows there's no portable mode-bit equivalent; load() must
    // succeed on a default-ACL file with a tracing::warn rather than erroring.
    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let id = harness_core::Identity::generate();
    save(root, &id).expect("save on Windows");
    let _ = load(root).expect("load on Windows must not error on permissions");
}
