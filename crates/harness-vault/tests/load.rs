//! Integration tests for `PlaintextStore` loading + `get` semantics.
//!
//! Tests that mutate process-global env vars are gated by
//! `serial_test::serial` so they don't race with each other when
//! `cargo test` runs the file's tests in parallel by default.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use harness_vault::{PlaintextStore, SecretsError, SecretsStore};
use serial_test::serial;
use tempfile::TempDir;

/// Write a `secrets.toml` with mode `0600` (Unix) and return its path
/// alongside the tempdir guard. Caller keeps the guard alive.
fn write_secrets(contents: &str) -> (TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secrets.toml");
    std::fs::write(&path, contents).expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod 0600");
    }
    (dir, path)
}

#[test]
fn t01_load_from_toml_resolves_tag() {
    let (_dir, path) = write_secrets(
        "\"secret/claude-api-key\" = \"sk-ant-deadbeef\"\n\
         \"secret/openai\"         = \"sk-openai-beef\"\n",
    );
    let store = PlaintextStore::load_from_path(&path).expect("load");
    let claude = store.get("secret/claude-api-key").expect("claude");
    assert_eq!(claude.as_bytes(), b"sk-ant-deadbeef");
    let openai = store.get("secret/openai").expect("openai");
    assert_eq!(openai.as_bytes(), b"sk-openai-beef");
}

#[cfg(unix)]
#[test]
fn t02_unix_perms_too_loose_refused() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secrets.toml");
    std::fs::write(&path, "\"secret/claude-api-key\" = \"x\"\n").expect("write");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod 0644");
    let err = PlaintextStore::load_from_path(&path).expect_err("must refuse");
    assert!(
        matches!(err, SecretsError::PermissionsTooLoose { .. }),
        "got {err:?}"
    );
}

#[test]
#[serial]
fn t03_env_var_overrides_file() {
    let (_dir, path) = write_secrets("\"secret/claude-api-key\" = \"file-value\"\n");
    let store = PlaintextStore::load_from_path(&path).expect("load");

    // Sanity: file value visible.
    assert_eq!(
        store.get("secret/claude-api-key").unwrap().as_bytes(),
        b"file-value"
    );

    // The workspace is edition 2021 / rust 1.85, where `set_var` is
    // safe. `#[serial]` keeps this test isolated from other env-mut
    // tests in the same process.
    std::env::set_var("HARNESS_SECRET_CLAUDE_API_KEY", "env-value");
    assert_eq!(
        store.get("secret/claude-api-key").unwrap().as_bytes(),
        b"env-value"
    );
    std::env::remove_var("HARNESS_SECRET_CLAUDE_API_KEY");

    // After removal the file value reappears.
    assert_eq!(
        store.get("secret/claude-api-key").unwrap().as_bytes(),
        b"file-value"
    );
}

#[test]
fn t04_unknown_tag_returns_none() {
    let (_dir, path) = write_secrets("\"secret/claude-api-key\" = \"x\"\n");
    let store = PlaintextStore::load_from_path(&path).expect("load");
    assert!(store.get("secret/openai").is_none());
    assert!(store.get("secret/unknown").is_none());
}

#[test]
fn t07_invalid_tag_format_rejected() {
    // Tag without `secret/` prefix.
    let (_dir, path) = write_secrets("\"FOO\" = \"x\"\n");
    let err = PlaintextStore::load_from_path(&path).expect_err("must reject");
    assert!(matches!(err, SecretsError::InvalidTag { .. }));

    // Uppercase suffix is rejected.
    let (_dir2, path2) = write_secrets("\"secret/Foo\" = \"x\"\n");
    let err = PlaintextStore::load_from_path(&path2).expect_err("must reject");
    assert!(matches!(err, SecretsError::InvalidTag { .. }));

    // Slash inside suffix is rejected.
    let (_dir3, path3) = write_secrets("\"secret/foo/bar\" = \"x\"\n");
    let err = PlaintextStore::load_from_path(&path3).expect_err("must reject");
    assert!(matches!(err, SecretsError::InvalidTag { .. }));
}

#[test]
#[serial]
fn t10_env_var_resolves_when_file_absent() {
    // Empty in-memory store; env-var fallback should still resolve.
    let store = PlaintextStore::empty();
    std::env::set_var("HARNESS_SECRET_OPENAI", "from-env");
    let v = store.get("secret/openai").expect("env");
    assert_eq!(v.as_bytes(), b"from-env");
    std::env::remove_var("HARNESS_SECRET_OPENAI");
    assert!(store.get("secret/openai").is_none());
}
