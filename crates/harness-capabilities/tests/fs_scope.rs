//! Phase 3.10a — `ScopeRegistry` + path-confinement integration tests.
//!
//! These tests exercise `cap_std::fs::Dir`'s confinement on the real
//! filesystem (under `tempfile::tempdir`), so they catch regressions
//! a future refactor that bypasses cap-std would introduce.

#![cfg(feature = "fs")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use harness_capabilities::fs::{ScopeError, ScopeRegistry};

fn write_scopes_toml(dir: &std::path::Path, contents: &str) -> std::path::PathBuf {
    let p = dir.join("scopes.toml");
    std::fs::write(&p, contents).expect("write");
    p
}

#[test]
fn t01_unknown_scope_is_none() {
    let r = ScopeRegistry::default();
    assert!(r.get("never-registered").is_none());
}

#[test]
fn t02_relative_path_resolves_under_root() {
    let scope_root = tempfile::tempdir().expect("scope tempdir");
    std::fs::write(scope_root.path().join("hello.txt"), "hi").expect("write");
    let cfg_dir = tempfile::tempdir().expect("cfg tempdir");
    let toml = format!(
        "[[scope]]\nid=\"docs\"\nkind=\"directory\"\nlabel=\"D\"\nroot=\"{}\"\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let r = ScopeRegistry::load_from_path(&p).expect("load");
    let scope = r.get("docs").expect("present");
    let _ = scope
        .root_dir
        .open("hello.txt")
        .expect("relative resolve under cap-std root works");
}

#[test]
fn t03_dotdot_in_path_rejected_at_parse_time() {
    use harness_capabilities::fs::scope::parse_relative_path;
    let err = parse_relative_path("foo/../etc").expect_err("must reject");
    assert!(matches!(err, ScopeError::ForbiddenComponent { .. }));
}

#[test]
fn t04_absolute_path_rejected_at_parse_time() {
    use harness_capabilities::fs::scope::parse_relative_path;
    let err = parse_relative_path("/etc/passwd").expect_err("must reject");
    assert!(matches!(err, ScopeError::ForbiddenComponent { .. }));
}

#[cfg(unix)]
#[test]
fn t05_symlink_inside_scope_resolves() {
    use std::os::unix::fs::symlink;
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("real.txt"), "hi").expect("write");
    // Relative symlink target — cap-std follows it because the
    // resolution stays under the Dir handle's tree. (Absolute targets
    // are rejected even when they point inside the scope; cap-std
    // requires relative resolution to stay confined.)
    symlink("real.txt", scope_root.path().join("link")).expect("symlink");

    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"d\"\nkind=\"directory\"\nlabel=\"D\"\nroot=\"{}\"\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let r = ScopeRegistry::load_from_path(&p).expect("load");
    let scope = r.get("d").expect("present");
    let _f = scope
        .root_dir
        .open("link")
        .expect("relative-target intra-scope symlink follows under cap-std confinement");
}

#[cfg(unix)]
#[test]
fn t06_symlink_escaping_scope_rejected_by_capstd() {
    use std::os::unix::fs::symlink;
    let outside = tempfile::tempdir().expect("outside tempdir");
    std::fs::write(outside.path().join("secret"), "leak").expect("write");
    let scope_root = tempfile::tempdir().expect("scope tempdir");
    symlink(
        outside.path().join("secret"),
        scope_root.path().join("escape"),
    )
    .expect("symlink");

    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"d\"\nkind=\"directory\"\nlabel=\"D\"\nroot=\"{}\"\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let r = ScopeRegistry::load_from_path(&p).expect("load");
    let scope = r.get("d").expect("present");

    // cap-std's `Dir::open` enforces confinement during resolution —
    // the symlink target leaves the scope tree, so open MUST fail.
    let err = scope
        .root_dir
        .open("escape")
        .expect_err("escape symlink must be rejected by cap-std confinement");
    // We assert a specific error kind so a future regression that
    // silently switches to a non-confining path fails this test.
    // cap-primitives reports `PermissionDenied` for sandbox violations.
    let kind = err.kind();
    assert!(
        matches!(
            kind,
            std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::NotFound
                | std::io::ErrorKind::InvalidInput
        ),
        "expected confinement error; got {kind:?}"
    );
}

#[cfg(unix)]
#[test]
fn t06b_symlink_chain_escapes_via_intermediate() {
    use std::os::unix::fs::symlink;
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("target"), "leak").expect("write");
    let scope_root = tempfile::tempdir().expect("scope");
    // a → b → outside/target. Both `a` and `b` are inside the scope;
    // the *target* of `b` leaves the scope. cap-std must reject.
    symlink(outside.path().join("target"), scope_root.path().join("b")).expect("link b -> outside");
    symlink(scope_root.path().join("b"), scope_root.path().join("a")).expect("link a -> b");

    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"d\"\nkind=\"directory\"\nlabel=\"D\"\nroot=\"{}\"\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let r = ScopeRegistry::load_from_path(&p).expect("load");
    let scope = r.get("d").expect("present");
    let err = scope
        .root_dir
        .open("a")
        .expect_err("symlink chain leaving scope must be rejected");
    let kind = err.kind();
    assert!(
        matches!(
            kind,
            std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::NotFound
                | std::io::ErrorKind::InvalidInput
        ),
        "expected confinement error; got {kind:?}"
    );
}

#[test]
fn t08_root_relative_path_is_dot() {
    use harness_capabilities::fs::scope::parse_relative_path;
    assert!(parse_relative_path("").is_ok());
    assert!(parse_relative_path(".").is_ok());
    assert!(parse_relative_path("./foo").is_ok());
    assert!(parse_relative_path("foo/./bar").is_ok());
}

#[test]
fn t09_scopes_toml_round_trip() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid = \"docs\"\nkind = \"directory\"\nlabel = \"Documents\"\nroot = \"{}\"\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let r = ScopeRegistry::load_from_path(&p).expect("load");
    let s = r.get("docs").expect("docs scope");
    assert_eq!(s.id, "docs");
    assert_eq!(s.kind, "directory");
    assert_eq!(s.label, "Documents");
    let manifest = r.manifest_scopes();
    assert_eq!(manifest.len(), 1);
    assert_eq!(manifest[0].id, "docs");
    assert!(!manifest[0].indexed);
    assert_eq!(manifest[0].last_indexed, None);
}

#[test]
fn t10_scopes_toml_unknown_field_rejected() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"x\"\nkind=\"directory\"\nlabel=\"X\"\nroot=\"{}\"\nbogus=true\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let err = ScopeRegistry::load_from_path(&p).expect_err("must reject unknown field");
    assert!(matches!(err, ScopeError::Parse { .. }));
}

#[test]
fn t10b_duplicate_scope_id_rejected() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"d\"\nkind=\"directory\"\nlabel=\"A\"\nroot=\"{}\"\n\
         [[scope]]\nid=\"d\"\nkind=\"directory\"\nlabel=\"B\"\nroot=\"{}\"\n",
        scope_root.path().display(),
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let err = ScopeRegistry::load_from_path(&p).expect_err("must reject duplicate id");
    assert!(matches!(err, ScopeError::DuplicateId(_)));
}

#[test]
fn t10c_empty_scope_id_rejected() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"\"\nkind=\"directory\"\nlabel=\"A\"\nroot=\"{}\"\n",
        scope_root.path().display()
    );
    let p = write_scopes_toml(cfg_dir.path(), &toml);
    let err = ScopeRegistry::load_from_path(&p).expect_err("must reject empty id");
    assert!(matches!(err, ScopeError::EmptyId));
}
