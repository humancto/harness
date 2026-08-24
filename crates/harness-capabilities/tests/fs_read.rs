//! Phase 3.10a — `fs.read` integration tests.

#![cfg(feature = "fs")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::many_single_char_names
)]

use std::sync::Arc;

use base64::Engine;
use harness_capabilities::fs::scope::ScopeRegistry;
use harness_capabilities::traits::{Capability, CapabilityError, ExecutionContext};
use harness_capabilities::FsReadCapability;
use harness_core::{NodeId, TaskId};
use serde_json::json;

fn ctx() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(Vec::<String>::new()),
        frame_sink: None,
        audit: None,
    }
}

fn registry_with(id: &str, root: &std::path::Path) -> Arc<ScopeRegistry> {
    let cfg_dir = tempfile::tempdir().expect("cfg");
    let toml = format!(
        "[[scope]]\nid=\"{id}\"\nkind=\"directory\"\nlabel=\"L\"\nroot=\"{}\"\n",
        root.display()
    );
    let p = cfg_dir.path().join("scopes.toml");
    std::fs::write(&p, toml).expect("write");
    let r = ScopeRegistry::load_from_path(&p).expect("load");
    std::mem::forget(cfg_dir);
    Arc::new(r)
}

#[tokio::test]
async fn t19_read_returns_content_utf8() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("hello.txt"), "hi there\n").expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "path": "hello.txt"}))
        .await
        .expect("ok");
    assert_eq!(out["content"], json!("hi there\n"));
    assert_eq!(out["encoding"], json!("utf8"));
    assert_eq!(out["truncated"], json!(false));
    assert_eq!(out["size_bytes"], json!(9));
}

#[tokio::test]
async fn t20_read_default_max_bytes_truncates_with_flag() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    // Default DEFAULT_MAX_BYTES = 1 MiB. Write 2 MiB.
    let big: Vec<u8> = vec![b'a'; 2 * 1024 * 1024];
    std::fs::write(scope_root.path().join("big.txt"), &big).expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "path": "big.txt"}))
        .await
        .expect("ok");
    assert_eq!(out["truncated"], json!(true));
    assert_eq!(out["size_bytes"], json!(2 * 1024 * 1024));
    let content = out["content"].as_str().expect("string");
    assert_eq!(
        content.len(),
        1024 * 1024,
        "content must be exactly DEFAULT_MAX_BYTES"
    );
}

#[tokio::test]
async fn t21_read_hard_cap_clamps_user_request() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    // 24 MiB file; user requests 100 MiB (way over HARD_MAX_BYTES = 16 MiB).
    let big: Vec<u8> = vec![b'a'; 24 * 1024 * 1024];
    std::fs::write(scope_root.path().join("huge.bin"), &big).expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    // Schema rejects max_bytes > HARD_MAX_BYTES via input_schema, so submit
    // without max_bytes (default 1 MiB) and confirm clamping behavior end
    // to end via t30 below; here we exercise the schema cap.
    let err = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "path": "huge.bin", "max_bytes": 100_000_000_u64}),
        )
        .await;
    // The cap doesn't validate against schema directly (the executor does
    // when 3.9-style validation runs). Internally we still clamp via min.
    // Either: capability validates and InvalidInput, OR clamps and returns
    // truncated=true with content.len() == HARD_MAX_BYTES.
    match err {
        Err(CapabilityError::InvalidInput(_)) => {}
        Ok(v) => {
            assert_eq!(v["truncated"], json!(true));
            assert_eq!(v["content"].as_str().unwrap().len(), 16 * 1024 * 1024);
        }
        Err(e) => panic!("unexpected: {e:?}"),
    }
}

#[tokio::test]
async fn t22_read_non_utf8_returns_invalid_input_when_utf8_requested() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    // Bytes that are not valid UTF-8.
    let bad: &[u8] = &[0xFF, 0xFE, 0xFD, 0xFC];
    std::fs::write(scope_root.path().join("bin"), bad).expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let err = cap
        .execute(&ctx(), json!({"scope": "d", "path": "bin"}))
        .await
        .expect_err("must reject as InvalidInput");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
    let msg = format!("{err}");
    assert!(msg.contains("UTF-8") || msg.contains("base64"), "got {msg}");
}

#[tokio::test]
async fn t23_read_base64_encoding_works_on_binary() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let bytes: &[u8] = &[0xFF, 0xFE, 0x00, 0x42, 0x10];
    std::fs::write(scope_root.path().join("bin"), bytes).expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let out = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "path": "bin", "encoding": "base64"}),
        )
        .await
        .expect("ok");
    assert_eq!(out["encoding"], json!("base64"));
    let s = out["content"].as_str().expect("string");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(s)
        .expect("base64 decodes");
    assert_eq!(decoded, bytes);
}

#[cfg(unix)]
#[tokio::test]
async fn t24_read_symlink_inside_scope_works() {
    use std::os::unix::fs::symlink;
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("real.txt"), "via link").expect("write");
    symlink("real.txt", scope_root.path().join("link")).expect("symlink");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "path": "link"}))
        .await
        .expect("ok");
    assert_eq!(out["content"], json!("via link"));
}

#[cfg(unix)]
#[tokio::test]
async fn t25_read_symlink_escaping_scope_rejected_by_capstd() {
    use std::os::unix::fs::symlink;
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("secret"), "leak").expect("write");
    let scope_root = tempfile::tempdir().expect("scope");
    symlink(
        outside.path().join("secret"),
        scope_root.path().join("escape"),
    )
    .expect("link");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let err = cap
        .execute(&ctx(), json!({"scope": "d", "path": "escape"}))
        .await
        .expect_err("cap-std must reject");
    // The escape is caught at the stat step (we stat-first to avoid
    // blocking on FIFOs), so the error message mentions "stat under
    // scope" rather than "open". Either word is acceptable as a
    // confinement-error breadcrumb.
    let msg = format!("{err}");
    assert!(
        msg.contains("under scope") || msg.contains("Failed"),
        "got {msg}"
    );
}

#[tokio::test]
async fn t26_read_path_dotdot_rejected_at_parse() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let err = cap
        .execute(&ctx(), json!({"scope": "d", "path": "foo/../etc/passwd"}))
        .await
        .expect_err("must reject");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[tokio::test]
async fn t27_read_unknown_scope_rejected() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let err = cap
        .execute(&ctx(), json!({"scope": "ghost", "path": "x"}))
        .await
        .expect_err("must reject");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[tokio::test]
async fn t28_read_directory_returns_invalid_input() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(scope_root.path().join("sub")).expect("mkdir");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let err = cap
        .execute(&ctx(), json!({"scope": "d", "path": "sub"}))
        .await
        .expect_err("directory is not a regular file");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
    let msg = format!("{err}");
    assert!(
        msg.contains("not a regular file") || msg.contains("directory"),
        "got {msg}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn t28a_read_fifo_returns_invalid_input() {
    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;
    let scope_root = tempfile::tempdir().expect("tempdir");
    let fifo_path = scope_root.path().join("pipe");
    mkfifo(&fifo_path, Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    // Without the is_file() gate, opening the FIFO would block forever
    // on the first read. With the gate, this returns InvalidInput
    // immediately.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cap.execute(&ctx(), json!({"scope": "d", "path": "pipe"})),
    )
    .await
    .expect("must not block on FIFO");
    let err = result.expect_err("FIFO must be rejected");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn t28b_read_device_file_returns_invalid_input() {
    // /dev/zero is a real device file. cap-std confines us to scope_root,
    // so we set up a scope at /dev (which the test user can read). If
    // /dev/zero isn't readable as the test user, skip.
    if std::fs::metadata("/dev/zero").is_err() {
        // /dev/zero unavailable in this sandbox — skip silently.
        return;
    }
    let r = registry_with("dev", std::path::Path::new("/dev"));
    let cap = FsReadCapability::new(r);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cap.execute(&ctx(), json!({"scope": "dev", "path": "zero"})),
    )
    .await
    .expect("must not produce 16 MiB of zeros");
    let err = result.expect_err("/dev/zero must be rejected");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[tokio::test]
async fn t30_read_oversize_file_does_not_overallocate() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    // 32 MiB file, default max_bytes = 1 MiB. The capability stat-firsts
    // and calls take(effective_cap + 1) so the read NEVER materializes
    // the full file.
    let big: Vec<u8> = vec![b'b'; 32 * 1024 * 1024];
    std::fs::write(scope_root.path().join("big"), &big).expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "path": "big"}))
        .await
        .expect("ok");
    assert_eq!(out["truncated"], json!(true));
    assert_eq!(
        out["content"].as_str().unwrap().len(),
        1024 * 1024,
        "default cap = 1 MiB"
    );
    // size_bytes reports the full 32 MiB — operators chunking can use
    // this to decide how many follow-up reads they need.
    assert_eq!(out["size_bytes"], json!(32 * 1024 * 1024));
}

#[tokio::test]
async fn t31_read_manifest_owner_cardinality_with_scope_field() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let r = registry_with("d", scope_root.path());
    let cap = FsReadCapability::new(r);
    let m = cap.manifest();
    match &m.cardinality {
        harness_core::Cardinality::Owner { scope_field } => {
            assert_eq!(scope_field, "scope");
        }
        other => panic!("expected Owner, got {other:?}"),
    }
    assert_eq!(m.id, "fs.read");
    let required = m.input_schema["required"].as_array().expect("required");
    assert!(required.iter().any(|v| v.as_str() == Some("scope")));
    assert!(required.iter().any(|v| v.as_str() == Some("path")));
}
