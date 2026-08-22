//! Phase 3.10-fts — `fs.grep` integration tests.

#![cfg(feature = "fs")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use harness_capabilities::fs::scope::ScopeRegistry;
use harness_capabilities::traits::{Capability, CapabilityError, ExecutionContext};
use harness_capabilities::FsGrepCapability;
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
async fn g01_grep_happy_path_regex_with_line_numbers() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        root.path().join("a.txt"),
        "first line\nsecond needle line\nthird\n",
    )
    .expect("write");
    std::fs::create_dir(root.path().join("sub")).expect("mkdir");
    std::fs::write(root.path().join("sub/b.txt"), "needle at top\nno match\n").expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "need.e"}))
        .await
        .expect("ok");
    let matches = out["matches"].as_array().expect("array");
    assert_eq!(matches.len(), 2);
    assert_eq!(out["truncated"], json!(false));
    // Deterministic walk order: a.txt before sub/b.txt.
    assert_eq!(matches[0]["path"], json!("a.txt"));
    assert_eq!(matches[0]["line_number"], json!(2));
    assert_eq!(matches[0]["line"], json!("second needle line"));
    assert_eq!(matches[1]["path"], json!("sub/b.txt"));
    assert_eq!(matches[1]["line_number"], json!(1));
    // Paths are scope-relative, never absolute.
    for m in matches {
        assert!(!m["path"].as_str().unwrap().starts_with('/'));
    }
}

#[tokio::test]
async fn g02_grep_literal_mode_escapes_metachars() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("a.txt"), "price is $1.99 (sale)\nx1z99\n").expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    // As a regex, "$1.99" can't match mid-line ($ anchors); literal mode
    // must find the exact substring.
    let out = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "pattern": "$1.99 (sale)", "literal": true}),
        )
        .await
        .expect("ok");
    let matches = out["matches"].as_array().expect("array");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["line_number"], json!(1));

    // Same pattern as a regex: "." would also match "x1z99" if the rest
    // weren't regex-broken; unbalanced "(" must be InvalidInput instead.
    let err = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "$1.99 (sale"}))
        .await
        .expect_err("invalid regex must be rejected");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[tokio::test]
async fn g03_grep_ignore_case() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        root.path().join("a.txt"),
        "Hello World\nhello world\nHELLO\n",
    )
    .expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let sensitive = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "hello"}))
        .await
        .expect("ok");
    assert_eq!(sensitive["matches"].as_array().unwrap().len(), 1);
    let insensitive = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "pattern": "hello", "ignore_case": true}),
        )
        .await
        .expect("ok");
    assert_eq!(insensitive["matches"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn g04_grep_skips_binary_files() {
    let root = tempfile::tempdir().expect("tempdir");
    // "needle" appears in the binary file too — but the NUL in the first
    // 8 KiB must exclude it.
    let mut binary = b"needle".to_vec();
    binary.push(0);
    binary.extend_from_slice(b"more needle");
    std::fs::write(root.path().join("blob.bin"), &binary).expect("write");
    std::fs::write(root.path().join("plain.txt"), "a needle here\n").expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "needle"}))
        .await
        .expect("ok");
    let matches = out["matches"].as_array().expect("array");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["path"], json!("plain.txt"));
    assert_eq!(out["files_skipped_binary"], json!(1));
    assert_eq!(out["files_scanned"], json!(1));
}

#[tokio::test]
async fn g05_grep_bounded_results_sets_truncated() {
    let root = tempfile::tempdir().expect("tempdir");
    let many: String = (0..50).fold(String::new(), |mut acc, i| {
        use std::fmt::Write as _;
        let _ = writeln!(acc, "needle {i}");
        acc
    });
    std::fs::write(root.path().join("a.txt"), &many).expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "pattern": "needle", "max_results": 7}),
        )
        .await
        .expect("ok");
    assert_eq!(out["matches"].as_array().unwrap().len(), 7);
    assert_eq!(out["truncated"], json!(true));
}

#[tokio::test]
async fn g06_grep_long_line_truncated_at_512_bytes() {
    let root = tempfile::tempdir().expect("tempdir");
    let long = format!("{}needle{}\n", "x".repeat(600), "y".repeat(100));
    std::fs::write(root.path().join("a.txt"), &long).expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "needle"}))
        .await
        .expect("ok");
    let m = &out["matches"].as_array().expect("array")[0];
    assert_eq!(m["line_truncated"], json!(true));
    assert_eq!(m["line"].as_str().unwrap().len(), 512);
}

#[tokio::test]
async fn g07_grep_file_glob_filters() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("a.rs"), "needle in rust\n").expect("write");
    std::fs::write(root.path().join("a.txt"), "needle in text\n").expect("write");
    std::fs::create_dir(root.path().join("sub")).expect("mkdir");
    std::fs::write(root.path().join("sub/b.rs"), "needle deeper\n").expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    // Basename glob matches at any depth.
    let out = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "pattern": "needle", "file_glob": "*.rs"}),
        )
        .await
        .expect("ok");
    let paths: Vec<&str> = out["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["a.rs", "sub/b.rs"]);
    // Path glob with `/` anchors to the full relative path.
    let out = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "pattern": "needle", "file_glob": "sub/*.rs"}),
        )
        .await
        .expect("ok");
    let paths: Vec<&str> = out["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["sub/b.rs"]);
}

#[cfg(unix)]
#[tokio::test]
async fn g08_grep_does_not_follow_symlink_out_of_scope() {
    use std::os::unix::fs::symlink;
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("secret.txt"), "needle secret\n").expect("write");
    let root = tempfile::tempdir().expect("scope");
    std::fs::write(root.path().join("ok.txt"), "needle fine\n").expect("write");
    // Symlink to a file outside + symlink to a whole directory outside.
    symlink(outside.path().join("secret.txt"), root.path().join("leak")).expect("link");
    symlink(outside.path(), root.path().join("leakdir")).expect("link");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "needle"}))
        .await
        .expect("ok");
    let matches = out["matches"].as_array().expect("array");
    assert_eq!(matches.len(), 1, "only the in-scope file may match");
    assert_eq!(matches[0]["path"], json!("ok.txt"));
}

#[tokio::test]
async fn g09_grep_empty_scope_returns_no_matches() {
    let root = tempfile::tempdir().expect("tempdir");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "anything"}))
        .await
        .expect("ok");
    assert_eq!(out["matches"].as_array().unwrap().len(), 0);
    assert_eq!(out["truncated"], json!(false));
    assert_eq!(out["files_scanned"], json!(0));
}

#[tokio::test]
async fn g10_grep_unknown_scope_rejected() {
    let root = tempfile::tempdir().expect("tempdir");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let err = cap
        .execute(&ctx(), json!({"scope": "ghost", "pattern": "x"}))
        .await
        .expect_err("must reject");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[tokio::test]
async fn g11_grep_skips_oversize_files() {
    let root = tempfile::tempdir().expect("tempdir");
    // > 8 MiB scan cap.
    let mut big = vec![b'a'; 9 * 1024 * 1024];
    big.extend_from_slice(b"\nneedle\n");
    std::fs::write(root.path().join("big.txt"), &big).expect("write");
    std::fs::write(root.path().join("small.txt"), "needle\n").expect("write");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "pattern": "needle"}))
        .await
        .expect("ok");
    assert_eq!(out["matches"].as_array().unwrap().len(), 1);
    assert_eq!(out["files_skipped_too_large"], json!(1));
}

#[tokio::test]
async fn g12_grep_manifest_owner_cardinality_with_scope_field() {
    let root = tempfile::tempdir().expect("tempdir");
    let cap = FsGrepCapability::new(registry_with("d", root.path()));
    let m = cap.manifest();
    match &m.cardinality {
        harness_core::Cardinality::Owner { scope_field } => assert_eq!(scope_field, "scope"),
        other => panic!("expected Owner, got {other:?}"),
    }
    assert_eq!(m.id, "fs.grep");
    let required = m.input_schema["required"].as_array().expect("required");
    assert!(required.iter().any(|v| v.as_str() == Some("scope")));
    assert!(required.iter().any(|v| v.as_str() == Some("pattern")));
}
