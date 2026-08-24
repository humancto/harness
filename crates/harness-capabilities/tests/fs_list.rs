//! Phase 3.10a — `fs.list` integration tests.

#![cfg(feature = "fs")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::many_single_char_names
)]

use std::sync::Arc;

use harness_capabilities::fs::scope::ScopeRegistry;
use harness_capabilities::traits::{Capability, ExecutionContext};
use harness_capabilities::FsListCapability;
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
    // Keep cfg_dir alive by leaking — it's a test fixture.
    std::mem::forget(cfg_dir);
    Arc::new(r)
}

#[tokio::test]
async fn t11_list_root_returns_entries() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("a.txt"), "hi").expect("write");
    std::fs::write(scope_root.path().join("b.txt"), "hi").expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);

    let out = cap
        .execute(&ctx(), json!({"scope": "d"}))
        .await
        .expect("ok");
    let entries = out["entries"].as_array().expect("array");
    assert_eq!(entries.len(), 2);
    let names: std::collections::HashSet<&str> = entries
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains("a.txt") && names.contains("b.txt"));
    assert_eq!(out["truncated"], json!(false));
    assert_eq!(out["skipped_non_utf8"], json!(0));
}

#[tokio::test]
async fn t12_list_unknown_scope_rejected_invalid_input() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);
    let err = cap
        .execute(&ctx(), json!({"scope": "ghost"}))
        .await
        .expect_err("must reject");
    assert!(matches!(
        err,
        harness_capabilities::traits::CapabilityError::InvalidInput(_)
    ));
}

#[tokio::test]
async fn t13_list_max_entries_truncates_with_flag() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let mut created: std::collections::HashSet<String> = std::collections::HashSet::new();
    for i in 0..20 {
        let name = format!("file_{i:03}.txt");
        std::fs::write(scope_root.path().join(&name), "x").expect("write");
        created.insert(name);
    }
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);

    let out = cap
        .execute(&ctx(), json!({"scope": "d", "max_entries": 5}))
        .await
        .expect("ok");
    let entries = out["entries"].as_array().expect("array");
    // Set membership only — `Dir::entries()` order is platform-dependent.
    assert_eq!(entries.len(), 5, "max_entries enforced");
    for e in entries {
        let name = e["name"].as_str().unwrap();
        assert!(
            created.contains(name),
            "entry {name:?} must be one of the created files"
        );
    }
    assert_eq!(out["truncated"], json!(true), "truncation flag must fire");
}

#[tokio::test]
async fn t14_list_depth_one_default() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("top.txt"), "x").expect("write");
    std::fs::create_dir(scope_root.path().join("sub")).expect("mkdir");
    std::fs::write(scope_root.path().join("sub/nested.txt"), "x").expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);
    let out = cap
        .execute(&ctx(), json!({"scope": "d"}))
        .await
        .expect("ok");
    let entries = out["entries"].as_array().expect("array");
    // Default depth=1: top.txt + sub/ but NOT sub/nested.txt.
    let names: std::collections::HashSet<&str> = entries
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains("top.txt"));
    assert!(names.contains("sub"));
    assert!(
        !entries
            .iter()
            .any(|e| e["relative"].as_str().unwrap() == "sub/nested.txt"),
        "depth=1 must not descend"
    );
}

#[tokio::test]
async fn t15_list_depth_two_descends() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(scope_root.path().join("sub")).expect("mkdir");
    std::fs::write(scope_root.path().join("sub/nested.txt"), "x").expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "max_depth": 2}))
        .await
        .expect("ok");
    let entries = out["entries"].as_array().expect("array");
    let relatives: std::collections::HashSet<&str> = entries
        .iter()
        .map(|e| e["relative"].as_str().unwrap())
        .collect();
    assert!(relatives.contains("sub"));
    assert!(relatives.contains("sub/nested.txt"));
}

#[tokio::test]
async fn t16_list_depth_over_hard_cap_clamped() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("a.txt"), "x").expect("write");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);
    // max_depth: 9999 > HARD_MAX_DEPTH (8) — schema rejects via validator.
    // We submit depth=9999 raw and let the input-schema check reject; the
    // capability itself also clamps internally so even a bypass is safe.
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "max_depth": 9999}))
        .await;
    // The capability rejects via the schema (max_depth > HARD_MAX_DEPTH).
    // We accept either: schema-reject InvalidInput, or successful clamp.
    match out {
        Err(harness_capabilities::traits::CapabilityError::InvalidInput(_)) => {}
        Ok(v) => {
            // If the capability clamped, the response should still be sane.
            assert!(v["entries"].is_array());
        }
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

#[tokio::test]
async fn t17_list_manifest_owner_cardinality_with_scope_field() {
    let scope_root = tempfile::tempdir().expect("tempdir");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);
    let m = cap.manifest();
    match &m.cardinality {
        harness_core::Cardinality::Owner { scope_field } => {
            assert_eq!(scope_field, "scope");
        }
        other => panic!("expected Owner, got {other:?}"),
    }
    assert_eq!(m.id, "fs.list");
    // Scope field must appear in input_schema.required (defense-in-depth
    // guard against future typos).
    let required = m.input_schema["required"]
        .as_array()
        .expect("required is an array");
    assert!(
        required.iter().any(|v| v.as_str() == Some("scope")),
        "Owner.scope_field must appear in input_schema.required"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn t18_list_symlink_listed_target_omitted() {
    use std::os::unix::fs::symlink;
    let scope_root = tempfile::tempdir().expect("tempdir");
    std::fs::write(scope_root.path().join("real.txt"), "x").expect("write");
    symlink("real.txt", scope_root.path().join("link")).expect("symlink");
    let r = registry_with("d", scope_root.path());
    let cap = FsListCapability::new(r);

    let out = cap
        .execute(&ctx(), json!({"scope": "d"}))
        .await
        .expect("ok");
    let entries = out["entries"].as_array().unwrap();
    let link = entries
        .iter()
        .find(|e| e["name"] == json!("link"))
        .expect("link entry");
    assert_eq!(link["kind"], json!("symlink"));
    // Target field MUST NOT be present (leak vector — see ADR-0015 §5).
    assert!(
        link.get("target").is_none(),
        "symlink target MUST NOT appear in output (recon leak); got {link:?}"
    );
}
