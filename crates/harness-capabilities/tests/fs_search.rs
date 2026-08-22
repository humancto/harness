//! Phase 3.10-fts — `fs.search` (FTS5 sidecar index) integration tests.

#![cfg(feature = "fs")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use harness_capabilities::fs::scope::ScopeRegistry;
use harness_capabilities::traits::{Capability, CapabilityError, ExecutionContext};
use harness_capabilities::FsSearchCapability;
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

/// Scope fixture + capability with a fresh sidecar index dir.
fn fixture(root: &std::path::Path) -> (FsSearchCapability, tempfile::TempDir) {
    let index_dir = tempfile::tempdir().expect("index dir");
    let cap = FsSearchCapability::new(registry_with("d", root), index_dir.path().to_path_buf());
    (cap, index_dir)
}

/// Bump a file's mtime well past its current value so the incremental
/// reindexer sees it as changed regardless of filesystem timestamp
/// granularity.
fn bump_mtime(path: &std::path::Path) {
    let f = std::fs::File::options()
        .write(true)
        .open(path)
        .expect("open for mtime bump");
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .expect("set_modified");
}

#[tokio::test]
async fn s01_search_lazy_builds_index_and_finds_hits() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        root.path().join("recipe.txt"),
        "Slow-cooked lentil soup with cumin and garlic.\n",
    )
    .expect("write");
    std::fs::create_dir(root.path().join("notes")).expect("mkdir");
    std::fs::write(
        root.path().join("notes/todo.txt"),
        "Buy garlic and onions for the soup project.\n",
    )
    .expect("write");
    std::fs::write(root.path().join("other.txt"), "Nothing relevant here.\n").expect("write");
    let (cap, _idx) = fixture(root.path());

    // No index exists — first call must lazily build it.
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "garlic"}))
        .await
        .expect("ok");
    assert_eq!(out["reindexed"], json!(true), "lazy first build");
    assert_eq!(out["index_stats"]["added"], json!(3));
    let hits = out["hits"].as_array().expect("array");
    assert_eq!(hits.len(), 2);
    for h in hits {
        let p = h["path"].as_str().unwrap();
        assert!(!p.starts_with('/'), "paths must be scope-relative: {p}");
        assert!(h["score"].as_f64().is_some());
    }

    // Second call: index present — no rebuild.
    let out2 = cap
        .execute(&ctx(), json!({"scope": "d", "query": "garlic"}))
        .await
        .expect("ok");
    assert_eq!(out2["reindexed"], json!(false));
    assert_eq!(out2["hits"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn s02_search_ranking_prefers_denser_match() {
    let root = tempfile::tempdir().expect("tempdir");
    // "zebra"-dense document vs one passing mention buried in filler.
    std::fs::write(
        root.path().join("dense.txt"),
        "zebra zebra zebra. A study of the zebra: zebra stripes, zebra herds.\n",
    )
    .expect("write");
    let filler = "wordy filler sentence with many terms. ".repeat(50);
    std::fs::write(
        root.path().join("sparse.txt"),
        format!("{filler} one zebra appears here. {filler}"),
    )
    .expect("write");
    let (cap, _idx) = fixture(root.path());
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "zebra"}))
        .await
        .expect("ok");
    let hits = out["hits"].as_array().expect("array");
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0]["path"], json!("dense.txt"), "bm25 best-first");
    let s0 = hits[0]["score"].as_f64().unwrap();
    let s1 = hits[1]["score"].as_f64().unwrap();
    assert!(s0 > s1, "score must be higher-is-better: {s0} vs {s1}");
}

#[tokio::test]
async fn s03_search_snippet_marks_match_context() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        root.path().join("doc.txt"),
        "The quick brown fox jumps over the lazy dog near the riverbank.\n",
    )
    .expect("write");
    let (cap, _idx) = fixture(root.path());
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "riverbank"}))
        .await
        .expect("ok");
    let hits = out["hits"].as_array().expect("array");
    assert_eq!(hits.len(), 1);
    let snippet = hits[0]["snippet"].as_str().expect("snippet");
    assert!(
        snippet.contains("[riverbank]"),
        "snippet must bracket the matched term, got: {snippet}"
    );
}

#[tokio::test]
async fn s04_search_incremental_reindex_picks_up_mtime_change() {
    let root = tempfile::tempdir().expect("tempdir");
    let file = root.path().join("story.txt");
    std::fs::write(&file, "the original dragon tale\n").expect("write");
    std::fs::write(root.path().join("stable.txt"), "unchanging content\n").expect("write");
    let (cap, _idx) = fixture(root.path());

    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "dragon"}))
        .await
        .expect("ok");
    assert_eq!(out["hits"].as_array().unwrap().len(), 1);

    // Rewrite the file; bump mtime explicitly (filesystem granularity).
    std::fs::write(&file, "now it is a phoenix story\n").expect("rewrite");
    bump_mtime(&file);

    // Without reindex: stale index still says "dragon".
    let stale = cap
        .execute(&ctx(), json!({"scope": "d", "query": "dragon"}))
        .await
        .expect("ok");
    assert_eq!(stale["reindexed"], json!(false));
    assert_eq!(
        stale["hits"].as_array().unwrap().len(),
        1,
        "stale by design"
    );

    // With reindex: only the changed file is re-read.
    let fresh = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "query": "phoenix", "reindex": true}),
        )
        .await
        .expect("ok");
    assert_eq!(fresh["reindexed"], json!(true));
    assert_eq!(fresh["index_stats"]["updated"], json!(1));
    assert_eq!(fresh["index_stats"]["unchanged"], json!(1));
    assert_eq!(fresh["index_stats"]["added"], json!(0));
    assert_eq!(fresh["hits"].as_array().unwrap().len(), 1);
    // And the old term is gone.
    let gone = cap
        .execute(&ctx(), json!({"scope": "d", "query": "dragon"}))
        .await
        .expect("ok");
    assert_eq!(gone["hits"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn s05_search_reindex_purges_deleted_files() {
    let root = tempfile::tempdir().expect("tempdir");
    let doomed = root.path().join("doomed.txt");
    std::fs::write(&doomed, "ephemeral kraken sighting\n").expect("write");
    let (cap, _idx) = fixture(root.path());
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "kraken"}))
        .await
        .expect("ok");
    assert_eq!(out["hits"].as_array().unwrap().len(), 1);

    std::fs::remove_file(&doomed).expect("rm");
    let out = cap
        .execute(
            &ctx(),
            json!({"scope": "d", "query": "kraken", "reindex": true}),
        )
        .await
        .expect("ok");
    assert_eq!(out["index_stats"]["removed"], json!(1));
    assert_eq!(out["hits"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn s06_search_skips_oversize_and_binary_files() {
    let root = tempfile::tempdir().expect("tempdir");
    // > 4 MiB index cap.
    let mut big = vec![b'a'; 5 * 1024 * 1024];
    big.extend_from_slice(b" bigfileterm");
    std::fs::write(root.path().join("big.txt"), &big).expect("write");
    // Binary (NUL in sniff window) containing the term.
    let mut blob = b"binterm".to_vec();
    blob.push(0);
    std::fs::write(root.path().join("blob.bin"), &blob).expect("write");
    std::fs::write(root.path().join("ok.txt"), "smallterm here\n").expect("write");
    let (cap, _idx) = fixture(root.path());

    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "smallterm"}))
        .await
        .expect("ok");
    assert_eq!(out["index_stats"]["added"], json!(1));
    assert_eq!(out["index_stats"]["skipped_too_large"], json!(1));
    assert_eq!(out["index_stats"]["skipped_binary"], json!(1));
    assert_eq!(out["hits"].as_array().unwrap().len(), 1);

    for term in ["bigfileterm", "binterm"] {
        let out = cap
            .execute(&ctx(), json!({"scope": "d", "query": term}))
            .await
            .expect("ok");
        assert_eq!(
            out["hits"].as_array().unwrap().len(),
            0,
            "{term} must not be indexed"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn s07_search_does_not_index_outside_scope() {
    use std::os::unix::fs::symlink;
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("secret.txt"), "confidential unicorn\n").expect("write");
    let root = tempfile::tempdir().expect("scope");
    std::fs::write(root.path().join("ok.txt"), "public unicorn\n").expect("write");
    symlink(outside.path().join("secret.txt"), root.path().join("leak")).expect("link");
    symlink(outside.path(), root.path().join("leakdir")).expect("link");
    let (cap, _idx) = fixture(root.path());
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "unicorn"}))
        .await
        .expect("ok");
    let hits = out["hits"].as_array().expect("array");
    assert_eq!(hits.len(), 1, "symlinked-out content must not be indexed");
    assert_eq!(hits[0]["path"], json!("ok.txt"));
}

#[tokio::test]
async fn s08_search_unknown_scope_rejected() {
    let root = tempfile::tempdir().expect("tempdir");
    let (cap, _idx) = fixture(root.path());
    let err = cap
        .execute(&ctx(), json!({"scope": "ghost", "query": "x"}))
        .await
        .expect_err("must reject");
    assert!(matches!(err, CapabilityError::InvalidInput(_)));
}

#[tokio::test]
async fn s09_search_bad_fts5_syntax_is_invalid_input() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("a.txt"), "hello\n").expect("write");
    let (cap, _idx) = fixture(root.path());
    let err = cap
        .execute(&ctx(), json!({"scope": "d", "query": "\"unbalanced"}))
        .await
        .expect_err("bad MATCH syntax must be InvalidInput");
    assert!(matches!(err, CapabilityError::InvalidInput(_)), "{err:?}");
}

#[tokio::test]
async fn s10_search_limit_clamps_hits() {
    let root = tempfile::tempdir().expect("tempdir");
    for i in 0..10 {
        std::fs::write(
            root.path().join(format!("f{i}.txt")),
            format!("common term in file {i}\n"),
        )
        .expect("write");
    }
    let (cap, _idx) = fixture(root.path());
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "common", "limit": 3}))
        .await
        .expect("ok");
    assert_eq!(out["hits"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn s11_search_empty_scope_builds_empty_index() {
    let root = tempfile::tempdir().expect("tempdir");
    let (cap, _idx) = fixture(root.path());
    let out = cap
        .execute(&ctx(), json!({"scope": "d", "query": "anything"}))
        .await
        .expect("ok");
    assert_eq!(out["reindexed"], json!(true));
    assert_eq!(out["hits"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn s12_search_marks_scope_indexed_in_manifest() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("a.txt"), "content\n").expect("write");
    let registry = registry_with("d", root.path());
    let index_dir = tempfile::tempdir().expect("index dir");
    let cap = FsSearchCapability::new(registry.clone(), index_dir.path().to_path_buf());
    assert!(!registry.manifest_scopes()[0].indexed, "starts unindexed");
    cap.execute(&ctx(), json!({"scope": "d", "query": "content"}))
        .await
        .expect("ok");
    let scopes = registry.manifest_scopes();
    assert!(scopes[0].indexed, "index build must flip manifest flag");
    assert!(scopes[0].last_indexed.is_some());
}

#[tokio::test]
async fn s13_search_manifest_owner_cardinality_with_scope_field() {
    let root = tempfile::tempdir().expect("tempdir");
    let (cap, _idx) = fixture(root.path());
    let m = cap.manifest();
    match &m.cardinality {
        harness_core::Cardinality::Owner { scope_field } => assert_eq!(scope_field, "scope"),
        other => panic!("expected Owner, got {other:?}"),
    }
    assert_eq!(m.id, "fs.search");
    let required = m.input_schema["required"].as_array().expect("required");
    assert!(required.iter().any(|v| v.as_str() == Some("scope")));
    assert!(required.iter().any(|v| v.as_str() == Some("query")));
}
