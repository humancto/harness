//! Build script for `harness-ui`.
//!
//! Two responsibilities:
//!
//! 1. Tell cargo to rebuild this crate whenever the `SvelteKit` `build/`
//!    output changes — without this, `rust-embed`'s compile-time scan
//!    won't pick up new UI bytes after `npm run build`.
//! 2. If `HARNESS_UI_REQUIRE=1` is in the environment (CI sets this),
//!    fail the build loudly when `ui/build/index.html` is missing —
//!    so a broken UI build cannot ship a daemon with no UI inside it.
//!
//! Locally (no `HARNESS_UI_REQUIRE`), a missing UI build downgrades to
//! a 404 served at runtime so Rust contributors don't have to install
//! Node just to compile.

// Build scripts run at compile time; panicking is the canonical way to
// surface a build failure to the user.
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::manual_assert,
    clippy::print_stderr
)]

use std::path::PathBuf;

fn main() {
    let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") else {
        // Cargo always sets this; if it's missing something is profoundly
        // wrong, and we cannot meaningfully proceed.
        eprintln!("CARGO_MANIFEST_DIR is unset; skipping ui-build presence check");
        return;
    };
    let manifest_dir = PathBuf::from(manifest_dir);
    let Some(workspace_root) = manifest_dir.parent().and_then(|p| p.parent()) else {
        eprintln!("unexpected workspace layout; skipping ui-build presence check");
        return;
    };
    let ui_build = workspace_root.join("ui").join("build");

    println!("cargo:rerun-if-changed={}", ui_build.display());
    println!(
        "cargo:rerun-if-changed={}",
        ui_build.join("index.html").display()
    );
    println!("cargo:rerun-if-env-changed=HARNESS_UI_REQUIRE");

    let require = std::env::var("HARNESS_UI_REQUIRE").ok().as_deref() == Some("1");
    let index = ui_build.join("index.html");

    if require && !index.exists() {
        panic!(
            "HARNESS_UI_REQUIRE=1 but {} is missing.\n\
             Run `cd ui && npm ci && npm run build` before `cargo test`/`cargo build`.",
            index.display()
        );
    }
}
