//! Built-in capability implementations.
//!
//! Phase 2.8 ships the [`Capability`] trait + [`CapabilityRegistry`] + the
//! `echo` capability (the simplest possible `Anyone`). Phase 3 layers on
//! `shell.exec` (3.2), `llm.local.*` (3.4), `llm.cloud.*` (3.6),
//! `mcp.proxy` (3.7), `fs.*` (3.10), `mesh.*` (3.11). Each of those is
//! its own feature flag.
//!
//! Registration is `assert!`-on-duplicate so registering twice is a
//! programmer error, not a runtime concern.

#![forbid(unsafe_code)]

pub mod registry;
pub mod traits;

#[cfg(feature = "echo")]
pub mod echo;

#[cfg(all(feature = "shell", unix))]
pub mod shell;

#[cfg(feature = "llm")]
pub mod llm_batcher;

#[cfg(feature = "brain")]
pub mod brain_plan;

#[cfg(feature = "llm")]
pub mod llm_cloud_claude;

#[cfg(feature = "llm")]
pub mod llm_local;

pub use registry::{CapabilityRegistry, RegistryError, WeakCapabilityRegistry};

#[cfg(feature = "brain")]
pub use brain_plan::{enrich_with_brain_plan, BrainPlanCapability};
pub use traits::{Capability, CapabilityError, ExecutionContext};

#[cfg(feature = "echo")]
pub use echo::EchoCapability;

#[cfg(all(feature = "shell", unix))]
pub use shell::ShellExecCapability;

#[cfg(feature = "llm")]
pub use llm_batcher::{Fingerprint, LlmBatcher};

#[cfg(feature = "llm")]
pub use llm_cloud_claude::{enrich_with_llm_cloud_claude, LlmCloudClaudeCapability};

#[cfg(feature = "llm")]
pub use llm_local::LlmLocalCapability;

/// Build a registry preloaded with every built-in capability gated by
/// the active feature set. Phase 3 expands this to include `shell.exec`
/// (Unix-only — see ADR-0008) and `llm.local.*`, `llm.cloud.*`,
/// `mcp.proxy`, `fs.*`, `mesh.*` as their feature flags activate.
#[must_use]
pub fn default_registry(
    #[cfg_attr(not(all(feature = "shell", unix)), allow(unused_variables))] policy: std::sync::Arc<
        harness_policy::PolicyEngine,
    >,
) -> CapabilityRegistry {
    let registry = CapabilityRegistry::new();
    #[cfg(feature = "echo")]
    {
        let _ = registry.register(std::sync::Arc::new(EchoCapability::new()));
    }
    #[cfg(all(feature = "shell", unix))]
    {
        let _ = registry.register(std::sync::Arc::new(ShellExecCapability::new(policy)));
    }
    registry
}

/// Discover locally-installed Ollama models via `GET /api/tags` and
/// register one `llm.local.<model>` capability per discovered model.
///
/// Best-effort: failures (Ollama not running, network errors, decode
/// errors) log + return without registering anything. The daemon
/// continues normally with whatever caps were already registered.
///
/// Panics on `Duplicate` registration — this function is startup-once,
/// and a duplicate would be a programming bug, not a runtime concern.
/// Defense-in-depth: dedupes the discovered list before registering.
#[cfg(feature = "llm")]
pub async fn enrich_with_llm_local(
    registry: &CapabilityRegistry,
    policy: std::sync::Arc<harness_policy::PolicyEngine>,
) {
    use std::time::Duration;

    let host_str =
        std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "http://127.0.0.1:11434".to_string());
    let host = if let Ok(u) = llm_local::parse_ollama_host(&host_str) {
        u
    } else {
        tracing::warn!(
            target: "harness.llm.local",
            ollama_host = %host_str,
            "malformed OLLAMA_HOST; falling back to http://127.0.0.1:11434"
        );
        // The literal default is statically valid; if Url::parse ever
        // rejects it we have a deeper problem (broken url crate).
        #[allow(clippy::expect_used)]
        {
            url::Url::parse("http://127.0.0.1:11434").expect("default host parses")
        }
    };

    let discovery_client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "harness.llm.local", ?e, "build discovery client");
            return;
        }
    };

    let mut models = llm_local::discover_local_models(&host, &discovery_client).await;
    models.sort();
    models.dedup();

    if models.is_empty() {
        tracing::info!(
            target: "harness.llm.local",
            host = %host,
            "no Ollama models discovered (is `ollama serve` running?)"
        );
        return;
    }

    // Separate client for execution: per-request timeouts apply via
    // `.timeout(...)` on the request builder; we don't want a global
    // 2s cap here.
    let exec_client = reqwest::Client::new();
    // One batcher per daemon, shared across all `llm.local.<model>` caps.
    // The batcher key includes the model so cross-model isolation is
    // preserved. Window from `HARNESS_LLM_BATCH_WINDOW_MS` (default 50ms;
    // 0 disables).
    let batcher = std::sync::Arc::new(llm_batcher::LlmBatcher::from_env());
    for model in models {
        let cap = LlmLocalCapability::new(
            policy.clone(),
            model.clone(),
            host.clone(),
            exec_client.clone(),
            batcher.clone(),
        );
        // BUG-on-duplicate is intentional: enrich_with_llm_local is a
        // startup-once function; the model list was deduped above. A
        // duplicate here is a programming bug (function called twice
        // or registry already had the model).
        #[allow(clippy::expect_used)]
        registry
            .register(std::sync::Arc::new(cap))
            .expect("BUG: enrich_with_llm_local called twice or duplicate model");
    }
}
