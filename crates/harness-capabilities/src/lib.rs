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

pub use registry::{CapabilityRegistry, RegistryError};
pub use traits::{Capability, CapabilityError, ExecutionContext};

#[cfg(feature = "echo")]
pub use echo::EchoCapability;

/// Build a registry preloaded with every built-in capability gated by
/// the active feature set. Phase 3 expands this to include `shell.exec`
/// and friends as their feature flags activate.
#[must_use]
pub fn default_registry() -> CapabilityRegistry {
    let registry = CapabilityRegistry::new();
    #[cfg(feature = "echo")]
    {
        // Registration is infallible here — we just constructed the
        // registry, so there's no prior `echo` entry to collide with.
        let _ = registry.register(std::sync::Arc::new(EchoCapability::new()));
    }
    registry
}
