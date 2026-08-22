//! `mcp.proxy` (roadmap 3.7) — expose operator-configured MCP servers'
//! tools as `mcp.<server>.<tool>` mesh capabilities. PRD §16.4 / §21.7;
//! design decisions in ADR-0018.

pub mod config;
pub mod proxy;

use std::path::Path;
use std::sync::Arc;

pub use config::{McpConfig, McpConfigError, McpServerConfig};
pub use proxy::{McpServerHandle, McpSpawnError, McpToolCapability};

use harness_policy::PolicyEngine;

use crate::registry::CapabilityRegistry;
use crate::traits::Capability as _;

/// Load `mcp.toml` from `path` and register the discovered tool
/// capabilities. Semantics mirror `scopes.toml` (3.10a):
///
/// - missing file → info log, no MCP capabilities, `Ok(())`;
/// - parse / validation error → `Err` — the daemon treats this as
///   fatal (silently skipping a misconfigured integration is worse
///   than refusing to start);
/// - each configured server is then best-effort: one server failing
///   to spawn / initialize / list logs a warning and is skipped, and
///   the daemon still boots (see [`enrich_with_mcp`]).
pub async fn enrich_with_mcp_from_path(
    registry: &CapabilityRegistry,
    policy: Arc<PolicyEngine>,
    path: &Path,
) -> Result<(), McpConfigError> {
    let config = match McpConfig::load_from_path(path) {
        Ok(c) => c,
        Err(McpConfigError::Io { ref source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            tracing::info!(
                target: "harness.mcp",
                path = %path.display(),
                "no mcp.toml found; no MCP capabilities registered"
            );
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    enrich_with_mcp(registry, policy, &config).await;
    Ok(())
}

/// Spawn every configured MCP server and register one
/// `mcp.<server>.<tool>` capability per tool it advertises.
///
/// Best-effort **per server**: spawn / initialize / `tools/list`
/// failures log a warning and skip that server — the daemon boots with
/// whatever else registered. Best-effort **per tool** too: a tool
/// whose name cannot form a capability id segment, or that collides
/// with an already-registered id, is skipped with a warning (tool
/// names are third-party data; they must not be able to panic the
/// daemon).
///
/// The child process for each server lives as long as its tool
/// capabilities: every `McpToolCapability` holds the shared
/// `Arc<McpServerHandle>`, and rmcp kills the subprocess when the
/// handle (and with it the transport) drops.
pub async fn enrich_with_mcp(
    registry: &CapabilityRegistry,
    policy: Arc<PolicyEngine>,
    config: &McpConfig,
) {
    for server_cfg in &config.servers {
        let handle = match McpServerHandle::spawn(server_cfg).await {
            Ok(h) => Arc::new(h),
            Err(err) => {
                tracing::warn!(
                    target: "harness.mcp",
                    server = %server_cfg.name,
                    command = %server_cfg.command,
                    %err,
                    "failed to start MCP server; skipping"
                );
                continue;
            }
        };

        let tools = match handle.list_tools().await {
            Ok(t) => t,
            Err(err) => {
                tracing::warn!(
                    target: "harness.mcp",
                    server = %server_cfg.name,
                    %err,
                    "failed to list MCP tools; skipping server"
                );
                continue;
            }
        };

        if tools.is_empty() {
            tracing::warn!(
                target: "harness.mcp",
                server = %server_cfg.name,
                "MCP server advertises no tools; nothing to register"
            );
            continue;
        }

        let mut registered = 0usize;
        for tool in &tools {
            if !proxy::valid_tool_name(&tool.name) {
                tracing::warn!(
                    target: "harness.mcp",
                    server = %server_cfg.name,
                    tool = %tool.name,
                    "MCP tool name cannot form a capability id segment; skipping tool"
                );
                continue;
            }
            let cap = McpToolCapability::new(handle.clone(), tool, policy.clone());
            let id = cap.id().to_string();
            match registry.register(Arc::new(cap)) {
                Ok(()) => registered += 1,
                Err(err) => {
                    // Duplicate ids here are third-party data (a
                    // server listing a tool twice, or two tools
                    // mapping to one id) — warn + skip, never panic.
                    tracing::warn!(
                        target: "harness.mcp",
                        server = %server_cfg.name,
                        capability = %id,
                        %err,
                        "skipping MCP tool registration"
                    );
                }
            }
        }
        tracing::info!(
            target: "harness.mcp",
            server = %server_cfg.name,
            tools = registered,
            "registered MCP proxy capabilities"
        );
    }
}
