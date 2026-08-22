//! `mcp.<server>.<tool>` — proxy capabilities over subprocess MCP
//! servers, via the `rmcp` SDK (PRD §16.4, §21.7; roadmap 3.7).
//!
//! One persistent child process + MCP client per configured server,
//! shared (`Arc<McpServerHandle>`) across every tool capability that
//! server exposes. rmcp's `TokioChildProcess` kills the child when the
//! transport drops (same discipline as `shell.exec`'s
//! `kill_on_drop(true)`), so dropping the last capability Arc reaps
//! the subprocess.
//!
//! Child death is surfaced, not hidden: once the transport closes,
//! every call on that server fails with a clear "not running" error.
//! There is deliberately **no** auto-restart in 3.7 — a crashing
//! server restarting silently would mask misconfiguration and lose
//! any in-server state without a trace. Restart = restart the daemon.
//! (Lazy restart-on-next-call is a possible 3.7 follow-up; ADR-0018.)

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use harness_core::protocol::{CostHint, CpuClass, DiskIoClass, NetworkClass, ResourceHints};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use harness_policy::{Action, Decision, EvalContext, PolicyEngine};
use rmcp::model::{CallToolRequestParam, CallToolResult, Tool};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::TokioChildProcess;
use rmcp::ServiceExt;
use serde_json::Value as JsonValue;
use thiserror::Error;

use super::config::McpServerConfig;
use crate::traits::{Capability, CapabilityError, ExecutionContext};

/// Handshake + discovery deadline. A server that cannot initialize or
/// answer `tools/list` within this window is skipped (best-effort per
/// server; the daemon keeps booting).
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum McpSpawnError {
    #[error("spawn {command:?} for MCP server {server:?}: {source}")]
    Spawn {
        server: String,
        command: String,
        #[source]
        source: std::io::Error,
    },

    #[error("initialize MCP server {server:?}: {source}")]
    Initialize {
        server: String,
        #[source]
        source: Box<rmcp::service::ClientInitializeError>,
    },

    #[error("MCP server {server:?} did not complete {phase} within {timeout_secs}s")]
    Timeout {
        server: String,
        phase: &'static str,
        timeout_secs: u64,
    },

    #[error("list tools on MCP server {server:?}: {source}")]
    ListTools {
        server: String,
        #[source]
        source: Box<rmcp::ServiceError>,
    },
}

/// One running MCP server subprocess + its initialized client.
///
/// Shared across the server's tool capabilities via `Arc`; the child
/// process lives exactly as long as the last clone (rmcp kills it on
/// transport drop).
pub struct McpServerHandle {
    name: String,
    service: RunningService<RoleClient, ()>,
}

impl std::fmt::Debug for McpServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServerHandle")
            .field("name", &self.name)
            .field("transport_closed", &self.service.is_transport_closed())
            .finish_non_exhaustive()
    }
}

impl McpServerHandle {
    /// Spawn the configured subprocess and run the MCP `initialize`
    /// handshake. The child inherits the daemon's environment with
    /// `config.env` overlaid (MCP servers routinely need `PATH`,
    /// `HOME`, npm config, ...).
    pub async fn spawn(config: &McpServerConfig) -> Result<Self, McpSpawnError> {
        let mut cmd = tokio::process::Command::new(&config.command);
        cmd.args(&config.args);
        for (k, v) in &config.env {
            cmd.env(k, v);
        }

        let transport = TokioChildProcess::new(cmd).map_err(|source| McpSpawnError::Spawn {
            server: config.name.clone(),
            command: config.command.clone(),
            source,
        })?;

        let service = tokio::time::timeout(STARTUP_TIMEOUT, ().serve(transport))
            .await
            .map_err(|_| McpSpawnError::Timeout {
                server: config.name.clone(),
                phase: "initialize",
                timeout_secs: STARTUP_TIMEOUT.as_secs(),
            })?
            .map_err(|source| McpSpawnError::Initialize {
                server: config.name.clone(),
                source: Box::new(source),
            })?;

        Ok(Self {
            name: config.name.clone(),
            service,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `tools/list` (all pages).
    pub async fn list_tools(&self) -> Result<Vec<Tool>, McpSpawnError> {
        tokio::time::timeout(STARTUP_TIMEOUT, self.service.list_all_tools())
            .await
            .map_err(|_| McpSpawnError::Timeout {
                server: self.name.clone(),
                phase: "tools/list",
                timeout_secs: STARTUP_TIMEOUT.as_secs(),
            })?
            .map_err(|source| McpSpawnError::ListTools {
                server: self.name.clone(),
                source: Box::new(source),
            })
    }

    /// `tools/call`. Maps MCP-level failures to `CapabilityError`:
    /// transport death → clear "not running"; `isError: true` results
    /// → `Failed` carrying the server's message.
    pub async fn call(
        &self,
        tool: &str,
        arguments: Option<rmcp::model::JsonObject>,
    ) -> Result<JsonValue, CapabilityError> {
        if self.service.is_transport_closed() {
            return Err(CapabilityError::Failed(format!(
                "MCP server {:?} is not running (child process exited); \
                 restart the daemon to recover",
                self.name
            )));
        }

        let params = CallToolRequestParam {
            name: tool.to_owned().into(),
            arguments,
        };

        let result = self.service.call_tool(params).await.map_err(|e| {
            CapabilityError::Failed(format!(
                "MCP server {:?} tool {tool:?} call failed: {e}",
                self.name
            ))
        })?;

        if result.is_error == Some(true) {
            return Err(CapabilityError::Failed(format!(
                "MCP tool {}/{tool} returned an error: {}",
                self.name,
                flatten_content_text(&result)
            )));
        }

        // Pass the MCP result through verbatim (camelCase wire shape:
        // `content`, `structuredContent`, `isError`). Callers that
        // want typed output read `structuredContent`; text-only tools
        // are read from `content[].text`. ADR-0018.
        serde_json::to_value(&result)
            .map_err(|e| CapabilityError::Failed(format!("encode MCP result: {e}")))
    }
}

/// Best-effort human-readable message from a tool result's content
/// blocks (used for `isError: true` mapping).
fn flatten_content_text(result: &CallToolResult) -> String {
    let texts: Vec<&str> = result
        .content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.as_str()))
        .collect();
    if texts.is_empty() {
        serde_json::to_string(&result.content).unwrap_or_else(|_| "<unrenderable>".to_string())
    } else {
        texts.join("\n")
    }
}

/// `true` iff `tool` can serve as the final `mcp.<server>.<tool>` id
/// segment. Wider than server names because tool names are chosen by
/// third-party servers: ASCII alphanumerics plus `_ - . :` (the same
/// alphabet `llm.local.<model>` ids already use, e.g. `llama3.2:1b`).
#[must_use]
pub fn valid_tool_name(tool: &str) -> bool {
    !tool.is_empty()
        && tool
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
}

/// One `mcp.<server>.<tool>` capability.
///
/// **Cardinality: `Anyone`** — an MCP tool proxied here is a node-local
/// resource like `shell.exec`, and like shell it is deny-by-default
/// behind the policy engine (`Action::Mcp`, evaluated on the executing
/// node), so advertising it mesh-wide grants nothing by itself. Tools
/// that front owner-scoped data should be gated by the operator's
/// `[mcp]` policy rules rather than by cardinality. Documented per
/// repo rule 7 + ADR-0018.
pub struct McpToolCapability {
    id: String,
    tool_name: String,
    input_schema: JsonValue,
    output_schema: JsonValue,
    server: Arc<McpServerHandle>,
    policy: Arc<PolicyEngine>,
}

impl std::fmt::Debug for McpToolCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpToolCapability")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl McpToolCapability {
    /// Build the capability for `tool` as advertised by `server`.
    /// `input_schema` comes straight from the MCP tool's
    /// `inputSchema`; the dispatcher-side schema hash therefore tracks
    /// the server's own contract.
    #[must_use]
    pub fn new(server: Arc<McpServerHandle>, tool: &Tool, policy: Arc<PolicyEngine>) -> Self {
        let id = format!("mcp.{}.{}", server.name(), tool.name);
        let input_schema = JsonValue::Object((*tool.input_schema).clone());
        // Advertise the tool's own outputSchema when it declares one;
        // otherwise the generic pass-through result shape.
        let output_schema = tool.output_schema.as_ref().map_or_else(
            || {
                serde_json::json!({
                    "type": "object",
                    "required": ["content"],
                    "properties": {
                        "content":           { "type": "array" },
                        "structuredContent": {},
                        "isError":           { "type": "boolean" },
                    },
                })
            },
            |s| JsonValue::Object((**s).clone()),
        );
        Self {
            id,
            tool_name: tool.name.to_string(),
            input_schema,
            output_schema,
            server,
            policy,
        }
    }
}

#[async_trait]
impl Capability for McpToolCapability {
    fn id(&self) -> &str {
        &self.id
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: self.id.clone(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            // `Anyone` — see the type-level doc-comment for rationale.
            cardinality: Cardinality::Anyone,
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            // Local subprocess round-trip; no model inference, no
            // cloud spend.
            cost_hint: CostHint::LocalFast,
            tags: vec!["mcp".to_string()],
            rate_limit: None,
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                // The proxied server may well hit the network; we
                // cannot know, so advertise the conservative hint.
                network_class: NetworkClass::Light,
                disk_io_class: DiskIoClass::Light,
                estimated_duration_ms: None,
            },
            requires_secrets: vec![],
        }
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let arguments = match input {
            JsonValue::Object(map) => Some(map),
            JsonValue::Null => None,
            other => {
                return Err(CapabilityError::InvalidInput(format!(
                    "MCP tool arguments must be a JSON object (got {})",
                    json_type_name(&other)
                )));
            }
        };

        // Policy gate — evaluated on the executing node, BEFORE the
        // subprocess sees anything (PRD §10.4; default deny like
        // shell). Same inline pattern as shell.exec.
        let decision = self.policy.evaluate(&EvalContext {
            from_node: ctx.issued_by_name.as_ref(),
            local_node: ctx.local_node_name.as_ref(),
            action: Action::Mcp {
                server: self.server.name(),
                tool: &self.tool_name,
            },
        });
        match decision {
            Decision::Allow => {}
            Decision::Deny { reason } => {
                return Err(CapabilityError::Failed(format!("policy denied: {reason}")));
            }
            // `Decision` is `#[non_exhaustive]`; fail closed.
            _ => {
                return Err(CapabilityError::Failed(
                    "policy returned unknown decision (fail-closed)".to_string(),
                ));
            }
        }

        self.server.call(&self.tool_name, arguments).await
    }
}

fn json_type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn valid_tool_name_accepts_common_shapes() {
        for good in ["add", "read_file", "search-code", "v2.run", "ns:tool"] {
            assert!(valid_tool_name(good), "{good:?}");
        }
    }

    #[test]
    fn valid_tool_name_rejects_dangerous_shapes() {
        for bad in ["", "two words", "tab\there", "emoji✨", "slash/y"] {
            assert!(!valid_tool_name(bad), "{bad:?}");
        }
    }
}
