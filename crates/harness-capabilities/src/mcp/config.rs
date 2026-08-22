//! `~/.harness/mcp.toml` — operator-configured MCP servers.
//!
//! ```toml
//! [[server]]
//! name    = "fs"
//! command = "npx"
//! args    = ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
//!
//! [server.env]
//! LOG_LEVEL = "warn"
//! ```
//!
//! Loading semantics mirror `scopes.toml` (3.10a): missing file → no
//! MCP capabilities (info log in the daemon); parse / validation
//! errors are fatal at daemon startup. See ADR-0018.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

/// Parsed `mcp.toml`.
#[derive(Debug, Clone, Default)]
pub struct McpConfig {
    pub servers: Vec<McpServerConfig>,
}

/// One `[[server]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    /// Capability-id segment: `mcp.<name>.<tool>`. Restricted to
    /// `[a-z0-9_-]+` so every generated capability id stays a clean,
    /// predictable dotted path.
    pub name: String,

    /// Executable to spawn (resolved via `PATH` like any subprocess).
    pub command: String,

    #[serde(default)]
    pub args: Vec<String>,

    /// Extra environment for the child. The child inherits the
    /// daemon's environment (MCP servers routinely need `PATH`,
    /// `HOME`, npm/node config, etc.); entries here override it.
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum McpConfigError {
    #[error("io error reading {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("mcp.toml parse error")]
    Parse {
        #[source]
        source: toml::de::Error,
    },

    #[error("mcp.toml: invalid server name {0:?}; must match [a-z0-9_-]+")]
    InvalidName(String),

    #[error("mcp.toml: duplicate server name {0:?}")]
    DuplicateName(String),

    #[error("mcp.toml: server {0:?} has an empty command")]
    EmptyCommand(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpFile {
    #[serde(default)]
    server: Vec<McpServerConfig>,
}

/// `true` iff `name` is a valid `mcp.<name>.<tool>` server segment:
/// non-empty, only `[a-z0-9_-]`.
#[must_use]
pub fn valid_server_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

impl McpConfig {
    /// Parse + validate from a TOML string.
    pub fn parse(text: &str) -> Result<Self, McpConfigError> {
        let raw: McpFile =
            toml::from_str(text).map_err(|source| McpConfigError::Parse { source })?;

        let mut seen: HashSet<&str> = HashSet::with_capacity(raw.server.len());
        for entry in &raw.server {
            if !valid_server_name(&entry.name) {
                return Err(McpConfigError::InvalidName(entry.name.clone()));
            }
            if entry.command.is_empty() {
                return Err(McpConfigError::EmptyCommand(entry.name.clone()));
            }
            if !seen.insert(entry.name.as_str()) {
                return Err(McpConfigError::DuplicateName(entry.name.clone()));
            }
        }
        Ok(Self {
            servers: raw.server,
        })
    }

    /// Read + parse + validate from a path. Errors on a missing file —
    /// the daemon distinguishes `NotFound` (info log, no MCP caps)
    /// from every other error (fatal), mirroring `scopes.toml`.
    pub fn load_from_path(path: &Path) -> Result<Self, McpConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| McpConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&text)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn parses_full_entry() {
        let cfg = McpConfig::parse(
            r#"
[[server]]
name    = "fs"
command = "npx"
args    = ["-y", "@modelcontextprotocol/server-filesystem"]

[server.env]
LOG_LEVEL = "warn"
"#,
        )
        .expect("parse");
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].name, "fs");
        assert_eq!(cfg.servers[0].command, "npx");
        assert_eq!(cfg.servers[0].args.len(), 2);
        assert_eq!(cfg.servers[0].env.get("LOG_LEVEL").unwrap(), "warn");
    }

    #[test]
    fn empty_file_is_no_servers() {
        let cfg = McpConfig::parse("").expect("parse");
        assert!(cfg.servers.is_empty());
    }

    #[test]
    fn rejects_invalid_server_names() {
        for bad in ["", "Fs", "my server", "srv.dot", "srv/slash", "über"] {
            let toml = format!("[[server]]\nname = {bad:?}\ncommand = \"x\"\n");
            let err = McpConfig::parse(&toml).expect_err("must reject");
            assert!(
                matches!(err, McpConfigError::InvalidName(_)),
                "{bad:?} → {err}"
            );
        }
    }

    #[test]
    fn accepts_valid_server_names() {
        for good in ["fs", "gh-tools", "my_server", "s3"] {
            assert!(valid_server_name(good), "{good:?} should be valid");
        }
    }

    #[test]
    fn rejects_duplicate_names() {
        let err = McpConfig::parse(
            "[[server]]\nname = \"fs\"\ncommand = \"a\"\n[[server]]\nname = \"fs\"\ncommand = \"b\"\n",
        )
        .expect_err("must reject");
        assert!(matches!(err, McpConfigError::DuplicateName(_)));
    }

    #[test]
    fn rejects_empty_command() {
        let err = McpConfig::parse("[[server]]\nname = \"fs\"\ncommand = \"\"\n")
            .expect_err("must reject");
        assert!(matches!(err, McpConfigError::EmptyCommand(_)));
    }

    #[test]
    fn unknown_fields_fail_parse() {
        let err = McpConfig::parse("[[server]]\nname = \"fs\"\ncommand = \"x\"\ncwd = \"/\"\n")
            .expect_err("must reject");
        assert!(matches!(err, McpConfigError::Parse { .. }));
    }
}
