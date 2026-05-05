//! Operator-configured data domains (`~/.harness/scopes.toml`) +
//! TOCTOU-free path confinement via `cap_std::fs::Dir` handles.
//!
//! ## Confinement model
//!
//! Each scope holds a `cap_std::fs::Dir` opened once at registry build
//! time via [`ambient_authority`](cap_std::ambient_authority). Every
//! subsequent path resolution (`Dir::open`, `Dir::open_dir`,
//! `Dir::entries`) is enforced by cap-primitives to stay under that
//! one inode tree — symlinks whose target leaves the tree are rejected
//! at the resolution step, not just at the final canonical path.
//!
//! This eliminates the classic TOCTOU window between a
//! canonicalize-and-prefix-check and an `open()` call: the kernel
//! resolves at open time, and `cap-std`'s open IS the check.
//!
//! ## Defense in depth
//!
//! Even with cap-std enforcing confinement, we reject `..` segments
//! and absolute / drive-prefix components at parse time. Two reasons:
//! (1) A clear `InvalidInput("..\")` error is more useful than
//! `PermissionDenied`. (2) If a future refactor accidentally bypasses
//! cap-std for a path-handling helper, parse-time rejection limits
//! blast radius.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use serde::Deserialize;
use thiserror::Error;

/// One operator-configured data domain. The `Dir` handle anchors the
/// resolution root for every `fs.*` call against this scope.
pub struct ScopeConfig {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub canonical_root: PathBuf,
    pub root_dir: Dir,
    /// Always `false` in 3.10a; 3.10-fts will set `true` when an FTS
    /// index is built.
    pub indexed: bool,
}

impl std::fmt::Debug for ScopeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeConfig")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("label", &self.label)
            .field("canonical_root", &self.canonical_root)
            .field("indexed", &self.indexed)
            .finish_non_exhaustive()
    }
}

/// In-memory registry of every scope from `~/.harness/scopes.toml`.
/// Cloneable; backing storage is `Arc<HashMap<...>>`-shaped.
#[derive(Clone, Debug, Default)]
pub struct ScopeRegistry {
    by_id: HashMap<String, Arc<ScopeConfig>>,
}

impl ScopeRegistry {
    /// Load `~/.harness/scopes.toml`. Missing → empty registry; parse
    /// or open errors abort startup (so a misconfigured scope is a
    /// loud failure, not a silent skip).
    pub fn load_default() -> Result<Self, ScopeError> {
        let path = default_path()?;
        match std::fs::metadata(&path) {
            Ok(_) => Self::load_from_path(&path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(source) => Err(ScopeError::Io { path, source }),
        }
    }

    /// Load from an explicit path. Used by tests and the daemon's
    /// `--root` override. Errors on missing file (use
    /// [`Self::load_default`] for the optional-by-default semantics).
    pub fn load_from_path(path: &Path) -> Result<Self, ScopeError> {
        let text = std::fs::read_to_string(path).map_err(|source| ScopeError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let raw: ScopesFile =
            toml::from_str(&text).map_err(|source| ScopeError::Parse { source })?;

        let mut by_id = HashMap::with_capacity(raw.scope.len());
        for entry in raw.scope {
            if entry.id.is_empty() {
                return Err(ScopeError::EmptyId);
            }
            // Canonicalize the configured root once. Resolves any
            // symlinks the operator wrote into the path; the `Dir` is
            // then opened on the canonical absolute path.
            let canonical_root = entry.root.canonicalize().map_err(|source| ScopeError::Io {
                path: entry.root.clone(),
                source,
            })?;
            let root_dir =
                Dir::open_ambient_dir(&canonical_root, ambient_authority()).map_err(|source| {
                    ScopeError::Io {
                        path: canonical_root.clone(),
                        source,
                    }
                })?;
            let cfg = Arc::new(ScopeConfig {
                id: entry.id.clone(),
                kind: entry.kind,
                label: entry.label,
                canonical_root,
                root_dir,
                indexed: false,
            });
            if by_id.insert(entry.id.clone(), cfg).is_some() {
                return Err(ScopeError::DuplicateId(entry.id));
            }
        }
        Ok(Self { by_id })
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<ScopeConfig>> {
        self.by_id.get(id).cloned()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Snapshot for `NodeManifest::scopes`. `indexed: false`,
    /// `last_indexed: None` in 3.10a; 3.10-fts will set them after
    /// each index build.
    #[must_use]
    pub fn manifest_scopes(&self) -> Vec<harness_core::Scope> {
        let mut out: Vec<harness_core::Scope> = self
            .by_id
            .values()
            .map(|c| harness_core::Scope {
                kind: c.kind.clone(),
                id: c.id.clone(),
                label: c.label.clone(),
                indexed: c.indexed,
                last_indexed: None,
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

/// Validate a relative path at parse time. Defense-in-depth before
/// cap-std's runtime confinement. Accepts any number of
/// `Component::CurDir`; rejects every other non-`Normal` variant.
///
/// Returns `()` on success — the path is fed to cap-std verbatim and
/// cap-std handles the actual resolution.
pub fn parse_relative_path(s: &str) -> Result<(), ScopeError> {
    if s.is_empty() {
        return Ok(()); // empty → scope root
    }
    for component in Path::new(s).components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => {
                return Err(ScopeError::ForbiddenComponent {
                    component: "..".to_string(),
                });
            }
            Component::RootDir => {
                return Err(ScopeError::ForbiddenComponent {
                    component: "/".to_string(),
                });
            }
            Component::Prefix(p) => {
                return Err(ScopeError::ForbiddenComponent {
                    component: format!("{p:?}"),
                });
            }
        }
    }
    Ok(())
}

fn default_path() -> Result<PathBuf, ScopeError> {
    let home = dirs::home_dir().ok_or(ScopeError::NoHome)?;
    Ok(home.join(".harness").join("scopes.toml"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopesFile {
    #[serde(default)]
    scope: Vec<ScopesEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopesEntry {
    id: String,
    kind: String,
    label: String,
    root: PathBuf,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ScopeError {
    #[error("unknown scope {0:?}")]
    UnknownScope(String),

    #[error("path component {component:?} is forbidden")]
    ForbiddenComponent { component: String },

    #[error("io error opening {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("scopes.toml parse error")]
    Parse {
        #[source]
        source: toml::de::Error,
    },

    #[error("scopes.toml: $HOME could not be resolved")]
    NoHome,

    #[error("scopes.toml: scope id is empty")]
    EmptyId,

    #[error("scopes.toml: duplicate scope id {0:?}")]
    DuplicateId(String),
}

// Compile-time guarantees that `ScopeRegistry` and `ScopeConfig` (the
// shapes threaded through capabilities) are shareable across async
// tasks. cap_std::fs::Dir is documented as `Send + Sync`; this
// assertion catches a future regression.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Arc<ScopeConfig>>();
    assert_send_sync::<ScopeRegistry>();
    assert_send_sync::<cap_std::fs::Dir>();
};

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn parse_relative_path_accepts_normal() {
        assert!(parse_relative_path("foo/bar").is_ok());
        assert!(parse_relative_path("foo/./bar").is_ok());
        assert!(parse_relative_path("./foo").is_ok());
        assert!(parse_relative_path(".").is_ok());
        assert!(parse_relative_path("").is_ok());
    }

    #[test]
    fn parse_relative_path_rejects_dotdot() {
        let err = parse_relative_path("foo/../bar").expect_err("must reject");
        assert!(matches!(err, ScopeError::ForbiddenComponent { .. }));
    }

    #[test]
    fn parse_relative_path_rejects_absolute() {
        let err = parse_relative_path("/etc/passwd").expect_err("must reject");
        assert!(matches!(err, ScopeError::ForbiddenComponent { .. }));
    }
}
