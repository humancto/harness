//! Tagged credential store. Phase 3.6a — Plaintext-on-disk reference
//! implementation; encrypted-replicated form is Phase 3.6-encrypted
//! (PRD §10.5). Both forms implement the same [`SecretsStore`] trait.
//!
//! Capabilities reference credentials by **tag** (`secret/<name>`),
//! never by raw bytes. The tag is the only identifier that ever appears
//! in code, manifests, or telemetry.
//!
//! ## Backing files
//!
//! Default path is `~/.harness/secrets.toml`:
//!
//! ```toml
//! "secret/claude-api-key" = "sk-ant-api03-..."
//! ```
//!
//! On Unix the file mode must be `0600` or stricter (no group / other
//! bits). Loose permissions return [`SecretsError::PermissionsTooLoose`]
//! and refuse to load. Non-Unix platforms emit a `tracing::warn!` and
//! load unconditionally.
//!
//! ## Env-var fallback
//!
//! For development convenience, `get(tag)` first probes
//! `HARNESS_SECRET_<UPPER_TAG>` (where `<UPPER_TAG>` is the tag suffix
//! after `secret/`, with `-` replaced by `_` and uppercased). When that
//! env var is set, its value wins over the file. Production deployments
//! should rely on the file with `0600` perms.
//!
//! ## `SecretValue`
//!
//! Raw bytes are wrapped in [`SecretValue`]. The wrapper deliberately
//! omits `Display`, `Serialize`, `Hash`, `PartialEq`, `Deref`, and any
//! other `AsRef` impl that would let bytes leak into a log line, an API
//! response, or a `HashMap` key by accident. The only puncture in the
//! redaction wall is [`SecretValue::as_bytes`], which is `#[must_use]`
//! and documented as "do not log, do not persist."

#![forbid(unsafe_code)]

pub mod error;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use error::SecretsError;

/// Default config path: `~/.harness/secrets.toml`.
///
/// Returns `Err(SecretsError::NoHome)` if `$HOME` cannot be resolved.
pub fn default_path() -> Result<PathBuf, SecretsError> {
    let home = dirs::home_dir().ok_or(SecretsError::NoHome)?;
    Ok(home.join(".harness").join("secrets.toml"))
}

/// Convert a tag like `"secret/claude-api-key"` into its env-var name
/// `"HARNESS_SECRET_CLAUDE_API_KEY"`. Returns `None` if the tag is not
/// of the form `secret/[a-z0-9-]+` with a non-empty suffix.
///
/// Used by [`PlaintextStore::get`] to find the env-var fallback for a
/// given tag, AND at load time to reject tags that violate the rule.
#[must_use]
pub fn tag_to_env_var(tag: &str) -> Option<String> {
    let suffix = tag.strip_prefix("secret/")?;
    if suffix.is_empty() {
        return None;
    }
    if !suffix
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return None;
    }
    Some(format!(
        "HARNESS_SECRET_{}",
        suffix.replace('-', "_").to_uppercase()
    ))
}

/// Wraps an API key / password / similar. The raw bytes are accessible
/// ONLY via [`Self::as_bytes`]. All other ways to read the value are
/// deliberately omitted to make accidental leakage a compile error.
///
/// Specifically NOT implemented:
/// - `Display` (so `format!("{}", v)` doesn't compile)
/// - `PartialEq`, `Eq`, `Hash` (so it can't be a `HashMap` key, no
///   accidental log via assertion failure)
/// - `Serialize`, `Deserialize` (so it can't accidentally appear in
///   API responses, telemetry, or replicated state)
/// - `AsRef<[u8]>`, `AsRef<str>`, `Deref` (so generic-over-bytes
///   helpers don't accept it without thinking)
///
/// `Clone` IS implemented — each clone allocates a fresh
/// `Zeroizing<Vec<u8>>`. Cheap-but-not-free; documented.
///
/// ## Compile-time leakage guard
///
/// ```compile_fail
/// use harness_vault::SecretValue;
/// let v = SecretValue::from_bytes(b"hi".to_vec());
/// let _ = format!("{}", v);
/// ```
#[derive(Clone)]
pub struct SecretValue(zeroize::Zeroizing<Vec<u8>>);

impl SecretValue {
    /// Construct from owned bytes. The bytes are zeroized on drop.
    #[must_use]
    pub fn from_bytes(b: Vec<u8>) -> Self {
        Self(zeroize::Zeroizing::new(b))
    }

    /// Reveal the raw secret. Callers MUST NOT log, format, or persist
    /// the result. The only legal puncture in the redaction wall.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

// Compile-time guarantee that `SecretValue` is safe to share across
// async tasks. `Zeroizing<Vec<u8>>` is `Send + Sync` because `Vec<u8>`
// is — pinned here so a future `Zeroizing` change can't quietly break
// the contract.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SecretValue>();
};

/// Resolve secrets by tag. The plaintext form (Phase 3.6a) and the
/// future encrypted-replicated form (3.6-encrypted) implement the same
/// trait; capabilities depend only on this surface.
pub trait SecretsStore: Send + Sync + std::fmt::Debug {
    /// Look up a secret by tag. Returns `None` if the tag is unknown.
    /// Implementations MUST NOT log the returned bytes.
    fn get(&self, tag: &str) -> Option<SecretValue>;
}

/// In-process plaintext credential store. Backed by a TOML file (default
/// `~/.harness/secrets.toml`) plus a per-tag env-var fallback.
#[derive(Debug, Default)]
pub struct PlaintextStore {
    map: HashMap<String, SecretValue>,
}

impl PlaintextStore {
    /// Empty store — for tests and the `--no-secrets` daemon path.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load `~/.harness/secrets.toml` if it exists; return an empty
    /// store if the file is missing. All other errors propagate.
    pub fn load_default() -> Result<Self, SecretsError> {
        let path = default_path()?;
        match std::fs::metadata(&path) {
            Ok(_) => Self::load_from_path(&path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(e) => Err(SecretsError::Io { path, source: e }),
        }
    }

    /// Load from an explicit path. Errors on missing file (use
    /// [`Self::load_default`] for the optional-by-default semantics).
    pub fn load_from_path(path: &Path) -> Result<Self, SecretsError> {
        check_permissions(path)?;
        let text = std::fs::read_to_string(path).map_err(|e| SecretsError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let raw: HashMap<String, String> =
            toml::from_str(&text).map_err(|e| SecretsError::Parse {
                path: path.to_path_buf(),
                source: e,
            })?;
        let mut map = HashMap::with_capacity(raw.len());
        for (tag, value) in raw {
            if tag_to_env_var(&tag).is_none() {
                return Err(SecretsError::InvalidTag { tag });
            }
            map.insert(tag, SecretValue::from_bytes(value.into_bytes()));
        }
        Ok(Self { map })
    }
}

impl SecretsStore for PlaintextStore {
    fn get(&self, tag: &str) -> Option<SecretValue> {
        // Env first (12-factor / dev convenience); file second.
        if let Some(env_name) = tag_to_env_var(tag) {
            if let Ok(s) = std::env::var(&env_name) {
                return Some(SecretValue::from_bytes(s.into_bytes()));
            }
        }
        self.map.get(tag).cloned()
    }
}

#[cfg(unix)]
fn check_permissions(path: &Path) -> Result<(), SecretsError> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path).map_err(|e| SecretsError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let mode = meta.permissions().mode() & 0o777;
    // Reject any bit outside owner-{r,w,x}: i.e. group / other access.
    if mode & 0o077 != 0 {
        return Err(SecretsError::PermissionsTooLoose {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_permissions(path: &Path) -> Result<(), SecretsError> {
    tracing::warn!(
        target: "harness.vault",
        path = %path.display(),
        "permission enforcement is not implemented on this platform; \
         loading credential file without checking owner-only access"
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn t08_tag_to_env_var_round_trip() {
        // Happy paths — exhaustive over the cases that matter.
        assert_eq!(
            tag_to_env_var("secret/claude-api-key").as_deref(),
            Some("HARNESS_SECRET_CLAUDE_API_KEY")
        );
        assert_eq!(
            tag_to_env_var("secret/openai").as_deref(),
            Some("HARNESS_SECRET_OPENAI")
        );
        assert_eq!(
            tag_to_env_var("secret/k1").as_deref(),
            Some("HARNESS_SECRET_K1")
        );
        assert_eq!(
            tag_to_env_var("secret/a-b-c-1-2-3").as_deref(),
            Some("HARNESS_SECRET_A_B_C_1_2_3")
        );

        // Reject paths.
        assert_eq!(tag_to_env_var(""), None);
        assert_eq!(tag_to_env_var("FOO"), None, "no secret/ prefix");
        assert_eq!(tag_to_env_var("secret/"), None, "empty suffix");
        assert_eq!(tag_to_env_var("secret/Foo"), None, "uppercase");
        assert_eq!(tag_to_env_var("secret/foo/bar"), None, "slash in suffix");
        assert_eq!(
            tag_to_env_var("secret/foo_bar"),
            None,
            "underscore in suffix"
        );
        assert_eq!(tag_to_env_var("secret/foo bar"), None, "space");
        assert_eq!(tag_to_env_var("secret/foo!"), None, "punctuation");
    }

    #[test]
    fn t05_secret_value_debug_is_redacted() {
        let v = SecretValue::from_bytes(b"sk-ant-deadbeef".to_vec());
        let s = format!("{v:?}");
        assert_eq!(s, "<redacted>");
        assert!(!s.contains("deadbeef"));
    }

    #[test]
    fn t09_secret_value_is_send_sync() {
        // The `const _` block at module top is the real guard; this
        // test exists so a future regression shows up as a failing
        // test name in CI output, not just a build error.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SecretValue>();
        assert_send_sync::<PlaintextStore>();
    }

    #[test]
    fn t06_empty_file_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secrets.toml");
        std::fs::write(&path, "").expect("write empty");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        }
        let store = PlaintextStore::load_from_path(&path).expect("load");
        assert!(store.get("secret/anything").is_none());
    }
}
