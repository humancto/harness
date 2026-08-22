//! Encrypted-at-rest credential store (Phase 3.6-encrypted, ADR-0021).
//!
//! `~/.harness/secrets.enc` holds the same tag→value TOML map as the
//! legacy plaintext store, encrypted with ChaCha20-Poly1305 under a key
//! derived from the node identity secret:
//!
//! ```text
//! vault_key = blake3::derive_key("harness secrets v1", identity_seed)
//! ```
//!
//! The on-disk envelope is TOML (consistent with every other file in
//! `~/.harness/`):
//!
//! ```toml
//! format_version = 1
//! nonce = "<24 hex chars>"
//! ciphertext = "<hex>"
//! ```
//!
//! `format_version` is bound into the AEAD as associated data, so
//! editing the envelope's version field is detected as tampering, as is
//! any bit-flip in the ciphertext (Poly1305 tag) and any wrong key.
//!
//! ## Migration
//!
//! [`EncryptedStore::open_with_migration`] transparently upgrades a
//! legacy plaintext `secrets.toml`: the plaintext file is read (with
//! the full ADR-0012 validation: 0600 permissions, tag grammar), an
//! encrypted `secrets.enc` is written at mode 0600, and a warning tells
//! the operator to delete the plaintext file. User files are never
//! deleted by the daemon.
//!
//! Runtime semantics are identical to the plaintext store: env-var
//! overrides (`HARNESS_SECRET_<UPPER_TAG>`) win over the file, and all
//! values live behind [`SecretValue`]'s redaction wall.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::Path;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    check_permissions, env_override_tags, tag_to_env_var, SecretValue, SecretsError, SecretsStore,
};

/// Current envelope format version. Bound into the AEAD associated
/// data — see [`aad_for_version`].
pub const FORMAT_VERSION: u32 = 1;

/// ChaCha20-Poly1305 nonce length in bytes.
const NONCE_LEN: usize = 12;

/// BLAKE3 `derive_key` context string for the vault key. Changing this
/// string orphans every existing `secrets.enc` — never change it
/// without a `format_version` bump and a migration path.
const KDF_CONTEXT: &str = "harness secrets v1";

/// Associated data binding the envelope version into the AEAD.
fn aad_for_version(version: u32) -> Vec<u8> {
    format!("harness-secrets-enc-v{version}").into_bytes()
}

/// Derive the vault encryption key from the node identity's 32-byte
/// Ed25519 seed (`harness_core::Identity::to_secret_bytes`).
///
/// BLAKE3's KDF mode gives domain separation from the signing keypair
/// (which `ed25519-dalek` derives from the same seed through SHA-512):
/// neither output reveals anything about the other. See ADR-0021.
#[must_use]
pub fn derive_vault_key(identity_secret: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    Zeroizing::new(blake3::derive_key(KDF_CONTEXT, identity_secret))
}

/// How [`EncryptedStore::open_with_migration`] obtained its contents —
/// callers use this to log the right operator guidance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultOrigin {
    /// `secrets.enc` existed and decrypted cleanly.
    Encrypted,
    /// Only the legacy plaintext `secrets.toml` existed; its contents
    /// were re-encrypted into a fresh `secrets.enc`. The plaintext file
    /// is left on disk — the operator should delete it.
    MigratedFromPlaintext,
    /// Neither file existed; the store is empty.
    Missing,
}

/// On-disk envelope for `secrets.enc`.
#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    format_version: u32,
    /// Hex-encoded 12-byte AEAD nonce, freshly random on every write.
    nonce: String,
    /// Hex-encoded ChaCha20-Poly1305 ciphertext (includes the 16-byte
    /// Poly1305 tag).
    ciphertext: String,
}

/// Encrypted-at-rest credential store. Same [`SecretsStore`] surface as
/// the plaintext reference implementation.
#[derive(Debug, Default)]
pub struct EncryptedStore {
    map: HashMap<String, SecretValue>,
}

impl EncryptedStore {
    /// Empty store — for tests and the missing-file daemon path.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load `secrets.enc`, falling back to a legacy plaintext
    /// `secrets.toml` migration. See [`VaultOrigin`] for the three
    /// outcomes. Errors (permissions, parse, decrypt/tamper) propagate
    /// — callers should treat them as fatal rather than silently
    /// skipping a credential file.
    pub fn open_with_migration(
        enc_path: &Path,
        legacy_path: &Path,
        key: &[u8; 32],
    ) -> Result<(Self, VaultOrigin), SecretsError> {
        if enc_path.exists() {
            let store = Self::load_from_path(enc_path, key)?;
            if legacy_path.exists() {
                tracing::warn!(
                    target: "harness.vault",
                    enc = %enc_path.display(),
                    legacy = %legacy_path.display(),
                    "legacy plaintext credential file is IGNORED now that the encrypted \
                     store exists; delete it to remove the plaintext copy from disk"
                );
            }
            return Ok((store, VaultOrigin::Encrypted));
        }
        if legacy_path.exists() {
            let raw = load_plaintext_raw(legacy_path)?;
            write_encrypted(enc_path, key, &raw)?;
            tracing::warn!(
                target: "harness.vault",
                enc = %enc_path.display(),
                legacy = %legacy_path.display(),
                entries = raw.len(),
                "migrated plaintext credentials to the encrypted store; the plaintext \
                 file was NOT deleted — verify the daemon starts cleanly, then delete it"
            );
            let map = raw
                .into_iter()
                .map(|(tag, value)| (tag, SecretValue::from_bytes(value.into_bytes())))
                .collect();
            return Ok((Self { map }, VaultOrigin::MigratedFromPlaintext));
        }
        Ok((Self::empty(), VaultOrigin::Missing))
    }

    /// Load and decrypt an existing `secrets.enc`. Errors on a missing
    /// file (use [`Self::open_with_migration`] for optional-by-default
    /// semantics), loose permissions, envelope/inner parse failures,
    /// and — via the AEAD tag — wrong key or tampered ciphertext.
    pub fn load_from_path(path: &Path, key: &[u8; 32]) -> Result<Self, SecretsError> {
        check_permissions(path)?;
        let text = std::fs::read_to_string(path).map_err(|e| SecretsError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let envelope: Envelope = toml::from_str(&text).map_err(|e| SecretsError::Parse {
            path: path.to_path_buf(),
            source: e,
        })?;
        if envelope.format_version != FORMAT_VERSION {
            return Err(SecretsError::BadFormat {
                path: path.to_path_buf(),
                reason: format!(
                    "unsupported format_version {} (this build reads {FORMAT_VERSION})",
                    envelope.format_version
                ),
            });
        }
        let nonce_bytes = hex::decode(&envelope.nonce).map_err(|e| SecretsError::BadFormat {
            path: path.to_path_buf(),
            reason: format!("nonce is not valid hex: {e}"),
        })?;
        if nonce_bytes.len() != NONCE_LEN {
            return Err(SecretsError::BadFormat {
                path: path.to_path_buf(),
                reason: format!("nonce must be {NONCE_LEN} bytes, got {}", nonce_bytes.len()),
            });
        }
        let ciphertext =
            hex::decode(&envelope.ciphertext).map_err(|e| SecretsError::BadFormat {
                path: path.to_path_buf(),
                reason: format!("ciphertext is not valid hex: {e}"),
            })?;
        let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: &ciphertext,
                    aad: &aad_for_version(envelope.format_version),
                },
            )
            .map(Zeroizing::new)
            // The AEAD error is deliberately opaque: wrong key and
            // tampered ciphertext are indistinguishable by design.
            .map_err(|_| SecretsError::Decrypt {
                path: path.to_path_buf(),
            })?;
        let inner = std::str::from_utf8(&plaintext).map_err(|_| SecretsError::BadFormat {
            path: path.to_path_buf(),
            reason: "decrypted payload is not UTF-8".to_string(),
        })?;
        let raw: BTreeMap<String, String> =
            toml::from_str(inner).map_err(|e| SecretsError::Parse {
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

impl SecretsStore for EncryptedStore {
    fn get(&self, tag: &str) -> Option<SecretValue> {
        // Env first (12-factor / dev convenience); file second — same
        // semantics as the plaintext store.
        if let Some(env_name) = tag_to_env_var(tag) {
            if let Ok(s) = std::env::var(&env_name) {
                return Some(SecretValue::from_bytes(s.into_bytes()));
            }
        }
        self.map.get(tag).cloned()
    }

    fn tags(&self) -> Vec<String> {
        let mut tags: Vec<String> = self.map.keys().cloned().collect();
        tags.extend(env_override_tags());
        tags.sort();
        tags.dedup();
        tags
    }
}

/// Encrypt `raw` and write the envelope to `path` at mode 0600. Fresh
/// random nonce per call.
pub(crate) fn write_encrypted(
    path: &Path,
    key: &[u8; 32],
    raw: &BTreeMap<String, String>,
) -> Result<(), SecretsError> {
    let inner = toml::to_string(raw).map_err(|e| SecretsError::Serialize {
        path: path.to_path_buf(),
        source: e,
    })?;
    let inner = Zeroizing::new(inner);
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut nonce_bytes);
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: inner.as_bytes(),
                aad: &aad_for_version(FORMAT_VERSION),
            },
        )
        .map_err(|_| SecretsError::Encrypt {
            path: path.to_path_buf(),
        })?;
    let envelope = Envelope {
        format_version: FORMAT_VERSION,
        nonce: hex::encode(nonce_bytes),
        ciphertext: hex::encode(ciphertext),
    };
    let text = toml::to_string(&envelope).map_err(|e| SecretsError::Serialize {
        path: path.to_path_buf(),
        source: e,
    })?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let io_err = |e: std::io::Error| SecretsError::Io {
        path: path.to_path_buf(),
        source: e,
    };
    let mut file = options.open(path).map_err(io_err)?;
    // `mode(0o600)` only applies on create; enforce on rewrite too.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(io_err)?;
    }
    file.write_all(text.as_bytes()).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    Ok(())
}

/// Read a legacy plaintext `secrets.toml` as raw strings for migration.
/// Applies the full ADR-0012 validation (0600 permissions, tag grammar).
fn load_plaintext_raw(path: &Path) -> Result<BTreeMap<String, String>, SecretsError> {
    check_permissions(path)?;
    let text = std::fs::read_to_string(path).map_err(|e| SecretsError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let raw: BTreeMap<String, String> = toml::from_str(&text).map_err(|e| SecretsError::Parse {
        path: path.to_path_buf(),
        source: e,
    })?;
    for tag in raw.keys() {
        if tag_to_env_var(tag).is_none() {
            return Err(SecretsError::InvalidTag { tag: tag.clone() });
        }
    }
    Ok(raw)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    fn key_a() -> Zeroizing<[u8; 32]> {
        derive_vault_key(&[0x11u8; 32])
    }

    fn key_b() -> Zeroizing<[u8; 32]> {
        derive_vault_key(&[0x22u8; 32])
    }

    fn write_sample(dir: &Path, key: &[u8; 32]) -> std::path::PathBuf {
        let path = dir.join("secrets.enc");
        let mut raw = BTreeMap::new();
        raw.insert(
            "secret/claude-api-key".to_string(),
            "sk-ant-cafebabe".to_string(),
        );
        raw.insert("secret/openai-api-key".to_string(), "sk-oai-42".to_string());
        write_encrypted(&path, key, &raw).expect("write");
        path
    }

    #[test]
    fn t01_encrypt_decrypt_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        let store = EncryptedStore::load_from_path(&path, &key_a()).expect("load");
        let v = store.get("secret/claude-api-key").expect("present");
        assert_eq!(v.as_bytes(), b"sk-ant-cafebabe");
        assert!(store.get("secret/unknown").is_none());
        // The file itself must not contain the plaintext value.
        let on_disk = std::fs::read_to_string(&path).expect("read");
        assert!(!on_disk.contains("cafebabe"), "plaintext leaked to disk");
        assert!(on_disk.contains("format_version = 1"));
    }

    #[test]
    fn t02_wrong_key_fails_closed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        let err = EncryptedStore::load_from_path(&path, &key_b()).expect_err("must fail");
        assert!(matches!(err, SecretsError::Decrypt { .. }), "got {err:?}");
    }

    #[test]
    fn t03_tampered_ciphertext_detected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        let text = std::fs::read_to_string(&path).expect("read");
        let envelope: Envelope = toml::from_str(&text).expect("parse");
        // Flip one nibble in the middle of the ciphertext.
        let mut chars: Vec<char> = envelope.ciphertext.chars().collect();
        let mid = chars.len() / 2;
        chars[mid] = if chars[mid] == '0' { '1' } else { '0' };
        let tampered = Envelope {
            format_version: envelope.format_version,
            nonce: envelope.nonce,
            ciphertext: chars.into_iter().collect(),
        };
        std::fs::write(&path, toml::to_string(&tampered).expect("ser")).expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        }
        let err = EncryptedStore::load_from_path(&path, &key_a()).expect_err("must fail");
        assert!(matches!(err, SecretsError::Decrypt { .. }), "got {err:?}");
    }

    #[test]
    fn t04_downgraded_format_version_detected() {
        // Editing format_version alone breaks the AAD binding — the
        // load fails at the version gate; even if a future build
        // accepted version 0, the AEAD would reject the AAD mismatch.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        let text = std::fs::read_to_string(&path).expect("read");
        let downgraded = text.replace("format_version = 1", "format_version = 0");
        std::fs::write(&path, downgraded).expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        }
        let err = EncryptedStore::load_from_path(&path, &key_a()).expect_err("must fail");
        assert!(matches!(err, SecretsError::BadFormat { .. }), "got {err:?}");
    }

    #[test]
    fn t05_migration_from_plaintext_preserves_and_warns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let legacy = dir.path().join("secrets.toml");
        let enc = dir.path().join("secrets.enc");
        std::fs::write(&legacy, "\"secret/gemini-api-key\" = \"AIza-77\"\n").expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&legacy, std::fs::Permissions::from_mode(0o600))
                .expect("chmod");
        }
        let (store, origin) =
            EncryptedStore::open_with_migration(&enc, &legacy, &key_a()).expect("migrate");
        assert_eq!(origin, VaultOrigin::MigratedFromPlaintext);
        assert_eq!(
            store
                .get("secret/gemini-api-key")
                .expect("present")
                .as_bytes(),
            b"AIza-77"
        );
        // The .enc file was written; the legacy file was NOT deleted.
        assert!(enc.exists(), "secrets.enc must be written");
        assert!(legacy.exists(), "legacy file must never be deleted");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&enc).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "secrets.enc must be created 0600");
        }
        // Second open: the encrypted file wins.
        let (store2, origin2) =
            EncryptedStore::open_with_migration(&enc, &legacy, &key_a()).expect("reopen");
        assert_eq!(origin2, VaultOrigin::Encrypted);
        assert_eq!(
            store2
                .get("secret/gemini-api-key")
                .expect("present")
                .as_bytes(),
            b"AIza-77"
        );
    }

    #[test]
    fn t06_open_with_migration_missing_both_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, origin) = EncryptedStore::open_with_migration(
            &dir.path().join("secrets.enc"),
            &dir.path().join("secrets.toml"),
            &key_a(),
        )
        .expect("open");
        assert_eq!(origin, VaultOrigin::Missing);
        assert!(store.get("secret/anything").is_none());
        assert!(!dir.path().join("secrets.enc").exists());
    }

    #[cfg(unix)]
    #[test]
    fn t07_loose_permissions_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let err = EncryptedStore::load_from_path(&path, &key_a()).expect_err("must fail");
        assert!(
            matches!(err, SecretsError::PermissionsTooLoose { mode: 0o644, .. }),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn t08_migration_source_with_loose_permissions_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let legacy = dir.path().join("secrets.toml");
        let enc = dir.path().join("secrets.enc");
        std::fs::write(&legacy, "\"secret/k\" = \"v\"\n").expect("write");
        std::fs::set_permissions(&legacy, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let err =
            EncryptedStore::open_with_migration(&enc, &legacy, &key_a()).expect_err("must fail");
        assert!(matches!(err, SecretsError::PermissionsTooLoose { .. }));
        assert!(
            !enc.exists(),
            "no .enc must be written from a rejected source"
        );
    }

    #[test]
    fn t09_migration_rejects_invalid_tag() {
        let dir = tempfile::tempdir().expect("tempdir");
        let legacy = dir.path().join("secrets.toml");
        std::fs::write(&legacy, "\"secret/Bad_Tag\" = \"v\"\n").expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&legacy, std::fs::Permissions::from_mode(0o600))
                .expect("chmod");
        }
        let err =
            EncryptedStore::open_with_migration(&dir.path().join("secrets.enc"), &legacy, &key_a())
                .expect_err("must fail");
        assert!(matches!(err, SecretsError::InvalidTag { .. }));
    }

    #[test]
    #[serial_test::serial(harness_secret_env)]
    fn t10_env_override_wins_over_encrypted_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        let store = EncryptedStore::load_from_path(&path, &key_a()).expect("load");
        std::env::set_var("HARNESS_SECRET_CLAUDE_API_KEY", "sk-from-env");
        let v = store.get("secret/claude-api-key").expect("present");
        std::env::remove_var("HARNESS_SECRET_CLAUDE_API_KEY");
        assert_eq!(v.as_bytes(), b"sk-from-env");
    }

    #[test]
    #[serial_test::serial(harness_secret_env)]
    fn t11_tags_lists_file_and_env_names_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_sample(dir.path(), &key_a());
        let store = EncryptedStore::load_from_path(&path, &key_a()).expect("load");
        std::env::set_var("HARNESS_SECRET_EXTRA_ENV_KEY", "shh");
        let tags = store.tags();
        std::env::remove_var("HARNESS_SECRET_EXTRA_ENV_KEY");
        assert!(tags.contains(&"secret/claude-api-key".to_string()));
        assert!(tags.contains(&"secret/openai-api-key".to_string()));
        assert!(tags.contains(&"secret/extra-env-key".to_string()));
        // Names only — no value ever appears in the tag list.
        assert!(tags
            .iter()
            .all(|t| !t.contains("cafebabe") && !t.contains("shh")));
        // Sorted + deduped.
        let mut sorted = tags.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(tags, sorted);
    }

    #[test]
    fn t12_derive_vault_key_is_deterministic_and_seed_bound() {
        let a1 = derive_vault_key(&[0x11u8; 32]);
        let a2 = derive_vault_key(&[0x11u8; 32]);
        let b = derive_vault_key(&[0x12u8; 32]);
        assert_eq!(*a1, *a2);
        assert_ne!(*a1, *b);
        // And it is not the seed itself.
        assert_ne!(*a1, [0x11u8; 32]);
    }

    #[test]
    fn t13_nonce_is_fresh_per_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secrets.enc");
        let mut raw = BTreeMap::new();
        raw.insert("secret/k".to_string(), "v".to_string());
        write_encrypted(&path, &key_a(), &raw).expect("write 1");
        let e1: Envelope =
            toml::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        write_encrypted(&path, &key_a(), &raw).expect("write 2");
        let e2: Envelope =
            toml::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_ne!(e1.nonce, e2.nonce, "nonce must be random per write");
        assert_ne!(e1.ciphertext, e2.ciphertext);
    }
}
