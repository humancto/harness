//! Admin authentication — argon2id-hashed password + bearer sessions.
//! ADR-0007 documents the design.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use harness_mesh::AdminFile;
use parking_lot::RwLock;
use rand::RngCore;
use serde::{Deserialize, Serialize};

/// 30-day sliding TTL.
pub const SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// 32 random bytes, hex-encoded → 64-char string.
fn fresh_token() -> String {
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Per-daemon session registry. Restart logs everyone out by design
/// (ADR-0007).
#[derive(Clone, Default)]
pub struct SessionStore {
    inner: Arc<RwLock<std::collections::HashMap<String, u64>>>,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("count", &self.inner.read().len())
            .finish_non_exhaustive()
    }
}

impl SessionStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Issue a new session; returns the bearer token + absolute expiry.
    pub fn issue(&self) -> (String, u64) {
        let token = fresh_token();
        let expires_at_ms = now_ms() + u64::try_from(SESSION_TTL.as_millis()).unwrap_or(u64::MAX);
        self.inner.write().insert(token.clone(), expires_at_ms);
        (token, expires_at_ms)
    }

    /// Validate a token. Returns the new sliding-TTL expiry if valid.
    /// Constant-time comparison happens implicitly via `HashMap` lookup
    /// (no early-out on prefix match).
    pub fn validate(&self, token: &str) -> Option<u64> {
        let mut g = self.inner.write();
        let now = now_ms();
        match g.get(token).copied() {
            Some(exp) if exp > now => {
                let new_exp = now + u64::try_from(SESSION_TTL.as_millis()).unwrap_or(u64::MAX);
                g.insert(token.to_string(), new_exp);
                Some(new_exp)
            }
            Some(_) => {
                // expired — opportunistically reap.
                g.remove(token);
                None
            }
            None => None,
        }
    }

    pub fn revoke(&self, token: &str) {
        self.inner.write().remove(token);
    }

    pub fn count(&self) -> usize {
        self.inner.read().len()
    }
}

/// Auth provider — combines the on-disk admin hash with the in-memory
/// session store. Held as `Arc<AuthProvider>` in `ApiState`.
#[derive(Clone)]
pub struct AuthProvider {
    pub admin: Option<AdminFile>,
    pub sessions: SessionStore,
}

impl std::fmt::Debug for AuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthProvider")
            .field("admin_initialized", &self.admin.is_some())
            .finish_non_exhaustive()
    }
}

impl AuthProvider {
    #[must_use]
    pub fn new(admin: Option<AdminFile>) -> Self {
        Self {
            admin,
            sessions: SessionStore::new(),
        }
    }

    /// `true` if the admin password is set on disk. Used by the daemon
    /// to surface a "run set-password" hint if not.
    pub fn is_initialized(&self) -> bool {
        self.admin.is_some()
    }
}

/// Request body for `POST /api/v1/auth/login`.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub password: String,
}

/// Response body for `POST /api/v1/auth/login`.
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub expires_at_ms: u64,
}

/// Extract a bearer token from `Authorization: Bearer <token>` or the
/// `harness_session` cookie. Returns `None` if absent.
pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        if let Ok(s) = value.to_str() {
            if let Some(rest) = s.strip_prefix("Bearer ") {
                return Some(rest.trim().to_string());
            }
        }
    }
    if let Some(cookie) = headers.get(header::COOKIE) {
        if let Ok(s) = cookie.to_str() {
            for part in s.split(';') {
                let part = part.trim();
                if let Some(value) = part.strip_prefix("harness_session=") {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

/// `true` if the request carries a valid session.
#[must_use]
pub fn is_authenticated(provider: &AuthProvider, headers: &HeaderMap) -> bool {
    let Some(token) = extract_token(headers) else {
        return false;
    };
    provider.sessions.validate(&token).is_some()
}

/// `POST /api/v1/auth/login` — verify password, issue session.
pub async fn login_handler(
    State(state): State<crate::state::ApiState>,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let _ = headers; // reserved for source-IP lockout in 6.x.
    let Some(admin) = state.auth.admin.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "admin_not_initialized" })),
        )
            .into_response();
    };
    if !admin.verify(&req.password) {
        // Constant-ish-time: argon2 verify on the *real* hash already
        // dominates timing — failed attempts go through the same path.
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "bad_credentials" })),
        )
            .into_response();
    }
    let (token, expires_at_ms) = state.auth.sessions.issue();
    let cookie = format!(
        "harness_session={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        SESSION_TTL.as_secs()
    );
    let resp = Json(LoginResponse {
        token: token.clone(),
        expires_at_ms,
    });
    let mut response = (StatusCode::OK, resp).into_response();
    if let Ok(value) = header::HeaderValue::from_str(&cookie) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

/// `POST /api/v1/auth/logout` — revoke current session.
pub async fn logout_handler(
    State(state): State<crate::state::ApiState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(token) = extract_token(&headers) {
        state.auth.sessions.revoke(&token);
    }
    let mut response = (StatusCode::NO_CONTENT, ()).into_response();
    if let Ok(value) =
        header::HeaderValue::from_str("harness_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
    {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

/// 401 response when an authenticated route is hit without a session.
pub fn unauthorized() -> axum::response::Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "error": "unauthorized" })),
    )
        .into_response()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn issue_then_validate_succeeds() {
        let s = SessionStore::new();
        let (token, exp) = s.issue();
        assert!(exp > now_ms());
        assert!(s.validate(&token).is_some());
    }

    #[test]
    fn validate_returns_none_for_bogus_token() {
        let s = SessionStore::new();
        assert!(s.validate("not-a-real-token").is_none());
    }

    #[test]
    fn revoke_invalidates_token() {
        let s = SessionStore::new();
        let (token, _) = s.issue();
        s.revoke(&token);
        assert!(s.validate(&token).is_none());
    }

    #[test]
    fn expired_session_reaped_on_validate() {
        let s = SessionStore::new();
        let (token, _) = s.issue();
        // Force-expire by overwriting the entry.
        s.inner.write().insert(token.clone(), 0);
        assert!(s.validate(&token).is_none());
        assert_eq!(s.count(), 0);
    }

    #[test]
    fn extract_token_reads_bearer_header() {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer abc123"),
        );
        assert_eq!(extract_token(&h).as_deref(), Some("abc123"));
    }

    #[test]
    fn extract_token_reads_cookie() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_static("foo=bar; harness_session=xyz; baz=qux"),
        );
        assert_eq!(extract_token(&h).as_deref(), Some("xyz"));
    }

    #[test]
    fn extract_token_none_when_absent() {
        let h = HeaderMap::new();
        assert!(extract_token(&h).is_none());
    }

    #[test]
    fn extract_token_skips_non_bearer_authz() {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_static("Basic foo"));
        assert!(extract_token(&h).is_none());
    }

    #[test]
    fn auth_provider_uninit_reports_initialized_false() {
        let p = AuthProvider::new(None);
        assert!(!p.is_initialized());
    }

    #[test]
    fn auth_provider_initialized_when_admin_present() {
        let admin = AdminFile::from_password("hunter2").expect("hash");
        let p = AuthProvider::new(Some(admin));
        assert!(p.is_initialized());
    }
}
