//! Pairing code: 8 digits, displayed `NNNN-NNNN`. Generated via the
//! OS CSPRNG so two simultaneous `harness init` invocations on the
//! same host don't collide. Constant-time comparison on validation
//! so timing attacks can't recover the code.

use std::fmt;
use std::str::FromStr;

use rand::rngs::OsRng;
use rand::Rng;

/// Errors parsing a pairing code from a human-typed string.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PairingCodeError {
    #[error("pairing code must be exactly 8 digits (with optional dash)")]
    BadFormat,
}

/// 8-digit pairing code. The display form is `NNNN-NNNN`.
///
/// Storage is a `[u8; 8]` of '0'..='9' bytes so equality comparison
/// goes byte-for-byte without allocations and so we can use a
/// constant-time comparator.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PairingCode([u8; 8]);

impl PairingCode {
    pub const LEN: usize = 8;

    /// Construct from raw `[u8; 8]` digits. Caller is responsible for
    /// ensuring every byte is `b'0'..=b'9'`. Used by the pairing
    /// protocol's constant-time validator to wrap incoming code bytes
    /// for comparison.
    #[must_use]
    pub(super) fn from_raw(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// Generate a fresh code from `OsRng`. Each digit is uniform 0..=9.
    #[must_use]
    pub fn generate() -> Self {
        let mut digits = [0u8; 8];
        let mut rng = OsRng;
        for d in &mut digits {
            *d = b'0' + rng.gen_range(0..10);
        }
        Self(digits)
    }

    /// Constant-time equality. Use this from any code path where the
    /// comparison's branch could leak the secret via timing.
    #[must_use]
    pub fn ct_eq(&self, other: &Self) -> bool {
        let mut diff: u8 = 0;
        for i in 0..Self::LEN {
            diff |= self.0[i] ^ other.0[i];
        }
        diff == 0
    }

    /// Raw bytes — exposed for tests that need to construct a known
    /// code. Not for general use.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

impl fmt::Display for PairingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: every digit is '0'..='9'.
        let s = std::str::from_utf8(&self.0).unwrap_or("????????");
        write!(f, "{}-{}", &s[0..4], &s[4..8])
    }
}

impl fmt::Debug for PairingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Don't redact in Debug — these are short-lived (10min TTL),
        // and tests benefit from visibility. Production users should
        // not be `Debug`-printing pairing codes anyway.
        f.debug_tuple("PairingCode")
            .field(&self.to_string())
            .finish()
    }
}

impl FromStr for PairingCode {
    type Err = PairingCodeError;

    /// Parse `NNNN-NNNN` or `NNNNNNNN`. Whitespace is trimmed; a
    /// single `-` separator is allowed.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cleaned: String = s
            .chars()
            .filter(|c| !c.is_whitespace() && *c != '-')
            .collect();
        if cleaned.len() != Self::LEN {
            return Err(PairingCodeError::BadFormat);
        }
        let bytes = cleaned.as_bytes();
        let mut out = [0u8; 8];
        for (i, b) in bytes.iter().enumerate() {
            if !b.is_ascii_digit() {
                return Err(PairingCodeError::BadFormat);
            }
            out[i] = *b;
        }
        Ok(Self(out))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_8_digits() {
        let c = PairingCode::generate();
        for byte in c.as_bytes() {
            assert!(byte.is_ascii_digit());
        }
    }

    #[test]
    fn display_format_is_dashed_4_4() {
        let c = PairingCode([b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8']);
        assert_eq!(c.to_string(), "1234-5678");
    }

    #[test]
    fn parse_round_trip_with_dash() {
        let s = "1234-5678";
        let c: PairingCode = s.parse().expect("parse");
        assert_eq!(c.to_string(), s);
    }

    #[test]
    fn parse_round_trip_without_dash() {
        let c: PairingCode = "12345678".parse().expect("parse");
        assert_eq!(c.to_string(), "1234-5678");
    }

    #[test]
    fn parse_with_whitespace() {
        let c: PairingCode = " 1234 5678 ".parse().expect("parse");
        assert_eq!(c.to_string(), "1234-5678");
    }

    #[test]
    fn parse_rejects_wrong_length() {
        assert!(matches!(
            "12345".parse::<PairingCode>(),
            Err(PairingCodeError::BadFormat)
        ));
        assert!(matches!(
            "123456789".parse::<PairingCode>(),
            Err(PairingCodeError::BadFormat)
        ));
    }

    #[test]
    fn parse_rejects_non_digit() {
        assert!(matches!(
            "1234-abcd".parse::<PairingCode>(),
            Err(PairingCodeError::BadFormat)
        ));
    }

    #[test]
    fn ct_eq_true_for_equal() {
        let a: PairingCode = "1234-5678".parse().expect("parse");
        let b: PairingCode = "1234-5678".parse().expect("parse");
        assert!(a.ct_eq(&b));
    }

    #[test]
    fn ct_eq_false_for_different() {
        let a: PairingCode = "1234-5678".parse().expect("parse");
        let b: PairingCode = "1234-5679".parse().expect("parse");
        assert!(!a.ct_eq(&b));
    }

    #[test]
    fn two_generated_codes_are_different() {
        // Probabilistic — 1-in-10^8 chance of collision. Acceptable.
        let a = PairingCode::generate();
        let b = PairingCode::generate();
        assert!(!a.ct_eq(&b), "two random codes collided (1 in 10^8)");
    }
}
