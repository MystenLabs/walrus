// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use fastcrypto::encoding::{Encoding as _, Hex};
use jsonwebtoken::{Algorithm, DecodingKey};
use walrus_sdk::error::JwtDecodeError;

use super::daemon::CacheConfig;

/// Configuration for the JWT authentication on the publisher.
#[derive(Default, Clone)]
pub struct AuthConfig {
    /// The secret with which to authenticate the JWT.
    pub(crate) decoding_key: Option<DecodingKey>,
    /// The authentication algorithm for the JWT.
    pub(crate) algorithm: Option<Algorithm>,
    /// The duration, in seconds, after which the publisher will consider the JWT as expired.
    ///
    /// If set to `0`, the publisher will not check that the expiration is correctly set based in
    /// the issued-at time (iat) and expiration time (exp) in the JWT. I.e., if `expiring_sec > 0`,
    /// the publisher will check that `exp - iat == expiring_sec`.
    pub(crate) expiring_sec: i64,
    /// Verify the upload epochs and address for `send_object_to` in the request.
    ///
    /// The token expiration is still checked, even if `verify_upload == true`.
    pub(crate) verify_upload: bool,
    /// The configuration for the replay suppression cache.
    pub(crate) replay_suppression_config: CacheConfig,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field("algorithm", &self.algorithm)
            .field("expiring_sec", &self.expiring_sec)
            .field("verify_upload", &self.verify_upload)
            .finish()
    }
}

impl AuthConfig {
    /// Adds the decoding key to the configuration, parsing it form the given string.
    ///
    /// Uses `self.algorithm` to determine the decoding key type.
    pub fn with_key_from_str(&mut self, secret: &str) -> Result<(), JwtDecodeError> {
        let secret_bytes = Self::secret_to_bytes(secret)?;
        let decoding_key = self.decoding_key_from_secret(&secret_bytes);
        self.decoding_key = Some(decoding_key);
        Ok(())
    }

    fn decoding_key_from_secret(&self, secret: &[u8]) -> DecodingKey {
        match self.algorithm {
            None | Some(Algorithm::HS256) | Some(Algorithm::HS384) | Some(Algorithm::HS512) => {
                DecodingKey::from_secret(secret)
            }
            Some(Algorithm::EdDSA) => DecodingKey::from_ed_der(secret),
            Some(Algorithm::ES256) | Some(Algorithm::ES384) => DecodingKey::from_ec_der(secret),
            Some(Algorithm::RS256)
            | Some(Algorithm::RS384)
            | Some(Algorithm::RS512)
            | Some(Algorithm::PS256)
            | Some(Algorithm::PS384)
            | Some(Algorithm::PS512) => DecodingKey::from_rsa_der(secret),
        }
    }

    fn secret_to_bytes(secret: &str) -> Result<Vec<u8>, JwtDecodeError> {
        if secret.starts_with("0x") {
            if secret.len() % 2 != 0 {
                Err(JwtDecodeError)
            } else {
                Hex::decode(secret).map_err(|_| JwtDecodeError)
            }
        } else {
            Ok(secret.as_bytes().to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;

    param_test! {
        test_secret_to_bytes -> TestResult: [
            correct: ("0xff", Ok(vec![255])),
            invalid_hex: ("0xf", Err(JwtDecodeError)),
            invalid_hex_2: ("0xfg", Err(JwtDecodeError)),
        ]
    }
    fn test_secret_to_bytes(secret: &str, output: Result<Vec<u8>, JwtDecodeError>) -> TestResult {
        let bytes = AuthConfig::secret_to_bytes(secret);
        assert_eq!(bytes, output);
        Ok(())
    }
}
