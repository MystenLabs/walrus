// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{body::Body, extract::Query, http::Response};
use axum_extra::headers::{authorization::Bearer, Authorization};
use jsonwebtoken::{
    decode,
    errors::{Error as JwtError, ErrorKind as JwtErrorKind},
    DecodingKey,
    Validation,
};
use serde::{Deserialize, Serialize};
use sui_types::base_types::SuiAddress;
use tracing::error;
use walrus_core::EpochCount;
use walrus_proc_macros::RestApiError;

use super::routes::PublisherQuery;
use crate::{client::config::AuthConfig, common::api::RestApiError};

pub const PUBLISHER_AUTH_DOMAIN: &str = "auth.publisher.walrus.space";

/// Claim follow RFC7519 with extra storage parameters: send_object_to, epochs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Claim {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Token is issued at (timestamp).
    pub iat: Option<u64>,

    /// Token expires at (timestamp).
    pub exp: u64,

    /// The owner address of the sui blob object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_object_to: Option<SuiAddress>,

    /// The number of epochs the blob should be stored for.
    ///
    /// This is an exact number of epochs the blob should be stored for, no more, no less.
    /// If both `epochs` and `max_epochs` are present, this is considered a configuration mistake
    /// and the claim is rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epochs: Option<u32>,

    /// The maximum number of epochs the blob can be stored for (inclusive).
    ///
    /// If both `epochs` and `max_epochs` are present, this is considered a configuration mistake
    /// and the claim is rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_epochs: Option<u32>,

    /// The maximum size of the blob that can be stored, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_size: Option<u64>,
}

impl Claim {
    pub fn from_token(
        token: &str,
        decoding_key: &DecodingKey,
        validation: &Validation,
    ) -> Result<Self, PublisherAuthError> {
        let claim: Claim = decode(token, decoding_key, validation)
            .map_err(|err| {
                tracing::debug!(
                    error = &err as &dyn std::error::Error,
                    "failed to convert token to claim"
                );
                match err.kind() {
                    JwtErrorKind::ExpiredSignature => PublisherAuthError::ExpiredSignature(err),

                    JwtErrorKind::InvalidSignature
                    | JwtErrorKind::InvalidAlgorithmName
                    | JwtErrorKind::ImmatureSignature => PublisherAuthError::InvalidSignature(err),

                    JwtErrorKind::InvalidToken
                    | JwtErrorKind::InvalidAlgorithm
                    | JwtErrorKind::InvalidIssuer
                    | JwtErrorKind::InvalidAudience
                    | JwtErrorKind::InvalidSubject
                    | JwtErrorKind::Base64(_)
                    | JwtErrorKind::Json(_)
                    | JwtErrorKind::Utf8(_) => PublisherAuthError::InvalidToken(err),

                    JwtErrorKind::RsaFailedSigning => {
                        unreachable!("we are not signing")
                    }

                    // The error kind is non-exhaustive, so we need to handle the `_` case.
                    _ => PublisherAuthError::Internal(err.into()),
                }
            })?
            .claims;

        Ok(claim)
    }

    /// Checks that the query matches the claim.
    pub fn check_valid_upload(
        &self,
        query: &PublisherQuery,
        auth_config: &AuthConfig,
        body_size_hint: u64,
    ) -> Result<(), PublisherAuthError> {
        // The expiration check is always performed.
        if self.check_expiring_sec(auth_config) {
            return Err(PublisherAuthError::InvalidExpiration);
        }

        // If verify_upload is disabled, skip the rest of the checks.
        if !auth_config.verify_upload {
            return Ok(());
        }

        if let Some(max_size) = self.max_size {
            if body_size_hint > max_size {
                tracing::debug!(
                    max_size = max_size,
                    body_size_hint = body_size_hint,
                    "upload with body size greater than max_size"
                );
                return Err(PublisherAuthError::MaxSizeExceeded);
            }
        }

        if let Err(error) = self.check_epochs(query.epochs) {
            tracing::debug!(
                epochs = self.epochs,
                max_epochs = self.max_epochs,
                query = query.epochs,
                "upload with invalid number of epochs"
            );
            return Err(error);
        }

        self.check_send_object_to(query.send_object_to)?;

        Ok(())
    }

    /// Checks if the claim has the correct `exp` and `iat` fields.
    fn check_expiring_sec(&self, auth_config: &AuthConfig) -> bool {
        if auth_config.expiring_sec == 0 {
            return false;
        }

        // Check that the difference between `exp` and `iat` is equal to the configured
        if (self.exp - self.iat.unwrap_or_default()) != auth_config.expiring_sec {
            tracing::error!(
                exp = self.exp,
                iat = self.iat.unwrap_or(0),
                expiring_sec = auth_config.expiring_sec,
                "expiring_sec does not match the difference between exp and iat"
            );
            return true;
        }

        false
    }

    /// Checks if the number of epochs requested in the query is allowed by the claim.
    ///
    /// If both `epochs` and `max_epochs` are present, this is considered a configuration mistake
    /// and the claim is rejected.
    fn check_epochs(&self, query_epochs: EpochCount) -> Result<(), PublisherAuthError> {
        match (self.epochs, self.max_epochs) {
            (Some(epochs), None) => {
                if query_epochs == epochs {
                    Ok(())
                } else {
                    Err(PublisherAuthError::InvalidEpochs)
                }
            }
            (None, Some(max_epochs)) => {
                if query_epochs <= max_epochs {
                    Ok(())
                } else {
                    Err(PublisherAuthError::EpochsAboveMax)
                }
            }
            (Some(_), Some(_)) => Err(PublisherAuthError::InvalidEpochs),
            (None, None) => Ok(()),
        }
    }

    /// Checks if the `send_object_to` field is valid.
    fn check_send_object_to(
        &self,

        query_send_object_to: Option<SuiAddress>,
    ) -> Result<(), PublisherAuthError> {
        match (self.send_object_to, query_send_object_to) {
            (Some(expected), Some(actual)) if expected != actual => {
                tracing::error!(
                    expected = %expected,
                    actual = %actual,
                    "upload with invalid send_object_to field"
                );
                Err(PublisherAuthError::InvalidSendObjectTo)
            }
            (Some(expected), None) => {
                tracing::error!(
                    expected = %expected,
                    "send_object_to field is missing"
                );

                Err(PublisherAuthError::MissingSendObjectTo)
            }
            _ => Ok(()),
        }
    }
}

pub fn verify_jwt_claim(
    query: Query<PublisherQuery>,
    bearer: Authorization<Bearer>,
    auth_config: &AuthConfig,
    body_size_hint: u64,
) -> Result<(), Response<Body>> {
    let mut validation = if auth_config.decoding_key.is_some() {
        auth_config
            .algorithm
            .map(Validation::new)
            .unwrap_or_default()
    } else {
        Validation::default()
    };

    let default_key = DecodingKey::from_secret(&[]);
    let decode_key = auth_config.decoding_key.as_ref().unwrap_or_else(|| {
        // No decoding key is provided in the configuration, so we disable signature validation.
        validation.insecure_disable_signature_validation();
        &default_key
    });

    if auth_config.expiring_sec > 0 {
        validation.set_required_spec_claims(&["exp", "iat"]);
    }

    match Claim::from_token(bearer.token().trim(), decode_key, &validation) {
        Ok(claim) => {
            if let Err(error) = claim.check_valid_upload(&query.0, auth_config, body_size_hint) {
                Err(error.to_response())
            } else {
                Ok(())
            }
        }
        Err(code) => Err(code.to_response()),
    }
}

/// Type representing the possible errors that can occur during the authentication process.
#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = PUBLISHER_AUTH_DOMAIN)]
pub enum PublisherAuthError {
    /// The expiration in the query does not match the token.
    #[error("the expiration in the query does not match the token")]
    #[rest_api_error(reason = "INVALID_EXPIRATION", status = ApiStatusCode::FailedPrecondition)]
    InvalidExpiration,

    /// The epochs field in the query does not match the token.
    #[error("the epochs field in the query does not match the token")]
    #[rest_api_error(reason = "INVALID_EPOCHS", status = ApiStatusCode::FailedPrecondition)]
    InvalidEpochs,

    /// Epochs is above the maximum allowed.
    #[error("the epochs field in the query is above the maximum allowed")]
    #[rest_api_error(reason = "EPOCHS_ABOVE_MAX", status = ApiStatusCode::FailedPrecondition)]
    EpochsAboveMax,

    /// The send_object_to field in the query does not match the token, or is missing.
    #[error("the send_object_to field in the query does not match the token, or is missing")]
    #[rest_api_error(reason = "INVALID_SEND_OBJECT_TO", status = ApiStatusCode::FailedPrecondition)]
    InvalidSendObjectTo,

    /// The send_object_to field is missing from the query, but it is required.
    #[error("the send_object_to field is missing from the query, but it is required")]
    #[rest_api_error(reason = "MISSING_SEND_OBJECT_TO", status = ApiStatusCode::FailedPrecondition)]
    MissingSendObjectTo,

    /// The size of the body is above the maximum allowed.
    #[error("the size of the body is above the maximum allowed.")]
    #[rest_api_error(reason = "MAX_SIZE_EXCEEDED", status = ApiStatusCode::FailedPrecondition)]
    MaxSizeExceeded,

    /// The signature on the token has expired.
    #[error("the signature on the token has expired: {0}")]
    #[rest_api_error(reason = "EXPIRED_SIGNATURE", status = ApiStatusCode::DeadlineExceeded)]
    ExpiredSignature(JwtError),

    /// The signature on the token is invalid.
    #[error("the signature on the token is invalid: {0}")]
    #[rest_api_error(reason = "INVALID_SIGNATURE", status = ApiStatusCode::Unauthenticated)]
    InvalidSignature(JwtError),

    /// The JWT token is invalid.
    #[error("the JWT token is invalid: {0}")]
    #[rest_api_error(reason = "INVALID_TOKEN", status = ApiStatusCode::FailedPrecondition)]
    InvalidToken(JwtError),

    /// Other errors that are not covered by the other variants.
    #[error("an internal error occurred")]
    #[rest_api_error(delegate)]
    Internal(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use rand::distributions::{Alphanumeric, DistString};
    use ring::signature::{self, Ed25519KeyPair, KeyPair};
    use sui_types::base_types::SUI_ADDRESS_LENGTH;
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;
    use crate::client::{config::AuthConfig, daemon::auth_layer};

    // Fixtures and helpers for tests.

    const ADDRESS: [u8; SUI_ADDRESS_LENGTH] = [42; SUI_ADDRESS_LENGTH];
    const OTHER_ADDRESS: &str =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    fn auth_config_for_tests(
        secret: Option<&str>,
        algorithm: Option<Algorithm>,
        expiring_sec: u64,
        verify_upload: bool,
    ) -> AuthConfig {
        let mut config = AuthConfig {
            decoding_key: None,
            algorithm,
            expiring_sec,
            verify_upload,
        };

        if let Some(secret) = secret {
            config.with_key_from_str(secret).unwrap();
        }

        config
    }

    fn setup_router_and_token(
        expiring_sec: u64,
        verify_upload: bool,
        use_secret: bool,
        claim: Claim,
    ) -> (Router, String, EncodingKey) {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let secret_to_use = use_secret.then_some(secret.as_str());
        let auth_config = auth_config_for_tests(secret_to_use, None, expiring_sec, verify_upload);

        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
            Arc::new(auth_config),
            auth_layer,
        ));

        let router =
            Router::new().route("/v1/blobs", get(|| async {}).route_layer(publisher_layers));
        (router, token, encode_key)
    }

    /// A helper to build requests
    struct RequestHeadersAndData {
        uri: String,
        auth_header: Option<(String, String)>,
        body: Body,
    }

    impl RequestHeadersAndData {
        fn new(uri: &str, auth_header: Option<(String, String)>, body: Option<Body>) -> Self {
            Self {
                uri: uri.to_string(),
                auth_header,
                body: body.unwrap_or_else(Body::empty),
            }
        }

        fn into_request(self) -> Request<Body> {
            let mut request = Request::builder().uri(self.uri);

            if let Some((key, value)) = self.auth_header {
                request = request.header(key, value);
            }

            request.body(self.body).unwrap()
        }
    }

    async fn execute_requests(router: &Router, requests: Vec<(RequestHeadersAndData, StatusCode)>) {
        for (request_data, expected_status) in requests {
            let request = request_data.into_request();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected_status);
        }
    }

    fn correct_auth_header(token: String) -> Option<(String, String)> {
        Some(("authorization".to_string(), format!("Bearer {}", token)))
    }

    // Test bodies.

    #[tokio::test]
    async fn auth_layer_is_working() {
        let (router, token, _) = setup_router_and_token(
            // Expiring sec.
            0,
            // Verify upload.
            false,
            // Use secret.
            true,
            // Claim.
            Claim {
                iat: None,
                exp: u64::MAX,
                ..Default::default()
            },
        );

        let requests = vec![
            (
                // No token supplied.
                RequestHeadersAndData::new("/v1/blobs", None, None),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    "/v1/blobs",
                    // Incorrectly formatted auth header.
                    Some(("authorization".to_owned(), token.clone())),
                    None,
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new("/v1/blobs", correct_auth_header(token), None),
                StatusCode::OK,
            ),
        ];

        execute_requests(&router, requests).await;
    }

    #[tokio::test]
    async fn verify_upload() {
        let (router, token, _) = setup_router_and_token(
            // Expiring sec.
            0,
            // Verify upload.
            true,
            // Use secret.
            true,
            // Claim.
            Claim {
                iat: None,
                exp: u64::MAX,
                send_object_to: Some(SuiAddress::from_bytes(ADDRESS).expect("valid address")),
                epochs: Some(1),
                ..Default::default()
            },
        );

        let requests = vec![
            (
                // Epochs is too high.
                RequestHeadersAndData::new(
                    "/v1/blobs?epochs=100",
                    correct_auth_header(token.clone()),
                    None,
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    // The send_object_to field is does not match.
                    &format!("/v1/blobs?epochs=1&send_object_to={}", OTHER_ADDRESS),
                    correct_auth_header(token.clone()),
                    None,
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    // Valid epochs and send_object_to.
                    &format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ),
                    correct_auth_header(token),
                    None,
                ),
                StatusCode::OK,
            ),
        ];

        execute_requests(&router, requests).await;
    }

    #[tokio::test]
    async fn verify_upload_skip_check_signature() {
        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            send_object_to: Some(SuiAddress::from_bytes(ADDRESS).expect("valid address")),
            epochs: Some(1),
            ..Default::default()
        };

        let (router, _, _) = setup_router_and_token(
            // Expiring sec.
            0,
            // Verify upload.
            true,
            // Use secret.
            false,
            claim.clone(),
        );

        // Replace the token with one signed with another key.
        // The signature is not checked, so the behavior should be the same as `verify_upload`.
        let encode_key = EncodingKey::from_secret(&[42; 32]);
        let token_invalid_sig = encode(&Header::default(), &claim, &encode_key).unwrap();

        let requests = vec![
            (
                RequestHeadersAndData::new(
                    "/v1/blobs?epochs=100",
                    correct_auth_header(token_invalid_sig.clone()),
                    None,
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    &format!("/v1/blobs?epochs=1&send_object_to={}", OTHER_ADDRESS),
                    correct_auth_header(token_invalid_sig.clone()),
                    None,
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    &format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ),
                    correct_auth_header(token_invalid_sig),
                    None,
                ),
                StatusCode::OK,
            ),
        ];

        execute_requests(&router, requests).await;
    }

    #[tokio::test]
    async fn verify_exp() {
        let valid_claim = Claim {
            iat: Some(0),
            exp: u64::MAX - 1,
            ..Default::default()
        };
        let (router, valid_token, encode_key) = setup_router_and_token(
            // Expiring sec.
            u64::MAX - 1,
            // Verify upload.
            false,
            // Use secret.
            true,
            valid_claim,
        );

        // Other two tokens, correctly authenticated but with expiry too long.
        let invalid_claim = Claim {
            iat: Some(0),
            exp: u64::MAX,
            ..Default::default()
        };
        let invalid_claim_2 = Claim {
            iat: None,
            exp: u64::MAX,
            ..Default::default()
        };
        let invalid_token = encode(&Header::default(), &invalid_claim, &encode_key).unwrap();
        let invalid_token_2 = encode(&Header::default(), &invalid_claim_2, &encode_key).unwrap();

        let requests = vec![
            (
                RequestHeadersAndData::new("/v1/blobs", correct_auth_header(invalid_token), None),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new("/v1/blobs", correct_auth_header(invalid_token_2), None),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new("/v1/blobs", correct_auth_header(valid_token), None),
                StatusCode::OK,
            ),
        ];

        execute_requests(&router, requests).await;
    }

    #[tokio::test]
    async fn verify_body_size() {
        let (router, token, _) = setup_router_and_token(
            // Expiring sec.
            0,
            // Verify upload.
            true,
            // Use secret.
            true,
            // Claim.
            Claim {
                iat: None,
                exp: u64::MAX,
                send_object_to: Some(SuiAddress::from_bytes(ADDRESS).expect("valid address")),
                epochs: Some(1),
                max_size: Some(10),
                ..Default::default()
            },
        );

        let requests = vec![
            (
                RequestHeadersAndData::new(
                    &format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ),
                    correct_auth_header(token.clone()),
                    // Big body fails.
                    Some(Body::from(vec![42; 100])),
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    &format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ),
                    correct_auth_header(token.clone()),
                    // Small body is ok.
                    Some(Body::from(vec![42; 10])),
                ),
                StatusCode::OK,
            ),
            (
                RequestHeadersAndData::new(
                    &format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ),
                    correct_auth_header(token),
                    // No body is ok.
                    None,
                ),
                StatusCode::OK,
            ),
        ];

        execute_requests(&router, requests).await;
    }

    #[tokio::test]
    async fn eddsa_auth() {
        let doc =
            signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
        let public_key = pair.public_key().as_ref().to_vec();
        let secret = format!("0x{}", hex::encode(&public_key));

        let auth_config = auth_config_for_tests(
            Some(&secret),
            Some(jsonwebtoken::Algorithm::EdDSA),
            0,
            false,
        );

        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            ..Default::default()
        };
        let encode_key = EncodingKey::from_ed_der(doc.as_ref());
        let token = encode(
            &Header::new(jsonwebtoken::Algorithm::EdDSA),
            &claim,
            &encode_key,
        )
        .unwrap();

        let publisher_layers = ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
            Arc::new(auth_config.clone()),
            auth_layer,
        ));

        let router =
            Router::new().route("/v1/blobs", get(|| async {}).route_layer(publisher_layers));

        let requests = vec![
            (
                // No token supplied.
                RequestHeadersAndData::new("/v1/blobs", None, None),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new(
                    "/v1/blobs",
                    // Incorrectly formatted auth header.
                    Some(("authorization".to_owned(), token.clone())),
                    None,
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                RequestHeadersAndData::new("/v1/blobs", correct_auth_header(token), None),
                StatusCode::OK,
            ),
        ];

        execute_requests(&router, requests).await;
    }
}
