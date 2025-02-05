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
use serde::Deserialize;
use sui_types::base_types::SuiAddress;
use tracing::error;
use walrus_proc_macros::RestApiError;

use super::routes::PublisherQuery;
use crate::{client::config::AuthConfig, common::api::RestApiError};

pub const PUBLISHER_AUTH_DOMAIN: &str = "auth.publisher.walrus.space";

/// Claim follows RFC7519 with extra storage parameters: send_object_to, epochs.
#[derive(Clone, Deserialize, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Claim {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Token is issued at (timestamp).
    pub iat: Option<u64>,

    /// Token expires at (timestamp).
    pub exp: u64,

    /// The owner address of the sui blob object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_object_to: Option<SuiAddress>,

    /// The number of epochs the blob should be stored for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epochs: Option<u32>,
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
}

pub(crate) fn verify_jwt_claim(
    query: Query<PublisherQuery>,
    bearer: Authorization<Bearer>,
    auth_config: &AuthConfig,
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
            let mut publisher_auth_error = None;

            if auth_config.expiring_sec > 0
                && (claim.exp - claim.iat.unwrap_or_default()) != auth_config.expiring_sec
            {
                error!(toker = bearer.token(), "token with invalid expiration");
                publisher_auth_error = Some(PublisherAuthError::InvalidExpiration);
            }

            if auth_config.verify_upload {
                if let Some(epochs) = claim.epochs {
                    if query.epochs != epochs {
                        tracing::debug!(
                            expected = claim.epochs,
                            actual = query.epochs,
                            "upload with invalid epochs"
                        );
                        publisher_auth_error = Some(PublisherAuthError::InvalidEpochs);
                    }
                }

                match (claim.send_object_to, query.send_object_to) {
                    (Some(expected), Some(actual)) if expected != actual => {
                        tracing::debug!(
                            expected = %expected,
                            actual = %actual,
                            "upload with invalid send_object_to field"
                        );
                        publisher_auth_error = Some(PublisherAuthError::InvalidSendObjectTo);
                    }
                    (Some(expected), None) => {
                        tracing::debug!(
                            expected = %expected,
                            "send_object_to field is missing"
                        );

                        publisher_auth_error = Some(PublisherAuthError::MissingSendObjectTo);
                    }
                    _ => {}
                }
            }

            if let Some(publisher_auth_error) = publisher_auth_error {
                Err(publisher_auth_error.to_response())
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
enum PublisherAuthError {
    /// The expiration in the query does not match the token.
    #[error("the expiration in the query does not match the token")]
    #[rest_api_error(reason = "INVALID_EXPIRATION", status = ApiStatusCode::FailedPrecondition)]
    InvalidExpiration,

    /// The epochs field in the query does not match the token.
    #[error("the epochs field in the query does not match the token")]
    #[rest_api_error(reason = "INVALID_EPOCHS", status = ApiStatusCode::FailedPrecondition)]
    InvalidEpochs,

    /// The send_object_to field in the query does not match the token, or is missing.
    #[error("the send_object_to field in the query does not match the token, or is missing")]
    #[rest_api_error(reason = "INVALID_SEND_OBJECT_TO", status = ApiStatusCode::FailedPrecondition)]
    InvalidSendObjectTo,

    /// The send_object_to field is missing from the query, but it is required.
    #[error("the send_object_to field is missing from the query, but it is required")]
    #[rest_api_error(reason = "MISSING_SEND_OBJECT_TO", status = ApiStatusCode::FailedPrecondition)]
    MissingSendObjectTo,

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
    use http_body_util::Empty;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use rand::distributions::{Alphanumeric, DistString};
    use ring::signature::{self, Ed25519KeyPair, KeyPair};
    use sui_types::base_types::SUI_ADDRESS_LENGTH;
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;
    use crate::client::{config::AuthConfig, daemon::auth_layer};

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

    #[tokio::test]
    async fn auth_layer_is_working() {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let auth_config = auth_config_for_tests(Some(&secret), None, 0, false);

        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            send_object_to: None,
            epochs: None,
        };
        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
            Arc::new(auth_config),
            auth_layer,
        ));

        let router = Router::new().route("/", get(|| async {}).route_layer(publisher_layers));

        // Test token missing
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/").body(Empty::new()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Invalid Test bearer missing
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("authorization", token.clone())
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test valid
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_upload() {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let auth_config = auth_config_for_tests(Some(&secret), None, 0, true);

        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            send_object_to: Some(SuiAddress::from_bytes(ADDRESS).expect("valid address")),
            epochs: Some(1),
        };
        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
            Arc::new(auth_config),
            auth_layer,
        ));

        let router =
            Router::new().route("/v1/blobs", get(|| async {}).route_layer(publisher_layers));

        // Test invalid epoch
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/blobs?epochs=100")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test invalid address
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        OTHER_ADDRESS
                    ))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test valid
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_upload_skip_check_token() {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let auth_config = auth_config_for_tests(None, None, 0, true);

        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            send_object_to: Some(SuiAddress::from_bytes(ADDRESS).expect("valid address")),
            epochs: Some(1),
        };
        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
            Arc::new(auth_config.clone()),
            auth_layer,
        ));

        let router =
            Router::new().route("/v1/blobs", get(|| async {}).route_layer(publisher_layers));

        // Test invalid epoch
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/blobs?epochs=100")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test invalid address
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        OTHER_ADDRESS
                    ))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test valid
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/blobs?epochs=1&send_object_to={}",
                        SuiAddress::from_bytes(ADDRESS).expect("valid address")
                    ))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_exp() {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let auth_config = auth_config_for_tests(Some(&secret), None, u64::MAX - 1, false);

        let valid_claim = Claim {
            iat: Some(0),
            exp: u64::MAX - 1,
            send_object_to: None,
            epochs: None,
        };
        let invalid_claim = Claim {
            iat: Some(0),
            exp: u64::MAX,
            send_object_to: None,
            epochs: None,
        };
        let invalid_claim2 = Claim {
            iat: None,
            exp: u64::MAX,
            send_object_to: None,
            epochs: None,
        };

        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let valid_token = encode(&Header::default(), &valid_claim, &encode_key).unwrap();
        let invalid_token = encode(&Header::default(), &invalid_claim, &encode_key).unwrap();
        let invalid_token2 = encode(&Header::default(), &invalid_claim2, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
            Arc::new(auth_config.clone()),
            auth_layer,
        ));

        let router =
            Router::new().route("/v1/blobs", get(|| async {}).route_layer(publisher_layers));

        // Test invalid token
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/blobs")
                    .header("authorization", format!("Bearer {invalid_token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test invalid token
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/blobs")
                    .header("authorization", format!("Bearer {invalid_token2}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test valid token
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/blobs")
                    .header("authorization", format!("Bearer {valid_token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
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
            send_object_to: None,
            epochs: None,
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

        let router = Router::new().route("/", get(|| async {}).route_layer(publisher_layers));

        // Test token missing
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/").body(Empty::new()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Invalid Test bearer missing
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("authorization", token.clone())
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Test valid
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
