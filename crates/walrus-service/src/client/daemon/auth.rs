// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::Query,
    http::{Response, StatusCode},
};
use axum_extra::headers::{authorization::Bearer, Authorization};
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::Deserialize;
use sui_types::base_types::SuiAddress;
use tracing::error;

use super::routes::PublisherQuery;
use crate::client::config::AuthConfig;

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
    ) -> Result<Self, StatusCode> {
        let claim: Claim = decode(token, decoding_key, validation)
            .map_err(|err| {
                error!(
                    error = &err as &dyn std::error::Error,
                    "failed to convert token to claim"
                );
                match err.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                        StatusCode::from_u16(499).expect("status code is in a valid range")
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidSignature
                    | jsonwebtoken::errors::ErrorKind::InvalidAlgorithmName
                    | jsonwebtoken::errors::ErrorKind::InvalidIssuer
                    | jsonwebtoken::errors::ErrorKind::ImmatureSignature => {
                        StatusCode::UNAUTHORIZED
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidToken
                    | jsonwebtoken::errors::ErrorKind::InvalidAlgorithm
                    | jsonwebtoken::errors::ErrorKind::Base64(_)
                    | jsonwebtoken::errors::ErrorKind::Json(_)
                    | jsonwebtoken::errors::ErrorKind::Utf8(_) => StatusCode::BAD_REQUEST,
                    jsonwebtoken::errors::ErrorKind::MissingAlgorithm => {
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                    jsonwebtoken::errors::ErrorKind::Crypto(_) => StatusCode::SERVICE_UNAVAILABLE,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                }
            })?
            .claims;

        Ok(claim)
    }
}

pub(crate) fn verify_jwt_claim<B>(
    query: Query<PublisherQuery>,
    bearer: Authorization<Bearer>,
    auth_config: &AuthConfig,
) -> Result<(), Response<B>>
where
    B: Default,
{
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
            let mut valid_upload = true;
            if auth_config.expiring_sec > 0
                && (claim.exp - claim.iat.unwrap_or_default()) != auth_config.expiring_sec
            {
                error!(toker = bearer.token(), "token with invalid expiration");
                valid_upload = false;
            }
            // TODO(giac): We never actually check that the expiration date is in the future.
            // i.e., check that claim.exp > SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)

            if auth_config.verify_upload {
                if let Some(epochs) = claim.epochs {
                    if query.epochs != epochs {
                        tracing::debug!(
                            expected = claim.epochs,
                            actual = query.epochs,
                            "upload with invalid epochs"
                        );
                        valid_upload = false;
                    }
                }

                match (claim.send_object_to, query.send_object_to) {
                    (Some(expected), Some(actual)) if expected != actual => {
                        tracing::debug!(
                            expected = %expected,
                            actual = %actual,
                            "upload with invalid send_object_to field"
                        );
                        valid_upload = false;
                    }
                    (Some(expected), None) => {
                        tracing::debug!(
                            expected = %expected,
                            "send_object_to field is missing"
                        );

                        valid_upload = false
                    }
                    _ => {}
                }
            }
            if valid_upload {
                Ok(())
            } else {
                validation_failed_response(StatusCode::PRECONDITION_FAILED)
            }
        }
        Err(code) => validation_failed_response(code),
    }
}

fn validation_failed_response<B>(code: StatusCode) -> Result<(), Response<B>>
where
    B: Default,
{
    Err(Response::builder()
        .status(code)
        .body(Default::default())
        .expect("Response is valid without any customized headers"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{http::Request, routing::get, Router};
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

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

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

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

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

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

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

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

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

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

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

        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

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
