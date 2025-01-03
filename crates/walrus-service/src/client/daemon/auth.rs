// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, future::Future, marker::PhantomData, pin::Pin, task::Poll};

use axum::http::{Request, Response, StatusCode};
use axum_extra::headers::{authorization::Bearer, Authorization, HeaderMapExt};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use pin_project::pin_project;
use serde::Deserialize;
use tower::{Layer, Service};
use tracing::error;

use crate::client::config::AuthConfig;

/// Claim follow RFC7519 with extra storage parameters: address, epoch
#[derive(Clone, Deserialize, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Claim {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Token is issued at (timestamp)
    pub iat: Option<u64>,

    /// Token is expired at (timestamp)
    pub exp: u64,

    /// Address is the sui blob object owner
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,

    /// Epoch is the storage time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u32>,
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
                        StatusCode::from_u16(499).unwrap()
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

#[derive(Clone)]
pub struct JwtLayer {
    auth_config: AuthConfig,
    _phantom: PhantomData<Claim>,
}

impl JwtLayer {
    pub fn new(auth_config: AuthConfig) -> Self {
        Self {
            auth_config,
            _phantom: PhantomData,
        }
    }
}

impl<S> Layer<S> for JwtLayer {
    type Service = Jwt<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Jwt {
            inner,
            auth_config: Box::new(self.auth_config.clone()),
            _phantom: self._phantom,
        }
    }
}

/// Middleware for validating a valid JWT token is present on "authorization: bearer <token>"
#[derive(Clone)]
pub struct Jwt<S> {
    inner: S,
    auth_config: Box<AuthConfig>,
    _phantom: PhantomData<Claim>,
}

#[pin_project(project = JwtFutureProj, project_replace = JwtFutureProjOwn)]
pub enum JwtFuture<
    TService: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ReqBody,
    ResBody,
> {
    ValidateError(StatusCode),
    WaitForFuture {
        #[pin]
        future: TService::Future,
    },
}

impl<TService, ReqBody, ResBody> Future for JwtFuture<TService, ReqBody, ResBody>
where
    TService: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: Default,
    for<'de> Claim: Deserialize<'de> + Send + Sync + Clone + 'static,
{
    type Output = Result<TService::Response, TService::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().project() {
            JwtFutureProj::ValidateError(code) => {
                let response = Response::builder()
                    .status(*code)
                    .body(Default::default())
                    .unwrap();
                Poll::Ready(Ok(response))
            }
            JwtFutureProj::WaitForFuture { future } => future.poll(cx),
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for Jwt<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Send + Clone + 'static,
    S::Future: Send + 'static,
    ResBody: Default,
    for<'de> Claim: Deserialize<'de> + Send + Sync + Clone + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = JwtFuture<S, ReqBody, ResBody>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        match req.headers().typed_get::<Authorization<Bearer>>() {
            Some(bearer) => {
                let mut validation = if self.auth_config.secret.is_some() {
                    self.auth_config
                        .algorithm
                        .map(Validation::new)
                        .unwrap_or_default()
                } else {
                    Validation::default()
                };

                let decode_key = if let Some(secret) = &self.auth_config.secret {
                    match self.auth_config.algorithm {
                        None
                        | Some(Algorithm::HS256)
                        | Some(Algorithm::HS384)
                        | Some(Algorithm::HS512) => DecodingKey::from_secret(secret),
                        Some(Algorithm::EdDSA) => DecodingKey::from_ed_der(secret),
                        Some(Algorithm::ES256) | Some(Algorithm::ES384) => {
                            DecodingKey::from_ec_der(secret)
                        }
                        Some(Algorithm::RS256)
                        | Some(Algorithm::RS384)
                        | Some(Algorithm::RS512)
                        | Some(Algorithm::PS256)
                        | Some(Algorithm::PS384)
                        | Some(Algorithm::PS512) => DecodingKey::from_rsa_der(secret),
                    }
                } else {
                    validation.insecure_disable_signature_validation();
                    DecodingKey::from_secret(&[])
                };

                if self.auth_config.expiring_sec > 0 {
                    validation.set_required_spec_claims(&["exp", "iat"]);
                }

                match Claim::from_token(bearer.token().trim(), &decode_key, &validation) {
                    Ok(claim) => {
                        let mut valid_upload = true;
                        if self.auth_config.expiring_sec > 0
                            && (claim.exp - claim.iat.unwrap_or_default())
                                != self.auth_config.expiring_sec
                        {
                            error!("invalid expiring token: {}", bearer.token());
                            valid_upload = false;
                        }
                        if self.auth_config.verify_upload {
                            let query = req.uri().query();
                            if let Some(epoch) = claim.epoch {
                                if !check_query(query, "epochs", epoch.to_string()) {
                                    error!("upload with invalid epoch: {}", epoch);
                                    valid_upload = false;
                                }
                            }
                            if let Some(address) = claim.address {
                                if !check_query(query, "send_object_to", &address) {
                                    error!("upload to an invalid address: {}", address);
                                    valid_upload = false;
                                }
                            }
                        }
                        if valid_upload {
                            let future = self.inner.call(req);
                            Self::Future::WaitForFuture { future }
                        } else {
                            Self::Future::ValidateError(StatusCode::PRECONDITION_FAILED)
                        }
                    }
                    Err(code) => Self::Future::ValidateError(code),
                }
            }
            None => Self::Future::ValidateError(StatusCode::UNAUTHORIZED),
        }
    }
}

fn check_query(queries: Option<&str>, field: &str, value: impl Display) -> bool {
    if let Some(queries) = queries {
        for q in queries.split('&') {
            if q.starts_with(field) && q != format!("{field:}={value:}") {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use axum::{routing::get, Router};
    use http_body_util::Empty;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rand::distributions::{Alphanumeric, DistString};
    use ring::signature::{self, Ed25519KeyPair, KeyPair};
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;
    use crate::client::config::AuthConfig;

    #[test]
    fn query() {
        let query_example: &'static str = "epochs=100&send_object_to=0x1";
        assert!(check_query(Some(query_example), "epochs", 100));
        assert!(check_query(Some(query_example), "send_object_to", "0x1"));
        assert!(!check_query(Some(query_example), "epochs", 1));
        assert!(!check_query(Some(query_example), "send_object_to", "0x9"));

        let no_sender_example: &'static str = "epochs=100";
        assert!(check_query(Some(no_sender_example), "epochs", 100));
        assert!(check_query(
            Some(no_sender_example),
            "send_object_to",
            "0x1"
        ));
        assert!(!check_query(Some(no_sender_example), "epochs", 1));

        let empty_example: &'static str = "";
        assert!(check_query(Some(empty_example), "epochs", 100));
        assert!(check_query(Some(empty_example), "send_object_to", "0x1"));
    }

    #[tokio::test]
    async fn auth_layer() {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let auth_config = AuthConfig {
            secret: Some(secret.as_str().into()),
            algorithm: None,
            expiring_sec: 0,
            verify_upload: false,
        };
        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            address: None,
            epoch: None,
        };
        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(JwtLayer::new(auth_config));

        let router = Router::new().route("/", get(|| async {}).route_layer(publisher_layers));

        // Test token missing
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/").body(Empty::new()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

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

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

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
        let auth_config = AuthConfig {
            secret: Some(secret.as_str().into()),
            algorithm: None,
            expiring_sec: 0,
            verify_upload: true,
        };
        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            address: Some("0x1".to_string()),
            epoch: Some(1),
        };
        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(JwtLayer::new(auth_config));

        let router =
            Router::new().route("/v1/store", get(|| async {}).route_layer(publisher_layers));

        // Test invalid epoch
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/store?epochs=100")
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
                    .uri("/v1/store?epochs=1&send_object_to=0x2")
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
                    .uri("/v1/store?epochs=1&send_object_to=0x1")
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
        let auth_config = AuthConfig {
            secret: None, // No secret, and skip verify token
            algorithm: None,
            expiring_sec: 0,
            verify_upload: true,
        };
        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            address: Some("0x1".to_string()),
            epoch: Some(1),
        };
        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let token = encode(&Header::default(), &claim, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(JwtLayer::new(auth_config));

        let router =
            Router::new().route("/v1/store", get(|| async {}).route_layer(publisher_layers));

        // Test invalid epoch
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/store?epochs=100")
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
                    .uri("/v1/store?epochs=1&send_object_to=0x2")
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
                    .uri("/v1/store?epochs=1&send_object_to=0x1")
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
        let auth_config = AuthConfig {
            secret: Some(secret.as_str().into()),
            algorithm: None,
            expiring_sec: u64::MAX - 1,
            verify_upload: false,
        };
        let valid_claim = Claim {
            iat: Some(0),
            exp: u64::MAX - 1,
            address: None,
            epoch: None,
        };
        let invalid_claim = Claim {
            iat: Some(0),
            exp: u64::MAX,
            address: None,
            epoch: None,
        };
        let invalid_claim2 = Claim {
            iat: None,
            exp: u64::MAX,
            address: None,
            epoch: None,
        };

        let encode_key = EncodingKey::from_secret(secret.as_bytes());
        let valid_token = encode(&Header::default(), &valid_claim, &encode_key).unwrap();
        let invalid_token = encode(&Header::default(), &invalid_claim, &encode_key).unwrap();
        let invalid_token2 = encode(&Header::default(), &invalid_claim2, &encode_key).unwrap();

        let publisher_layers = ServiceBuilder::new().layer(JwtLayer::new(auth_config));

        let router =
            Router::new().route("/v1/store", get(|| async {}).route_layer(publisher_layers));

        // Test invalid token
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/store")
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
                    .uri("/v1/store")
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
                    .uri("/v1/store")
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
        let mut auth_config = AuthConfig::new(secret).unwrap();
        auth_config.algorithm = Some(jsonwebtoken::Algorithm::EdDSA);

        let claim = Claim {
            iat: None,
            exp: u64::MAX,
            address: None,
            epoch: None,
        };
        let encode_key = EncodingKey::from_ed_der(doc.as_ref());
        let token = encode(
            &Header::new(jsonwebtoken::Algorithm::EdDSA),
            &claim,
            &encode_key,
        )
        .unwrap();

        let publisher_layers = ServiceBuilder::new().layer(JwtLayer::new(auth_config));

        let router = Router::new().route("/", get(|| async {}).route_layer(publisher_layers));

        // Test token missing
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/").body(Empty::new()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

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

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

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
