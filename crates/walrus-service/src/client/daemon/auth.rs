// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, future::Future, marker::PhantomData, pin::Pin, task::Poll};

use axum::http::{Request, Response, StatusCode};
use axum_extra::headers::{authorization::Bearer, Authorization, HeaderMapExt};
use jsonwebtoken::{decode, DecodingKey, Validation};
use pin_project::pin_project;
use serde::Deserialize;
use tower::{Layer, Service};
use tracing::error;

use crate::client::config::AuthConfig;

/// Claim follow RFC7519 with extra storage parameters: address, epoch
#[derive(Clone, Deserialize, Debug)]
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
    Error,
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
            JwtFutureProj::Error => {
                let response = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Default::default())
                    .unwrap();
                Poll::Ready(Ok(response))
            }
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
        match req.headers().typed_try_get::<Authorization<Bearer>>() {
            Ok(Some(bearer)) => {
                let mut validation = Validation::default();
                let secret = if let Some(secret) = &self.auth_config.secret {
                    secret
                } else {
                    validation.insecure_disable_signature_validation();
                    ""
                };
                if self.auth_config.expiring_sec > 0 {
                    validation.set_required_spec_claims(&["exp", "iat"]);
                }
                match Claim::from_token(
                    bearer.token().trim(),
                    &DecodingKey::from_secret(secret.as_bytes()),
                    &validation,
                ) {
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
            Ok(None) => {
                let future = self.inner.call(req);
                Self::Future::WaitForFuture { future }
            }
            Err(_) => Self::Future::Error,
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
    use super::*;
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
}
