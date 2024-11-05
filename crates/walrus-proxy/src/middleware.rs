// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::sync::Arc;

use anyhow::Error;
use axum::{
    async_trait,
    body::{Body, Bytes},
    extract::{Extension, FromRequest},
    http::{header, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use axum_extra::{
    headers::{ContentLength, ContentType},
    typed_header::TypedHeader,
};
use base64::Engine;
use bytes::Buf;
use fastcrypto::{
    secp256r1,
    traits::{EncodeDecodeBase64, RecoverableSignature},
};
use hyper::header::CONTENT_ENCODING;
use once_cell::sync::Lazy;
use prometheus::{proto::MetricFamily, CounterVec, Opts};
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

use crate::{consumer::ProtobufDecoder, providers::WalrusNodeProvider, register_metric};

static MIDDLEWARE_OPS: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(CounterVec::new(
        Opts::new(
            "middleware_operations",
            "Operations counters and status for axum middleware.",
        ),
        &["operation", "status"]
    )
    .unwrap())
});

static MIDDLEWARE_HEADERS: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(CounterVec::new(
        Opts::new(
            "middleware_headers",
            "Operations counters and status for axum middleware.",
        ),
        &["header", "value"]
    )
    .unwrap())
});

/// we expect sui-node to send us an http header content-length encoding.
pub async fn expect_content_length(
    TypedHeader(content_length): TypedHeader<ContentLength>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    MIDDLEWARE_HEADERS.with_label_values(&["content-length", &format!("{}", content_length.0)]);
    Ok(next.run(request).await)
}

/// we expect sui-node to send us an http header content-type encoding.
pub async fn expect_mysten_proxy_header(
    TypedHeader(content_type): TypedHeader<ContentType>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    match format!("{content_type}").as_str() {
        prometheus::PROTOBUF_FORMAT => Ok(next.run(request).await),
        ct => {
            error!("invalid content-type; {ct}");
            MIDDLEWARE_OPS
                .with_label_values(&["expect_mysten_proxy_header", "invalid-content-type"])
                .inc();
            Err((StatusCode::BAD_REQUEST, "invalid content-type header"))
        }
    }
}

/// AuthInfo is an intermediate type used to decode the auth header
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthInfo {
    /// base64 encoded via fastcrypto
    pub signature: String,
    /// base64 encoded via fastcrypto
    pub message: String,
}

impl AuthInfo {
    /// recover will attempt to recover the pub key from a signature contained
    /// in self
    pub fn recover(&self) -> Result<secp256r1::Secp256r1PublicKey, Error> {
        let recovered_signature =
            secp256r1::recoverable::Secp256r1RecoverableSignature::decode_base64(&self.signature)
                .map_err(|e| anyhow::anyhow!(e))?;
        let uid = Uuid::parse_str(&self.message).map_err(|e| {
            error!("failed parsing uuid sent from client!");
            dbg!(e)
        })?;
        let recovered_pub_key = recovered_signature.recover(uid.as_bytes())?;
        Ok(recovered_pub_key)
    }
}

/// we expect that calling walrus-nodes are known on the blockchain
pub async fn expect_valid_recoverable_pubkey(
    Extension(allower): Extension<Arc<WalrusNodeProvider>>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    MIDDLEWARE_OPS
        .with_label_values(&["expect_valid_recoverable_pubkey", "begin-auth-verify"])
        .inc();
    // Extract the Authorization header
    let Some(auth_header) = request.headers().get(header::AUTHORIZATION) else {
        MIDDLEWARE_OPS
            .with_label_values(&[
                "expect_valid_recoverable_pubkey",
                "walrus-node-missing-auth-header",
            ])
            .inc();
        return Err((StatusCode::FORBIDDEN, "authorization header is required"));
    };

    let auth_header_value_with_scheme = auth_header
        .to_str()
        .map_err(|_| (StatusCode::FORBIDDEN, "authorization header is malformed"))?;

    // split the scheme from our b64 data, delimited by ': '
    let (_scheme, base64_data) = auth_header_value_with_scheme
        .split_once(": ")
        .unwrap_or_default();

    let decoded_data = base64::prelude::BASE64_STANDARD
        .decode(base64_data)
        .map_err(|_| (StatusCode::FORBIDDEN, "authorization header is malformed"))?;

    let auth_info: AuthInfo = serde_json::from_slice(&decoded_data)
        .map_err(|_| (StatusCode::FORBIDDEN, "authorization header is malformed"))?;

    let Some(peer) = allower.get(&auth_info.recover().map_err(|e| {
        error!("allower failed us; {}", e);
        (StatusCode::FORBIDDEN, "authorization header is malformed")
    })?) else {
        // no errors from allower, we just didn't match
        MIDDLEWARE_OPS
            .with_label_values(&[
                "expect_valid_recoverable_pubkey",
                "unknown-walrus-node-connection-attempt",
            ])
            .inc();
        return Err((StatusCode::FORBIDDEN, "unknown clients are not allowed"));
    };
    request.extensions_mut().insert(peer);
    MIDDLEWARE_OPS
        .with_label_values(&["expect_valid_recoverable_pubkey", "end-auth-verify"])
        .inc();
    Ok(next.run(request).await)
}

/// LenDelimProtobuf is an axum extractor that will consume protobuf content by
/// decompressing it and decoding it into protobuf metrics
#[derive(Debug)]
pub struct LenDelimProtobuf(pub Vec<MetricFamily>);

#[async_trait]
impl<S> FromRequest<S> for LenDelimProtobuf
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request(
        req: Request<axum::body::Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let should_be_snappy = req
            .headers()
            .get(CONTENT_ENCODING)
            .map(|v| v.as_bytes() == b"snappy")
            .unwrap_or(false);

        let body = Bytes::from_request(req, state).await.map_err(|e| {
            let msg = format!("error extracting bytes; {e}");
            error!(msg);
            MIDDLEWARE_OPS
                .with_label_values(&["LenDelimProtobuf_from_request", "unable-to-extract-bytes"])
                .inc();
            (e.status(), msg)
        })?;

        let intermediate = if should_be_snappy {
            let mut s = snap::raw::Decoder::new();
            let decompressed = s.decompress_vec(&body).map_err(|e| {
                let msg = format!("unable to decode snappy encoded protobufs; {e}");
                error!(msg);
                MIDDLEWARE_OPS
                    .with_label_values(&[
                        "LenDelimProtobuf_decompress_vec",
                        "unable-to-decode-snappy",
                    ])
                    .inc();
                (StatusCode::BAD_REQUEST, msg)
            })?;
            Bytes::from(decompressed).reader()
        } else {
            body.reader()
        };

        let mut decoder = ProtobufDecoder::new(intermediate);
        let decoded = decoder.parse::<MetricFamily>().map_err(|e| {
            let msg = format!("unable to decode len delimited protobufs; {e}");
            error!(msg);
            MIDDLEWARE_OPS
                .with_label_values(&[
                    "LenDelimProtobuf_from_request",
                    "unable-to-decode-protobufs",
                ])
                .inc();
            (StatusCode::BAD_REQUEST, msg)
        })?;
        Ok(Self(decoded))
    }
}
