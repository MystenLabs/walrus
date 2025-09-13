// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
use std::{collections::HashMap, sync::Arc};

use anyhow::Error;
use axum::{
    body::{Body, Bytes},
    extract::{Extension, FromRequest},
    http::{Request, StatusCode, header},
    middleware::Next,
    response::Response,
};
use axum_extra::{headers::ContentLength, typed_header::TypedHeader};
use base64::Engine;
use bytes::Buf;
use fastcrypto::{
    secp256r1,
    traits::{EncodeDecodeBase64, RecoverableSignature},
};
use hyper::header::CONTENT_ENCODING;
use once_cell::sync::Lazy;
use prometheus::{CounterVec, Opts, proto::MetricFamily};
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

use crate::{
    consumer::{Label, ProtobufDecoder},
    providers::WalrusNodeProvider,
    register_metric,
};

static MIDDLEWARE_OPS: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(
        CounterVec::new(
            Opts::new(
                "middleware_operations",
                "Operations counters and status for axum middleware.",
            ),
            &["operation", "status"]
        )
        .expect("metric creation cannot fail with valid parameters")
    )
});

/// we expect walrus-node to send us an http header content-length encoding.
pub async fn expect_content_length(
    TypedHeader(_content_length): TypedHeader<ContentLength>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    Ok(next.run(request).await)
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
    walrus_utils::with_label!(
        MIDDLEWARE_OPS,
        "expect_valid_recoverable_pubkey",
        "begin-auth-verify"
    )
    .inc();
    // Extract the Authorization header
    let Some(auth_header) = request.headers().get(header::AUTHORIZATION) else {
        walrus_utils::with_label!(
            MIDDLEWARE_OPS,
            "expect_valid_recoverable_pubkey",
            "walrus-node-missing-auth-header"
        )
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
        walrus_utils::with_label!(
            MIDDLEWARE_OPS,
            "expect_valid_recoverable_pubkey",
            "unknown-walrus-node-connection-attempt"
        )
        .inc();
        return Err((StatusCode::FORBIDDEN, "unknown clients are not allowed"));
    };
    request.extensions_mut().insert(peer);
    walrus_utils::with_label!(
        MIDDLEWARE_OPS,
        "expect_valid_recoverable_pubkey",
        "end-auth-verify"
    )
    .inc();
    Ok(next.run(request).await)
}

/// MetricFamilyWithStaticLabels takes labels that were signaled to us from the node as well
/// as their metrics and creates an axum Extension type param that can be used in middleware
#[derive(Debug)]
pub struct MetricFamilyWithStaticLabels {
    /// static labels defined in config, eg host, network, etc
    pub labels: Option<Vec<Label>>,
    /// the metrics the node sent us, decoded from protobufs
    pub metric_families: Vec<MetricFamily>,
}

/// LenDelimProtobuf is an axum extractor that will consume protobuf content by
/// decompressing it and decoding it into protobuf metrics. the body payload is
/// a json payload that is snappy compressed.  it has a structure seen in
/// MetricPayload.  The buf field is protobuf encoded `Vec<MetricFamily>`
#[derive(Debug)]
pub struct LenDelimProtobuf(pub MetricFamilyWithStaticLabels);

impl<S> FromRequest<S> for LenDelimProtobuf
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request(
        req: Request<axum::body::Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        req.headers()
            .get(CONTENT_ENCODING)
            .map(|v| v.as_bytes() == b"snappy")
            .unwrap_or(false)
            .then_some(())
            .ok_or((
                StatusCode::BAD_REQUEST,
                "snappy compression is required".into(),
            ))?;

        let body = Bytes::from_request(req, state).await.map_err(|e| {
            let msg = format!("error extracting bytes; {e}");
            error!(msg);
            walrus_utils::with_label!(
                MIDDLEWARE_OPS,
                "LenDelimProtobuf_from_request",
                "unable-to-extract-bytes"
            )
            .inc();
            (e.status(), msg)
        })?;

        let mut s = snap::raw::Decoder::new();
        let decompressed = s.decompress_vec(&body).map_err(|e| {
            let msg = format!("unable to decode snappy encoded protobufs; {e}");
            error!(msg);
            walrus_utils::with_label!(
                MIDDLEWARE_OPS,
                "LenDelimProtobuf_decompress_vec",
                "unable-to-decode-snappy"
            )
            .inc();
            (StatusCode::BAD_REQUEST, msg)
        })?;

        #[derive(Deserialize)]
        struct Payload {
            labels: Option<HashMap<String, String>>,
            buf: Vec<u8>,
        }
        let metric_payload: Payload = serde_json::from_slice(&decompressed).map_err(|error| {
            let msg = "unable to deserialize MetricPayload";
            error!(?error, msg);
            (StatusCode::BAD_REQUEST, msg.into())
        })?;

        let mut decoder = ProtobufDecoder::new(Bytes::from(metric_payload.buf).reader());
        let metric_families = decoder.parse::<MetricFamily>().map_err(|e| {
            let msg = format!("unable to decode len delimited protobufs; {e}");
            error!(msg);
            walrus_utils::with_label!(
                MIDDLEWARE_OPS,
                "LenDelimProtobuf_from_request",
                "unable-to-decode-protobufs"
            )
            .inc();
            (StatusCode::BAD_REQUEST, msg)
        })?;

        let labels: Option<Vec<Label>> = metric_payload.labels.map(|map| {
            map.into_iter()
                .map(|(k, v)| Label { name: k, value: v })
                .collect()
        });
        Ok(Self(MetricFamilyWithStaticLabels {
            labels,
            metric_families,
        }))
    }
}
