// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{
    async_trait,
    body::Bytes,
    extract::{rejection::BytesRejection, FromRequest, Request},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{de::DeserializeOwned, Serialize};

use crate::server::ServiceResponse;

#[derive(Debug, thiserror::Error)]
pub enum BcsRejection {
    #[error(
        "Expected request with `Content-Type: {}`",
        mime::APPLICATION_OCTET_STREAM
    )]
    UnsupportedContentType,
    #[error(transparent)]
    BytesRejection(#[from] BytesRejection),
    #[error("Unable to decode request body as BCS")]
    DecodeError(#[from] bcs::Error),
}

impl IntoResponse for BcsRejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            BcsRejection::UnsupportedContentType => {
                ServiceResponse::<()>::error(StatusCode::UNSUPPORTED_MEDIA_TYPE, self.to_string())
                    .into_response()
            }
            BcsRejection::DecodeError(_) => {
                ServiceResponse::<()>::error(StatusCode::BAD_REQUEST, self.to_string())
                    .into_response()
            }
            BcsRejection::BytesRejection(inner) => inner.into_response(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct Bcs<T>(pub T);

impl<T: DeserializeOwned> Bcs<T> {
    /// Construct a `Bcs<T>` from a byte slice. The `FromRequest` impl should be preferred, but
    /// special cases may require extracting a Request into Bytes then constructing a `Bcs<T>`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BcsRejection> {
        Ok(Bcs(bcs::from_bytes(bytes)?))
    }
}

#[async_trait]
impl<T, S> FromRequest<S> for Bcs<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = BcsRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if has_bcs_content_type(req.headers()) {
            let bytes = Bytes::from_request(req, state).await?;
            Self::from_bytes(&bytes)
        } else {
            Err(BcsRejection::UnsupportedContentType)
        }
    }
}

fn has_bcs_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        // No media-type is often just bytes.
        return true;
    };

    let Some(media_type) = content_type
        .to_str()
        .ok()
        .and_then(|s| s.parse::<mime::Mime>().ok())
    else {
        return false;
    };

    // Check the media type and subtype, but allow any params.
    media_type.type_() == mime::APPLICATION && media_type.subtype() == mime::OCTET_STREAM
}

impl<T> IntoResponse for Bcs<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // Use a small initial capacity of 128 bytes like serde_json::to_vec
        // https://docs.rs/serde_json/1.0.82/src/serde_json/ser.rs.html#2189
        let mut buf = Vec::with_capacity(128);
        match bcs::serialize_into(&mut buf, &self.0) {
            Ok(()) => (
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::APPLICATION_OCTET_STREAM.as_ref()),
                )],
                buf,
            )
                .into_response(),
            Err(err) => {
                tracing::error!(
                    ?err,
                    "failed to BCS encode an internal response type to the user"
                );
                ServiceResponse::<()>::internal_error().into_response()
            }
        }
    }
}
