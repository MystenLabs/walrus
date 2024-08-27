// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{
    async_trait,
    body::Bytes,
    extract::{rejection::BytesRejection, FromRequest, FromRequestParts, Request},
    http::{header, request::Parts, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use reqwest::header::AUTHORIZATION;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use walrus_sdk::error::ServiceError;

use crate::common::api::{RestApiError, RestApiJsonError};

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

impl RestApiError for BcsRejection {
    fn status(&self) -> StatusCode {
        match self {
            BcsRejection::UnsupportedContentType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            BcsRejection::BytesRejection(rejection) => rejection.status(),
            BcsRejection::DecodeError(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn body_text(&self) -> String {
        self.to_string()
    }

    fn service_error(&self) -> Option<ServiceError> {
        None
    }
}

impl IntoResponse for BcsRejection {
    fn into_response(self) -> axum::response::Response {
        self.to_response()
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
            Err(error) => {
                tracing::error!(
                    ?error,
                    "failed to BCS encode an internal response type to the user"
                );

                RestApiJsonError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    StatusCode::INTERNAL_SERVER_ERROR
                        .canonical_reason()
                        .unwrap(),
                    None,
                )
                .into_response()
            }
        }
    }
}

// The following code is used to extract the Authorization header from a request.
#[derive(Debug, Clone, Default, Deserialize)]
#[must_use]
pub struct Authorization(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for Authorization
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts.headers.get(AUTHORIZATION);

        if auth_header.is_none() {
            return Err((StatusCode::UNAUTHORIZED, "Missing Authorization header"));
        }

        let key_bytes = auth_header.unwrap().to_str();
        if key_bytes.is_err() {
            return Err((StatusCode::BAD_REQUEST, "Invalid Authorization header"));
        }

        Ok(Authorization(key_bytes.unwrap().to_string()))
    }
}
