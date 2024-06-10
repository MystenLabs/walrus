// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use serde_json::json;
use utoipa::{
    openapi::{
        response::Response as OpenApiResponse,
        schema::{self, Schema},
        ContentBuilder,
        ObjectBuilder,
        RefOr,
        ResponseBuilder,
        ResponsesBuilder,
    },
    IntoResponses,
    PartialSchema,
    ToSchema,
};

use super::extract::BcsRejection;
use crate::node::{
    BlobStatusError,
    ComputeStorageConfirmationError,
    InconsistencyProofError,
    RetrieveMetadataError,
    RetrieveSliverError,
    RetrieveSymbolError,
    StoreMetadataError,
    StoreSliverError,
};

/// Successful API response body as JSON.
///
/// Contains the HTTP code as well as a message or response object.
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub(super) enum ApiSuccess<T> {
    Success { code: u16, data: T },
}

impl<T> ApiSuccess<T> {
    pub fn new(code: StatusCode, data: T) -> Self {
        Self::Success {
            code: code.as_u16(),
            data,
        }
    }

    pub fn ok(data: T) -> Self {
        Self::Success {
            code: StatusCode::OK.as_u16(),
            data,
        }
    }

    pub(super) fn schema_with_data(data: RefOr<Schema>) -> RefOr<Schema> {
        let object = ObjectBuilder::new()
            .property(
                "success",
                ObjectBuilder::new()
                    .property("code", <u16 as PartialSchema>::schema())
                    .property("data", data),
            )
            .build();

        object.into()
    }
}

impl<'s, T: ToSchema<'s>> PartialSchema for ApiSuccess<T> {
    fn schema() -> RefOr<Schema> {
        Self::schema_with_data(T::schema().1)
    }
}

impl<T: Serialize> IntoResponse for ApiSuccess<T> {
    fn into_response(self) -> Response {
        let Self::Success { ref code, .. } = self;
        (StatusCode::from_u16(*code).unwrap(), Json(self)).into_response()
    }
}

/// API response body for error responses as JSON.
///
/// Contains the HTTP code as well as the textual reason.
#[derive(ToSchema, Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RestApiJsonError<'a> {
    Error { code: u16, message: &'a str },
}

impl<'a> RestApiJsonError<'a> {
    pub fn new(code: StatusCode, message: &'a str) -> Self {
        Self::Error {
            code: code.as_u16(),
            message,
        }
    }
}

impl IntoResponse for RestApiJsonError<'_> {
    fn into_response(self) -> Response {
        let Self::Error { ref code, .. } = self;
        (StatusCode::from_u16(*code).unwrap(), Json(self)).into_response()
    }
}

/// Defines a trait over errors that can be converted into a [`RestApiJsonError`]-based response.
pub(crate) trait RestApiError: Sized {
    fn status(&self) -> StatusCode;
    fn body_text(&self) -> String;
    fn to_response(self) -> Response {
        RestApiJsonError::new(self.status(), &self.body_text()).into_response()
    }
}

macro_rules! rest_api_error {
    ($enum:ident: [
        $( ($variant:pat, $code:ident, $($desc:tt)*) ),+
    ]) => {
        impl RestApiError for $enum {
            fn status(&self) -> StatusCode {
                use $enum::*;
                match self {
                    $( $variant => StatusCode::$code ),+
                }
            }

            fn body_text(&self) -> String {
                self.to_string()
            }
        }

        impl IntoResponse for $enum {
            fn into_response(self) -> Response {
                // TODO(jsmith): Unify with the errors being attached to traces (#463).
                if self.status() == StatusCode::INTERNAL_SERVER_ERROR {
                    tracing::error!(error = ?self, "internal error");
                }
                self.to_response()
            }
        }

        impl IntoResponses for $enum {
            fn responses() -> BTreeMap<String, RefOr<OpenApiResponse>> {
                into_responses([
                    $( (StatusCode::$code, error_description!($code, $($desc)* ))),+
                ])
            }
        }
    };
}

macro_rules! error_description {
    ($code:ident, @canonical) => {
        StatusCode::$code
            .canonical_reason()
            .expect("code must have a canonical reason")
            .to_string()
    };
    ($code:ident, $desc:expr) => {
        $desc.into()
    };
}

fn into_responses<I>(iter: I) -> BTreeMap<String, RefOr<OpenApiResponse>>
where
    I: IntoIterator<Item = (StatusCode, String)>,
{
    let mut builder = ResponsesBuilder::new();

    for (code, description) in iter {
        if code == StatusCode::INTERNAL_SERVER_ERROR {
            continue;
        }

        // Ensure that the first letter is uppercase, as descriptions taken from errors would
        // default to a first lowercase letter.
        let description: String = description
            .char_indices()
            .map(|(i, mut char_)| {
                if i == 0 {
                    char_.make_ascii_uppercase();
                }
                char_
            })
            .collect();

        let canonical_reason = code
            .canonical_reason()
            .expect("all used codes have canonical reasons");
        let example_reason = format!("'{canonical_reason}' or more detailed information");
        let example = RestApiJsonError::new(code, &example_reason);
        let content = ContentBuilder::new()
            .schema(schema::Ref::from_schema_name("RestApiJsonError"))
            .example(Some(json!(example)))
            .build();
        let response = ResponseBuilder::new()
            .description(description)
            .content("application/json", content)
            .build();
        builder = builder.response(code.as_str(), response);
    }

    builder.build().into()
}

rest_api_error! {
    RetrieveMetadataError: [
        (Unavailable, NOT_FOUND, Self::Unavailable.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    StoreMetadataError: [
        (NotRegistered, CONFLICT, Self::NotRegistered.to_string()),
        (InvalidMetadata(_), BAD_REQUEST, "the provided metadata cannot be verified"),
        (InvalidBlob(_), CONFLICT, "the blob for the provided metadata is invalid"),
        (BlobExpired, GONE, Self::BlobExpired.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    RetrieveSliverError: [
        (ShardNotAssigned(_), MISDIRECTED_REQUEST,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (SliverOutOfRange(_), BAD_REQUEST, "the requested sliver index is out of range"),
        (Unavailable, NOT_FOUND, Self::Unavailable.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    StoreSliverError: [
        (SliverOutOfRange(_), BAD_REQUEST, "the requested sliver index is out of range"),
        (MissingMetadata, CONFLICT, Self::MissingMetadata.to_string()),
        (InvalidSliver(_), BAD_REQUEST, "the provided sliver failed verification"),
        (ShardNotAssigned(_), MISDIRECTED_REQUEST,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    RetrieveSymbolError: [
        (RecoverySymbolOutOfRange(_), BAD_REQUEST, "the requested recovery symbol is out of range"),
        (RetrieveSliver(RetrieveSliverError::Internal(_)), INTERNAL_SERVER_ERROR, @canonical),
        (RetrieveSliver(RetrieveSliverError::SliverOutOfRange(_)), BAD_REQUEST,
        "invalid index for the sliver providing the recovery symbol"),
        (RetrieveSliver(RetrieveSliverError::ShardNotAssigned(_)), MISDIRECTED_REQUEST,
        "the sliver providing the requested symbol is not stored at this node's shards"),
        (RetrieveSliver(RetrieveSliverError::Unavailable), NOT_FOUND,
        "the sliver providing the requested symbol was not found"),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    ComputeStorageConfirmationError: [
        (NotFullyStored, NOT_FOUND, Self::NotFullyStored.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    InconsistencyProofError: [
        (MissingMetadata, NOT_FOUND, Self::MissingMetadata.to_string()),
        (InvalidProof(_), BAD_REQUEST, "the provided inconsistency proof was invalid"),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    BlobStatusError: [
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

/// Helper type for attaching [`BcsRejection`]s to errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OrRejection<T> {
    #[error(transparent)]
    Err(T),
    #[error(transparent)]
    BcsRejection(#[from] BcsRejection),
}

impl<T: RestApiError> RestApiError for OrRejection<T> {
    fn status(&self) -> StatusCode {
        match self {
            OrRejection::Err(err) => err.status(),
            OrRejection::BcsRejection(err) => err.status(),
        }
    }

    fn body_text(&self) -> String {
        match self {
            OrRejection::Err(err) => err.body_text(),
            OrRejection::BcsRejection(err) => err.body_text(),
        }
    }
}

impl<T: RestApiError> IntoResponse for OrRejection<T> {
    fn into_response(self) -> Response {
        self.to_response()
    }
}

impl<T: IntoResponses + RestApiError> IntoResponses for OrRejection<T> {
    fn responses() -> BTreeMap<String, RefOr<OpenApiResponse>> {
        T::responses()
    }
}

// We manually implement these from conversions to avoid multiple `From<BcsRejection>`
// implementations caused by the generic.

impl From<StoreSliverError> for OrRejection<StoreSliverError> {
    fn from(value: StoreSliverError) -> Self {
        Self::Err(value)
    }
}
impl From<InconsistencyProofError> for OrRejection<InconsistencyProofError> {
    fn from(value: InconsistencyProofError) -> Self {
        Self::Err(value)
    }
}
