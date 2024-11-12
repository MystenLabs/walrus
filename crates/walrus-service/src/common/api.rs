// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Formats and tools for success and error messages of our REST APIs.

// Not all functions here are used in every feature.
#![allow(dead_code)]

use std::{collections::BTreeMap, fmt::Write as _};

use axum::{
    response::{IntoResponse, Response},
    Json,
};
use opentelemetry::trace::TraceContextExt as _;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::{serde_as, DisplayFromStr};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use utoipa::{
    openapi::{
        schema,
        ContentBuilder,
        ObjectBuilder,
        RefOr,
        Response as OpenApiResponse,
        ResponseBuilder,
        ResponsesBuilder,
        Schema,
    },
    PartialSchema,
    ToSchema,
};
use walrus_core::BlobId;
use walrus_sdk::error::ServiceError;

/// A blob ID encoded as a URL-safe Base64 string, without the trailing equal (=) signs.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(
    as = BlobId,
    value_type = String,
    format = Byte,
    example = json!("E7_nNXvFU_3qZVu3OH1yycRG7LZlyn1-UxEDCDDqGGU"),
)]
pub(crate) struct BlobIdString(#[serde_as(as = "DisplayFromStr")] pub(crate) BlobId);

// Schema for the [`sui_types::event::EventID`] type.
#[allow(missing_docs)]
#[derive(Debug, ToSchema)]
#[schema(
    as = EventID,
    rename_all = "camelCase",
    example = json!({
        "txDigest": "EhtoQF9UpPyg5PsPUs69LdkcRrjQ3R4cTsHnwxZVTNrC",
        "eventSeq": 0
    })
)]
pub(crate) struct EventIdSchema {
    #[schema(format = Byte)]
    tx_digest: Vec<u8>,
    // u64 represented as a string
    #[schema(value_type = String)]
    event_seq: u64,
}

// Schema for the [`sui_types::ObjectID`] type.
#[allow(missing_docs)]
#[derive(Debug, ToSchema)]
#[schema(
    as = ObjectID,
    title = "Sui object ID as a hex string",
    example = "0x56ae1c86e17db174ea002f8340e28880bc8a8587c56e8604a4fa6b1170b23a60"
)]
pub(crate) struct ObjectIdSchema(String);

/// Successful API response body as JSON.
///
/// Contains the HTTP code as well as a message or response object.
#[allow(missing_docs)]
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ApiSuccess<T> {
    Success {
        /// INV: This is a valid status code.
        code: u16,
        data: T,
    },
}

impl<T> ApiSuccess<T> {
    /// Generates a new message with the provided status code.
    pub(crate) fn new(code: StatusCode, data: T) -> Self {
        Self::Success {
            code: code.as_u16(),
            data,
        }
    }

    /// Generates a new message with status code 200.
    pub(crate) fn ok(data: T) -> Self {
        Self::Success {
            code: StatusCode::OK.as_u16(),
            data,
        }
    }

    /// Generates the OpenAPI schema for this message.
    pub(crate) fn schema_with_data(data: RefOr<Schema>) -> RefOr<Schema> {
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
        (
            StatusCode::from_u16(*code).expect("this is guaranteed to be a valid status code"),
            Json(self),
        )
            .into_response()
    }
}

/// Creates `ToSchema` API implementations and type aliases for `ApiSuccess<T>`.
///
/// This is required as utoipa's current method for handling generics in schemas is not
/// working for enums. See <https://github.com/juhaku/utoipa/issues/835>.
#[macro_export]
macro_rules! api_success_alias {
    (@schema PartialSchema $name:ident) => {
        $crate::common::api::ApiSuccess::<$name>::schema_with_data($name::schema())
    };
    (@schema ToSchema $name:ident) => {
        <$crate::common::api::ApiSuccess<$name> as PartialSchema>::schema()
    };
    ($name:ident as $alias:ident, $method:tt) => {
        pub(crate) struct $alias;

        impl<'r> ToSchema<'r> for $alias {
            fn schema() -> (&'r str, RefOr<Schema>) {
                (stringify!($alias), $crate::api_success_alias!(@schema $method $name))
            }
        }
    };
}

/// API response body for error responses as JSON.
///
/// Contains the HTTP code as well as the textual reason.
#[allow(missing_docs)]
#[derive(ToSchema, Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub(crate) enum RestApiJsonError<'a> {
    Error {
        /// INV: This is a valid status code.
        code: u16,
        message: &'a str,
        #[serde(flatten)]
        reason: Option<ServiceError>,
    },
}

impl<'a> RestApiJsonError<'a> {
    /// Creates a new error with the provided status code and message.
    pub(crate) fn new(code: StatusCode, message: &'a str, reason: Option<ServiceError>) -> Self {
        Self::Error {
            code: code.as_u16(),
            message,
            reason,
        }
    }
}

impl IntoResponse for RestApiJsonError<'_> {
    fn into_response(self) -> Response {
        let Self::Error { ref code, .. } = self;
        (
            StatusCode::from_u16(*code).expect("this is guaranteed to be a valid status code"),
            Json(self),
        )
            .into_response()
    }
}

/// Defines a trait over errors that can be converted into a [`RestApiJsonError`]-based response.
pub(crate) trait RestApiError: Sized {
    /// Returns the HTTP status code.
    fn status(&self) -> StatusCode;

    /// Returns the text to be written to the HTTP body.
    fn body_text(&self) -> String;

    /// Returns the detailed server side error, if any.
    fn service_error(&self) -> Option<ServiceError>;

    /// Converts the error into a [`Response`].
    fn to_response(&self) -> Response {
        let mut message = self.body_text();
        if self.status() == StatusCode::INTERNAL_SERVER_ERROR {
            let trace_id = Span::current().context().span().span_context().trace_id();
            write!(&mut message, " (TraceID: {:x})", trace_id)
                .expect("writing to a string must succeed");
        }

        RestApiJsonError::new(self.status(), &self.body_text(), self.service_error())
            .into_response()
    }
}

/// Implements various conversions for the error enum.
#[macro_export]
macro_rules! rest_api_error {
    (@description $code:ident, @canonical) => {
        StatusCode::$code
            .canonical_reason()
            .expect("code must have a canonical reason")
            .to_string()
    };
    (@description $code:ident, $desc:expr) => {
        $desc.into()
    };

    ($enum:ident: [
        $( ($variant:pat, $code:ident, $reason:expr, $($desc:tt)*) ),+$(,)?
    ]) => {
        impl RestApiError for $enum {
            fn status(&self) -> StatusCode {
                use $enum::*;
                #[allow(unused_variables)]
                match self {
                    $( $variant => StatusCode::$code ),+
                }
            }

            fn body_text(&self) -> String {
                self.to_string()
            }

            fn service_error(&self) -> Option<ServiceError> {
                use $enum::*;
                match self {
                    $( $variant => $reason ),+
                }
            }
        }

        impl IntoResponse for $enum {
            fn into_response(self) -> Response {
                let mut response = self.to_response();

                if self.status() == StatusCode::INTERNAL_SERVER_ERROR {
                    response.extensions_mut().insert(
                        $crate::common::telemetry::InternalError(Arc::new(self))
                    );
                }

                response
            }
        }

        impl IntoResponses for $enum {
            fn responses() -> BTreeMap<String, RefOr<OpenApiResponse>> {
                $crate::common::api::into_responses([
                    $( (StatusCode::$code, rest_api_error!(@description $code, $($desc)* ))),+
                ])
            }
        }
    };
}

/// Generates OpenAPI schemas for error responses.
pub(crate) fn into_responses<I>(iter: I) -> BTreeMap<String, RefOr<OpenApiResponse>>
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
        let example = RestApiJsonError::new(code, &example_reason, None);
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

/// Convert the path with variables of the form `:id` to the form `{id}`.
pub(crate) fn rewrite_route(path: &str) -> String {
    regex::Regex::new(r":(?<param>\w+)")
        .expect("this is a valid regex")
        .replace_all(path, "{$param}")
        .as_ref()
        .into()
}
