// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Formats and tools for success and error messages of our REST APIs.

// Not all functions here are used in every feature.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};

use axum::{
    response::{IntoResponse, Response},
    Json,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use sui_types::base_types::ObjectID;
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
use walrus_sdk::api::errors::{ErrorInfo, Status, StatusCode as ApiStatusCode};

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
#[serde_as]
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    as = ObjectID,
    value_type = String,
    format = Byte,
    title = "Sui object ID as a hex string",
    example = "0x56ae1c86e17db174ea002f8340e28880bc8a8587c56e8604a4fa6b1170b23a60"
)]
pub(crate) struct ObjectIdSchema(#[serde_as(as = "DisplayFromStr")] pub(crate) ObjectID);

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

/// Trait identifying objects that can be converted into an [`Status`].
pub(crate) trait RestApiError: Sized + std::fmt::Display {
    /// The status code of the error.
    fn status_code(&self) -> ApiStatusCode;

    /// The domain of the error.
    ///
    /// Used with `Self::reason()` to identify the originating error.
    fn domain(&self) -> String;

    /// The reason of the error as an UPPER_SNAKE_CASE string.
    ///
    /// This is a constant value that identifies the direct cause of the error, and should be unique
    /// within a particular domain of errors.
    // TODO(jsmith): Add metadata to the errors
    fn reason(&self) -> String;

    /// Adds any additional error details to the status.
    fn add_details(&self, _status: &mut Status);

    /// Returns a map of HTTP status codes that may be returned, along with the various situations
    /// resulting in them.
    fn response_descriptions() -> HashMap<StatusCode, Vec<String>>;

    /// A detailed error messages.
    ///
    /// Defaults to the display representation of the object.
    fn message(&self) -> String {
        self.to_string()
    }

    /// Converts the error into a [`Response`].
    fn to_response(&self) -> Response {
        let info = ErrorInfo::new(self.reason(), self.domain());
        let mut status = Status::new(self.status_code(), self.message(), info);

        self.add_details(&mut status);

        (self.status_code().http_code(), Json(status)).into_response()
    }
}

/// Ensure that the response codes are unique.
///
/// When creating response descriptions, several errors may have the same HTTP error code. Make each
/// code unique by appending a numerical suffix (1), (2), etc. to it, so that it can be stored in
/// the map required by the [`IntoResponses`] trait.
pub(crate) fn ensure_unique_responses(
    responses: Vec<(String, RefOr<OpenApiResponse>)>,
) -> BTreeMap<String, RefOr<OpenApiResponse>> {
    let mut output: BTreeMap<(String, u16), _> = BTreeMap::default();

    for (code, response) in responses {
        let mut key = (code, 0);

        // Insert the code with the first index that it not already present.
        while output.contains_key(&key) {
            key.1 += 1;
        }

        output.insert(key, response);
    }

    output
        .into_iter()
        .map(|((code, index), value)| {
            if index == 0 {
                return (code, value);
            }
            (format!("{code} ({index})"), value)
        })
        .collect()
}

pub(crate) fn describe_error_response(
    code: u16,
    description: String,
) -> (String, RefOr<OpenApiResponse>) {
    let content = ContentBuilder::new()
        .schema(schema::Ref::from_schema_name("RestApiJsonError"))
        .build();
    let response = ResponseBuilder::new()
        .description(description)
        .content("application/json", content)
        .build();

    (code.to_string(), response.into())
}

/// Generates OpenAPI schemas for error responses.
pub(crate) fn into_responses(
    code_description_map: HashMap<StatusCode, Vec<String>>,
) -> BTreeMap<String, RefOr<OpenApiResponse>> {
    let mut builder = ResponsesBuilder::new();

    for (code, mut descr_list) in code_description_map {
        let description = if descr_list.len() > 1 {
            let mut output = "May be returned when".to_owned();
            for (i, item) in descr_list.into_iter().enumerate() {
                let list_num = i + 1;
                write!(&mut output, " ({list_num}) {item}").expect("write to string does not fail");
            }
            output
        } else {
            descr_list.pop().expect("checked length")
        };

        let content = ContentBuilder::new()
            .schema(schema::Ref::from_schema_name("Status"))
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
