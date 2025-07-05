// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Formats and tools for success and error messages of our REST APIs.

// Not all functions here are used in every feature.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use utoipa::{
    ToSchema,
    openapi::{
        ContentBuilder,
        RefOr,
        Response as OpenApiResponse,
        ResponseBuilder,
        ResponsesBuilder,
        schema,
    },
};
use walrus_core::{BlobId, QuiltPatchId};
use walrus_storage_node_client::api::errors::{ErrorInfo, Status, StatusCode as ApiStatusCode};

/// A blob ID encoded as a URL-safe Base64 string, without the trailing equal (=) signs.
#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct BlobIdString(#[serde_as(as = "DisplayFromStr")] pub(crate) BlobId);

/// A quilt patch ID encoded as a URL-safe Base64 string, without the trailing equal (=) signs.
#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct QuiltPatchIdString(#[serde_as(as = "DisplayFromStr")] pub(crate) QuiltPatchId);

/// Successful API response body as JSON.
///
/// Contains the HTTP code as well as a message or response object.
#[allow(missing_docs)]
#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ApiSuccess<T> {
    Success {
        /// INV: This is a valid status code.
        code: u16,
        data: T,
    },
}

#[derive(Debug, ToSchema)]
#[schema(value_type = String, format = Binary)]
pub(crate) struct Binary(());

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
        .schema(Some(schema::Ref::from_schema_name("RestApiJsonError")))
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
        // Remove duplicates. This is necessary as responses derived from nested errors may repeat,
        // such as an InternalError from multiple sources.
        descr_list.sort();
        descr_list.dedup();
        descr_list.retain(|description| !description.trim().is_empty());

        let description = if descr_list.len() > 1 {
            let mut output = "May be returned when".to_owned();
            for (i, item) in descr_list.into_iter().enumerate() {
                let list_num = i + 1;
                write!(&mut output, " ({list_num}) {item}").expect("write to string does not fail");
            }
            output
        } else if !descr_list.is_empty() {
            descr_list.pop().expect("checked length")
        } else {
            String::default()
        };

        let content = ContentBuilder::new()
            .schema(Some(schema::Ref::from_schema_name("Status")))
            .build();

        let response = ResponseBuilder::new()
            .description(description)
            .content("application/json", content)
            .build();

        builder = builder.response(code.as_str(), response);
    }

    builder.build().into()
}
