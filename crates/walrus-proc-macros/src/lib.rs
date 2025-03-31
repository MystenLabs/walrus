// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//! Proc-macros used within various walrus crates.
//!
//! Provides the attribute-macro `walrus_simtest` which is used to rename and annotate simtests; and
//! the derive-macro `RestApiError` which is used to derive API errors
use proc_macro::TokenStream;

#[cfg(feature = "walrus-simtest")]
mod walrus_simtest;

/// This attribute macro is used to add `simtest_` prefix to all the tests annotated with
/// `[walrus_simtest]`, and annotate them with `[sim_test]` attribute.
#[cfg(feature = "walrus-simtest")]
#[proc_macro_attribute]
pub fn walrus_simtest(args: TokenStream, item: TokenStream) -> TokenStream {
    walrus_simtest::attribute_macro(args, item)
}

#[cfg(feature = "derive-api-errors")]
mod derive_api_errors;

/// Derive the `RestApiError`, `IntoResponse`, and `IntoResponses` traits for error types.
///
/// By implementing these three traits, the definitions of error enums and structs can be used to
/// populate `utoipa` OpenAPI schema for the errors and convert the errors to `axum::Response`s
/// with the format provided by `walrus_sdk::api::errors::Status`.
///
/// Most notably, the docstrings of the enum are used for the descriptions of the errors in the
/// OpenAPI schema.
///
/// To use, add `derive(RestApiError)` to a struct or enum. Enum variants or a struct can then be
/// annotated with the `#[rest_api_error(...)]` to define the contents of the methods of
/// the `RestApiError` trait.
///
/// # `rest_api_error` fields
///
/// The attribute fields which are added to the type are best explained with the following examples:
///
/// ## The `domain`, `status`, and `reason` fields.
///
/// The `domain` field is added outside a struct or enum. It accepts a string that defines the
/// error domain (`Status::domain()`) for struct or enum variants.
///
/// The `status` and `reason` fields accept a `walrus_sdk::api::errors::StatusCode` and a
/// UPPER_SNAKE_CASE `String` respectively, that define the generic status corresponding
/// to the error and an error specific label for the reason of the error.
///
/// The `domain` field is always required. The `status` and `reason` fields are required when the
/// `delegate` field is not present (described below).
///
/// ```ignore
/// #[derive(RestApiError, thiserror::Error)]
/// #[error("the provided sliver is invalid")]
/// #[rest_api_error(
///     domain = "walrus.space", status = StatusCode::InvalidArgument, reason = "INVALID_SLIVER"
/// )]
/// struct InvalidSliver {
///     blob_id: BlobId,
///     index: SliverIndex,
/// }
/// ```
/// or
/// ```ignore
/// #[derive(RestApiError)]
/// #[rest_api_error(domain = "docs.walrus.space")]
/// enum GetSliverError {
///     #[rest_api_error(status = StatusCode::NotFound, reason = "SLIVER_NOT_FOUND")]
///     NotFound,
///     #[rest_api_error(status = StatusCode::FailedPrecondition, reason = "LATE_REQUEST")]
///     LateRequest,
/// }
/// ```
///
/// # The `delegate` field
///
/// The delegate field can be added to new-type enum variants to delegate the implementation of
/// `RestApiError` to the variant's inner type, which implements `RestApiError`.
///
/// ```ignore
/// #[derive(RestApiError)]
/// #[rest_api_error(domain = "docs.walrus.space")]
/// enum GetSliverError {
///     #[rest_api_error(status = StatusCode::NotFound, reason = "SLIVER_NOT_FOUND")]
///     NotFound,
///     #[rest_api_error(delegate)]
///     Internal(anyhow::Error),
/// }
/// ```
///
/// When present, the `status()`, `reason()`, and `add_details()` methods all return the result (or
/// extend for `add_details`) the results of the inner type.
///
/// The `status` and `reason` fields cannot be provided when the `delegate` field is present.
///
/// # The `details` nested attribute
///
/// The nested attribute `details(serialize)` can be specified on structs which implement
/// `serde::ser::Serialize`. When present, the struct's fields are serialized as metadata on the
/// `ErrorInfo` detail block in the `Status`, when converting the error to a server response.
///
/// ```ignore
/// #[derive(RestApiError, thiserror::Error)]
/// #[error("the provided sliver is invalid")]
/// #[rest_api_error(
///     domain = "walrus.space", status = StatusCode::InvalidArgument, reason = "INVALID_SLIVER",
///     details(serialize)
/// )]
/// struct InvalidSliver {
///     blob_id: BlobId,
///     index: SliverIndex,
/// }
/// ```
#[cfg(feature = "derive-api-errors")]
#[proc_macro_derive(RestApiError, attributes(rest_api_error))]
pub fn derive_rest_api_error(item: TokenStream) -> TokenStream {
    derive_api_errors::derive(item)
}
