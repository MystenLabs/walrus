// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Error status and codes.
//!
//! # JSON error responses
//!
//! This module contains the [`Status`] type, which encodes and decodes the JSON error
//! responses returned by Walrus services.  The error format follows the [API Improvement Proposal
//! #193 (AIP-193)](https://google.aip.dev/193).  An example of an error is given below:
//!
//! ```json
//! {
//!  "error": {
//!    "code": 400,
//!    "message": "shard 0 is not assigned to this node in epoch 17",
//!    "status": "FAILED_PRECONDITION",
//!    "details": [
//!      {
//!        "@type": "ErrorInfo",
//!        "reason": "SHARD_NOT_ASSIGNED",
//!        "domain": "storage-node.walrus/RetrieveSliverError",
//!        "metadata": { "shard": 0, "epoch": 17 }
//!      },
//!    ]
//!  }
//! }
//! ```
//!
//! An error response contains
//!
//! - `message`, a detailed message about the failure that occurred;
//! - `status`, one of a small set of general error types (see [`StatusCode`]);
//! - `code`, the HTTP error code associated with `status`; and
//! - `details`, a list of additional machine-readable error details.
//!
//! The `details` field will always contain an object of type `ErrorInfo`. Its fields
//! (`domain`, `reason`) provide a machine-readable way of identifying the specific error returned
//! by the API.  Additionally, its `metadata` field can be used to retrieve arguments specific to
//! the error.

use core::mem;
use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
};

use reqwest::StatusCode as HttpStatusCode;
use serde::{Deserialize, Serialize, de::Error};
use serde_with::{TryFromInto, serde_as};
use utoipa::ToSchema;

/// Error domain for service-agnostic errors.
pub const GLOBAL_ERROR_DOMAIN: &str = "global";
/// Error domain for storage node errors.
pub const STORAGE_NODE_ERROR_DOMAIN: &str = "storage.walrus.space";
/// Error domain for storage daemon errors.
pub const DAEMON_ERROR_DOMAIN: &str = "daemon.walrus.space";

/// A message returned from a failed API call.
///
/// Contains both human-readable and machine-readable details of the error,
/// to assist in resolving the error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Status {
    #[serde(rename = "error")]
    #[schema(inline)]
    inner: StatusInner,
}

impl Status {
    /// Create a new error status with the provided status code, message, and error details.
    pub fn new(code: StatusCode, message: String, info: ErrorInfo) -> Self {
        Self {
            inner: StatusInner {
                status_code: code,
                message,
                details: vec![info.into()],
            },
        }
    }

    /// The status code associated with the response.
    pub fn code(&self) -> StatusCode {
        self.inner.status_code
    }

    /// The status message, a developer-facing, human-readable "debug message".
    pub fn message(&self) -> &str {
        &self.inner.message
    }

    /// Returns the reason for the error.
    pub fn reason(&self) -> Option<&str> {
        self.error_info().map(ErrorInfo::reason)
    }

    /// Returns the error's domain.
    pub fn domain(&self) -> Option<&str> {
        self.error_info().map(ErrorInfo::domain)
    }

    /// Machine-readable details about the error.
    pub fn error_info(&self) -> Option<&ErrorInfo> {
        if let Some(ErrorDetails::ErrorInfo(info)) = self
            .inner
            .details
            .iter()
            .find(|detail| matches!(detail, ErrorDetails::ErrorInfo(_)))
        {
            return Some(info);
        }
        None
    }

    /// Machine-readable details about the error.
    pub fn error_info_mut(&mut self) -> Option<&mut ErrorInfo> {
        if let Some(ErrorDetails::ErrorInfo(info)) = self
            .inner
            .details
            .iter_mut()
            .find(|detail| matches!(detail, ErrorDetails::ErrorInfo(_)))
        {
            return Some(info);
        }
        None
    }

    /// Insert error details into the status response, returning any
    /// previous instance of the same type.
    pub fn insert_details(&mut self, mut details: ErrorDetails) -> Option<ErrorDetails> {
        for item in self.inner.details.iter_mut() {
            if mem::discriminant(&details) == mem::discriminant(item) {
                mem::swap(item, &mut details);
                return Some(details);
            }
        }

        self.inner.details.push(details);
        None
    }

    /// Returns true if the error's reason and domain match those specified.
    pub fn is_for_reason(&self, reason: &str, domain: &str) -> bool {
        self.reason() == Some(reason) && self.domain() == Some(domain)
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = &self.inner.message;
        let status_code = self.inner.status_code;

        write!(
            f,
            "{message} ({}, {})",
            status_code.as_str(),
            status_code.http_code()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
struct StatusInner {
    /// The status code corresponding to the error.
    #[serde(flatten)]
    #[schema(inline, value_type = StatusCodeFields)]
    status_code: StatusCode,

    /// A message describing the error in detail.
    // Messages should use simple descriptive language that is easy to understand. They should be
    // free of technical jargon and clearly state the problem resulting in an error and offer an
    // actionable resolution to it.
    message: String,

    /// Machine readable details of the error.
    ///
    /// Always contains an [`ErrorInfo`], which provides a machine-readable
    /// representation of the of the `message` field.
    // The details vector is where we can add additional, standardized extensions to the error
    // response. For example, for internal errors we add `ErrorDetails::DebugInfo` with the ID of
    // the current trace. Other possible "extensions" include region-localized error messages,
    // the duration after which to retry failed requests, details as to which fields in the request
    // are invalid and why, links to documentation, etc. The following link gives further examples
    // https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
    #[schema(value_type = Vec<Object>)]
    details: Vec<ErrorDetails>,
}

/// Various types of error details that may be included in an error response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "@type")]
pub enum ErrorDetails {
    /// Additional details about the type of error that occurred.
    ErrorInfo(ErrorInfo),
    /// Details to aid in debugging.
    DebugInfo(DebugInfo),
}

impl From<ErrorInfo> for ErrorDetails {
    fn from(value: ErrorInfo) -> Self {
        Self::ErrorInfo(value)
    }
}

impl From<DebugInfo> for ErrorDetails {
    fn from(value: DebugInfo) -> Self {
        Self::DebugInfo(value)
    }
}

/// Machine-readable details of the error.
///
/// The pair of (reason, domain) uniquely identifies the error across components in the system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorInfo {
    /// A short, UPPER_SNAKE_CASE description of the error, which is unique within the domain.
    reason: String,
    /// A logical grouping to which the reason belongs, such as the name of the service generating
    /// the error.
    domain: String,
    /// Metadata associated with the error.
    ///
    /// All dynamic values present in `Status::message` should be present here. This avoids
    /// receivers from needing to parse the message to retrieve values.
    metadata: serde_json::Map<String, serde_json::Value>,
}

impl ErrorInfo {
    /// Create a new instance with the specified reason and domain, and empty metadata.
    pub fn new(reason: String, domain: String) -> Self {
        Self {
            reason,
            domain,
            metadata: Default::default(),
        }
    }

    /// Returns a metadata field if it exists and successfully deserializes to the expected type.
    pub fn field<T>(&self, key: &str) -> Option<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.metadata
            .get(key)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    /// Returns a mutable reference to the metadata attached to the error info.
    pub fn metadata_mut(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
        &mut self.metadata
    }

    /// Returns the reason for the error.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the error's domain.
    pub fn domain(&self) -> &str {
        &self.domain
    }
}

/// Information to aid in debugging.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugInfo {
    /// The stack trace entries indicating where the error occurred.
    pub stack_entries: Vec<String>,

    /// Additional debugging information provided by the server.
    pub detail: String,
}

macro_rules! status_codes {
    ($(
        $(#[$attr:meta])*
        ($status:ident, $string_repr:literal, $http_code:path)
    ),+$(,)?) => {
        /// A limited set of status codes which identify the general class of the error.
        ///
        /// The number of status codes are intentionally kept small, as each code represents a
        /// general group of error types, and should give a developer a general idea as to where the
        /// fault lies.  They are a subset of those listed
        /// [`here`](https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto)
        ///
        /// Each status code has a unique string tag associated with it, such as `"NOT_FOUND"` or
        /// `"INTERNAL"`, which can be retrieved with [`StatusCode::as_str()`].
        ///
        /// Each status code also has an associated HTTP code which can be retrieved with
        /// [`StatusCode::http_code()`]. Several status codes may map to the same HTTP error code.
        #[derive(Debug, Copy, Clone, PartialEq, Eq, ToSchema)]
        pub enum StatusCode {
            $($(#[$attr])* $status),+
        }

        impl StatusCode {
            /// The HTTP status code associated with the instance.
            pub const fn http_code(&self) -> HttpStatusCode {
                match self {
                    $(Self::$status => $http_code),+
                }
            }

            /// Constant, machine-readable string representation of a status code.
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$status => $string_repr),+
                }
            }

            fn from_fields(code: HttpStatusCode, status: &str) -> Option<Self> {
                match (code, status) {
                    $(($http_code, $string_repr) => Some(Self::$status),)+
                    _ => None,
                }
            }
        }
    };
}

status_codes![
    /// The message does not correspond to an error, may be returned on success.
    (Ok, "OK", HttpStatusCode::OK),

    /// The requested resource is unavailable for legal reasons.
    (
        UnavailableForLegalReasons,
        "UNAVAILABLE_FOR_LEGAL_REASONS",
        HttpStatusCode::UNAVAILABLE_FOR_LEGAL_REASONS
    ),

    /// The client specified an invalid argument.
    ///
    /// This indicates arguments that are problematic regardless of the state of the system,
    /// such as a malformed argument.
    (InvalidArgument, "INVALID_ARGUMENT", HttpStatusCode::BAD_REQUEST),

    /// The requested resource was not found.
    (NotFound, "NOT_FOUND", HttpStatusCode::NOT_FOUND),

    /// The operation was rejected because the system is not in a required state.
    ///
    /// For example, the system is not currently responsible for the shard to which the request is
    /// directed, or metadata has not yet been uploaded or registered.
    (FailedPrecondition, "FAILED_PRECONDITION", HttpStatusCode::BAD_REQUEST),

    /// The operation failed as the caller has been identified but does not have permission to
    /// execute the specified operation.
    (PermissionDenied, "PERMISSION_DENIED", HttpStatusCode::FORBIDDEN),

    /// The requested resource exceeds the maximum allowed size that was configured for this
    /// service.
    (SizeExceeded, "SIZE_EXCEEDED", HttpStatusCode::FORBIDDEN),

    /// An internal error.
    ///
    /// Some invariant expected by the underlying system has been broken.  This error code is
    /// reserved for serious errors.
    (Internal, "INTERNAL", HttpStatusCode::INTERNAL_SERVER_ERROR),

    /// The deadline expired before the operation could complete.
    (DeadlineExceeded, "DEADLINE_EXCEEDED", HttpStatusCode::GATEWAY_TIMEOUT),

    /// The request does not have valid authentication credentials for the
    /// operation.
    (Unauthenticated, "UNAUTHENTICATED", HttpStatusCode::UNAUTHORIZED),

    /// Some resource has been exhausted, perhaps a per-user quota, or
    /// perhaps the entire file system is out of space.
    (ResourceExhausted, "RESOURCE_EXHAUSTED", HttpStatusCode::TOO_MANY_REQUESTS),

    /// The service is currently unavailable.
    ///
    /// This is likely a temporary situation and may be corrected by retrying with a backoff.
    (Unavailable, "UNAVAILABLE", HttpStatusCode::SERVICE_UNAVAILABLE),
];

#[serde_as]
#[derive(Serialize, Deserialize, ToSchema)]
struct StatusCodeFields<'a> {
    /// General type of error, given as an UPPER_SNAKE_CASE string.
    status: Cow<'a, str>,
    /// HTTP status code associated with the error.
    #[serde_as(as = "TryFromInto<u16>")]
    #[schema(value_type = u16)]
    code: HttpStatusCode,
}

impl Serialize for StatusCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StatusCodeFields {
            code: self.http_code(),
            status: self.as_str().into(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StatusCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields = StatusCodeFields::deserialize(deserializer)?;

        StatusCode::from_fields(fields.code, &fields.status).ok_or_else(|| {
            D::Error::custom(format!(
                "error status ({0}) and code ({1}) do not correspond",
                fields.status, fields.code
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::Result as TestResult;

    use super::*;

    fn example_response() -> (Status, serde_json::Value) {
        let message = "shard 0 is not assigned to this node in epoch 17";
        let reason = "SHARD_NOT_ASSIGNED";
        let domain = "storage-node.walrus/RetrieveSliverError";

        let mut info = ErrorInfo::new(reason.to_owned(), domain.to_owned());
        let metadata = info.metadata_mut();
        metadata.insert("shard".to_string(), 0.into());
        metadata.insert("epoch".to_string(), 17.into());

        let status = Status::new(StatusCode::FailedPrecondition, message.to_owned(), info);

        let json = serde_json::json!({
            "error": {
                "code": 400,
                "status": "FAILED_PRECONDITION",
                "message": message,
                "details": [
                {
                    "@type": "ErrorInfo",
                    "reason": reason,
                    "domain": domain,
                    "metadata": { "shard": 0, "epoch": 17 }
                },
                ]
            }
        });
        (status, json)
    }

    #[test]
    fn serialize() -> TestResult {
        let (status, json) = example_response();

        assert_eq!(serde_json::to_value(status)?, json);

        Ok(())
    }

    #[test]
    fn deserialize() -> TestResult {
        let (status, json) = example_response();

        assert_eq!(serde_json::from_value::<Status>(json)?, status);

        Ok(())
    }
}
