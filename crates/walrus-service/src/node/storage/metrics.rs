// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use prometheus::HistogramVec;
use typed_store::TypedStoreError;

walrus_utils::metrics::define_metric_set! {
    #[namespace = "db_client"]
    /// Metrics exported by database operations.
    ///
    /// Some metrics, such as `operation_duration_seconds` follow [OTel semantic conventions][otel]
    /// for database client metrics.
    ///
    /// [otel]: https://opentelemetry.io/docs/specs/semconv/database/database-metrics/
    pub(super) struct CommonDatabaseMetrics {
        #[help = "Duration (in seconds) of database client operations."]
        operation_duration_seconds: HistogramVec {
            labels: [
                "db_collection_name", "db_operation_name", "db_query_summary",
                "db_response_status_code", "error_type"
            ],
            buckets: vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
        }
    }
}

impl CommonDatabaseMetrics {
    pub fn observe_operation_duration(&self, labels: Labels, duration: Duration) {
        walrus_utils::with_label!(
            self.operation_duration_seconds,
            labels.collection_name,
            labels.operation_name,
            labels.query_summary,
            labels.response_status_code,
            labels.error_type
        )
        .observe(duration.as_secs_f64());
    }
}

/// Labels that are commonly attached to metrics for database operations.
#[derive(Debug, Default, Copy, Clone)]
pub(super) struct Labels<'a> {
    /// The name of the collection (e.g., RocksDB column family).
    pub collection_name: &'a str,
    /// Type of the operation.
    pub operation_name: OperationType,
    /// A low-cardinality summary of the query (e.g., "GET sliver BY blob_id").
    pub query_summary: &'static str,
    /// Status code capturing the result of the query.
    pub response_status_code: DatabaseStatusCode,
    /// An low-cardinality identifier for the error type, for when `response_status_code`
    /// is [`DatabaseStatusCode::Error`].
    pub error_type: &'a str,
}

impl Labels<'_> {
    /// Attaches labels as if the response was `Err(error)`.
    pub fn with_error(self, error: &TypedStoreError) -> Self {
        self.with_response::<()>(Err(error))
    }

    /// Attaches labels from the response.
    pub fn with_response<T>(self, response: Result<&T, &TypedStoreError>) -> Self
    where
        for<'a> Result<&'a T, &'a TypedStoreError>: Into<DatabaseStatusCode>,
    {
        Self {
            error_type: typed_store_error_type(response),
            response_status_code: response.into(),
            ..self
        }
    }

    /// Like [`Self::with_response`], but treats an `Ok(T)` as the unit value `Ok(())`, thereby
    /// tracking only error or failure.
    pub fn with_response_as_unit<T>(self, response: Result<&T, &TypedStoreError>) -> Self {
        self.with_response(response.map(|_| &()))
    }
}

/// Various database operation types.
#[derive(Debug, Default, Copy, Clone)]
pub(crate) enum OperationType {
    #[default]
    Get,
    MultiGet,
    Insert,
    ContainsKey,
    Create,
}

impl AsRef<str> for OperationType {
    fn as_ref(&self) -> &str {
        match self {
            OperationType::Get => "get",
            OperationType::MultiGet => "multi-get",
            OperationType::Insert => "insert",
            OperationType::ContainsKey => "contains-key",
            OperationType::Create => "create",
        }
    }
}

/// Database status codes which capture how the operation ended.
#[derive(Debug, Default, Copy, Clone)]
pub(crate) enum DatabaseStatusCode {
    /// Operation succeeded.
    #[default]
    Ok,
    /// When the query results in a boolean response, represent it directly,
    Bool(bool),
    /// Succeeded, but there were no values to return.
    NotFound,
    /// An error occurred.
    Error,
}

impl From<Result<&(), &TypedStoreError>> for DatabaseStatusCode {
    fn from(value: Result<&(), &TypedStoreError>) -> Self {
        match value {
            Ok(_) => Self::Ok,
            Err(_) => Self::Error,
        }
    }
}

impl<T> From<Result<&Option<T>, &TypedStoreError>> for DatabaseStatusCode {
    fn from(value: Result<&Option<T>, &TypedStoreError>) -> Self {
        match value {
            Ok(Some(_)) => Self::Ok,
            Ok(None) => Self::NotFound,
            Err(_) => Self::Error,
        }
    }
}

impl From<Result<&bool, &TypedStoreError>> for DatabaseStatusCode {
    fn from(value: Result<&bool, &TypedStoreError>) -> Self {
        match value {
            Ok(value) => Self::Bool(*value),
            Err(_) => Self::Error,
        }
    }
}

impl AsRef<str> for DatabaseStatusCode {
    fn as_ref(&self) -> &str {
        match self {
            DatabaseStatusCode::Ok => "ok",
            DatabaseStatusCode::Bool(true) => "ok(true)",
            DatabaseStatusCode::Bool(false) => "ok(false)",
            DatabaseStatusCode::NotFound => "not-found",
            DatabaseStatusCode::Error => "error",
        }
    }
}

/// Converts a response into a `&'static str` represting the failure mode.
///
/// Returns the empty string for `Ok`.
pub(super) fn typed_store_error_type<T>(result: Result<&T, &TypedStoreError>) -> &'static str {
    let Err(ref error) = result else { return "" };

    match error {
        TypedStoreError::RocksDBError(_) => "TypedStoreError::RocksDBError",
        TypedStoreError::SerializationError(_) => "TypedStoreError::SerializationError",
        TypedStoreError::UnregisteredColumn(_) => "TypedStoreError::UnregisteredColumn",
        TypedStoreError::CrossDBBatch => "TypedStoreError::CrossDBBatch",
        TypedStoreError::MetricsReporting => "TypedStoreError::MetricsReporting",
        TypedStoreError::RetryableTransactionError => "TypedStoreError::RetryableTransactionError",
        TypedStoreError::IteratorNotInitialized => "TypedStoreError::IteratorNotInitialized",
        TypedStoreError::TaskError(_) => "TypedStoreError::TaskError",
        _ => "unrecognized",
    }
}
