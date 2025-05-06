// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use utoipa::{
    IntoResponses,
    openapi::{RefOr, response::Response as OpenApiResponse},
};
use walrus_storage_node_client::api::errors::{Status, StatusCode as ApiStatusCode};

use super::extract::BcsRejection;
use crate::{
    common::api::RestApiError,
    node::{InconsistencyProofError, StoreSliverError, SyncShardServiceError},
};

/// Helper type for attaching [`BcsRejection`]s to errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OrRejection<T> {
    #[error(transparent)]
    Err(T),
    #[error(transparent)]
    BcsRejection(#[from] BcsRejection),
}

impl<T: RestApiError> RestApiError for OrRejection<T> {
    fn status_code(&self) -> ApiStatusCode {
        match self {
            Self::Err(inner) => inner.status_code(),
            Self::BcsRejection(inner) => inner.status_code(),
        }
    }

    fn domain(&self) -> String {
        match self {
            Self::Err(inner) => inner.domain(),
            Self::BcsRejection(inner) => inner.domain(),
        }
    }

    fn reason(&self) -> String {
        match self {
            Self::Err(inner) => inner.reason(),
            Self::BcsRejection(inner) => inner.reason(),
        }
    }

    fn response_descriptions() -> HashMap<StatusCode, Vec<String>> {
        let mut output = T::response_descriptions();
        for (code, descriptions) in BcsRejection::response_descriptions() {
            output.entry(code).or_default().extend(descriptions);
        }
        output
    }

    fn add_details(&self, status: &mut Status) {
        match self {
            Self::Err(inner) => inner.add_details(status),
            Self::BcsRejection(inner) => inner.add_details(status),
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

impl From<SyncShardServiceError> for OrRejection<SyncShardServiceError> {
    fn from(value: SyncShardServiceError) -> Self {
        Self::Err(value)
    }
}
