// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use reqwest::{
    header::{self, HeaderMap},
    Response,
};
use serde::de::DeserializeOwned;
use walrus_core::ensure;

use crate::{
    api::ServiceResponse,
    error::{Kind, NodeError},
};

pub(crate) trait NodeResponse: Sized {
    /// Converts the response to an error if the status is a client or service error code,
    /// with any server-provided error message.
    async fn response_error_for_status(self) -> Result<Self, NodeError>;

    /// Decode the body of the response using BCS encoding.
    async fn bcs<T: DeserializeOwned>(self) -> Result<T, NodeError>;

    /// Decode the body of the response as a [`ServiceResponse`].
    async fn service_response<T: DeserializeOwned>(self) -> Result<T, NodeError>;
}

impl NodeResponse for Response {
    async fn response_error_for_status(self) -> Result<Self, NodeError> {
        let Err(inner) = self.error_for_status_ref() else {
            return Ok(self);
        };
        if !is_content_type_json(self.headers()) {
            return Err(Kind::Reqwest(inner).into());
        }

        match self.json().await {
            Ok(ServiceResponse::<()>::Error(status)) => Err(Kind::Status { inner, status }.into()),
            _ => {
                tracing::debug!("unable to parse the service's JSON response");
                Err(Kind::Reqwest(inner).into())
            }
        }
    }

    async fn bcs<T: DeserializeOwned>(self) -> Result<T, NodeError> {
        ensure!(
            is_content_type_octet_stream(self.headers()),
            NodeError::from(Kind::InvalidContentType)
        );

        let body = self.bytes().await.map_err(Kind::Reqwest)?;

        Ok(bcs::from_bytes(&body).map_err(Kind::Bcs)?)
    }

    async fn service_response<T: DeserializeOwned>(self) -> Result<T, NodeError> {
        let non_error_response = self.response_error_for_status().await?;

        ensure!(
            is_content_type_json(non_error_response.headers()),
            NodeError::from(Kind::InvalidContentType)
        );

        match non_error_response
            .json::<ServiceResponse<T>>()
            .await
            .map_err(Kind::Reqwest)?
        {
            ServiceResponse::Success { data, .. } => Ok(data),
            ServiceResponse::Error(status) => Err(Kind::ErrorInNonErrorMessage(status).into()),
        }
    }
}

fn is_content_type_octet_stream(headers: &HeaderMap) -> bool {
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

fn is_content_type_json(headers: &header::HeaderMap) -> bool {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };

    let Some(media_type) = content_type
        .to_str()
        .ok()
        .and_then(|s| s.parse::<mime::Mime>().ok())
    else {
        return false;
    };

    // Check the media type and subtype, but allow any params.
    media_type.type_() == mime::APPLICATION && media_type.subtype() == mime::JSON
}
