// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use futures::stream::FuturesUnordered;
use reqwest::Response;
use serde::{Deserialize, Serialize};

use super::error::CommunicationError;
use crate::server::ServiceResponse;

/// Takes a [`Response`], deserializes the json content, removes the [`ServiceResponse`] and
/// returns the contained type.
pub(crate) async fn unwrap_response<T>(response: Response) -> Result<T, CommunicationError>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    if response.status().is_success() {
        let body = response.json::<ServiceResponse<T>>().await?;
        match body {
            ServiceResponse::Success { code: _code, data } => Ok(data),
            ServiceResponse::Error { code, message } => {
                Err(CommunicationError::ServiceResponseError { code, message })
            }
        }
    } else {
        Err(CommunicationError::HttpFailure(response.status()))
    }
}

/// Pushes `count` futures to the `FuturesUnordered` without consuming them.
pub(crate) fn push_futures<T>(
    count: usize,
    futures: &mut impl Iterator<Item = T>,
    requests: &mut FuturesUnordered<T>,
) {
    for _ in 0..count {
        if let Some(future) = futures.next() {
            requests.push(future);
        }
    }
}
