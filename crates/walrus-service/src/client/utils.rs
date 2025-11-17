// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_core::encoding::ConsistencyCheckType;
use walrus_proc_macros::RestApiError;
use walrus_storage_node_client::api::errors::DAEMON_ERROR_DOMAIN;

use crate::common::api::RestApiError;

/// The consistency check options are invalid.
#[derive(Debug, thiserror::Error, RestApiError)]
#[error("cannot set both strict and skip consistency check options")]
#[rest_api_error(
    domain = DAEMON_ERROR_DOMAIN,
    reason = "INVALID_CONSISTENCY_CHECK",
    status = ApiStatusCode::InvalidArgument
)]
pub(crate) struct InvalidConsistencyCheck;

pub(crate) fn consistency_check_type_from_flags(
    strict_consistency_check: bool,
    skip_consistency_check: bool,
) -> Result<ConsistencyCheckType, InvalidConsistencyCheck> {
    Ok(match (strict_consistency_check, skip_consistency_check) {
        (true, false) => ConsistencyCheckType::Strict,
        (false, true) => ConsistencyCheckType::Skip,
        (true, true) => {
            return Err(InvalidConsistencyCheck);
        }
        (false, false) => ConsistencyCheckType::Default,
    })
}
