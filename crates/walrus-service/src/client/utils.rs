// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use walrus_core::encoding::ConsistencyCheckType;

pub(crate) fn consistency_check_type_from_flags(
    strict_consistency_check: bool,
    skip_consistency_check: bool,
) -> anyhow::Result<ConsistencyCheckType> {
    Ok(match (strict_consistency_check, skip_consistency_check) {
        (true, false) => ConsistencyCheckType::Strict,
        (false, true) => ConsistencyCheckType::Skip,
        (true, true) => anyhow::bail!("cannot set both strict and skip consistency check options"),
        (false, false) => ConsistencyCheckType::Default,
    })
}
