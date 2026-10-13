// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use sui_types::crypto::AuthorityPublicKeyBytes;

/// A protocol-agnostic committee info response.
#[derive(Debug, Clone)]
pub struct SuiCommitteeInfo {
    /// The epoch number for which this committee is valid.
    pub epoch: u64,
    /// The validators in the committee, each with their public key and voting weight.
    pub validators: Vec<(AuthorityPublicKeyBytes, u64)>,
}
