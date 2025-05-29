// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The configuration for the fan-out proxy's tipping system.

use serde::{Deserialize, Serialize};
use sui_types::base_types::SuiAddress;
use utoipa::ToSchema;
use walrus_sui::SuiAddressSchema;

/// The kinds of tip that the proxy can choose to configure.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TipKind {
    /// A constant tip.
    Const(u64),
    /// A tip that linearly depends on the encoded size of the blob.
    // TODO: make this linear in the KB or MB size of the blob?
    Linear { base: u64, encoded_size_mul: u64 },
}

impl TipKind {
    /// Returns the tip required for a blob of the given size, or `None` if no tip is required.
    pub(crate) fn compute_tip(&self, encoded_size: u64) -> u64 {
        match self {
            TipKind::Const(constant) => *constant,
            TipKind::Linear {
                base,
                encoded_size_mul,
            } => base + encoded_size * encoded_size_mul,
        }
    }
}

/// The configuration for the tips of to the proxy.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TipConfig {
    /// The publisher does not require tips.
    NoTip,
    /// The address to which to pay the tip, and the tip computation.
    SendTip {
        #[schema(value_type = SuiAddressSchema)]
        address: SuiAddress,
        kind: TipKind,
    },
}
