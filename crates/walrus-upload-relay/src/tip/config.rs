// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The configuration for the Walrus Upload Relay's tipping system.

use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};
use sui_types::base_types::SuiAddress;
use utoipa::ToSchema;
use walrus_sdk::{
    core::{EncodingType, encoding::encoded_blob_length_for_n_shards},
    sui::SuiAddressSchema,
};

/// The kinds of tip that the proxy can choose to configure.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(examples(
        json!(TipKind::Const(31415)),
        json!(TipKind::Linear{base: 101, encoded_size_mul: 42})
))]
pub(crate) enum TipKind {
    /// A constant tip.
    Const(u64),
    /// A tip that linearly depends on the encoded size of the blob.
    // TODO: make this linear in the KB or MB size of the blob?
    Linear { base: u64, encoded_size_mul: u64 },
}

impl TipKind {
    /// Returns the tip required for a blob of the given size, or `None` if no tip is required.
    ///
    /// Returns None if the blob size cannot be computed.
    pub(crate) fn compute_tip(
        &self,
        n_shards: NonZeroU16,
        unencoded_length: u64,
        encoding_type: EncodingType,
    ) -> Option<u64> {
        Some(match self {
            TipKind::Const(constant) => *constant,
            TipKind::Linear {
                base,
                encoded_size_mul,
            } => {
                base + encoded_blob_length_for_n_shards(n_shards, unencoded_length, encoding_type)?
                    * encoded_size_mul
            }
        })
    }
}

/// The configuration for the tips to the proxy.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TipConfig {
    /// The publisher does not require tips.
    NoTip,
    /// The address to which to the tip is paid and the tip computation.
    SendTip {
        #[schema(value_type = SuiAddressSchema)]
        address: SuiAddress,
        kind: TipKind,
    },
}

impl TipConfig {
    /// Checks if the tip config requires payment; returns `false` if no tip is required.
    pub(crate) fn requires_payment(&self) -> bool {
        matches!(self, TipConfig::SendTip { .. })
    }
}
