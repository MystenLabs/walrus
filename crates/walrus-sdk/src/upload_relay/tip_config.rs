// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The configuration for the Walrus Upload Relay's tipping system.

use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};
use sui_types::base_types::SuiAddress;
use utoipa::ToSchema;

use crate::{
    core::{EncodingType, encoding::encoded_blob_length_for_n_shards},
    sui::SuiAddressSchema,
};

/// The kinds of tip that the proxy can choose to configure.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(examples(
        json!(TipKind::Const(31415)),
        json!(TipKind::Linear{base: 101, encoded_size_mul_per_kib: 42})
))]
pub enum TipKind {
    /// A constant tip.
    Const(u64),
    /// A tip that linearly depends on the encoded size of the blob.
    ///
    /// If `encoded_size` is the size of the encoded data in KiB (rounded up), then the final tip is
    /// `base + encoded_size * encoded_size_mul_per_kib`.
    Linear {
        /// The base tip amount to be charged for each upload.
        base: u64,
        /// The amount of tip charged per kilobyte of the encoded blob size. Note that this adds to
        /// the base tip, so the total tip for a blob is:
        /// `base + encoded_size_mul_per_kib * ⌊encoded_size_in_bytes / 1024⌋`.
        encoded_size_mul_per_kib: u64,
    },
}

impl TipKind {
    /// Returns the tip required for a blob of the given size, or `None` if no tip is required.
    ///
    /// Returns None if the blob size cannot be computed.
    pub fn compute_tip(
        &self,
        n_shards: NonZeroU16,
        unencoded_length: u64,
        encoding_type: EncodingType,
    ) -> Option<u64> {
        Some(match self {
            TipKind::Const(constant) => *constant,
            TipKind::Linear {
                base,
                encoded_size_mul_per_kib,
            } => {
                base + encoded_blob_length_for_n_shards(n_shards, unencoded_length, encoding_type)?
                    .div_ceil(1024)
                    * encoded_size_mul_per_kib
            }
        })
    }
}

/// The configuration for the tips to the relay.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TipConfig {
    /// The publisher does not require tips.
    NoTip,
    /// The address to which to the tip is paid and the tip computation.
    SendTip {
        /// The address to which a tip.
        #[schema(value_type = SuiAddressSchema)]
        address: SuiAddress,
        /// The formula to compute the tip.
        kind: TipKind,
    },
}

impl TipConfig {
    /// Checks if the tip config requires payment; returns `false` if no tip is required.
    pub fn requires_payment(&self) -> bool {
        matches!(self, TipConfig::SendTip { .. })
    }
}
