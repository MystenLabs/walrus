// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Manages the storage and blob resources in the Wallet on behalf of the client.

use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};
use walrus_core::EpochCount;
use walrus_sui::utils::price_for_unencoded_length;

#[derive(Debug, Clone)]
pub(crate) struct PriceComputation {
    write_price_per_unit_size: u64,
    storage_price_per_unit_size: u64,
    n_shards: NonZeroU16,
}

/// Struct to compute the cost of operations with blob and storage resources.
impl PriceComputation {
    pub(crate) fn new(
        write_price_per_unit_size: u64,
        storage_price_per_unit_size: u64,
        n_shards: NonZeroU16,
    ) -> Self {
        Self {
            write_price_per_unit_size,
            storage_price_per_unit_size,
            n_shards,
        }
    }

    /// Computes the cost of the operation.
    ///
    /// Returns `None` if `unencoded_length` is invalid for the current encoding and `n_shards`, and
    /// therefore the encoded length cannot be computed.
    pub(crate) fn operation_cost(&self, operation: &ResourceOperation) -> Option<u64> {
        match operation {
            ResourceOperation::RegisterFromScratch {
                unencoded_length,
                epochs_ahead,
            } => self
                .storage_fee_for_unencoded_length(*unencoded_length, *epochs_ahead)
                .map(|storage_fee| {
                    storage_fee + self.write_fee_for_unencoded_length(*unencoded_length)
                }),
            ResourceOperation::ReuseStorage { unencoded_length } => {
                Some(self.write_fee_for_unencoded_length(*unencoded_length))
            }
            ResourceOperation::ReuseRegistration => Some(0), // No cost for reusing registration
        }
    }

    /// Computes the write fee for the given unencoded length.
    pub fn write_fee_for_unencoded_length(&self, unencoded_length: u64) -> u64 {
        // The write price is independent of the number of epochs, hence the `1`.
        self.price_for_unencoded_length(unencoded_length, self.write_price_per_unit_size, 1)
            .expect("the price computation should not fail")
    }

    /// Computes the storage fee given the unencoded blob size and the number of epochs.
    pub fn storage_fee_for_unencoded_length(
        &self,
        unencoded_length: u64,
        epochs: EpochCount,
    ) -> Option<u64> {
        self.price_for_unencoded_length(unencoded_length, self.storage_price_per_unit_size, epochs)
    }

    fn price_for_unencoded_length(
        &self,
        unencoded_length: u64,
        price_per_unit_size: u64,
        epochs: EpochCount,
    ) -> Option<u64> {
        price_for_unencoded_length(unencoded_length, self.n_shards, price_per_unit_size, epochs)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceOperation {
    /// The storage and blob resources are purchased from scratch.
    RegisterFromScratch {
        unencoded_length: u64,
        epochs_ahead: EpochCount,
    },
    /// The storage is reused, but the blob was not registered.
    ReuseStorage { unencoded_length: u64 },
    /// A registration was already present.
    ReuseRegistration,
}
