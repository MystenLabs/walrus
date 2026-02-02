// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A module defining the Coin struct and its associated methods.
use sui_types::base_types::{ObjectDigest, ObjectID, ObjectRef, SequenceNumber, TransactionDigest};

/// Represents a Coin object with its essential details.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Coin {
    /// The type of the coin (e.g., "0x2::sui::SUI").
    pub coin_type: String,
    /// The unique identifier of the coin object.
    pub coin_object_id: ObjectID,
    /// The version number of the coin object.
    pub version: SequenceNumber,
    /// The digest of the coin object.
    pub digest: ObjectDigest,
    /// The balance held by this coin object.
    pub balance: u64,
    /// The digest of the previous transaction that modified this coin.
    pub previous_transaction: TransactionDigest,
}

impl Coin {
    /// Returns the ObjectRef for this coin object.
    pub fn object_ref(&self) -> ObjectRef {
        (self.coin_object_id, self.version, self.digest)
    }
}

impl From<sui_sdk::rpc_types::Coin> for Coin {
    fn from(coin: sui_sdk::rpc_types::Coin) -> Self {
        Self {
            coin_type: coin.coin_type,
            coin_object_id: coin.coin_object_id,
            version: coin.version,
            digest: coin.digest,
            balance: coin.balance,
            previous_transaction: coin.previous_transaction,
        }
    }
}
