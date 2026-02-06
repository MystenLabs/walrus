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
    /// The SUI coin type string. Note that this is NOT the full object_type, which is
    /// 0x2::coin::Coin<0x2::sui::SUI>.
    pub const SUI: &str = "0x2::sui::SUI";
    // NB: WAL cannot be specified statically because the package address varies across networks.

    /// Returns the ObjectRef for this coin object.
    pub fn object_ref(&self) -> ObjectRef {
        (self.coin_object_id, self.version, self.digest)
    }

    /// Formats the full object type for a given coin type.
    pub fn format_object_type(coin_type: &str) -> String {
        #[cfg(test)]
        assert!(!coin_type.starts_with("0x::coin::Coin"));
        format!("0x2::coin::Coin<{}>", coin_type)
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

/// The type of coin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoinType {
    /// The WAL coin type.
    Wal,
    /// The SUI coin type.
    Sui,
}

impl CoinType {
    /// Returns the string representation of the coin type.
    pub fn as_str(self, wal: &str) -> &str {
        match self {
            CoinType::Wal => wal,
            CoinType::Sui => Coin::SUI,
        }
    }
}
