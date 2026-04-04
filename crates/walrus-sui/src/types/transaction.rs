// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Protocol-agnostic transaction types for Walrus.
//!
//! These types replace `sui_sdk::rpc_types` transaction types so that Walrus code does not depend
//! on SDK-specific representations. Only fields that Walrus actually uses are included.

use sui_types::{TypeTag, base_types::SuiAddress, digests::TransactionDigest};

use super::events::EventEnvelope;

/// A protocol-agnostic balance change from a transaction.
#[derive(Debug, Clone)]
pub struct BalanceChange {
    /// The address affected by the balance change.
    pub address: SuiAddress,
    /// The coin type.
    pub coin_type: TypeTag,
    /// The signed amount of the change.
    pub amount: i128,
}

/// A protocol-agnostic transaction query response containing only the fields Walrus uses.
#[derive(Debug, Clone)]
pub struct TransactionResponse {
    /// The transaction digest.
    pub digest: TransactionDigest,
    /// The checkpoint number.
    pub checkpoint: Option<u64>,
    /// The timestamp in milliseconds.
    pub timestamp_ms: Option<u64>,
    /// The BCS-encoded `SenderSignedData`.
    pub raw_transaction: Vec<u8>,
    /// Balance changes from the transaction.
    pub balance_changes: Option<Vec<BalanceChange>>,
    /// Events emitted by the transaction.
    pub events: Option<Vec<EventEnvelope>>,
}

/// Options controlling which fields to include in a [`TransactionResponse`].
#[derive(Debug, Clone, Default)]
pub struct TransactionResponseOptions {
    /// Whether to include the raw transaction input.
    pub show_raw_input: bool,
    /// Whether to include balance changes.
    pub show_balance_changes: bool,
    /// Whether to include events.
    pub show_events: bool,
}

impl TransactionResponseOptions {
    /// Creates a new [`TransactionResponseOptions`] with all fields disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables the raw transaction input field.
    pub fn with_raw_input(mut self) -> Self {
        self.show_raw_input = true;
        self
    }

    /// Enables the balance changes field.
    pub fn with_balance_changes(mut self) -> Self {
        self.show_balance_changes = true;
        self
    }

    /// Enables the events field.
    pub fn with_events(mut self) -> Self {
        self.show_events = true;
        self
    }
}
