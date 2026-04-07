// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Protocol-agnostic transaction types for Walrus.
//!
//! These types replace `sui_sdk::rpc_types` transaction types so that Walrus code does not depend
//! on SDK-specific representations. Only fields that Walrus actually uses are included.

use move_core_types::language_storage::StructTag;
use sui_types::{
    TypeTag,
    base_types::{ObjectID, SuiAddress},
    digests::TransactionDigest,
};

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

/// The execution status of a transaction's effects.
#[derive(Debug, Clone)]
pub enum TransactionEffectsStatus {
    /// The transaction executed successfully.
    Success,
    /// The transaction failed with the given error.
    Failure {
        /// The error message.
        error: String,
    },
}

/// A protocol-agnostic object change from a transaction.
#[derive(Debug, Clone)]
pub enum ObjectChangeEntry {
    /// A package was published.
    Published {
        /// The package ID.
        package_id: ObjectID,
        /// The version.
        version: u64,
    },
    /// An object was created.
    Created {
        /// The sender of the transaction.
        sender: SuiAddress,
        /// The object type.
        object_type: StructTag,
        /// The object ID.
        object_id: ObjectID,
    },
    /// An object was mutated.
    Mutated {
        /// The sender of the transaction.
        sender: SuiAddress,
        /// The object type.
        object_type: StructTag,
        /// The object ID.
        object_id: ObjectID,
    },
    /// An object was deleted.
    Deleted {
        /// The sender of the transaction.
        sender: SuiAddress,
        /// The object type.
        object_type: StructTag,
        /// The object ID.
        object_id: ObjectID,
    },
    /// An object was wrapped.
    Wrapped {
        /// The sender of the transaction.
        sender: SuiAddress,
        /// The object type.
        object_type: StructTag,
        /// The object ID.
        object_id: ObjectID,
    },
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
    /// The execution status of the transaction effects.
    pub effects_status: Option<TransactionEffectsStatus>,
    /// Object changes from the transaction.
    pub object_changes: Option<Vec<ObjectChangeEntry>>,
    /// Errors from the transaction.
    pub errors: Vec<String>,
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
    /// Whether to include effects.
    pub show_effects: bool,
    /// Whether to include object changes.
    pub show_object_changes: bool,
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

    /// Enables the effects field.
    pub fn with_effects(mut self) -> Self {
        self.show_effects = true;
        self
    }

    /// Enables the object changes field.
    pub fn with_object_changes(mut self) -> Self {
        self.show_object_changes = true;
        self
    }
}
