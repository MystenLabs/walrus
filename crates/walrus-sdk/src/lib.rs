// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The Walrus Rust SDK.

/// The configuration of the Walrus client.
pub mod active_committees;
/// Configuration.
pub mod config;
pub mod error;
pub mod refresh;
pub mod responses;
pub mod store_when;
/// Utilities for the Walrus SDK.
pub mod utils;

pub use sui_types::event::EventID;

/// Format the event ID as the transaction digest and the sequence number.
pub fn format_event_id(event_id: &EventID) -> String {
    format!("(tx: {}, seq: {})", event_id.tx_digest, event_id.event_seq)
}
