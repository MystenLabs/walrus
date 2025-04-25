// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The Walrus Rust SDK.

pub mod active_committees;
pub mod blocklist;
pub mod client;
pub mod config;
pub mod error;
pub mod store_when;
pub mod utils;
pub mod walrus;

pub use sui_types::event::EventID;
pub use walrus_sui as sui;

/// Format the event ID as the transaction digest and the sequence number.
pub fn format_event_id(event_id: &EventID) -> String {
    format!("(tx: {}, seq: {})", event_id.tx_digest, event_id.event_seq)
}
