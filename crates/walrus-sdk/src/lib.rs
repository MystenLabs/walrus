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

pub use sui_types::{base_types::ObjectID, event::EventID};
pub use walrus_core as core;
pub use walrus_sui as sui;
pub use walrus_sui::{client::SuiReadClient, config::load_wallet_context_from_path};
pub use walrus_utils as core_utils;

/// Format the event ID as the transaction digest and the sequence number.
pub fn format_event_id(event_id: &EventID) -> String {
    format!("(tx: {}, seq: {})", event_id.tx_digest, event_id.event_seq)
}
