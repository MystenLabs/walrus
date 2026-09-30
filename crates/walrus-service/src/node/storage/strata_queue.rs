// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Attach Strata's pending-operation tables to the Walrus control database.
//! Walrus supplies event indexes, filters replays, and completes fan-out before publishing
//! barriers. Reference checks and shared blob coordination remain Walrus responsibilities.
//! This milestone does not connect event producers, run a worker, or change foreground puts.

use std::sync::Arc;

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use strata_index::queue::PendingQueue;
use sui_types::event::EventID;
use typed_store::rocks::RocksDB;

use super::DatabaseTableOptionsFactory;

mod database;

pub(crate) type StrataQueue = PendingQueue;

pub(super) fn options(factory: &DatabaseTableOptionsFactory) -> [(&'static str, Options); 2] {
    strata_index::queue::cf_options(factory.standard())
}

pub(super) fn reopen(database: &Arc<RocksDB>) -> StrataQueue {
    PendingQueue::new(Arc::new(database::Database(Arc::clone(database))))
}

/// Application provenance, encoded as opaque bytes in generic queue commands.
/// The event index also identifies the lifetime initialization that a foreground put waits for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataSourceEvent {
    pub event_index: u64,
    pub event_id: EventID,
}

#[cfg(test)]
mod tests;
