// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Bookkeeping for moving RocksDB sliver column families into Strata on a running node.
//!
//! A node that switches its sliver backend from RocksDB to Strata keeps the existing per-shard
//! sliver column families as a read fallback. New writes go to Strata only. A row in this table
//! exists for every shard that still has legacy column families and says whether they may still
//! serve reads. The row is removed together with the column families.

use std::sync::Arc;

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, ShardIndex, SliverType};

use super::DatabaseTableOptionsFactory;

pub(crate) const STRATA_MIGRATION_CF: &str = "strata_migration";

/// Progress of copying one shard's legacy RocksDB slivers into Strata.
// Important: this enum is committed to database. Do not modify the existing variants. Only add
// new variants at the end.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ShardMigrationState {
    /// Legacy column families serve reads for keys that are not in Strata. `next` is the first
    /// legacy key the background copy has not committed yet; `None` means it has not started.
    Copying { next: Option<(SliverType, BlobId)> },
    /// Every legacy sliver that is still registered is in Strata. Reads no longer fall back; the
    /// legacy column families are only kept until they are dropped.
    Done,
}

impl ShardMigrationState {
    /// Whether reads may still be answered from the legacy column families.
    pub(crate) fn serves_reads(&self) -> bool {
        matches!(self, Self::Copying { .. })
    }
}

/// Per-shard migration state, keyed by shard index.
#[derive(Debug, Clone)]
pub(crate) struct StrataMigrationTable {
    states: DBMap<ShardIndex, ShardMigrationState>,
}

impl StrataMigrationTable {
    pub(super) fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        Ok(Self {
            states: DBMap::reopen(
                database,
                Some(STRATA_MIGRATION_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
        })
    }

    pub(super) fn options(factory: &DatabaseTableOptionsFactory) -> (&'static str, Options) {
        (STRATA_MIGRATION_CF, factory.standard())
    }

    pub(crate) fn get(
        &self,
        shard: ShardIndex,
    ) -> Result<Option<ShardMigrationState>, TypedStoreError> {
        self.states.get(&shard)
    }

    pub(crate) fn set(
        &self,
        shard: ShardIndex,
        state: &ShardMigrationState,
    ) -> Result<(), TypedStoreError> {
        self.states.insert(&shard, state)
    }

    pub(crate) fn remove(&self, shard: ShardIndex) -> Result<(), TypedStoreError> {
        self.states.remove(&shard)
    }

    pub(crate) fn shards(&self) -> Result<Vec<ShardIndex>, TypedStoreError> {
        self.states
            .safe_iter()?
            .map(|entry| entry.map(|(shard, _)| shard))
            .collect()
    }
}
