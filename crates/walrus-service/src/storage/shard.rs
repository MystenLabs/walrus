// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus shard storage.
//!
use std::{borrow::Borrow, sync::Arc};

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, MergeOperands, Options, DB};
use serde::{Deserialize, Serialize};
use typed_store::{
    rocks::{errors::typed_store_err_from_rocks_err, DBMap, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};
use walrus_core::{
    encoding::{EncodingAxis, PrimarySliver, SecondarySliver},
    BlobId,
    ShardIndex,
    Sliver,
    SliverType,
};

type PrimarySliverKey = SliverKey<true>;
type SecondarySliverKey = SliverKey<false>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Deserialize, Serialize)]
struct SliverKey<const P: bool>((bool, BlobId));

impl<const P: bool> SliverKey<P> {
    fn new(blob_id: BlobId) -> Self {
        Self((P, blob_id))
    }
}

impl<const P: bool, T: Borrow<BlobId>> From<T> for SliverKey<P> {
    fn from(value: T) -> Self {
        Self::new(*value.borrow())
    }
}

#[derive(Debug)]
pub(crate) struct ShardStorage {
    id: ShardIndex,
    primary_slivers: DBMap<PrimarySliverKey, PrimarySliver>,
    secondary_slivers: DBMap<SecondarySliverKey, SecondarySliver>,
}

impl ShardStorage {
    pub fn create_or_reopen(
        id: ShardIndex,
        database: &Arc<RocksDB>,
    ) -> Result<Self, TypedStoreError> {
        let rw_options = ReadWriteOptions::default();

        // Both primary and secondary slivers are written into the same column family,
        // and are differentiated by their keys.
        let cf_name = slivers_column_family_name(id);

        if database.cf_handle(&cf_name).is_none() {
            let (_, options) = Self::slivers_column_family_options(id);
            database
                .create_cf(&cf_name, &options)
                .map_err(typed_store_err_from_rocks_err)?;
        }

        // Open both typed storage as maps over the same column family.
        let primary_slivers = DBMap::reopen(database, Some(&cf_name), &rw_options)?;
        let secondary_slivers = DBMap::reopen(database, Some(&cf_name), &rw_options)?;

        Ok(Self {
            id,
            primary_slivers,
            secondary_slivers,
        })
    }

    /// Stores the provided primary or secondary sliver for the given blob ID.
    pub fn put_sliver(&self, blob_id: &BlobId, sliver: &Sliver) -> Result<(), TypedStoreError> {
        match sliver {
            Sliver::Primary(primary) => self.primary_slivers.insert(&blob_id.into(), primary),
            Sliver::Secondary(secondary) => {
                self.secondary_slivers.insert(&blob_id.into(), secondary)
            }
        }
    }

    pub fn id(&self) -> ShardIndex {
        self.id
    }

    /// Returns the sliver of the specified type that is stored for that Blob ID, if any.
    pub fn get_sliver(
        &self,
        blob_id: &BlobId,
        sliver_type: SliverType,
    ) -> Result<Option<Sliver>, TypedStoreError> {
        match sliver_type {
            SliverType::Primary => self
                .get_primary_sliver(blob_id)
                .map(|s| s.map(Sliver::Primary)),
            SliverType::Secondary => self
                .get_secondary_sliver(blob_id)
                .map(|s| s.map(Sliver::Secondary)),
        }
    }

    /// Retrieves the stored primary sliver for the given blob ID.
    pub fn get_primary_sliver(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<PrimarySliver>, TypedStoreError> {
        self.primary_slivers.get(&blob_id.into())
    }

    /// Retrieves the stored secondary sliver for the given blob ID.
    pub fn get_secondary_sliver(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<SecondarySliver>, TypedStoreError> {
        self.secondary_slivers.get(&blob_id.into())
    }

    /// Returns true iff the sliver-pair for the given blob ID is stored by the shard.
    pub fn is_sliver_pair_stored(&self, blob_id: &BlobId) -> Result<bool, TypedStoreError> {
        Ok(self.primary_slivers.contains_key(&blob_id.into())?
            && self.secondary_slivers.contains_key(&blob_id.into())?)
    }

    fn slivers_column_family_options(id: ShardIndex) -> (String, Options) {
        // TODO(jsmith): Optimize for sliver storage (#65).
        let mut options = Options::default();
        options.set_enable_blob_files(true);

        (slivers_column_family_name(id), options)
    }
}

#[inline]
fn base_column_family_name(id: ShardIndex) -> String {
    format!("shard-{}", id.0)
}

#[inline]
fn slivers_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + "/slivers"
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use walrus_core::{Sliver, SliverType};
    use walrus_test_utils::{async_param_test, param_test, Result as TestResult, WithTempDir};

    use crate::storage::tests::{
        empty_storage,
        empty_storage_with_shards,
        get_sliver,
        BLOB_ID,
        OTHER_SHARD_INDEX,
        SHARD_INDEX,
    };

    async_param_test! {
        can_store_and_retrieve_sliver -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn can_store_and_retrieve_sliver(sliver_type: SliverType) -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let sliver = get_sliver(sliver_type, 1);

        shard.put_sliver(&BLOB_ID, &sliver)?;
        let retrieved = shard.get_sliver(&BLOB_ID, sliver_type)?;

        assert_eq!(retrieved, Some(sliver));

        Ok(())
    }

    #[tokio::test]
    async fn stores_separate_primary_and_secondary_sliver() -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        let primary = get_sliver(SliverType::Primary, 1);
        let secondary = get_sliver(SliverType::Secondary, 2);

        shard.put_sliver(&BLOB_ID, &primary)?;
        shard.put_sliver(&BLOB_ID, &secondary)?;

        let retrieved_primary = shard.get_sliver(&BLOB_ID, SliverType::Primary)?;
        let retrieved_secondary = shard.get_sliver(&BLOB_ID, SliverType::Secondary)?;

        assert_eq!(retrieved_primary, Some(primary), "invalid primary sliver");
        assert_eq!(
            retrieved_secondary,
            Some(secondary),
            "invalid secondary sliver"
        );

        Ok(())
    }

    async_param_test! {
        stores_and_retrieves_for_multiple_shards -> TestResult: [
            primary_primary: (SliverType::Primary, SliverType::Primary),
            secondary_secondary: (SliverType::Secondary, SliverType::Secondary),
            mixed: (SliverType::Primary, SliverType::Secondary),
        ]
    }
    async fn stores_and_retrieves_for_multiple_shards(
        type_first: SliverType,
        type_second: SliverType,
    ) -> TestResult {
        let storage = empty_storage_with_shards(&[SHARD_INDEX, OTHER_SHARD_INDEX]);

        let first_shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let first_sliver = get_sliver(type_first, 1);

        let second_shard = storage.as_ref().shard_storage(OTHER_SHARD_INDEX).unwrap();
        let second_sliver = get_sliver(type_second, 2);

        first_shard.put_sliver(&BLOB_ID, &first_sliver)?;
        second_shard.put_sliver(&BLOB_ID, &second_sliver)?;

        let first_retrieved = first_shard.get_sliver(&BLOB_ID, type_first)?;
        let second_retrieved = second_shard.get_sliver(&BLOB_ID, type_second)?;

        assert_eq!(
            first_retrieved,
            Some(first_sliver),
            "invalid sliver from first shard"
        );
        assert_eq!(
            second_retrieved,
            Some(second_sliver),
            "invalid sliver from second shard"
        );

        Ok(())
    }

    async_param_test! {
        indicates_when_sliver_pair_is_stored -> TestResult: [
            neither: (false, false),
            only_primary: (true, false),
            only_secondary: (false, true),
            both: (true, true),
        ]
    }
    async fn indicates_when_sliver_pair_is_stored(
        store_primary: bool,
        store_secondary: bool,
    ) -> TestResult {
        let is_pair_stored: bool = store_primary & store_secondary;

        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        if store_primary {
            shard.put_sliver(&BLOB_ID, &get_sliver(SliverType::Primary, 3))?;
        }
        if store_secondary {
            shard.put_sliver(&BLOB_ID, &get_sliver(SliverType::Secondary, 4))?;
        }

        assert_eq!(shard.is_sliver_pair_stored(&BLOB_ID)?, is_pair_stored);

        Ok(())
    }
}
