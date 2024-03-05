#![allow(unused)]

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use anyhow::Context;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};
use typed_store::{
    rocks::{self, DBMap, MetricConf, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};
use walrus_core::BlobId;

use self::shard::ShardStorage;
use crate::config::ShardIndex;

mod shard;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata(Vec<[u8; 32]>);

/// Storage backing a [`StorageNode`][crate::StorageNode].
///
/// Enables storing blob metadata, which is shared across all shards. The method
/// [`shard_storage()`][Self::shard_storage] can be used to retrieve shard-specific storage.
#[derive(Debug)]
pub(crate) struct Storage {
    database: Arc<RocksDB>,
    metadata: DBMap<BlobId, Metadata>,
    shards: HashMap<ShardIndex, ShardStorage>,
}

impl Storage {
    const METADATA_COLUMN_FAMILY_NAME: &'static str = "metadata";

    /// Opens the storage database located at the specified path, creating the database if absent.
    pub fn open(path: &Path, metrics_config: MetricConf) -> Result<Self, anyhow::Error> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let (metadata_cf_name, metadata_options) = Self::metadata_options();
        let database = rocks::open_cf_opts(
            path,
            Some(db_opts),
            metrics_config,
            &[(metadata_cf_name, metadata_options)],
        )?;
        let metadata = DBMap::reopen(
            &database,
            Some(metadata_cf_name),
            &ReadWriteOptions::default(),
        )?;

        Ok(Self {
            database,
            metadata,
            shards: HashMap::default(),
        })
    }

    /// Creates storage for the specified shard, and returns it, or just returns the shard's storage
    /// if it already exists.
    pub fn create_storage_for_shard(
        &mut self,
        shard: ShardIndex,
    ) -> Result<&ShardStorage, TypedStoreError> {
        match self.shards.entry(shard) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let shard_storage = ShardStorage::create_or_reopen(shard, &self.database)?;
                Ok(entry.insert(shard_storage))
            }
        }
    }

    /// Returns a handle over the storage for a single shard.
    pub fn shard_storage(&self, shard: ShardIndex) -> Option<&ShardStorage> {
        self.shards.get(&shard)
    }

    /// Store the metadata associated with the provided blob_id.
    pub fn put_metadata(
        &self,
        blob_id: &BlobId,
        metadata: &Metadata,
    ) -> Result<(), TypedStoreError> {
        self.metadata.insert(blob_id, metadata)
    }

    /// Gets the metadata for a given [`BlobId`] or None.
    pub fn get_metadata(&self, blob_id: &BlobId) -> Result<Option<Metadata>, TypedStoreError> {
        self.metadata.get(blob_id)
    }

    /// Returns an iterator over the identifiers of the shards that store their respecitive sliver
    /// for the specified blob.
    pub fn shards_with_sliver_pairs(
        &self,
        blob_id: &BlobId,
    ) -> Result<impl Iterator<Item = ShardIndex>, anyhow::Error> {
        let mut shards_with_sliver_pairs = Vec::with_capacity(self.shards.len());

        for shard in self.shards.values() {
            if shard.is_sliver_pair_stored(blob_id)? {
                shards_with_sliver_pairs.push(shard.id());
            }
        }

        Ok(shards_with_sliver_pairs.into_iter())
    }

    fn metadata_options() -> (&'static str, Options) {
        let mut options = Options::default();

        // TODO(jsmith): Tune storage for metadata and slivers (#65)
        options.set_enable_blob_files(true);

        (Self::METADATA_COLUMN_FAMILY_NAME, options)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{fmt, time::Duration};

    use prometheus::Registry;
    use typed_store::metrics::SamplingInterval;
    use walrus_core::{
        encoding::{EncodingAxis, Primary, Secondary, Sliver as TypedSliver},
        Sliver,
        SliverType,
    };
    use walrus_test_utils::{param_test, Result as TestResult, WithTempDir};

    use super::*;

    type StorageSpec<'a> = &'a [(ShardIndex, Vec<(BlobId, WhichSlivers)>)];

    pub(crate) enum WhichSlivers {
        Primary,
        Secondary,
        Both,
    }

    pub(crate) const BLOB_ID: BlobId = [7; 32];
    pub(crate) const SHARD_INDEX: ShardIndex = 17;
    pub(crate) const OTHER_SHARD_INDEX: ShardIndex = 831;

    /// Returns an empty storage, with the column families for [`SHARD_INDEX`] already created.
    pub(crate) fn empty_storage() -> WithTempDir<Storage> {
        typed_store::metrics::DBMetrics::init(&Registry::new());
        empty_storage_with_shards(&[SHARD_INDEX])
    }

    /// Returns an empty storage, with the column families for the specified shards already created.
    pub(crate) fn empty_storage_with_shards(shards: &[ShardIndex]) -> WithTempDir<Storage> {
        let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
        let mut storage = Storage::open(temp_dir.path(), MetricConf::default())
            .expect("storage creation must succeed");

        for shard in shards {
            storage
                .create_storage_for_shard(*shard)
                .expect("shard should be successfully created");
        }

        WithTempDir {
            inner: storage,
            temp_dir,
        }
    }

    /// Returns an arbitrary metadata object.
    pub(crate) fn arbitrary_metadata() -> Metadata {
        Metadata((0..100u8).map(|i| [i; 32]).collect())
    }

    pub(crate) fn get_typed_sliver<E: EncodingAxis>(seed: u8) -> TypedSliver<E> {
        TypedSliver::new(vec![seed; seed as usize * 512], 16)
    }

    pub(crate) fn get_sliver(sliver_type: SliverType, seed: u8) -> Sliver {
        match sliver_type {
            SliverType::Primary => Sliver::Primary(get_typed_sliver(seed)),
            SliverType::Secondary => Sliver::Secondary(get_typed_sliver(seed)),
        }
    }

    pub(crate) fn populated_storage(spec: StorageSpec) -> TestResult<WithTempDir<Storage>> {
        let mut storage = empty_storage();

        let mut seed = 10u8;
        for (shard, sliver_list) in spec {
            storage.as_mut().create_storage_for_shard(*shard)?;
            let shard_storage = storage.as_ref().shard_storage(*shard).unwrap();

            for (blob_id, which) in sliver_list.iter() {
                if matches!(*which, WhichSlivers::Primary | WhichSlivers::Both) {
                    shard_storage.put_sliver(blob_id, &get_sliver(SliverType::Primary, seed))?;
                    seed += 1;
                }
                if matches!(*which, WhichSlivers::Secondary | WhichSlivers::Both) {
                    shard_storage.put_sliver(blob_id, &get_sliver(SliverType::Secondary, seed))?;
                    seed += 1;
                }
            }
        }

        Ok(storage)
    }

    #[tokio::test]
    async fn can_write_then_read_metadata() -> TestResult {
        let storage = empty_storage();
        let storage = storage.as_ref();
        let metadata = arbitrary_metadata();

        storage.put_metadata(&BLOB_ID, &metadata)?;
        let retrieved = storage.get_metadata(&BLOB_ID)?;

        assert_eq!(retrieved, Some(metadata));

        Ok(())
    }

    mod shards_with_sliver_pairs {
        use walrus_test_utils::async_param_test;

        use super::*;

        async_param_test! {
            returns_shard_if_it_stores_both -> TestResult: [
                both: (WhichSlivers::Both, true),
                only_primary: (WhichSlivers::Primary, false),
                only_secondary: (WhichSlivers::Secondary, false),
            ]
        }
        async fn returns_shard_if_it_stores_both(
            which: WhichSlivers,
            is_retrieved: bool,
        ) -> TestResult {
            let storage = populated_storage(&[(SHARD_INDEX, vec![(BLOB_ID, which)])])?;

            let result: Vec<_> = storage
                .as_ref()
                .shards_with_sliver_pairs(&BLOB_ID)?
                .collect();

            if is_retrieved {
                assert_eq!(result, &[SHARD_INDEX]);
            } else {
                assert!(result.is_empty());
            }

            Ok(())
        }

        #[tokio::test]
        async fn identifies_all_shards_storing_sliver_pairs() -> TestResult {
            let storage = populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])?;

            let mut result: Vec<_> = storage
                .as_ref()
                .shards_with_sliver_pairs(&BLOB_ID)?
                .collect();

            result.sort();

            assert_eq!(result, [SHARD_INDEX, OTHER_SHARD_INDEX]);

            Ok(())
        }

        #[tokio::test]
        async fn ignores_shards_without_both_sliver_pairs() -> TestResult {
            let storage = populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Primary)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])?;

            let result: Vec<_> = storage
                .as_ref()
                .shards_with_sliver_pairs(&BLOB_ID)?
                .collect();

            assert_eq!(result, [OTHER_SHARD_INDEX]);

            Ok(())
        }
    }
}
