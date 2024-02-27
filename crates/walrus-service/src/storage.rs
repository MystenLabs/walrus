#![allow(unused)]
use std::{collections::HashSet, path::Path};

use anyhow::Context;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, DB};
use walrus_core::BlobId;

use crate::config::ShardIndex;

pub type Metadata = Vec<[u8; 32]>;

/// Storage backing a [`StorageNode`][crate::StorageNode].
///
/// Enables storing blob metadata, which is shared across all shards. The method
/// [`shard_storage()`][Self::shard_storage] can be used to retrieve shard-specific storage.
pub(crate) struct Storage {
    database: DB,
    shards: HashSet<ShardIndex>,
}

impl Storage {
    const METADATA_COLUMN_FAMILY_NAME: &'static str = "metadata";

    /// Opens the storage database located at the specified path, creating the database if absent.
    pub fn open(path: &Path) -> Result<Self, anyhow::Error> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let database = DB::open_cf_descriptors(&db_opts, path, [Self::metadata_descriptor()])
            .context("storage unable to open database")?;

        Ok(Self {
            database,
            shards: HashSet::default(),
        })
    }

    /// Creates storage for the specified shard, or does nothing if the storage already exists.
    pub fn create_storage_for_shard(&mut self, shard: ShardIndex) -> Result<(), anyhow::Error> {
        if !self.shards.contains(&shard) {
            ShardStorage::create(shard, &mut self.database)?;
            self.shards.insert(shard);
        }

        Ok(())
    }

    /// Returns a handle over the storage for a single shard.
    pub fn shard_storage(&self, shard: ShardIndex) -> Option<ShardStorage> {
        self.shards
            .contains(&shard)
            .then(|| ShardStorage::new(shard, &self.database))
    }

    /// Store the metadata associated with the provided blob_id.
    pub fn put_metadata(&self, blob_id: BlobId, metadata: &Metadata) -> Result<(), anyhow::Error> {
        // TODO(jsmith): Guarantee that serialization of metadata representation is
        // within bcs::MAX_SEQUENCE_LENGTH and otherwise infallible.
        let encoded_metadata =
            bcs::to_bytes(metadata).expect("metadata should be always serializable");

        self.database
            .put_cf(self.metadata_handle(), blob_id, encoded_metadata)
            .context("unable to put metadata")
    }

    /// Gets the metadata for a given [`BlobId`] or None.
    pub fn get_metadata(&self, blob_id: &BlobId) -> Result<Option<Metadata>, anyhow::Error> {
        self.database
            .get_pinned_cf(self.metadata_handle(), blob_id)
            .context("error retrieving metadata")?
            .map(|raw| bcs::from_bytes(raw.as_ref()).context("failed to decode metadata"))
            .transpose()
    }

    fn descriptors(shards: &[ShardIndex]) -> Vec<ColumnFamilyDescriptor> {
        let mut descriptors = vec![Self::metadata_descriptor()];
        descriptors.extend(shards.iter().map(|shard| ShardStorage::descriptors(*shard)));

        descriptors
    }

    fn metadata_descriptor() -> ColumnFamilyDescriptor {
        let mut options = Options::default();

        // TODO(jsmith): Tune storage for metadata and slivers
        // See https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html
        options.set_enable_blob_files(true);

        ColumnFamilyDescriptor::new(Self::METADATA_COLUMN_FAMILY_NAME, options)
    }

    fn metadata_handle(&self) -> &ColumnFamily {
        self.database
            .cf_handle(Self::METADATA_COLUMN_FAMILY_NAME)
            .expect("metadata column family must exist")
    }
}

pub(crate) struct ShardStorage<'a> {
    id: ShardIndex,
    database: &'a DB,
    shard_columns: &'a ColumnFamily,
}

impl<'a> ShardStorage<'a> {
    fn new(id: ShardIndex, database: &'a DB) -> Self {
        let shard_columns = database
            .cf_handle(&Self::column_family_name(id))
            .expect("shard's column family must be present in database");

        Self {
            id,
            database,
            shard_columns,
        }
    }

    fn create(id: ShardIndex, database: &'a mut DB) -> Result<Self, anyhow::Error> {
        let mut options = Options::default();
        // TODO(jsmith): Tune storage for metadata and slivers
        // See https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html
        options.set_enable_blob_files(true);

        database.create_cf(Self::column_family_name(id), &options)?;

        Ok(ShardStorage::new(id, database))
    }

    /// Stores the provided primary or secondary sliver for the given blob ID.
    pub fn put_sliver(
        &self,
        blob_id: &BlobId,
        sliver: &[u8],
        is_primary: bool,
    ) -> Result<(), anyhow::Error> {
        self.database
            .put_cf(
                self.shard_columns,
                Self::sliver_key(blob_id, is_primary),
                sliver,
            )
            .context("unable to store sliver")
    }

    /// Retrieves the stored primary or secondary sliver for the given blob ID.
    pub fn get_sliver(
        &self,
        blob_id: &BlobId,
        is_primary: bool,
    ) -> Result<Option<Vec<u8>>, anyhow::Error> {
        self.database
            .get_cf(self.shard_columns, Self::sliver_key(blob_id, is_primary))
            .context("unable to store sliver")
    }

    fn column_family_name(id: ShardIndex) -> String {
        format!("shard-{}", id)
    }

    fn sliver_key(blob_id: &BlobId, is_primary: bool) -> Vec<u8> {
        if is_primary {
            [b"P:".as_slice(), blob_id.as_slice()].concat()
        } else {
            [b"S:".as_slice(), blob_id.as_slice()].concat()
        }
    }

    fn descriptors(shard: ShardIndex) -> ColumnFamilyDescriptor {
        let mut options = Options::default();

        // TODO(jsmith): Tune storage for metadata and slivers
        // See https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html
        options.set_enable_blob_files(true);

        ColumnFamilyDescriptor::new(Self::column_family_name(shard), options)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;

    const BLOB_ID: BlobId = [7; 32];
    const SHARD_INDEX: ShardIndex = 17;
    const OTHER_SHARD_INDEX: ShardIndex = 831;

    struct TempStorage {
        storage: Storage,
        _directory: TempDir,
    }

    impl AsRef<Storage> for TempStorage {
        fn as_ref(&self) -> &Storage {
            &self.storage
        }
    }

    fn empty_storage() -> TempStorage {
        empty_storage_with_shards(&[SHARD_INDEX])
    }

    fn empty_storage_with_shards(shards: &[ShardIndex]) -> TempStorage {
        let directory = tempfile::tempdir().expect("temporary directory creation must succeed");
        let mut storage = Storage::open(directory.path()).expect("storage creation must succeed");

        for shard in shards {
            storage
                .create_storage_for_shard(*shard)
                .expect("shard should be successfully created");
        }

        TempStorage {
            storage,
            _directory: directory,
        }
    }

    fn arbitrary_metadata() -> Metadata {
        (0..100u8).map(|i| [i; 32]).collect()
    }

    fn arbitrary_sliver() -> Vec<u8> {
        vec![7; 1024]
    }
    fn other_sliver() -> Vec<u8> {
        vec![201; 512]
    }

    #[test]
    fn can_write_then_read_metadata() -> TestResult {
        let storage = empty_storage();
        let storage = storage.as_ref();
        let metadata = arbitrary_metadata();

        storage.put_metadata(BLOB_ID, &metadata)?;
        let retrieved = storage.get_metadata(&BLOB_ID)?;

        assert_eq!(retrieved, Some(metadata));

        Ok(())
    }

    param_test! {
        can_store_and_retrieve_sliver -> TestResult: [
            primary: (true),
            secondary: (false),
        ]
    }
    fn can_store_and_retrieve_sliver(is_primary: bool) -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let sliver = arbitrary_sliver();

        shard.put_sliver(&BLOB_ID, &sliver, is_primary)?;
        let retrieved = shard.get_sliver(&BLOB_ID, is_primary)?;

        assert_eq!(retrieved, Some(sliver));

        Ok(())
    }

    #[test]
    fn stores_separate_primary_and_secondary_sliver() -> TestResult {
        let storage = empty_storage();
        let shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();

        let primary = arbitrary_sliver();
        let secondary = other_sliver();
        assert_ne!(primary, secondary);

        shard.put_sliver(&BLOB_ID, &primary, true)?;
        shard.put_sliver(&BLOB_ID, &secondary, false)?;

        let retrieved_primary = shard.get_sliver(&BLOB_ID, true)?;
        let retrieved_secondary = shard.get_sliver(&BLOB_ID, false)?;

        assert_eq!(retrieved_primary, Some(primary), "invalid primary sliver");
        assert_eq!(
            retrieved_secondary,
            Some(secondary),
            "invalid secondary sliver"
        );

        Ok(())
    }

    param_test! {
        stores_and_retrieves_for_multiple_shards -> TestResult: [
            primary_primary: (true, true),
            secondary_secondary: (false, false),
            mixed: (true, false),
        ]
    }
    fn stores_and_retrieves_for_multiple_shards(
        is_first_primary: bool,
        is_second_primary: bool,
    ) -> TestResult {
        let storage = empty_storage_with_shards(&[SHARD_INDEX, OTHER_SHARD_INDEX]);

        let first_shard = storage.as_ref().shard_storage(SHARD_INDEX).unwrap();
        let first_sliver = arbitrary_sliver();

        let second_shard = storage.as_ref().shard_storage(OTHER_SHARD_INDEX).unwrap();
        let second_sliver = other_sliver();
        assert_ne!(first_sliver, second_sliver);

        first_shard.put_sliver(&BLOB_ID, &first_sliver, is_first_primary)?;
        second_shard.put_sliver(&BLOB_ID, &second_sliver, is_second_primary)?;

        let first_retrieved = first_shard.get_sliver(&BLOB_ID, is_first_primary)?;
        let second_retrieved = second_shard.get_sliver(&BLOB_ID, is_second_primary)?;

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
}
