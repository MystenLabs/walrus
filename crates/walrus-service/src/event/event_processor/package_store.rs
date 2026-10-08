// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Package store module for storing package objects.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use move_core_types::account_address::AccountAddress;
use sui_package_resolver::{
    Package,
    PackageStore,
    PackageStoreWithLruCache,
    error::Error as PackageResolverError,
};
use sui_types::{
    base_types::ObjectID,
    object::{Data, Object},
};
use tokio::sync::RwLock;
use typed_store::{Map, rocks::DBMap};
use walrus_sui::client::retry_client::RetriableRpcClient;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoff};

pub(crate) type PackageCache = PackageStoreWithLruCache<LocalDBPackageStore>;

/// Backoff for fetching a package from the full node when the request fails for a reason other
/// than the package not existing (rate limiting, timeouts, failover exhausted). Event processing
/// cannot continue without the package, so the fetch is retried for a few minutes before the
/// error is surfaced.
const PACKAGE_FETCH_MIN_BACKOFF: Duration = Duration::from_secs(1);
const PACKAGE_FETCH_MAX_BACKOFF: Duration = Duration::from_secs(30);
const PACKAGE_FETCH_MAX_RETRIES: u32 = 10;

/// Store which keeps package objects in a local rocksdb store. It is expected that this store is
/// kept updated with latest version of package objects while iterating over checkpoints. If the
/// local db is missing (or gets deleted), packages are fetched from a full node and local store is
/// updated.
#[derive(Clone)]
pub struct LocalDBPackageStore {
    /// The table which stores the package objects.
    package_store_table: DBMap<ObjectID, Object>,
    /// The full node REST client.
    fallback_client: RetriableRpcClient,
    /// Cache for original package ids.
    original_id_cache: Arc<RwLock<HashMap<AccountAddress, ObjectID>>>,
}

impl std::fmt::Debug for LocalDBPackageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDBPackageStore")
            .field("package_store_table", &self.package_store_table)
            .finish()
    }
}

impl LocalDBPackageStore {
    /// Creates a new instance of the local package store.
    pub fn new(package_store_table: DBMap<ObjectID, Object>, client: RetriableRpcClient) -> Self {
        Self {
            package_store_table,
            fallback_client: client,
            original_id_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Updates the package store with the given object.
    pub fn update(&self, object: &Object) -> Result<()> {
        if object.is_package() {
            let mut write_batch = self.package_store_table.batch();
            write_batch.insert_batch(
                &self.package_store_table,
                std::iter::once((object.id(), object)),
            )?;
            write_batch.write()?;
        }
        Ok(())
    }

    /// Updates the package store with the given objects.
    pub fn update_batch(&self, objects: &[Object]) -> Result<()> {
        let mut write_batch = self.package_store_table.batch();
        for object in objects {
            // Update the package store with the given object.We want to track not just the
            // walrus package but all its transitive dependencies as well. While it is possible
            // to get the transitive dependencies for a package, it is more efficient to just
            // track all packages as we are not expecting a large number of packages.
            if !object.is_package() {
                continue;
            }
            write_batch.insert_batch(
                &self.package_store_table,
                std::iter::once((object.id(), object)),
            )?;
        }
        write_batch.write()?;
        Ok(())
    }

    /// Gets the object with the given id. If the object is not found in the local store, it will be
    /// fetched from the full node.
    pub async fn get(&self, id: AccountAddress) -> Result<Object, PackageResolverError> {
        let object = if let Some(object) = self
            .package_store_table
            .get(&ObjectID::from(id))
            .map_err(|store_error| PackageResolverError::Store {
                store: "RocksDB",
                error: store_error.to_string(),
            })? {
            object
        } else {
            let object = self.fetch_from_full_node(id).await?;
            self.update(&object)
                .map_err(|store_error| PackageResolverError::Store {
                    store: "RocksDB",
                    error: store_error.to_string(),
                })?;
            object
        };
        Ok(object)
    }

    /// Fetches the object from the full node.
    ///
    /// Only a `NotFound` answer means the package does not exist; any other failure is retried
    /// with backoff and, if it persists, reported as a store error rather than as a missing
    /// package, so that an unavailable full node is never mistaken for a nonexistent package.
    async fn fetch_from_full_node(
        &self,
        id: AccountAddress,
    ) -> Result<Object, PackageResolverError> {
        let mut backoff = ExponentialBackoff::new_with_seed(
            PACKAGE_FETCH_MIN_BACKOFF,
            PACKAGE_FETCH_MAX_BACKOFF,
            Some(PACKAGE_FETCH_MAX_RETRIES),
            0,
        );
        loop {
            match self.fallback_client.get_object(ObjectID::from(id)).await {
                Ok(object) => return Ok(object),
                Err(error) if error.is_not_found() => {
                    return Err(PackageResolverError::PackageNotFound(id));
                }
                Err(error) => {
                    let Some(delay) = backoff.next_delay() else {
                        return Err(PackageResolverError::Store {
                            store: "full node",
                            error: error.to_string(),
                        });
                    };
                    tracing::warn!(
                        ?error,
                        package = %id,
                        ?delay,
                        "failed to fetch a package from the full node; retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Gets the original package id for the given package id.
    pub async fn get_original_package_id(&self, id: AccountAddress) -> Result<ObjectID> {
        if let Some(&original_id) = self.original_id_cache.read().await.get(&id) {
            return Ok(original_id);
        }

        let object = self.get(id).await?;
        let Data::Package(package) = &object.data else {
            return Err(anyhow::anyhow!("Object is not a package"));
        };

        let original_id = package.original_package_id();

        self.original_id_cache.write().await.insert(id, original_id);

        Ok(original_id)
    }
}

#[async_trait::async_trait]
impl PackageStore for LocalDBPackageStore {
    async fn fetch(&self, id: AccountAddress) -> sui_package_resolver::Result<Arc<Package>> {
        let object = self.get(id).await?;
        Ok(Arc::new(Package::read_from_object(&object)?))
    }
}
