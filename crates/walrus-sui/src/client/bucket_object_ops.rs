// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::types::{
    move_errors::{BucketObjectError, MoveExecutionError},
    move_structs::{BucketObject, ObjectHeaders, ObjectMetadata, ObjectTags, ObjectVersion},
};

/// Materialized state for a shared bucket object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketObjectState {
    /// The outer shared object.
    pub bucket_object: BucketObject,
    /// The linked blob bucket object ID.
    pub blob_bucket_id: ObjectID,
    /// The exact key for this object.
    pub key: String,
    /// The latest promoted generation.
    pub generation: u64,
    /// The current live version, if any.
    pub current_version: Option<ObjectVersion>,
    /// The pending staged version, if any.
    pub pending_version: Option<ObjectVersion>,
}

/// Input for registering and staging a new bucket-object version.
#[derive(Debug, Clone)]
pub struct BucketObjectVersionInput {
    /// Blob registration metadata.
    pub blob_metadata: BlobObjectMetadata,
    /// Whether the backing pooled blob is deletable.
    pub persistence: BlobPersistence,
    /// Standard object headers.
    pub headers: ObjectHeaders,
    /// Custom user metadata.
    pub metadata: ObjectMetadata,
    /// Object tags.
    pub tags: ObjectTags,
    /// Content etag for the payload.
    pub content_etag: String,
    /// Object-version etag for CAS.
    pub object_etag: String,
}

impl SuiContractClient {
    /// Creates a shared bucket-object registry for the specified blob bucket.
    pub async fn create_bucket_object_registry(
        &self,
        bucket_object_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        self.inner
            .lock()
            .await
            .create_bucket_object_registry(bucket_object_package_id, blob_bucket_object_id)
            .await
    }

    /// Resolves a bucket object by key, creating it if absent.
    pub async fn resolve_or_create_bucket_object(
        &self,
        registry_object_id: ObjectID,
        key: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(registry_object_id)
            .await?;
        self.inner
            .lock()
            .await
            .resolve_or_create_bucket_object(bucket_object_package_id, registry_object_id, key)
            .await
    }

    /// Copies a bucket object to a new key if the destination is absent.
    pub async fn copy_bucket_object_if_absent(
        &self,
        registry_object_id: ObjectID,
        source_bucket_object_id: ObjectID,
        destination_key: String,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(registry_object_id)
            .await?;
        self.inner
            .lock()
            .await
            .copy_bucket_object_if_absent(
                bucket_object_package_id,
                registry_object_id,
                source_bucket_object_id,
                destination_key,
                object_etag,
            )
            .await
    }

    /// Renames a bucket object to a new key.
    pub async fn rename_bucket_object(
        &self,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        new_key: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(registry_object_id)
            .await?;
        self.inner
            .lock()
            .await
            .rename_bucket_object(
                bucket_object_package_id,
                registry_object_id,
                bucket_object_id,
                new_key,
            )
            .await
    }

    /// Renames a bucket object if the current object etag matches.
    pub async fn rename_bucket_object_if_match(
        &self,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        new_key: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(registry_object_id)
            .await?;
        self.inner
            .lock()
            .await
            .rename_bucket_object_if_match(
                bucket_object_package_id,
                registry_object_id,
                bucket_object_id,
                expected_object_etag,
                new_key,
            )
            .await
    }

    /// Registers and stages a new object version if the object is absent.
    pub async fn put_bucket_object_if_absent_and_register(
        &self,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .put_bucket_object_if_absent_and_register(
                bucket_object_package_id,
                bucket_object_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id,
                version_input.clone(),
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .put_bucket_object_if_absent_and_register(
                            refreshed_package_id,
                            bucket_object_id,
                            blob_bucket_object_id,
                            blob_bucket_cap_object_id,
                            version_input,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Registers and stages a new object version if the current object etag matches.
    pub async fn update_bucket_object_if_match_and_register(
        &self,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        expected_object_etag: String,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .update_bucket_object_if_match_and_register(
                bucket_object_package_id,
                bucket_object_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id,
                expected_object_etag.clone(),
                version_input.clone(),
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .update_bucket_object_if_match_and_register(
                            refreshed_package_id,
                            bucket_object_id,
                            blob_bucket_object_id,
                            blob_bucket_cap_object_id,
                            expected_object_etag,
                            version_input,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Updates bucket-object headers, metadata, and tags without registering a new blob.
    pub async fn update_bucket_object_attributes(
        &self,
        bucket_object_id: ObjectID,
        headers: ObjectHeaders,
        metadata: ObjectMetadata,
        tags: ObjectTags,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .update_bucket_object_attributes(
                bucket_object_package_id,
                bucket_object_id,
                headers.clone(),
                metadata.clone(),
                tags.clone(),
                object_etag.clone(),
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .update_bucket_object_attributes(
                            refreshed_package_id,
                            bucket_object_id,
                            headers,
                            metadata,
                            tags,
                            object_etag,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Conditionally updates bucket-object headers, metadata, and tags.
    pub async fn update_bucket_object_attributes_if_match(
        &self,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        headers: ObjectHeaders,
        metadata: ObjectMetadata,
        tags: ObjectTags,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .update_bucket_object_attributes_if_match(
                bucket_object_package_id,
                bucket_object_id,
                expected_object_etag.clone(),
                headers.clone(),
                metadata.clone(),
                tags.clone(),
                object_etag.clone(),
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .update_bucket_object_attributes_if_match(
                            refreshed_package_id,
                            bucket_object_id,
                            expected_object_etag,
                            headers,
                            metadata,
                            tags,
                            object_etag,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Deletes a bucket object by creating a delete-marker version.
    pub async fn delete_bucket_object(
        &self,
        bucket_object_id: ObjectID,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .delete_bucket_object(
                bucket_object_package_id,
                bucket_object_id,
                object_etag.clone(),
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .delete_bucket_object(refreshed_package_id, bucket_object_id, object_etag)
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Conditionally deletes a bucket object by creating a delete-marker version.
    pub async fn delete_bucket_object_if_match(
        &self,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .delete_bucket_object_if_match(
                bucket_object_package_id,
                bucket_object_id,
                expected_object_etag.clone(),
                object_etag.clone(),
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .delete_bucket_object_if_match(
                            refreshed_package_id,
                            bucket_object_id,
                            expected_object_etag,
                            object_etag,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Finalizes a pending bucket-object version once the underlying pooled blob is certified.
    pub async fn finalize_bucket_object_if_certified(
        &self,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> SuiClientResult<BucketObjectState> {
        let bucket_object_package_id = self
            .read_client()
            .bucket_object_package_id(bucket_object_id)
            .await?;
        match self
            .inner
            .lock()
            .await
            .finalize_bucket_object_if_certified(
                bucket_object_package_id,
                bucket_object_id,
                blob_bucket_object_id,
            )
            .await
        {
            Err(
                error @ SuiClientError::TransactionExecutionError(MoveExecutionError::BucketObject(
                    BucketObjectError::EWrongVersion(_),
                )),
            ) => {
                let refreshed_package_id = self
                    .read_client()
                    .bucket_object_package_id(bucket_object_id)
                    .await?;
                if refreshed_package_id != bucket_object_package_id {
                    self.inner
                        .lock()
                        .await
                        .finalize_bucket_object_if_certified(
                            refreshed_package_id,
                            bucket_object_id,
                            blob_bucket_object_id,
                        )
                        .await
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }
}

impl SuiContractClientInner {
    /// Creates a new shared bucket-object registry.
    pub async fn create_bucket_object_registry(
        &mut self,
        bucket_object_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .create_bucket_object_registry(bucket_object_package_id, blob_bucket_object_id)?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "create_bucket_object_registry")
            .await?;
        let registry_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::bucket_object_registry::BucketObjectRegistry
                .to_move_struct_tag_with_package(bucket_object_package_id, &[])?,
        )?;
        ensure!(
            registry_ids.len() == 1,
            "unexpected number of BucketObjectRegistry objects created: {}",
            registry_ids.len()
        );
        Ok(registry_ids[0])
    }

    /// Resolves or creates the bucket object for the given key.
    pub async fn resolve_or_create_bucket_object(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        key: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .resolve_or_create_bucket_object(
                bucket_object_package_id,
                registry_object_id,
                key.clone(),
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "resolve_or_create_bucket_object")
            .await?;
        self.read_client
            .get_bucket_object_state_by_key(registry_object_id, &key)
            .await?
            .ok_or_else(|| {
                anyhow!("bucket object for key '{key}' not found after resolve/create").into()
            })
    }

    /// Copies a bucket object to a new key.
    pub async fn copy_bucket_object_if_absent(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        source_bucket_object_id: ObjectID,
        destination_key: String,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .copy_bucket_object_if_absent(
                bucket_object_package_id,
                registry_object_id,
                source_bucket_object_id,
                destination_key.clone(),
                object_etag,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "copy_bucket_object_if_absent")
            .await?;
        self.read_client
            .get_bucket_object_state_by_key(registry_object_id, &destination_key)
            .await?
            .ok_or_else(|| {
                anyhow!("bucket object for copied key '{destination_key}' not found after copy")
                    .into()
            })
    }

    /// Renames a bucket object.
    pub async fn rename_bucket_object(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        new_key: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .rename_bucket_object(
                bucket_object_package_id,
                registry_object_id,
                bucket_object_id,
                new_key,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "rename_bucket_object")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Conditionally renames a bucket object.
    pub async fn rename_bucket_object_if_match(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        new_key: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .rename_bucket_object_if_match(
                bucket_object_package_id,
                registry_object_id,
                bucket_object_id,
                expected_object_etag,
                new_key,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "rename_bucket_object_if_match")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Registers and stages a new version for an absent bucket object.
    pub async fn put_bucket_object_if_absent_and_register(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .put_bucket_object_if_absent_and_register(
                bucket_object_package_id,
                bucket_object_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id.into(),
                version_input,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "put_bucket_object_if_absent_and_register")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Registers and stages a new version for a matching current object.
    pub async fn update_bucket_object_if_match_and_register(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap_object_id: ObjectID,
        expected_object_etag: String,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .update_bucket_object_if_match_and_register(
                bucket_object_package_id,
                bucket_object_id,
                blob_bucket_object_id,
                blob_bucket_cap_object_id.into(),
                expected_object_etag,
                version_input,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "update_bucket_object_if_match_and_register")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Updates bucket-object headers, metadata, and tags.
    pub async fn update_bucket_object_attributes(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        headers: ObjectHeaders,
        metadata: ObjectMetadata,
        tags: ObjectTags,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .update_bucket_object_attributes(
                bucket_object_package_id,
                bucket_object_id,
                headers,
                metadata,
                tags,
                object_etag,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "update_bucket_object_attributes")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Conditionally updates bucket-object headers, metadata, and tags.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_bucket_object_attributes_if_match(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        headers: ObjectHeaders,
        metadata: ObjectMetadata,
        tags: ObjectTags,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .update_bucket_object_attributes_if_match(
                bucket_object_package_id,
                bucket_object_id,
                expected_object_etag,
                headers,
                metadata,
                tags,
                object_etag,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "update_bucket_object_attributes_if_match")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Deletes a bucket object via a delete marker.
    pub async fn delete_bucket_object(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .delete_bucket_object(bucket_object_package_id, bucket_object_id, object_etag)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "delete_bucket_object")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Conditionally deletes a bucket object via a delete marker.
    pub async fn delete_bucket_object_if_match(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        object_etag: String,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .delete_bucket_object_if_match(
                bucket_object_package_id,
                bucket_object_id,
                expected_object_etag,
                object_etag,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "delete_bucket_object_if_match")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }

    /// Finalizes a pending bucket-object version once the blob is certified.
    pub async fn finalize_bucket_object_if_certified(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> SuiClientResult<BucketObjectState> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .finalize_bucket_object_if_certified(
                bucket_object_package_id,
                bucket_object_id,
                blob_bucket_object_id,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "finalize_bucket_object_if_certified")
            .await?;
        self.read_client
            .get_bucket_object_state(bucket_object_id)
            .await
    }
}
