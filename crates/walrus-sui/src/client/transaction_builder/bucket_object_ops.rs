// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    client::BucketObjectVersionInput,
    types::{ObjectHeaders, ObjectMetadata, ObjectTags},
};

impl WalrusPtbBuilder {
    /// Adds a call to `bucket_object_registry::new`.
    pub fn create_bucket_object_registry(
        &mut self,
        bucket_object_package_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        let args = vec![self.pt_builder.pure(blob_bucket_object_id)?];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object_registry::new,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object_registry::resolve_or_create_bucket_object`.
    pub async fn resolve_or_create_bucket_object(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        key: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(registry_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(key)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object_registry::resolve_or_create_bucket_object,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object_registry::copy_object_if_absent`.
    pub async fn copy_bucket_object_if_absent(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        source_bucket_object_id: ObjectID,
        destination_key: String,
        object_etag: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(registry_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        source_bucket_object_id,
                        SharedObjectMutability::Immutable,
                    )
                    .await?,
            )?,
            self.pt_builder.pure(destination_key)?,
            self.pt_builder.pure(object_etag)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object_registry::copy_object_if_absent,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object_registry::rename_object`.
    pub async fn rename_bucket_object(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        new_key: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(registry_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(new_key)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object_registry::rename_object,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object_registry::rename_object_if_match`.
    pub async fn rename_bucket_object_if_match(
        &mut self,
        bucket_object_package_id: ObjectID,
        registry_object_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        new_key: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(registry_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(expected_object_etag)?,
            self.pt_builder.pure(new_key)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object_registry::rename_object_if_match,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object::put_object_if_absent_and_register`.
    pub async fn put_bucket_object_if_absent_and_register(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<()> {
        self.put_or_update_bucket_object_and_register(
            bucket_object_package_id,
            bucket_object_id,
            blob_bucket_object_id,
            blob_bucket_cap,
            None,
            version_input,
        )
        .await
    }

    /// Adds a call to `bucket_object::update_object_if_match_and_register`.
    pub async fn update_bucket_object_if_match_and_register(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
        expected_object_etag: String,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<()> {
        self.put_or_update_bucket_object_and_register(
            bucket_object_package_id,
            bucket_object_id,
            blob_bucket_object_id,
            blob_bucket_cap,
            Some(expected_object_etag),
            version_input,
        )
        .await
    }

    async fn put_or_update_bucket_object_and_register(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
        blob_bucket_cap: ArgumentOrOwnedObject,
        expected_object_etag: Option<String>,
        version_input: BucketObjectVersionInput,
    ) -> SuiClientResult<()> {
        let is_update = expected_object_etag.is_some();
        let price = self
            .write_price_for_encoded_length(version_input.blob_metadata.encoded_size, false)
            .await?;
        self.fill_wal_balance(price).await?;

        let mut args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Mutable,
                    )
                    .await?,
            )?,
            self.argument_from_arg_or_obj(blob_bucket_cap).await?,
            self.system_arg(SharedObjectMutability::Mutable)?,
        ];
        if let Some(expected_object_etag) = expected_object_etag {
            args.push(self.pt_builder.pure(expected_object_etag)?);
        }
        args.extend([
            self.pt_builder
                .pure(version_input.blob_metadata.root_hash.bytes())?,
            self.pt_builder
                .pure(version_input.blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(version_input.blob_metadata.encoding_type))?,
            self.pt_builder
                .pure(version_input.persistence.is_deletable())?,
            self.wal_coin_arg()?,
            self.pt_builder.pure(version_input.headers)?,
            self.pt_builder.pure(version_input.metadata)?,
            self.pt_builder.pure(version_input.tags)?,
            self.pt_builder.pure(version_input.content_etag)?,
            self.pt_builder.pure(version_input.object_etag)?,
        ]);

        let function = if is_update {
            contracts::bucket_object::update_object_if_match_and_register
        } else {
            contracts::bucket_object::put_object_if_absent_and_register
        };
        self.move_call(bucket_object_package_id, function, args)?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }

    /// Adds a call to `bucket_object::update_object_attributes`.
    pub async fn update_bucket_object_attributes(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        headers: ObjectHeaders,
        metadata: ObjectMetadata,
        tags: ObjectTags,
        object_etag: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(headers)?,
            self.pt_builder.pure(metadata)?,
            self.pt_builder.pure(tags)?,
            self.pt_builder.pure(object_etag)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object::update_object_attributes,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object::update_object_attributes_if_match`.
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
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(expected_object_etag)?,
            self.pt_builder.pure(headers)?,
            self.pt_builder.pure(metadata)?,
            self.pt_builder.pure(tags)?,
            self.pt_builder.pure(object_etag)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object::update_object_attributes_if_match,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object::delete_object`.
    pub async fn delete_bucket_object(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        object_etag: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(object_etag)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object::delete_object,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object::delete_object_if_match`.
    pub async fn delete_bucket_object_if_match(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        expected_object_etag: String,
        object_etag: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.pure(expected_object_etag)?,
            self.pt_builder.pure(object_etag)?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object::delete_object_if_match,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `bucket_object::finalize_pending_version_if_certified`.
    pub async fn finalize_bucket_object_if_certified(
        &mut self,
        bucket_object_package_id: ObjectID,
        bucket_object_id: ObjectID,
        blob_bucket_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(bucket_object_id, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(
                        blob_bucket_object_id,
                        SharedObjectMutability::Immutable,
                    )
                    .await?,
            )?,
        ];
        self.move_call(
            bucket_object_package_id,
            contracts::bucket_object::finalize_pending_version_if_certified,
            args,
        )?;
        Ok(())
    }
}
