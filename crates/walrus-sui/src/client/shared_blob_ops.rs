// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl SuiContractClient {
    /// Funds the shared blob object.
    pub async fn fund_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .fund_shared_blob(shared_blob_obj_id, amount)
                .await
        })
        .await
    }

    /// Extends the shared blob's lifetime by `epochs_extended` epochs.
    pub async fn extend_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .extend_shared_blob(shared_blob_obj_id, epochs_extended)
                .await
        })
        .await
    }

    /// Shares the blob object with the given object ID. If amount is specified, also fund the blob.
    pub async fn share_and_maybe_fund_blob(
        &self,
        blob_obj_id: ObjectID,
        amount: Option<u64>,
    ) -> SuiClientResult<ObjectID> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .share_and_maybe_fund_blob(blob_obj_id, amount)
                .await
        })
        .await
    }
}

impl SuiContractClientInner {
    /// Funds the shared blob object.
    pub async fn fund_shared_blob(
        &mut self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .fund_shared_blob(shared_blob_obj_id, amount)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "fund_shared_blob")
            .await?;
        Ok(())
    }

    /// Extends the shared blob's lifetime by `epochs_extended` epochs.
    pub async fn extend_shared_blob(
        &mut self,
        shared_blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .extend_shared_blob(shared_blob_obj_id, epochs_extended)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_shared_blob")
            .await?;
        Ok(())
    }

    /// Shares the blob object with the given object ID. If amount is specified, also fund the blob.
    pub async fn share_and_maybe_fund_blob(
        &mut self,
        blob_obj_id: ObjectID,
        amount: Option<u64>,
    ) -> SuiClientResult<ObjectID> {
        let blob: Blob = self
            .read_client
            .retriable_sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let mut pt_builder = self.transaction_builder();

        if let Some(amount) = amount {
            ensure!(amount > 0, "must fund with non-zero amount");
            pt_builder
                .new_funded_shared_blob(blob.id.into(), amount)
                .await?;
        } else {
            pt_builder.new_shared_blob(blob.id.into()).await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "share_and_maybe_fund_blob")
            .await?;
        let shared_blob_obj_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            shared_blob_obj_id.len() == 1,
            "unexpected number of `SharedBlob`s created: {}",
            shared_blob_obj_id.len()
        );
        Ok(shared_blob_obj_id[0])
    }

    pub(crate) async fn get_share_blob_result(
        &self,
        certify_and_extend_parameters: &[CertifyAndExtendBlobParams<'_>],
        res: &SuiTransactionBlockResponse,
    ) -> SuiClientResult<Vec<PostStoreActionResult>> {
        match self
            .get_shared_blob_mapping(certify_and_extend_parameters, res)
            .await
        {
            Ok(shared_mapping) => {
                let results = certify_and_extend_parameters
                    .iter()
                    .map(|param| {
                        let shared_result = match shared_mapping.get(&param.blob.id) {
                            Some(object_id) => GetSharedBlobResult::Success(*object_id),
                            None => GetSharedBlobResult::Failed(
                                "Object ID not found in mapping".to_string(),
                            ),
                        };
                        PostStoreActionResult::Shared(shared_result)
                    })
                    .collect();
                Ok(results)
            }
            Err(err) => {
                let results = certify_and_extend_parameters
                    .iter()
                    .map(|_| {
                        PostStoreActionResult::Shared(GetSharedBlobResult::Failed(err.to_string()))
                    })
                    .collect();
                Ok(results)
            }
        }
    }

    async fn get_shared_blob_mapping(
        &self,
        params: &[CertifyAndExtendBlobParams<'_>],
        res: &SuiTransactionBlockResponse,
    ) -> SuiClientResult<HashMap<ObjectID, ObjectID>> {
        let object_ids = get_created_sui_object_ids_by_type(
            res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            object_ids.len() == params.len(),
            "unexpected number of shared blob objects created: {} (expected {})",
            object_ids.len(),
            params.len()
        );

        let shared_blobs = self
            .retriable_sui_client()
            .get_sui_objects::<SharedBlob>(&object_ids)
            .await?;

        Ok(shared_blobs
            .into_iter()
            .map(|shared_blob| (shared_blob.blob.id, shared_blob.id))
            .collect())
    }

    /// Helper function to create a mapping from blob IDs to shared blob object IDs.
    pub(crate) async fn create_blob_id_to_shared_mapping(
        &self,
        res: &SuiTransactionBlockResponse,
        blobs_ids: &[BlobId],
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        let object_ids = get_created_sui_object_ids_by_type(
            res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            object_ids.len() == blobs_ids.len(),
            "unexpected number of shared blob objects created: {} (expected {})",
            object_ids.len(),
            blobs_ids.len()
        );

        if object_ids.len() == 1 {
            Ok(HashMap::from([(blobs_ids[0], object_ids[0])]))
        } else {
            let shared_blobs = self
                .retriable_sui_client()
                .get_sui_objects::<SharedBlob>(&object_ids)
                .await?;
            Ok(shared_blobs
                .into_iter()
                .map(|shared_blob| (shared_blob.blob.blob_id, shared_blob.id))
                .collect())
        }
    }

    /// Applies the post-store action for a single blob ID to the transaction builder.
    pub(crate) async fn apply_post_store_action(
        pt_builder: &mut WalrusPtbBuilder,
        blob_id: ObjectID,
        post_store: PostStoreAction,
    ) -> SuiClientResult<()> {
        match post_store {
            PostStoreAction::TransferTo(address) => {
                pt_builder
                    .transfer(Some(address), vec![blob_id.into()])
                    .await?;
            }
            PostStoreAction::Burn => {
                pt_builder.burn_blob(blob_id.into()).await?;
            }
            PostStoreAction::Keep => (),
            PostStoreAction::Share => {
                pt_builder.new_shared_blob(blob_id.into()).await?;
            }
        }
        Ok(())
    }
}
