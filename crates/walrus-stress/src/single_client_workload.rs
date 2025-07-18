// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Single client workload.

use std::time::Duration;

use blob_pool::BlobPool;
use client_op_generator::{ClientOpGenerator, WalrusClientOp};
use single_client_workload_config::{
    RequestTypeDistributionConfig,
    SizeDistributionConfig,
    StoreLengthDistributionConfig,
};
use tokio::time::{Interval, MissedTickBehavior};
use walrus_core::{DEFAULT_ENCODING, encoding::Primary};
use walrus_sdk::{
    client::{Client, responses::BlobStoreResult},
    config::ClientConfig,
    store_optimizations::StoreOptimizations,
};
use walrus_sui::{
    client::{BlobPersistence, PostStoreAction, SuiContractClient},
    utils::SuiNetwork,
};

pub(crate) mod blob_generator;
pub(crate) mod blob_pool;
pub(crate) mod client_op_generator;
pub(crate) mod epoch_length_generator;
pub(crate) mod single_client_workload_arg;
pub(crate) mod single_client_workload_config;

pub struct SingleClientWorkload {
    client: Client<SuiContractClient>,
    target_requests_per_minute: u64,
    check_read_result: bool,
    size_distribution_config: SizeDistributionConfig,
    store_length_distribution_config: StoreLengthDistributionConfig,
    request_type_distribution: RequestTypeDistributionConfig,
    data_check: bool,
}

impl SingleClientWorkload {
    pub fn new(
        client: Client<SuiContractClient>,
        target_requests_per_minute: u64,
        check_read_result: bool,
        size_distribution_config: SizeDistributionConfig,
        store_length_distribution_config: StoreLengthDistributionConfig,
        request_type_distribution: RequestTypeDistributionConfig,
        data_check: bool,
    ) -> Self {
        Self {
            client,
            target_requests_per_minute,
            check_read_result,
            size_distribution_config,
            store_length_distribution_config,
            request_type_distribution,
            data_check,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut rng = rand::thread_rng();
        let mut blob_pool = BlobPool::new();
        let client_op_generator = ClientOpGenerator::new(
            self.request_type_distribution.clone(),
            self.size_distribution_config.clone(),
            self.store_length_distribution_config.clone(),
        );

        let mut request_interval =
            tokio::time::interval(Duration::from_secs(60 / self.target_requests_per_minute));
        request_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            request_interval.tick().await;
            let client_op = client_op_generator.generate_client_op(&blob_pool, &mut rng);
            self.execute_client_op(&client_op, &mut blob_pool).await?;
        }
    }

    // TODO: add metrics.
    async fn execute_client_op(
        &self,
        client_op: &WalrusClientOp,
        blob_pool: &mut BlobPool,
    ) -> anyhow::Result<()> {
        match client_op {
            WalrusClientOp::Read { blob_id } => {
                // TODO: also read using secondary slivers.
                let blob = self.client.read_blob::<Primary>(blob_id).await?;
                if self.data_check {
                    blob_pool.assert_blob_data(*blob_id, &blob);
                }
            }
            WalrusClientOp::Write {
                blob,
                deletable,
                store_length,
            } => {
                let store_result = self
                    .client
                    .reserve_and_store_blobs_retry_committees(
                        &[blob.as_slice()],
                        DEFAULT_ENCODING,
                        *store_length,
                        StoreOptimizations::none(),
                        BlobPersistence::from_deletable_and_permanent(*deletable, !deletable)?,
                        PostStoreAction::Keep,
                        None,
                    )
                    .await?;
                match &store_result[0] {
                    BlobStoreResult::NewlyCreated { blob_object, .. } => {
                        blob_pool.update_blob_pool(
                            blob_object.blob_id,
                            Some(blob_object.id),
                            client_op.clone(),
                        );
                    }
                    _ => {
                        anyhow::bail!(
                            "client op {:?} received unexpected store \
                        result: {:?}",
                            client_op,
                            store_result[0]
                        );
                    }
                }
            }
            WalrusClientOp::Delete { blob_id } => {
                self.client.delete_owned_blob(blob_id).await?;
                blob_pool.update_blob_pool(*blob_id, None, client_op.clone());
            }
            WalrusClientOp::Extend {
                blob_id,
                object_id,
                store_length,
            } => {
                self.client
                    .sui_client()
                    .extend_blob(*object_id, *store_length)
                    .await?;
                blob_pool.update_blob_pool(*blob_id, Some(*object_id), client_op.clone());
            }
        }
        Ok(())
    }
}
