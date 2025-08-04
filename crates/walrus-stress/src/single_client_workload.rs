// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Single client workload.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use blob_pool::BlobPool;
use client_op_generator::{ClientOpGenerator, WalrusClientOp};
use rand::SeedableRng;
use single_client_workload_config::{
    RequestTypeDistributionConfig,
    SizeDistributionConfig,
    StoreLengthDistributionConfig,
};
use tokio::time::MissedTickBehavior;
use walrus_core::{
    DEFAULT_ENCODING,
    SliverType,
    encoding::{Primary, Secondary},
};
use walrus_sdk::{
    client::{
        Client,
        StoreArgs,
        metrics::{self, ClientMetrics},
        responses::BlobStoreResult,
    },
    store_optimizations::StoreOptimizations,
};
use walrus_sui::client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient};

pub(crate) mod blob_generator;
pub(crate) mod blob_pool;
pub(crate) mod client_op_generator;
pub(crate) mod epoch_length_generator;
pub mod single_client_workload_arg;
pub mod single_client_workload_config;

/// A single client workload.
///
/// This workload is used to exercise walrus system behaviors using a single client. The client
/// will submit randomized client operations to the system.
#[derive(Debug)]
pub struct SingleClientWorkload {
    /// The client to use for the workload.
    client: Client<SuiContractClient>,
    /// The target requests per minute. Note that since there is only one client, if the target
    /// request rate is higher than the maximum request rate the client can issue, the workload
    /// is essentially the same as a sequential workload. There won't be any parallelism of
    /// client operations. To use the SingleClientWorkload for stress testing, the tester needs
    /// to create and manage multiple clients.
    target_requests_per_minute: u64,
    /// Whether to check the read result matches the record of writes.
    check_read_result: bool,
    /// The maximum number of blobs to keep in the blob pool.
    max_blobs_in_pool: usize,
    /// The size distribution of uploaded blobs.
    size_distribution_config: SizeDistributionConfig,
    /// The store length distribution of store and extend operations.
    store_length_distribution_config: StoreLengthDistributionConfig,
    /// The request type distribution configuration.
    request_type_distribution: RequestTypeDistributionConfig,
    /// Metrics tracks workload.
    metrics: Arc<ClientMetrics>,
}

impl SingleClientWorkload {
    /// Creates a new single client workload.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Client<SuiContractClient>,
        target_requests_per_minute: u64,
        check_read_result: bool,
        max_blobs_in_pool: usize,
        size_distribution_config: SizeDistributionConfig,
        store_length_distribution_config: StoreLengthDistributionConfig,
        request_type_distribution: RequestTypeDistributionConfig,
        metrics: Arc<ClientMetrics>,
    ) -> Self {
        Self {
            client,
            target_requests_per_minute,
            check_read_result,
            max_blobs_in_pool,
            size_distribution_config,
            store_length_distribution_config,
            request_type_distribution,
            metrics,
        }
    }

    /// Runs the single client workload.
    pub async fn run(&self) -> anyhow::Result<()> {
        let mut rng = rand::rngs::StdRng::from_entropy();

        // Use a blob pool to manage existing blobs.
        let mut blob_pool = BlobPool::new(self.check_read_result, self.max_blobs_in_pool);
        let client_op_generator = ClientOpGenerator::new(
            self.request_type_distribution.clone(),
            self.size_distribution_config.clone(),
            self.store_length_distribution_config.clone(),
        );

        let mut request_interval = tokio::time::interval(Duration::from_millis(
            60_000 / self.target_requests_per_minute,
        ));
        request_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // TODO(WAL-936): for read heavy workloads, we should pre-create a pool of blobs so that
        // there are enough blobs to read from.

        let mut current_epoch = 0;

        loop {
            request_interval.tick().await;

            // TODO(WAL-937): tracking epoch more efficiently. This one reads per request is not
            // necessary.
            let epoch = self.client.sui_client().current_epoch().await?;
            if epoch != current_epoch {
                blob_pool.expire_blobs_in_new_epoch(epoch);
                current_epoch = epoch;
            }
            let client_op = client_op_generator.generate_client_op(&blob_pool, &mut rng);
            self.execute_client_op(&client_op, &mut blob_pool).await?;
        }
    }

    async fn execute_client_op(
        &self,
        client_op: &WalrusClientOp,
        blob_pool: &mut BlobPool,
    ) -> anyhow::Result<()> {
        match client_op {
            WalrusClientOp::Read {
                blob_id,
                sliver_type,
            } => {
                let now = Instant::now();
                let blob = match sliver_type {
                    SliverType::Primary => self.client.read_blob::<Primary>(blob_id).await?,
                    SliverType::Secondary => self.client.read_blob::<Secondary>(blob_id).await?,
                };
                self.metrics
                    .observe_latency(metrics::READ_WORKLOAD, now.elapsed());
                if self.check_read_result {
                    blob_pool.assert_blob_data(*blob_id, &blob);
                }
            }
            WalrusClientOp::Write {
                blob,
                deletable,
                store_epoch_ahead,
            } => {
                let now = Instant::now();
                // TODO(WAL-945): test more StoreOptimizations.
                let mut store_args = StoreArgs::new(
                    DEFAULT_ENCODING,
                    *store_epoch_ahead,
                    StoreOptimizations::none(),
                    BlobPersistence::from_deletable_and_permanent(*deletable, !deletable)?,
                    // TODO(WAL-954): test more PostStoreAction.
                    PostStoreAction::Keep,
                );
                store_args = store_args.with_metrics(self.metrics.clone());
                let store_result = self
                    .client
                    .reserve_and_store_blobs_retry_committees(&[blob.as_slice()], &[], &store_args)
                    .await?;
                self.metrics.observe_latency("store_blob", now.elapsed());
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
                            "client op {:?} received unexpected store result: {:?}",
                            client_op,
                            store_result[0]
                        );
                    }
                }
            }
            WalrusClientOp::Delete { blob_id } => {
                let now = Instant::now();
                self.client.delete_owned_blob(blob_id).await?;
                self.metrics.observe_latency("delete_blob", now.elapsed());
                blob_pool.update_blob_pool(*blob_id, None, client_op.clone());
            }
            WalrusClientOp::Extend {
                blob_id,
                object_id,
                store_epoch_ahead,
            } => {
                let now = Instant::now();
                self.client
                    .sui_client()
                    .extend_blob(*object_id, *store_epoch_ahead)
                    .await?;
                self.metrics.observe_latency("extend_blob", now.elapsed());
                blob_pool.update_blob_pool(*blob_id, Some(*object_id), client_op.clone());
            }
            WalrusClientOp::None => {
                tracing::info!("none op received, skipping");
            }
        }
        Ok(())
    }
}
