// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! BlobStoreOrchestrator - centralized component for managing the blob storage workflow.
//!
//! This module provides a high-level orchestrator that coordinates the entire blob storage
//! process, including status checking, resource registration, uploading, and certification.

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Instant;

use sui_types::base_types::ObjectID;

use super::{
    ClientResult,
    StoreArgs,
    WalrusNodeClient,
    client_types::{WalrusStoreBlob, WalrusStoreBlobApi},
};
use crate::sui::client::{CertifyAndExtendBlobParams, CertifyAndExtendBlobResult, SuiContractClient};
use crate::client::resource::PriceComputation;

/// Orchestrates the storing of blobs, handling status checks, resource management,
/// and partitioning blobs based on the required actions.
#[derive(Debug)]
pub struct BlobStoreOrchestrator<'a, T: Debug + Clone + Send + Sync> {
    client: &'a WalrusNodeClient<SuiContractClient>,

    /// Blobs that are already fully certified and require no further action.
    pub completed_blobs: Vec<WalrusStoreBlob<'a, T>>,

    /// Blobs that have been uploaded and just need to be certified on-chain.
    pub to_be_certified: Vec<WalrusStoreBlob<'a, T>>,

    /// Blobs that are already certified but need their expiry extended.
    pub to_be_extended: Vec<WalrusStoreBlob<'a, T>>,

    /// Blobs that need to be uploaded to storage nodes.
    pub to_be_uploaded: Vec<WalrusStoreBlob<'a, T>>,
}

impl<'a, T: Debug + Clone + Send + Sync + 'a> BlobStoreOrchestrator<'a, T> {
    /// Creates a new orchestrator.
    pub fn new(client: &'a WalrusNodeClient<SuiContractClient>) -> Self {
        Self {
            client,
            completed_blobs: Vec::new(),
            to_be_certified: Vec::new(),
            to_be_extended: Vec::new(),
            to_be_uploaded: Vec::new(),
        }
    }

    /// Prepares the blobs for storing by checking their status, registering them,
    /// and partitioning them into action groups.
    pub async fn prepare(
        &mut self,
        encoded_blobs: Vec<WalrusStoreBlob<'a, T>>,
        store_args: &StoreArgs,
    ) -> ClientResult<()> {
        if encoded_blobs.is_empty() {
            return Ok(());
        }

        let committees = self.client.get_committees().await?;

        // 1. Get blob statuses
        let status_start_timer = Instant::now();
        let encoded_blobs_with_status = self
            .client
            .await_while_checking_notification(self.client.get_blob_statuses(encoded_blobs))
            .await?;
        let status_timer_duration = status_start_timer.elapsed();
        tracing::info!(
            duration = ?status_timer_duration,
            "retrieved {} blob statuses",
            encoded_blobs_with_status.len()
        );
        store_args.maybe_observe_checking_blob_status(status_timer_duration);

        // 2. Register blobs to get store operations
        let store_op_timer = Instant::now();
        let registered_blobs = self
            .client
            .resource_manager(&committees)
            .await
            .register_walrus_store_blobs(
                encoded_blobs_with_status,
                store_args.epochs_ahead,
                store_args.persistence,
                store_args.store_optimizations,
            )
            .await?;
        let store_op_duration = store_op_timer.elapsed();
        tracing::info!(
            duration = ?store_op_duration,
            "{} blob resources obtained",
            registered_blobs.len(),
        );
        store_args.maybe_observe_store_operation(store_op_duration);

        // 3. Partition blobs based on the operation required
        for registered_blob in registered_blobs {
            if registered_blob.is_completed() {
                self.completed_blobs.push(registered_blob);
            } else if registered_blob.ready_to_extend() {
                self.to_be_extended.push(registered_blob);
            } else if registered_blob.ready_to_store_to_nodes() {
                self.to_be_uploaded.push(registered_blob);
            } else {
                // This case should ideally not be reached if logic is correct
                return Err(crate::error::ClientError::store_blob_internal(format!(
                    "unexpected blob state after registration: {registered_blob:?}"
                )));
            }
        }

        Ok(())
    }

    /// Certifies the blobs on-chain using the Sui client.
    ///
    /// This method takes blobs that have certificates and certifies them on the blockchain.
    /// Returns the certification results.
    pub async fn certify(
        &self,
        blobs_with_certificates: Vec<WalrusStoreBlob<'a, T>>,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<CertifyAndExtendBlobResult>> {
        if blobs_with_certificates.is_empty() {
            return Ok(Vec::new());
        }

        let cert_and_extend_params: Vec<CertifyAndExtendBlobParams> = blobs_with_certificates
            .iter()
            .map(|blob| {
                blob.get_certify_and_extend_params()
                    .expect("should be a CertifyAndExtendBlobParams")
            })
            .collect();

        tracing::info!(
            "certifying {} blobs on Sui",
            cert_and_extend_params.len()
        );

        let sui_cert_timer = Instant::now();
        let cert_and_extend_results = self
            .client
            .sui_client()
            .certify_and_extend_blobs(&cert_and_extend_params, store_args.post_store)
            .await
            .map_err(|error| {
                tracing::warn!(
                    %error,
                    "failure occurred while certifying and extending blobs on Sui"
                );
                crate::error::ClientError::from(crate::error::ClientErrorKind::CertificationFailed(error))
            })?;

        let sui_cert_timer_duration = sui_cert_timer.elapsed();
        tracing::info!(
            duration = ?sui_cert_timer_duration,
            "certified {} blobs on Sui",
            cert_and_extend_params.len()
        );
        store_args.maybe_observe_upload_certificate(sui_cert_timer_duration);

        Ok(cert_and_extend_results)
    }

    /// Completes blobs by matching them with their certification results and price computation.
    pub async fn complete_blobs(
        blobs_to_complete: Vec<WalrusStoreBlob<'a, T>>,
        cert_results: Vec<CertifyAndExtendBlobResult>,
        price_computation: &PriceComputation,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        // Build map from ObjectID to CertifyAndExtendBlobResult
        let result_map: HashMap<ObjectID, CertifyAndExtendBlobResult> = cert_results
            .into_iter()
            .map(|result| (result.blob_object_id, result))
            .collect();

        let mut completed_blobs = Vec::new();
        for blob in blobs_to_complete {
            let Some(object_id) = blob.get_object_id() else {
                return Err(crate::error::ClientError::store_blob_internal(format!(
                    "Invalid blob state {blob:?}"
                )));
            };
            if let Some(result) = result_map.get(&object_id) {
                completed_blobs.push(blob.with_certify_and_extend_result(result.clone(), price_computation)?);
            } else {
                return Err(crate::error::ClientError::store_blob_internal(format!(
                    "No certification result found for blob {blob:?}"
                )));
            }
        }

        Ok(completed_blobs)
    }
}
