// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use sui_types::{base_types::ObjectID, digests::TransactionDigest, event::EventID};
use tokio::sync::broadcast::{self, Sender};
use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use walrus_core::{messages::ConfirmationCertificate, BlobId, EncodingType, Epoch};

use crate::{
    client::{ContractClient, ReadClient, SuiClientResult},
    types::{
        Blob,
        BlobCertified,
        BlobRegistered,
        Committee,
        EpochStatus,
        StorageResource,
        SystemObject,
    },
};

/// Mock `ReadClient` for testing.
#[derive(Debug, Clone)]
pub struct MockSuiReadClient {
    registered_events: Arc<Mutex<Vec<BlobRegistered>>>,
    certified_events: Arc<Mutex<Vec<BlobCertified>>>,
    registered_events_channel: Sender<BlobRegistered>,
    certified_events_channel: Sender<BlobCertified>,
    committee: Committee,
}

impl Default for MockSuiReadClient {
    fn default() -> Self {
        // A channel capacity of 1024 should be enough capacity to not feel backpressure for testing
        let (registered_events_channel, _) = broadcast::channel(1024);
        let (certified_events_channel, _) = broadcast::channel(1024);
        Self {
            registered_events: Arc::default(),
            certified_events: Arc::default(),
            registered_events_channel,
            certified_events_channel,
            committee: Committee::default(),
        }
    }
}

impl MockSuiReadClient {
    /// Create a new mock client that returns the provided events in the event streams.
    fn new_with_events(
        registered_events: Vec<BlobRegistered>,
        certified_events: Vec<BlobCertified>,
        committee: Committee,
    ) -> Self {
        // A channel capacity of 1024 should be enough capacity to not feel backpressure for testing
        let (registered_events_channel, _) = broadcast::channel(1024);
        let (certified_events_channel, _) = broadcast::channel(1024);
        Self {
            registered_events: Arc::new(Mutex::new(registered_events)),
            certified_events: Arc::new(Mutex::new(certified_events)),
            registered_events_channel,
            certified_events_channel,
            committee,
        }
    }

    /// Create a new mock client that returns registered and certified events for
    /// the given `blob_ids` with the specified committee (if provided, default otherwise).
    pub fn new_with_blob_ids(
        blob_ids: impl IntoIterator<Item = BlobId>,
        committee: Option<Committee>,
    ) -> Self {
        let (registered_events, certified_events) = blob_ids
            .into_iter()
            .map(|blob_id| {
                (
                    BlobRegistered::for_testing(blob_id),
                    BlobCertified::for_testing(blob_id),
                )
            })
            .unzip();
        Self::new_with_events(
            registered_events,
            certified_events,
            committee.unwrap_or_default(),
        )
    }

    /// Add a `BlobRegistered` event to the event streams provided by this client.
    pub fn add_registered_event(&self, event: BlobRegistered) {
        // ignore unsuccessful sends, we might have new receivers in the future
        let _ = self.registered_events_channel.send(event.clone());
        // unwrap `LockResult` since we are not expecting
        // threads to ever fail while holding the lock.
        (*self.registered_events.lock().unwrap()).push(event);
    }

    /// Add a `BlobCertified` event to the event streams provided by this client.
    pub fn add_certified_event(&self, event: BlobCertified) {
        // ignore unsuccessful sends, we might have new receivers in the future
        let _ = self.certified_events_channel.send(event.clone());
        // unwrap `LockResult` since we are not expecting
        // threads to ever fail while holding the lock.
        (*self.certified_events.lock().unwrap()).push(event);
    }
}

impl ReadClient for MockSuiReadClient {
    async fn price_per_unit_size(&self) -> SuiClientResult<u64> {
        Ok(10)
    }

    async fn blob_registered_events(
        &self,
        polling_interval: Duration,
        _cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = BlobRegistered>> {
        let rx = self.registered_events_channel.subscribe();

        let registered_events_guard = self.registered_events.lock().unwrap();
        let old_event_stream = tokio_stream::iter((*registered_events_guard).clone());
        // release lock
        drop(registered_events_guard);
        Ok(old_event_stream.chain(
            BroadcastStream::from(rx)
                .filter_map(|res| res.ok())
                .throttle(polling_interval),
        ))
    }

    async fn blob_certified_events(
        &self,
        polling_interval: Duration,
        _cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = BlobCertified>> {
        let rx = self.certified_events_channel.subscribe();

        let certified_events_guard = self.certified_events.lock().unwrap();
        let old_event_stream = tokio_stream::iter((*certified_events_guard).clone());
        // release lock
        drop(certified_events_guard);
        Ok(old_event_stream.chain(
            BroadcastStream::from(rx)
                .filter_map(|res| res.ok())
                .throttle(polling_interval),
        ))
    }

    async fn get_system_object(&self) -> SuiClientResult<SystemObject> {
        Ok(system_object_from_committee(self.committee.to_owned()))
    }

    async fn current_committee(&self) -> SuiClientResult<Committee> {
        Ok(self.committee.to_owned())
    }
}

/// Mock `ContractClient` for testing.
/// Currently only covers the happy case, i.e. every call succeeds.
#[derive(Debug)]
pub struct MockContractClient {
    /// Client to read Walrus on-chain state
    read_client: MockSuiReadClient,
    current_epoch: Epoch,
}

impl MockContractClient {
    /// Constructor for [`MockContractClient`].
    pub fn new(current_epoch: Epoch) -> Self {
        let read_client = MockSuiReadClient::default();
        Self {
            read_client,
            current_epoch,
        }
    }

    /// Construct a [`MockContractClient`] with a provided [`MockSuiReadClient`].
    pub fn new_with_read_client(current_epoch: Epoch, read_client: MockSuiReadClient) -> Self {
        Self {
            read_client,
            current_epoch,
        }
    }
}

impl Default for MockContractClient {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ContractClient for MockContractClient {
    async fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: u64,
    ) -> SuiClientResult<StorageResource> {
        Ok(StorageResource {
            id: ObjectID::random(),
            start_epoch: self.current_epoch,
            end_epoch: self.current_epoch + epochs_ahead,
            storage_size: encoded_size,
        })
    }

    async fn register_blob(
        &self,
        storage: &StorageResource,
        blob_id: BlobId,
        encoded_size: u64,
        erasure_code_type: EncodingType,
    ) -> SuiClientResult<Blob> {
        self.read_client.add_registered_event(BlobRegistered {
            epoch: self.current_epoch,
            blob_id,
            size: encoded_size,
            erasure_code_type,
            end_epoch: storage.end_epoch,
            event_id: event_id_for_testing(),
        });
        Ok(Blob {
            id: ObjectID::random(),
            stored_epoch: self.current_epoch,
            blob_id,
            encoded_size,
            erasure_code_type,
            certified: None,
            storage: storage.clone(),
        })
    }

    async fn certify_blob(
        &self,
        blob: &Blob,
        _certificate: &ConfirmationCertificate,
    ) -> SuiClientResult<Blob> {
        self.read_client.add_certified_event(BlobCertified {
            epoch: self.current_epoch,
            blob_id: blob.blob_id,
            end_epoch: blob.storage.end_epoch,
            event_id: event_id_for_testing(),
        });
        let mut blob = blob.clone();
        blob.certified = Some(self.current_epoch);
        Ok(blob)
    }

    fn read_client(&self) -> &impl ReadClient {
        &self.read_client
    }
}

/// Returns a random `EventID` for testing.
pub fn event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::random(),
        event_seq: 0,
    }
}

fn system_object_from_committee(committee: Committee) -> SystemObject {
    SystemObject {
        id: ObjectID::from_single_byte(42),
        current_committee: committee,
        epoch_status: EpochStatus::Done,
        total_capacity_size: 1_000_000_000_000_000,
        used_capacity_size: 0,
        price_per_unit_size: 10,
        past_committees_object: ObjectID::from_single_byte(37),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use fastcrypto::bls12381::min_pk::BLS12381AggregateSignature;

    use super::*;

    #[tokio::test]
    async fn test_register_mock_clients() -> anyhow::Result<()> {
        let walrus_client = MockContractClient::default();

        // Get event streams for the events
        let polling_duration = std::time::Duration::from_millis(1);
        let mut registered_events = pin!(
            walrus_client
                .read_client()
                .blob_registered_events(polling_duration, None)
                .await?
        );
        let mut certified_events = pin!(
            walrus_client
                .read_client()
                .blob_certified_events(polling_duration, None)
                .await?
        );

        let size = 10000;
        let storage_resource = walrus_client.reserve_space(size, 3).await?;
        assert_eq!(storage_resource.start_epoch, 0);
        assert_eq!(storage_resource.end_epoch, 3);
        assert_eq!(storage_resource.storage_size, size);
        #[rustfmt::skip]
        let blob_id = BlobId([
            1, 2, 3, 4, 5, 6, 7, 8,
            1, 2, 3, 4, 5, 6, 7, 8,
            1, 2, 3, 4, 5, 6, 7, 8,
            1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        let blob_obj = walrus_client
            .register_blob(&storage_resource, blob_id, size, EncodingType::RedStuff)
            .await?;
        assert_eq!(blob_obj.blob_id, blob_id);
        assert_eq!(blob_obj.encoded_size, size);
        assert_eq!(blob_obj.certified, None);
        assert_eq!(blob_obj.storage, storage_resource);
        assert_eq!(blob_obj.stored_epoch, 0);

        // Make sure that we got the expected event
        let blob_registered = registered_events.next().await.unwrap();
        assert_eq!(blob_registered.blob_id, blob_id);
        assert_eq!(blob_registered.epoch, blob_obj.stored_epoch);
        assert_eq!(
            blob_registered.erasure_code_type,
            blob_obj.erasure_code_type
        );
        assert_eq!(blob_registered.end_epoch, storage_resource.end_epoch);
        assert_eq!(blob_registered.size, blob_obj.encoded_size);

        let blob_obj = walrus_client
            .certify_blob(
                &blob_obj,
                // Dummy certificate, currently not checked by the mock client
                &ConfirmationCertificate {
                    signers: vec![],
                    confirmation: vec![],
                    signature: BLS12381AggregateSignature::default(),
                },
            )
            .await?;
        assert_eq!(blob_obj.certified, Some(0));

        // Make sure that we got the expected event
        let blob_certified = certified_events.next().await.unwrap();
        assert_eq!(blob_certified.blob_id, blob_id);
        assert_eq!(Some(blob_registered.epoch), blob_obj.certified);
        assert_eq!(blob_certified.end_epoch, storage_resource.end_epoch);

        // Get new event stream to check if we receive previous events
        let mut certified_events = pin!(
            walrus_client
                .read_client
                .blob_certified_events(polling_duration, None)
                .await?
        );

        // Make sure that we got the expected event
        let blob_certified = certified_events.next().await.unwrap();
        assert_eq!(blob_certified.blob_id, blob_id);

        Ok(())
    }
}
