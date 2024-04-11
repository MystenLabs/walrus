// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

mod mock_clients;
pub mod system_setup;

use fastcrypto::{
    bls12381::min_pk::{BLS12381AggregateSignature, BLS12381PrivateKey},
    traits::{Signer, ToFromBytes},
};
pub use mock_clients::{MockContractClient, MockSuiReadClient};
use sui_types::{digests::TransactionDigest, event::EventID};
use walrus_core::{
    messages::{Confirmation, ConfirmationCertificate},
    BlobId,
    EncodingType,
    Epoch,
};

use crate::types::{BlobCertified, BlobRegistered};

/// Returns a random `EventID` for testing.
pub fn event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::random(),
        event_seq: 0,
    }
}

/// Returns a certificate on the provided `blob_id` from the default test committee.
/// The default test committee is currently a single storage node with sk = 117.
pub fn get_default_blob_certificate(blob_id: BlobId, epoch: Epoch) -> ConfirmationCertificate {
    let mut sk = [0; 32];
    sk[31] = 117;
    let sk = BLS12381PrivateKey::from_bytes(&sk).unwrap();
    let confirmation = bcs::to_bytes(&Confirmation::new(epoch, blob_id)).unwrap();
    let signature = BLS12381AggregateSignature::from(sk.sign(&confirmation));
    ConfirmationCertificate {
        confirmation,
        signature,
        signers: vec![0],
    }
}

/// Trait to provide an event with the specified `blob_id` for testing.
pub trait EventForTesting {
    /// Returns an event with the specified `blob_id` for testing.
    fn for_testing(blob_id: BlobId) -> Self;
}

impl EventForTesting for BlobRegistered {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 0,
            blob_id,
            size: 10000,
            erasure_code_type: EncodingType::RedStuff,
            end_epoch: 42,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for BlobCertified {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 0,
            blob_id,
            end_epoch: 42,
            event_id: event_id_for_testing(),
        }
    }
}
