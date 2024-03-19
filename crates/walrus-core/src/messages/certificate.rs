// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::bls12381::min_pk::BLS12381AggregateSignature;
use serde::{Deserialize, Serialize};

/// A certificate from storage nodes over a [`super::storage_confirmation::Confirmation`]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmationCertificate {
    /// The indices of the signing nodes
    pub signers: Vec<u16>,
    /// The BCS-encoded [`super::storage_confirmation::Confirmation`].
    pub confirmation: Vec<u8>,
    /// The aggregate signature over the BCS encoded confirmation.
    pub signature: BLS12381AggregateSignature,
}
