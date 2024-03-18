// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::{
    bls12381::min_pk::{BLS12381KeyPair, BLS12381PublicKey, BLS12381Signature},
    hash::Blake2b256,
};

/// A public key.
pub type PublicKey = BLS12381PublicKey;
/// A key pair.
pub type KeyPair = BLS12381KeyPair;
/// A signature for a blob.
pub type Signature = BLS12381Signature;
/// A certificate for a blob, represented as a list of signer-signature pairs.
pub type Certificate = Vec<(PublicKey, Signature)>;
/// The hash function used for building metadata.
pub type HashFunction = Blake2b256;
