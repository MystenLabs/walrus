// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::hash::Blake2b256;

/// A public key.
pub type PublicKey = String;
/// A key pair.
pub type KeyPair = String;
/// A signature for a blob.
pub type Signature = String;
/// A certificate for a blob, represented as a list of signer-signature pairs.
pub type Certificate = Vec<(PublicKey, Signature)>;
/// The hash function used for building metadata.
pub type HashFunction = Blake2b256;
