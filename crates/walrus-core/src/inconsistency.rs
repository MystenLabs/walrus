// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Proofs for inconsistent encoding.
//!
//! There are several ways in which a blob can be inconsistent:
//!
//! 1. **Inconsistency in the blob-ID computation:** the blob ID is not computed correctly from the
//!    sliver hashes and other metadata.
//! 2. **Inconsistency in the sliver-hash computation:** the sliver hash is not correctly computed
//!    from the individual symbols.
//! 3. **Inconsistency in the encoding:** some symbols are not computed correctly; in particular,
//!    this covers cases where a symbol, which is always part of two (expanded) slivers, is stored
//!    differently on those slivers.
//!
//! Case 1 is a global inconsistency, which can be checked by *all* storage nodes. As such, a blob
//! with this type of inconsistency will never be certified and thus never has to be marked as
//! inconsistent.
//!
//! Therefore, we only require inconsistency proofs for the "local" cases 2 and 3. These are not
//! always distinguishable in practice as the computation of the sliver hashes is hidden. We thus
//! treat them equally and provide a single type of inconsistency proof for both cases.
//!
//! This proof emerges when a sliver cannot be recovered from recovery symbols. Consider a storage
//! node attempting to recover a primary sliver (without loss of generality). It will receive
//! authenticated (with their respective Merkle proofs) recovery symbols computed from other nodes’
//! secondary slivers. If it can decode some sliver from these symbols that is inconsistent with
//! that target sliver’s hash in the metadata, either the encoding or the computation of the hashes
//! must be inconsistent (this can be case 2 or 3 above).
//!
//! An inconsistency proof consists of the following:
//!
//! 1. The blob metadata containing the two sliver hashes (implicit, as this is stored on all
//!    storage nodes anyway).
//! 2. A number of recovery symbols for the same target sliver with their respective Merkle proofs
//!    from the source sliver that can be successfully decoded.
//!
//! Given these pieces, any entity can verify the proof as follows:
//!
//! 1. Verify the Merkle proofs of all recovery symbols based on their respective sliver hashes in
//!    the metadata.
//! 2. Decode the target sliver.
//! 3. Compute the hash of the target sliver (by re-encoding it and constructing the Merkle tree).
//! 4. Check that this hash is different from the one stored in the metadata.

use std::marker::PhantomData;

use crate::{
    encoding::{DecodingSymbol, EncodingAxis, EncodingConfig, Sliver, SliverVerificationError},
    merkle::MerkleAuth,
    metadata::BlobMetadata,
    SliverIndex,
};

/// Failure cases when verifying an [`InconsistencyProof`].
#[derive(thiserror::Error, Debug, Eq, PartialEq)]
pub enum InconsistencyVerificationError {
    /// No sliver can be decoded from the authentic recovery symbols.
    #[error("no sliver can be decoded from the recovery symbols")]
    RecoveryFailure,
    /// An error occurred during the verification of the target sliver.
    #[error(transparent)]
    VerificationError(#[from] SliverVerificationError),
    /// The recovered sliver is consistent with the metadata.
    #[error("the target sliver is authentic")]
    AuthenticTargetSliver,
}

/// The structure of an inconsistency proof.
///
/// See [the module documentation][self] for further details.
#[derive(Debug, Clone)]
pub struct InconsistencyProof<T: EncodingAxis, U: MerkleAuth> {
    target_sliver_index: SliverIndex,
    recovery_symbols: Vec<DecodingSymbol<T, U>>,
    _encoding_axis: PhantomData<T>,
}

impl<T: EncodingAxis, U: MerkleAuth> InconsistencyProof<T, U> {
    /// Creates a new inconsistency proof from the provided index and recovery symbols.
    ///
    /// This does *not* verify that the proof is correct. Use [`Self::verify`] for that.
    pub fn new(
        target_sliver_index: SliverIndex,
        recovery_symbols: Vec<DecodingSymbol<T, U>>,
    ) -> Self {
        Self {
            target_sliver_index,
            recovery_symbols,
            _encoding_axis: PhantomData,
        }
    }

    /// Verifies the inconsistency proof.
    ///
    /// Returns `Ok(())` if the proof is correct, otherwise returns an
    /// [`InconsistencyVerificationError`].
    pub fn verify(
        self,
        metadata: &BlobMetadata,
        encoding_config: &EncodingConfig,
    ) -> Result<(), InconsistencyVerificationError> {
        let span = tracing::warn_span!("verifying inconsistency proof", ?metadata);
        let _guard = span.enter();

        let sliver = Sliver::recover_sliver_with_verification(
            self.recovery_symbols,
            self.target_sliver_index,
            metadata,
            encoding_config,
        )
        .ok_or(InconsistencyVerificationError::RecoveryFailure)?;
        match sliver.verify(encoding_config, metadata) {
            Ok(()) => Err(InconsistencyVerificationError::AuthenticTargetSliver),
            Err(SliverVerificationError::MerkleRootMismatch) => Ok(()),
            // Any other error indicates an internal problem, not an inconsistent blob.
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use fastcrypto::hash::Blake2b256;
    use walrus_test_utils::Result;

    use super::*;
    use crate::{merkle::Node, test_utils};

    #[test]
    fn successful_inconsistency_proof() -> Result<()> {
        let blob = walrus_test_utils::random_data(314);
        let encoding_config = test_utils::encoding_config();
        let (sliver_pairs, metadata) = encoding_config
            .get_blob_encoder(&blob)?
            .encode_with_metadata();
        let target_sliver_index = SliverIndex(0);
        let recovery_symbols = walrus_test_utils::random_subset(
            (1..encoding_config.n_shards.get()).map(|i| {
                sliver_pairs[i as usize]
                    .secondary
                    .recovery_symbol_for_sliver_with_proof::<Blake2b256>(
                        target_sliver_index.into(),
                        &encoding_config,
                    )
                    .unwrap()
            }),
            encoding_config.n_secondary_source_symbols().get().into(),
        )
        .collect();
        let mut metadata = metadata.metadata().clone();
        metadata.hashes[0].primary_hash = Node::Digest([0; 32]);
        let inconsistency_proof = InconsistencyProof::new(0.into(), recovery_symbols);

        inconsistency_proof.verify(&metadata, &encoding_config)?;
        Ok(())
    }
}
