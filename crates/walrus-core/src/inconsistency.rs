// Copyright (c) Walrus Foundation
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

use alloc::vec::Vec;
use core::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::{
    encoding::{
        EncodingAxis,
        EncodingConfig,
        Primary,
        RecoverySymbol,
        Secondary,
        SliverData,
        SliverRecoveryError,
        SliverVerificationError,
    },
    merkle::MerkleAuth,
    metadata::BlobMetadata,
    SliverIndex,
};

/// Failure cases when verifying an [`InconsistencyProof`].
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum InconsistencyVerificationError {
    /// No sliver can be decoded from the authentic recovery symbols.
    #[error(transparent)]
    RecoveryFailure(#[from] SliverRecoveryError),
    /// An error occurred during the verification of the target sliver.
    #[error(transparent)]
    VerificationError(#[from] SliverVerificationError),
    /// The recovered sliver is consistent with the metadata.
    #[error("the target sliver is consistent with the metadata")]
    SliverNotInconsistent,
}

/// An inconsistency proof for an encoding on the primary axis.
pub type PrimaryInconsistencyProof<U> = InconsistencyProof<Primary, U>;

/// An inconsistency proof for an encoding on the secondary axis.
pub type SecondaryInconsistencyProof<U> = InconsistencyProof<Secondary, U>;

/// The structure of an inconsistency proof.
///
/// See [the module documentation][self] for further details.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    deserialize = "for<'a> RecoverySymbol<T, U>: Deserialize<'a>",
    serialize = "RecoverySymbol<T, U>: Serialize"
))]
pub struct InconsistencyProof<T: EncodingAxis, U: MerkleAuth> {
    target_sliver_index: SliverIndex,
    recovery_symbols: Vec<RecoverySymbol<T, U>>,
    _encoding_axis: PhantomData<T>,
}

impl<T: EncodingAxis, U: MerkleAuth> InconsistencyProof<T, U> {
    /// Creates a new inconsistency proof from the provided index and recovery symbols.
    ///
    /// This does *not* verify that the proof is correct. Use [`Self::verify`] for that.
    pub fn new(
        target_sliver_index: SliverIndex,
        recovery_symbols: Vec<RecoverySymbol<T, U>>,
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
        let sliver = SliverData::recover_sliver(
            self.recovery_symbols,
            self.target_sliver_index,
            metadata,
            encoding_config,
        )?;
        match sliver.verify(encoding_config, metadata) {
            Ok(()) => Err(InconsistencyVerificationError::SliverNotInconsistent),
            Err(SliverVerificationError::MerkleRootMismatch) => Ok(()),
            // Any other error indicates an internal problem, not an inconsistent blob.
            Err(e) => Err(e.into()),
        }
    }
}

/// Return type when attempting to recover a sliver.
///
/// On successful recovery and verification, this contains the target [`SliverData`]. Otherwise, it
/// contains a generated [`InconsistencyProof`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SliverOrInconsistencyProof<T: EncodingAxis, U: MerkleAuth> {
    /// The recovered sliver.
    Sliver(SliverData<T>),
    /// An inconsistency proof for the blob.
    InconsistencyProof(InconsistencyProof<T, U>),
}

impl<T: EncodingAxis, U: MerkleAuth> From<SliverData<T>> for SliverOrInconsistencyProof<T, U> {
    fn from(value: SliverData<T>) -> Self {
        Self::Sliver(value)
    }
}

impl<T: EncodingAxis, U: MerkleAuth> From<InconsistencyProof<T, U>>
    for SliverOrInconsistencyProof<T, U>
{
    fn from(value: InconsistencyProof<T, U>) -> Self {
        Self::InconsistencyProof(value)
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::Result;

    use super::*;
    use crate::{
        encoding::EncodingConfigTrait as _,
        merkle::Node,
        test_utils::generate_config_metadata_and_valid_recovery_symbols,
    };

    #[test]
    fn valid_inconsistency_proof() -> Result<()> {
        let (encoding_config, metadata, target_sliver_index, recovery_symbols) =
            generate_config_metadata_and_valid_recovery_symbols()?;
        let mut metadata = metadata.metadata().clone();
        metadata.mut_inner().hashes[0].primary_hash = Node::Digest([0; 32]);
        let inconsistency_proof = InconsistencyProof::new(target_sliver_index, recovery_symbols);

        inconsistency_proof.verify(&metadata, &encoding_config)?;
        Ok(())
    }

    #[test]
    fn invalid_inconsistency_proof_when_just_changing_the_target_index() -> Result<()> {
        let (encoding_config, metadata, target_sliver_index, recovery_symbols) =
            generate_config_metadata_and_valid_recovery_symbols()?;
        let inconsistency_proof =
            InconsistencyProof::new(SliverIndex(target_sliver_index.get() + 1), recovery_symbols);

        assert!(matches!(
            inconsistency_proof.verify(metadata.metadata(), &encoding_config),
            Err(InconsistencyVerificationError::RecoveryFailure(_))
        ));
        Ok(())
    }

    #[test]
    fn invalid_inconsistency_proof_because_sliver_not_inconsistent() -> Result<()> {
        let (encoding_config, metadata, target_sliver_index, recovery_symbols) =
            generate_config_metadata_and_valid_recovery_symbols()?;
        let inconsistency_proof = InconsistencyProof::new(target_sliver_index, recovery_symbols);

        assert!(matches!(
            inconsistency_proof.verify(metadata.metadata(), &encoding_config),
            Err(InconsistencyVerificationError::SliverNotInconsistent)
        ));
        Ok(())
    }

    #[test]
    fn invalid_inconsistency_proof_because_sliver_cannot_be_decoded() -> Result<()> {
        let (encoding_config, metadata, target_sliver_index, mut recovery_symbols) =
            generate_config_metadata_and_valid_recovery_symbols()?;
        recovery_symbols.truncate(
            usize::from(
                encoding_config
                    .get_for_type(metadata.metadata().encoding_type())
                    .n_secondary_source_symbols()
                    .get(),
            ) - 1,
        );
        let inconsistency_proof = InconsistencyProof::new(target_sliver_index, recovery_symbols);

        assert!(matches!(
            inconsistency_proof.verify(metadata.metadata(), &encoding_config),
            Err(InconsistencyVerificationError::RecoveryFailure(_))
        ));
        Ok(())
    }
}
