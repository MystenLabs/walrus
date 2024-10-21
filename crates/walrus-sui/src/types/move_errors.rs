// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus move error bindings. Converts Move execution errors from a string into matchable errors.

use std::{fmt::Display, str::FromStr};

use move_core_types::account_address::AccountAddress;
use sui_types::base_types::ObjectID;
use thiserror;

#[derive(Debug, thiserror::Error)]
#[error("could not convert the raw move error: {0}")]
/// Error occurs when converting a RawMoveError to its corresponding error kind.
pub struct ConversionError(RawMoveError);

#[derive(Debug, thiserror::Error)]
#[error("could not convert the str to a raw move error")]
/// Error occurs when converting a string to a RawMoveError.
pub struct MoveParseError;

macro_rules! move_error_kind {
    (
        $errorname:ident,
        $packagename:ident::$modulename:ident,
        $($errortype:ident, $errorcode:expr),+ $(,)?
    ) => {
        #[doc=stringify!(Error kinds for the Move $modulename in package $packagename)]
        #[derive(Debug, Clone)]
        #[allow(clippy::enum_variant_names)]
        pub enum $errorname {
            $(
                #[doc=stringify!(Error kind for the move error $errortype)]
                $errortype(RawMoveError)
            ),*
        }


        impl Display for $errorname {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let (raw, variant) = match self {
                    $(
                        Self::$errortype(raw) => (raw, stringify!($errortype))
                    ),*
                };
                write!(
                    f,
                    "contract execution failed in {}::{}::{} at address {} with error type {}",
                    stringify!($packagename),
                    stringify!($modulename),
                    raw.function,
                    raw.package,
                    variant,
                )
            }
        }

        impl TryFrom<RawMoveError> for $errorname {
            type Error = ConversionError;

            fn try_from(value: RawMoveError) -> Result<Self, Self::Error> {
                match value.error_code {
                    $($errorcode => Ok(Self::$errortype(value))),* ,
                    _ => Err(ConversionError(value)),
                }
            }
        }
    };
}

/// Parsed, but non-typed Move execution error.
#[derive(Debug, Clone)]
pub struct RawMoveError {
    /// The name of the function where the error occurred.
    pub function: String,
    /// The name of the module where the error occurred.
    pub module: String,
    /// The name of the package where the error occurred.
    #[allow(unused)]
    pub package: ObjectID,
    /// The error code used for the move error.
    pub error_code: u64,
    /// The unparsed error.
    #[allow(unused)]
    pub unparsed: String,
    /// The index of the command in which the error occurred
    #[allow(unused)]
    pub command_index: u16,
    /// The instruction in which the error occurred
    #[allow(unused)]
    pub instruction: u16,
}

impl Display for RawMoveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "foo")
    }
}

impl FromStr for RawMoveError {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Taken with small modifications from
        // https://github.com/MystenLabs/sui/blob/main/crates/sui/src/clever_error_rendering.rs
        // The comment there applies here equally.
        use regex::Regex;
        let re = Regex::new(
            r#"MoveAbort.*address:\s*(.*?),.* name:.*Identifier\((.*?)\).*instruction:\s+(\d+),.*function_name:.*Some\((.*?)\).*},\s*(\d+).*in command\s*(\d+)"#, // editorconfig-checker-disable
        )
        .unwrap();
        let Some(captures) = re.captures(s) else {
            anyhow::bail!(
                "Cannot parse abort status string: {} as a move abort string",
                s
            );
        };

        // Remove any escape characters from the string if present.
        let clean_string = |s: &str| s.replace(['\\', '\"'], "");

        let address = AccountAddress::from_hex(&captures[1])?;
        let package = ObjectID::from_address(address);
        let module = clean_string(&captures[2]);
        let instruction = captures[3].parse::<u16>()?;
        let function = clean_string(&captures[4]);
        let error_code = captures[5].parse::<u64>()?;
        let command_index = captures[6].parse::<u16>()?;
        Ok(RawMoveError {
            function,
            module,
            package,
            error_code,
            unparsed: s.to_owned(),
            command_index,
            instruction,
        })
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
/// An error occurring during a Move contract execution.
pub enum MoveExecutionError {
    #[error("{0}")]
    /// Error in `walrus::staking_inner`.
    StakingInner(StakingInnerError),
    #[error("{0}")]
    /// Error in `walrus::storage_node`.
    StorageNode(StorageNodeError),
    #[error("{0}")]
    /// Error in `walrus::blob`.
    Blob(BlobError),
    #[error("{0}")]
    /// Error in `walrus::staked_wal`.
    StakedWal(StakedWalError),
    #[error("{0}")]
    /// Error in `walrus::messages`.
    Messages(MessagesError),
    #[error("{0}")]
    /// Error in `walrus::storage_resource`.
    StorageResource(StorageResourceError),
    #[error("{0}")]
    /// Error in `walrus::staking_pool`.
    StakingPool(StakingPoolError),
    #[error("{0}")]
    /// Error in `walrus::system_state_inner`.
    SystemStateInner(SystemStateInnerError),
    #[error("{0}")]
    /// Error in `walrus::blob`.
    StorageAccounting(StorageAccountingError),
    #[error("{0}")]
    /// Error in `walrus::encoding`.
    Encoding(EncodingError),
    #[error("{0}")]
    /// Error in `walrus::bls_aggregate`.
    BlsAggregate(BlsAggregateError),
    #[error("{0}")]
    /// Error in `wal_exchange::wal_exchange`.
    WalExchange(WalExchangeError),
    #[error("Unknown move error occurred: {0}")]
    /// Error in other move modules.
    OtherMoveModule(RawMoveError),
    #[error("unparsable move error occurred: {0}")]
    /// A non-parsable move error.
    NotParsable(String),
}

impl From<RawMoveError> for MoveExecutionError {
    fn from(value: RawMoveError) -> Self {
        match value.module.as_str() {
            "staking_inner" => StakingInnerError::try_from(value).map(Self::StakingInner),
            "storage_node" => StorageNodeError::try_from(value).map(Self::StorageNode),
            "blob" => BlobError::try_from(value).map(Self::Blob),
            "staked_wal" => StakedWalError::try_from(value).map(Self::StakedWal),
            "messages" => MessagesError::try_from(value).map(Self::Messages),
            "storage_resource" => StorageResourceError::try_from(value).map(Self::StorageResource),
            "staking_pool" => StakingPoolError::try_from(value).map(Self::StakingPool),
            "system_state_inner" => {
                SystemStateInnerError::try_from(value).map(Self::SystemStateInner)
            }
            "storage_accounting" => {
                StorageAccountingError::try_from(value).map(Self::StorageAccounting)
            }
            "encoding" => EncodingError::try_from(value).map(Self::Encoding),
            "bls_aggregate" => BlsAggregateError::try_from(value).map(Self::BlsAggregate),
            "wal_exchange" => WalExchangeError::try_from(value).map(Self::WalExchange),
            _ => Ok(Self::OtherMoveModule(value)),
        }
        .unwrap_or_else(|e| Self::OtherMoveModule(e.0))
    }
}

impl From<String> for MoveExecutionError {
    fn from(value: String) -> Self {
        RawMoveError::from_str(&value)
            .map(Self::from)
            .unwrap_or_else(|_| Self::NotParsable(value))
    }
}

move_error_kind!(
    StakingInnerError,
    walrus::staking_inner,
    EWrongEpochState,
    0,
    EInvalidSyncEpoch,
    1,
    EDuplicateSyncDone,
    2,
    ENoStake,
    3,
    ENotInCommittee,
    4,
    ECommitteeSelected,
    5,
    ENextCommitteeIsEmpty,
    6,
    ENotImplemented,
    264,
);

move_error_kind!(
    StorageNodeError,
    walrus::storage_node,
    EInvalidNetworkPublicKey,
    0,
);

move_error_kind!(
    BlobError,
    walrus::blob,
    ENotCertified,
    0,
    EBlobNotDeletable,
    1,
    EResourceBounds,
    2,
    EResourceSize,
    3,
    EWrongEpoch,
    4,
    EAlreadyCertified,
    5,
    EInvalidBlobId,
    6,
);

move_error_kind!(
    StakedWalError,
    walrus::staked_wal,
    ENotWithdrawing,
    0,
    EMetadataMismatch,
    1,
    EInvalidAmount,
    2,
    ENonZeroPrincipal,
    3,
    ECantJoinWithdrawing,
    4,
    ECantSplitWithdrawing,
    5,
);

move_error_kind!(
    MessagesError,
    walrus::messages,
    EIncorrectAppId,
    0,
    EIncorrectEpoch,
    1,
    EInvalidMsgType,
    2,
    EIncorrectIntentVersion,
    3,
    EInvalidKeyLength,
    4,
);

move_error_kind!(
    StorageResourceError,
    walrus::storage_resource,
    EInvalidEpoch,
    0,
    EIncompatibleEpochs,
    1,
    EIncompatibleAmount,
    2,
);

move_error_kind!(
    StakingPoolError,
    walrus::staking_pool,
    EPoolAlreadyUpdated,
    0,
    ECalculationError,
    1,
    EIncorrectEpochAdvance,
    2,
    EPoolNotEmpty,
    3,
    EInvalidProofOfPossession,
    4,
    EPoolAlreadyWithdrawing,
    5,
    EPoolIsNotActive,
    6,
    EZeroStake,
    7,
    EPoolIsNotNew,
    8,
    EIncorrectPoolId,
    9,
    ENotWithdrawing,
    10,
    EWithdrawEpochNotReached,
    11,
    EActivationEpochNotReached,
    12,
);

move_error_kind!(
    SystemStateInnerError,
    walrus::system_state_inner,
    EInvalidMaxEpochsAhead,
    0,
    EStorageExceeded,
    1,
    EInvalidEpochsAhead,
    2,
    EInvalidIdEpoch,
    3,
    EIncorrectCommittee,
    4,
    EInvalidAccountingEpoch,
    5,
    EIncorrectAttestation,
    6,
    ERepeatedAttestation,
    7,
    ENotCommitteeMember,
    8,
);

move_error_kind!(
    StorageAccountingError,
    walrus::storage_accounting,
    ETooFarInFuture,
    0,
);

move_error_kind!(EncodingError, walrus::encoding, EInvalidEncoding, 0,);

move_error_kind!(
    BlsAggregateError,
    walrus::bls_aggregate,
    ETotalMemberOrder,
    0,
    ESigVerification,
    1,
    ENotEnoughStake,
    2,
    EIncorrectCommittee,
    3,
);

move_error_kind!(
    WalExchangeError,
    wal_exchange::wal_exchange,
    EInsufficientFundsInExchange,
    0,
    EInsufficientInputBalance,
    1,
    EUnauthorizedAdminCap,
    2,
    EInvalidExchangeRate,
    3,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_abort_status_string() {
        let error_strings = [
            r#"MoveAbort(MoveLocation { module: ModuleId { address: a8e03746f47f5a7eca4c465ec40c22d4ea1a7fd7d70f265c2dc53e248c18a061, name: Identifier(\"staking_pool\") }, function: 0, instruction: 24, function_name: Some(\"new\") }, 4) in command 0"#,
            r#"MoveAbort(MoveLocation { module: ModuleId { address: a8e03746f47f5a7eca4c465ec40c22d4ea1a7fd7d70f265c2dc53e248c18a061, name: Identifier(\"staking_pool\") }, function: 0, instruction: 24, function_name: Some(\"new\") }, 9223372608085950473) in command 1"#,
            r#"MoveAbort(MoveLocation { module: ModuleId { address: bf0eb79ce75d6cf6c18b6d480b85726aab06a2639569e54ddf1e25d89b549239, name: Identifier(\"staking_inner\") }, function: 23, instruction: 14, function_name: Some(\"epoch_sync_done\") }, 1) in command 0"#,
            "Not matching",
        ];

        match error_strings[0].to_owned().into() {
            MoveExecutionError::StakingPool(StakingPoolError::EInvalidProofOfPossession(raw)) => {
                assert!(raw.function == "new");
                assert!(raw.command_index == 0);
            }
            _ => panic!("wrong error"),
        }

        // Second error does not match a know type
        match error_strings[1].to_owned().into() {
            MoveExecutionError::OtherMoveModule(raw) => {
                assert!(raw.function == "new");
                assert!(raw.command_index == 1);
            }
            _ => panic!("wrong error"),
        }

        match error_strings[2].to_owned().into() {
            MoveExecutionError::StakingInner(StakingInnerError::EInvalidSyncEpoch(raw)) => {
                assert!(raw.function == "epoch_sync_done");
                assert!(raw.command_index == 0);
            }
            _ => panic!("wrong error"),
        }

        match error_strings[3].to_owned().into() {
            MoveExecutionError::NotParsable(_) => {}
            _ => panic!("wrong error"),
        }
    }
}
