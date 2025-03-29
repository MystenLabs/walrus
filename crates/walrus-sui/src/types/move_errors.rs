// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus move error bindings. Converts Move execution errors from a string into matchable errors.

use std::{fmt::Display, str::FromStr};

use move_core_types::account_address::AccountAddress;
use sui_types::base_types::ObjectID;
use thiserror;

#[derive(Debug, Clone, thiserror::Error)]
#[error("could not convert the raw move error: {0}")]
/// Error occurs when converting a RawMoveError to its corresponding error kind.
pub struct ConversionError(RawMoveError);

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
                $errortype(Box<RawMoveError>)
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
                    $($errorcode => Ok(Self::$errortype(Box::new(value)))),* ,
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
        write!(
            f,
            "contract execution failed in {}::{}::{} with error code {}: {}",
            self.package, self.module, self.function, self.error_code, self.unparsed,
        )
    }
}

impl FromStr for RawMoveError {
    type Err = anyhow::Error;
    /// Parses a Move abort string into a `RawMoveError`.
    ///
    /// # Panics
    ///
    /// This function will panic if the hardcoded regex is invalid.
    /// This is not expected to happen, as the regex is static and known to be valid.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Taken with small modifications from
        // https://github.com/MystenLabs/sui/blob/main/crates/sui/src/clever_error_rendering.rs
        // The comment there applies here equally.
        use regex::Regex;
        let re = Regex::new(
            r#"MoveAbort.*address:\s*(.*?),.* name:.*Identifier\((.*?)\).*instruction:\s+(\d+),.*function_name:.*Some\((.*?)\).*},\s*(\d+).*in command\s*(\d+)"#, // editorconfig-checker-disable
        )
        .expect("Regex is hardcoded and known to be valid");
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

// Include the auto-generated error definitions based on the Move source code.
include!(concat!(env!("OUT_DIR"), "/contracts_error_defs.rs"));

impl From<&str> for MoveExecutionError {
    fn from(value: &str) -> Self {
        RawMoveError::from_str(value)
            .map(Self::from)
            .unwrap_or_else(|_| Self::NotParsable(value.to_owned()))
    }
}

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

        match error_strings[0].into() {
            MoveExecutionError::StakingPool(StakingPoolError::EInvalidProofOfPossession(raw)) => {
                assert!(raw.function == "new");
                assert!(raw.command_index == 0);
            }
            _ => panic!("wrong error"),
        }

        // Second error does not match a know type
        match error_strings[1].into() {
            MoveExecutionError::OtherMoveModule(raw) => {
                assert!(raw.function == "new");
                assert!(raw.command_index == 1);
            }
            _ => panic!("wrong error"),
        }

        match error_strings[2].into() {
            MoveExecutionError::StakingInner(StakingInnerError::EInvalidSyncEpoch(raw)) => {
                assert!(raw.function == "epoch_sync_done");
                assert!(raw.command_index == 0);
            }
            _ => panic!("wrong error"),
        }

        match error_strings[3].into() {
            MoveExecutionError::NotParsable(_) => {}
            _ => panic!("wrong error"),
        }
    }
}
