// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub const N_SHARDS: u16 = 1000;
// Likely values for the number of source symbols for the primary and secondary encoding.
// These values are consistent with BFT and are supported source-block sizes that do not require
// padding, see https://datatracker.ietf.org/doc/html/rfc6330#section-5.6.
pub const SOURCE_SYMBOLS_PRIMARY: u16 = 324;
pub const SOURCE_SYMBOLS_SECONDARY: u16 = 648;
