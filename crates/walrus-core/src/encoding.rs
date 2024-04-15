// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TODO(mlegner): Describe encoding algorithm in detail (#50).

mod basic_encoding;
pub use basic_encoding::{Decoder, Encoder};

mod blob_encoding;
pub use blob_encoding::{BlobDecoder, BlobEncoder};

mod common;
pub use common::{EncodingAxis, Primary, Secondary, MAX_SOURCE_SYMBOLS_PER_BLOCK, MAX_SYMBOL_SIZE};

mod config;
pub use config::{decoding_safety_limit, source_symbols_for_n_shards, EncodingConfig};

mod errors;
pub use errors::{
    DecodingVerificationError,
    EncodeError,
    RecoveryError,
    SliverVerificationError,
    WrongSliverVariantError,
    WrongSymbolSizeError,
};

mod mapping;
pub use mapping::{rotate_pairs, rotate_pairs_unchecked, SliverAssignmentError};

mod slivers;
pub use slivers::{PrimarySliver, SecondarySliver, Sliver, SliverPair};

mod symbols;
pub use symbols::{
    DecodingSymbol,
    DecodingSymbolPair,
    PrimaryDecodingSymbol,
    SecondaryDecodingSymbol,
    Symbols,
};

mod utils;
