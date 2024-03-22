// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TODO(mlegner): Describe encoding algorithm in detail (#50).

mod basic_encoding;
pub use basic_encoding::{ Decoder, Encoder };

mod blob_encoding;
pub use blob_encoding::{ BlobDecoder, BlobEncoder };

mod common;
pub use common::{
    EncodingAxis,
    Primary,
    Secondary,
    MAX_N_SHARDS,
    MAX_SOURCE_SYMBOLS_PER_BLOCK,
    MAX_SYMBOL_SIZE,
};

mod config;
pub use config::{ get_encoding_config, initialize_encoding_config, EncodingConfig };

mod errors;
pub use errors::{
    DataTooLargeError,
    DecodingVerificationError,
    EncodeError,
    RecoveryError,
    WrongSymbolSizeError,
};

mod slivers;
pub use slivers::{ PrimarySliver, SecondarySliver, Sliver, SliverPair };

mod symbols;
pub use symbols::{ DecodingSymbol, DecodingSymbolPair, Symbols };

mod utils;
