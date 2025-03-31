// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob encoding functionality, using the RedStuff algorithm.

// TODO(giac): Link or import the `../../../docs/red-stuff.md` documentation here (#307).

mod basic_encoding;
pub use basic_encoding::{
    raptorq::{RaptorQDecoder, RaptorQEncoder},
    reed_solomon::{ReedSolomonDecoder, ReedSolomonEncoder},
    Decoder,
};

mod blob_encoding;
pub use blob_encoding::{BlobDecoder, BlobDecoderEnum, BlobEncoder};

mod common;
pub use common::{EncodingAxis, Primary, Secondary, MAX_SOURCE_SYMBOLS_PER_BLOCK, MAX_SYMBOL_SIZE};

mod config;
pub use config::{
    decoding_safety_limit,
    encoded_blob_length_for_n_shards,
    encoded_slivers_length_for_n_shards,
    max_blob_size_for_n_shards,
    max_sliver_size_for_n_secondary,
    max_sliver_size_for_n_shards,
    metadata_length_for_n_shards,
    source_symbols_for_n_shards,
    EncodingConfig,
    EncodingConfigEnum,
    EncodingConfigTrait,
    RaptorQEncodingConfig,
    ReedSolomonEncodingConfig,
};

mod errors;
pub use errors::{
    DataTooLargeError,
    DecodingVerificationError,
    EncodeError,
    InvalidDataSizeError,
    RecoverySymbolError,
    SliverRecoveryError,
    SliverRecoveryOrVerificationError,
    SliverVerificationError,
    SymbolVerificationError,
    WrongSliverVariantError,
    WrongSymbolSizeError,
};

mod mapping;
pub use mapping::{rotate_pairs, rotate_pairs_unchecked, SliverAssignmentError};

mod slivers;
pub use slivers::{PrimarySliver, SecondarySliver, SliverData, SliverPair};

mod symbols;
pub use symbols::{
    min_symbols_for_recovery,
    DecodingSymbol,
    EitherDecodingSymbol,
    GeneralRecoverySymbol,
    PrimaryRecoverySymbol,
    RecoverySymbol,
    RecoverySymbolPair,
    SecondaryRecoverySymbol,
    Symbols,
};

mod utils;
