// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob encoding functionality, using the RedStuff algorithm.

// TODO(giac): Link or import the `../../../docs/red-stuff.md` documentation here (#307).

mod basic_encoding;
pub use basic_encoding::{
    Decoder,
    raptorq::{RaptorQDecoder, RaptorQEncoder},
    reed_solomon::{ReedSolomonDecoder, ReedSolomonEncoder},
};

mod blob_encoding;
pub use blob_encoding::{BlobDecoder, BlobDecoderEnum, BlobEncoder};

mod quilt_encoding;
pub use quilt_encoding::{QuiltDecoderV1, QuiltEncoderV1, QuiltV1};

mod common;
pub use common::{EncodingAxis, MAX_SOURCE_SYMBOLS_PER_BLOCK, MAX_SYMBOL_SIZE, Primary, Secondary};

mod config;
pub use config::{
    EncodingConfig,
    EncodingConfigEnum,
    EncodingConfigTrait,
    RaptorQEncodingConfig,
    ReedSolomonEncodingConfig,
    decoding_safety_limit,
    encoded_blob_length_for_n_shards,
    encoded_slivers_length_for_n_shards,
    max_blob_size_for_n_shards,
    max_sliver_size_for_n_secondary,
    max_sliver_size_for_n_shards,
    metadata_length_for_n_shards,
    source_symbols_for_n_shards,
};

mod errors;
pub use errors::{
    DataTooLargeError,
    DecodingVerificationError,
    EncodeError,
    InvalidDataSizeError,
    QuiltError,
    RecoverySymbolError,
    SliverRecoveryError,
    SliverRecoveryOrVerificationError,
    SliverVerificationError,
    SymbolVerificationError,
    WrongSliverVariantError,
    WrongSymbolSizeError,
};

mod mapping;
pub use mapping::{SliverAssignmentError, rotate_pairs, rotate_pairs_unchecked};

mod slivers;
pub use slivers::{PrimarySliver, SecondarySliver, SliverData, SliverPair};

mod symbols;
pub use symbols::{
    DecodingSymbol,
    EitherDecodingSymbol,
    GeneralRecoverySymbol,
    PrimaryRecoverySymbol,
    RecoverySymbol,
    RecoverySymbolPair,
    SecondaryRecoverySymbol,
    Symbols,
    min_symbols_for_recovery,
};

mod utils;
