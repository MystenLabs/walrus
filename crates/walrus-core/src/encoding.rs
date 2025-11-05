// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob encoding functionality, using the RedStuff algorithm.

// TODO(giac): Link or import the `../../../docs/red-stuff.md` documentation here (#307).

mod basic_encoding;
pub use basic_encoding::{
    BLOB_TYPE_ATTRIBUTE_KEY,
    Decoder,
    QUILT_TYPE_VALUE,
    ReedSolomonDecoder,
    ReedSolomonEncoder,
};

mod blob_encoding;
pub use blob_encoding::{BlobDecoder, BlobEncoder};

pub mod quilt_encoding;

mod common;
pub use common::{ConsistencyCheckType, EncodingAxis, Primary, Secondary};

mod config;
pub use config::{
    EncodingConfig,
    EncodingConfigEnum,
    EncodingFactory,
    MAX_SOURCE_SYMBOLS,
    ReedSolomonEncodingConfig,
    RequiredCount,
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
    DecodeError,
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
};

mod utils;
pub mod disk_encoder;
