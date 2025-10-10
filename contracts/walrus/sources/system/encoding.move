// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::encoding;

use walrus::redstuff;

// Supported Encoding Types
// RedStuff with Reed-Solomon
const RS2: u8 = 1;
// RedStuff with Reed-Solomon supporting chunked encoding
const RS2_CHUNKED: u8 = 2;

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// The encoding type is invalid.
const EInvalidEncodingType: u64 = 0;

/// Computes the encoded length of a blob given its unencoded length, encoding type
/// and number of shards `n_shards`.
/// For RS2_CHUNKED, `chunk_size` specifies the size of each chunk. For RS2, it's ignored.
public fun encoded_blob_length(unencoded_length: u64, encoding_type: u8, n_shards: u16): u64 {
    // Both RS2 and RS2_CHUNKED use RedStuff Reed-Solomon encoding.
    // RS2_CHUNKED adds additional metadata for chunk-level hashes.
    assert!(encoding_type == RS2);
    redstuff::encoded_blob_length(unencoded_length, n_shards, encoding_type, 0)
}

/// Computes the encoded length of a blob given its unencoded length, encoding type
/// and number of shards `n_shards`.
/// For RS2_CHUNKED, `chunk_size` specifies the size of each chunk. For RS2, it's ignored.
public fun encoded_blob_length_v2(
    unencoded_length: u64, encoding_type: u8,
    n_shards: u16,
    chunk_size: u64,
): u64 {
    // Both RS2 and RS2_CHUNKED use RedStuff Reed-Solomon encoding.
    // RS2_CHUNKED adds additional metadata for chunk-level hashes.
    assert!(encoding_type == RS2 || encoding_type == RS2_CHUNKED, EInvalidEncodingType);
    redstuff::encoded_blob_length(unencoded_length, n_shards, encoding_type, chunk_size)
}
