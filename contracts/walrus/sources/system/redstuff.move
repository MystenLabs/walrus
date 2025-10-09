// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::redstuff;

// The length of a hash used for the Red Stuff metadata
const DIGEST_LEN: u64 = 32;

// The length of a blob id in the stored metadata
const BLOB_ID_LEN: u64 = 32;

/// Computes the encoded length of a blob for the Red Stuff encoding using
/// Reed-Solomon, given its unencoded size, number of shards, and encoding type.
/// The output length includes the size of the metadata hashes and the blob ID.
/// For RS2_CHUNKED, `chunk_size` specifies the size of each chunk. For RS2, it's ignored.
/// This computation is the same as done by the function of the same name in
/// `crates/walrus_core/encoding/config.rs` and should be kept in sync.
public(package) fun encoded_blob_length(
    unencoded_length: u64,
    n_shards: u16,
    encoding_type: u8,
    chunk_size: u64,
): u64 {
    if (encoding_type == 2) {
        // RS2_CHUNKED
        encoded_blob_length_chunked(unencoded_length, n_shards, chunk_size)
    } else {
        encoded_blob_length_single(unencoded_length, n_shards, encoding_type, chunk_size)
    }
}

/// Computes the encoded length for a single-chunk blob (RS2) or one chunk of a chunked blob.
/// The `chunk_size` parameter is only used for RS2_CHUNKED metadata calculation.
fun encoded_blob_length_single(
    unencoded_length: u64,
    n_shards: u16,
    encoding_type: u8,
    chunk_size: u64,
): u64 {
    // prettier-ignore
    let slivers_size = (
        source_symbols_primary(n_shards) as u64
            + (source_symbols_secondary(n_shards) as u64),
    ) * (symbol_size(unencoded_length, n_shards) as u64);

    (n_shards as u64) * (slivers_size + metadata_size(n_shards, unencoded_length, encoding_type, chunk_size))
}

/// Computes the encoded length for a chunked blob (RS2_CHUNKED)
fun encoded_blob_length_chunked(unencoded_length: u64, n_shards: u16, chunk_size: u64): u64 {
    let num_chunks = compute_num_chunks(unencoded_length, chunk_size);

    if (num_chunks == 1) {
        // Single chunk case - chunk_size equals blob size
        encoded_blob_length_single(unencoded_length, n_shards, 2, chunk_size)
    } else {
        // Multiple chunks: compute encoded size for full chunks and last chunk
        let full_chunks = num_chunks - 1;
        let last_chunk_size = unencoded_length - ((full_chunks as u64) * chunk_size);

        // Encoded size for one full chunk
        let chunk_encoded_size = encoded_blob_length_single(chunk_size, n_shards, 2, chunk_size);

        // Encoded size for the last chunk (may be smaller)
        let last_chunk_encoded_size = encoded_blob_length_single(
            last_chunk_size,
            n_shards,
            2,
            chunk_size,
        );

        // Total: (num_full_chunks * full_chunk_size) + last_chunk_size
        (full_chunks as u64) * chunk_encoded_size + last_chunk_encoded_size
    }
}

/// The number of primary source symbols per sliver given `n_shards`.
fun source_symbols_primary(n_shards: u16): u16 {
    n_shards - 2 * max_byzantine(n_shards)
}

/// The number of secondary source symbols per sliver given `n_shards`.
fun source_symbols_secondary(n_shards: u16): u16 {
    n_shards - max_byzantine(n_shards)
}

/// The total number of source symbols given `n_shards`.
fun n_source_symbols(n_shards: u16): u64 {
    (source_symbols_primary(n_shards) as u64)
        * (source_symbols_secondary(n_shards) as u64)
}

/// Computes the symbol size given the `unencoded_length` and number of shards
/// `n_shards`. If the resulting symbols would be larger than a `u16`, this
/// results in an Error.
fun symbol_size(mut unencoded_length: u64, n_shards: u16): u16 {
    if (unencoded_length == 0) {
        unencoded_length = 1;
    };
    let n_symbols = n_source_symbols(n_shards);
    let mut symbol_size = ((unencoded_length - 1) / n_symbols + 1) as u16;
    if (symbol_size % 2 == 1) {
        // For Reed-Solomon, the symbol size must be a multiple of 2.
        symbol_size = symbol_size + 1;
    };
    symbol_size
}

/// The size of the metadata, i.e. sliver root hashes, blob_id, and chunk metadata.
/// For RS2 (encoding_type == 1): blob-level hashes + blob_id
/// For RS2_CHUNKED (encoding_type == 2): blob-level hashes + chunk-level hashes + blob_id + chunk info
fun metadata_size(n_shards: u16, unencoded_length: u64, encoding_type: u8, chunk_size: u64): u64 {
    // Base metadata: blob-level hashes + blob_id
    let base_size = (n_shards as u64) * DIGEST_LEN * 2 + BLOB_ID_LEN;

    // For RS2_CHUNKED (encoding_type == 2), add chunk metadata
    if (encoding_type == 2) {
        let num_chunks = compute_num_chunks(unencoded_length, chunk_size);
        // Chunk-level hashes: num_chunks * n_shards * 2 * DIGEST_LEN
        let chunk_hashes_size = (num_chunks as u64) * (n_shards as u64) * DIGEST_LEN * 2;
        // Additional fields: num_chunks (4 bytes) + chunk_size (8 bytes)
        let chunk_fields_size = 4 + 8;
        base_size + chunk_hashes_size + chunk_fields_size
    } else {
        base_size
    }
}

/// Computes the number of chunks needed for a blob of the given size and chunk size.
/// This matches compute_chunk_parameters in Rust.
fun compute_num_chunks(blob_size: u64, chunk_size: u64): u32 {
    // num_chunks = (blob_size + chunk_size - 1) / chunk_size (ceiling division)
    (((blob_size + chunk_size - 1) / chunk_size) as u32)
}

/// Maximum number of byzantine shards, given `n_shards`.
fun max_byzantine(n_shards: u16): u16 {
    (n_shards - 1) / 3
}

// Tests

#[test_only]
use walrus::test_utils::assert_eq;

#[test_only]
fun assert_encoded_size(
    unencoded_length: u64,
    n_shards: u16,
    encoding_type: u8,
    chunk_size: u64,
    encoded_size: u64,
) {
    assert_eq!(
        encoded_blob_length(unencoded_length, n_shards, encoding_type, chunk_size),
        encoded_size,
    );
}

#[test]
fun test_encoded_size_reed_solomon() {
    // RS2 encoding (encoding_type = 1) - chunk_size is ignored for RS2
    let dummy_chunk_size = 0;
    assert_encoded_size(1, 10, 1, dummy_chunk_size, 10 * (2*(4 + 7) + 10 * 2 * 32 + 32));
    assert_encoded_size(1, 1000, 1, dummy_chunk_size, 1000 * (2*(334 + 667) + 1000 * 2 * 32 + 32));
    assert_encoded_size(
        (4 * 7) * 100,
        10,
        1,
        dummy_chunk_size,
        10 * ((4 + 7) * 100 + 10 * 2 * 32 + 32),
    );
    assert_encoded_size(
        (334 * 667) * 100,
        1000,
        1,
        dummy_chunk_size,
        1000 * ((334 + 667) * 100 + 1000 * 2 * 32 + 32),
    );
}

#[test]
fun test_zero_size() {
    // RS2 encoding (encoding_type = 1)
    let dummy_chunk_size = 0;
    assert_encoded_size(0, 10, 1, dummy_chunk_size, 10 * (2*(4 + 7) + 10 * 2 * 32 + 32));
}

#[test]
fun test_chunked_encoding_single_chunk() {
    // Small blob that fits in a single chunk should have same size as RS2
    // except for the additional 12 bytes (4 for num_chunks + 8 for chunk_size)
    let n_shards: u16 = 10;
    let blob_size: u64 = 1000;
    let chunk_size: u64 = blob_size; // Single chunk
    let encoding_type_rs2: u8 = 1;
    let encoding_type_chunked: u8 = 2;
    let dummy_chunk_size = 0;

    let rs2_size = encoded_blob_length(blob_size, n_shards, encoding_type_rs2, dummy_chunk_size);
    let chunked_size = encoded_blob_length(blob_size, n_shards, encoding_type_chunked, chunk_size);

    // For single chunk: chunked adds num_chunks(4) + chunk_size(8) = 12 bytes per shard
    // Plus: chunk hashes are same as blob hashes, so add n_shards * 2 * 32 bytes per shard
    let expected_overhead = (n_shards as u64) * (12 + (n_shards as u64) * 2 * 32);
    assert_eq!(chunked_size, rs2_size + expected_overhead);
}

#[test]
fun test_compute_num_chunks() {
    // 10 MB default chunk size (same as Rust DEFAULT_CHUNK_SIZE)
    let chunk_size = 10 * 1024 * 1024;
    // Small blob: 1 chunk
    assert_eq!(compute_num_chunks(1000, chunk_size), 1);

    // Large blob requiring multiple chunks
    let large_blob_size = 100 * 1024 * 1024; // 100 MB
    let expected_chunks = ((large_blob_size + chunk_size - 1) / chunk_size) as u32;
    assert_eq!(compute_num_chunks(large_blob_size, chunk_size), expected_chunks);
}

#[test_only]
fun assert_primary_secondary_source_symbols(n_shards: u16, primary: u16, secondary: u16) {
    assert_eq!(source_symbols_primary(n_shards), primary);
    assert_eq!(source_symbols_secondary(n_shards), secondary);
}

#[test]
fun test_source_symbols_number() {
    // Using RedStuff with Reed-Solomon. These are the standard BFT values.
    assert_primary_secondary_source_symbols(7, 3, 5);
    assert_primary_secondary_source_symbols(10, 4, 7);
    assert_primary_secondary_source_symbols(31, 11, 21);
    assert_primary_secondary_source_symbols(100, 34, 67);
    assert_primary_secondary_source_symbols(301, 101, 201);
    assert_primary_secondary_source_symbols(1000, 334, 667);
}
