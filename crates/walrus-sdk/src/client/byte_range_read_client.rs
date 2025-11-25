// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for reading byte ranges from blobs.

use std::{num::NonZeroUsize, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    SliverIndex,
    encoding::{EncodingFactory, Primary, SliverData},
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
};
use walrus_sui::client::ReadClient;

use crate::{
    client::WalrusNodeClient,
    error::{ClientError, ClientErrorKind, ClientResult},
};

/// Configuration for the ByteRangeReadClient.
#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ByteRangeReadClientConfig {
    /// The maximum number of attempts to retrieve slivers.
    pub max_retrieve_slivers_attempts: usize,
    /// The timeout duration for retrieving slivers.
    #[serde_as(as = "serde_with::DurationSeconds")]
    #[serde(rename = "timeout_secs")]
    pub timeout: Duration,
}

impl ByteRangeReadClientConfig {
    /// Creates a new ByteRangeReadClientConfig.
    pub fn new(max_retrieve_slivers_attempts: usize, timeout: Duration) -> Self {
        Self {
            max_retrieve_slivers_attempts,
            timeout,
        }
    }
}

impl Default for ByteRangeReadClientConfig {
    fn default() -> Self {
        Self {
            max_retrieve_slivers_attempts: 2,
            timeout: Duration::from_secs(10),
        }
    }
}

/// A client for reading specific byte ranges from blobs.
#[derive(Debug, Clone)]
pub struct ByteRangeReadClient<'a, T> {
    client: &'a WalrusNodeClient<T>,
    config: ByteRangeReadClientConfig,
}

impl<'a, T> ByteRangeReadClient<'a, T> {
    /// Creates a new ByteRangeReadClient.
    pub fn new(client: &'a WalrusNodeClient<T>, config: ByteRangeReadClientConfig) -> Self {
        Self { client, config }
    }
}

/// Result of reading a byte range from a blob.
#[derive(Debug)]
pub struct ReadByteRangeResult {
    /// The data of the requested byte range.
    pub data: Vec<u8>,
    /// The unencoded size of the blob.
    pub unencoded_blob_size: u64,
}

impl<T: ReadClient> ByteRangeReadClient<'_, T> {
    /// Reads a specific byte range from a blob.
    pub async fn read_byte_range(
        &self,
        blob_id: &BlobId,
        start_byte_position: u64,
        byte_length: u64,
    ) -> ClientResult<ReadByteRangeResult> {
        // To read the byte range of the original blob file, we find the corresponding primary
        // slivers that covers the byte range, and retrieve the slivers from the Walrus shards,
        // and extract the requested data from the slivers.

        tracing::debug!(
            %blob_id,
            start_byte_position,
            byte_length,
            "start reading byte range"
        );

        // First, validate the blob ID and make sure it is valid.
        self.client.check_blob_id(blob_id)?;

        // Convert the request range from u64 to usize. If the request range cannot be represented
        // as usize, this machine cannot handle such request given that the data will need to be
        // hold and extracted from memory.
        let start_byte_position = usize::try_from(start_byte_position).map_err(|_| {
            ClientError::from(ClientErrorKind::ByteRangeReadInputError(
                "start byte position is too large to convert to usize".to_string(),
            ))
        })?;
        let byte_length = NonZeroUsize::new(usize::try_from(byte_length).map_err(|_| {
            ClientError::from(ClientErrorKind::ByteRangeReadInputError(
                "byte length is too large to convert to usize".to_string(),
            ))
        })?)
        .ok_or_else(|| {
            ClientError::from(ClientErrorKind::ByteRangeReadInputError(
                "byte length cannot be zero".to_string(),
            ))
        })?;

        // Get blob status and certified epoch
        let (certified_epoch, _) = self
            .client
            .get_blob_status_and_certified_epoch(blob_id, None)
            .await?;

        // Retrieve metadata
        let metadata = self
            .client
            .retrieve_metadata(certified_epoch, blob_id)
            .await?;

        if metadata.metadata().encoding_type() != EncodingType::RS2 {
            return Err(ClientError::from(ClientErrorKind::ByteRangeReadError(
                format!(
                    "byte range read client only supports RS2 encoding, got {}",
                    metadata.metadata().encoding_type()
                ),
            )));
        }

        let blob_size = usize::try_from(metadata.metadata().unencoded_length()).map_err(|_| {
            ClientError::from(ClientErrorKind::ByteRangeReadError(format!(
                "invalid blob size from metadata: {}",
                metadata.metadata().unencoded_length()
            )))
        })?;

        // Makes sure that the byte range input is valid.
        calculate_and_validate_end_byte_position(start_byte_position, byte_length, blob_size)?;
        let primary_sliver_size = self.get_primary_sliver_size(blob_size, &metadata)?;

        // Calculate which slivers we need, and the start byte position of the first sliver
        // that we need to read from.
        let (new_start_byte_position, sliver_indices) =
            calculate_sliver_indices(primary_sliver_size, start_byte_position, byte_length)?;

        // Retrieve the necessary slivers. Note that the returned slivers may not be in the same
        // order as the sliver_indices.
        let slivers = self
            .retrieve_slivers_for_range(&metadata, &sliver_indices, certified_epoch)
            .await?;

        let data =
            construct_requested_data_from_slivers(&slivers, new_start_byte_position, byte_length)?;

        Ok(ReadByteRangeResult {
            data,
            unencoded_blob_size: metadata.metadata().unencoded_length(),
        })
    }

    // Gets the size of the primary sliver for the given blob size and metadata.
    fn get_primary_sliver_size(
        &self,
        blob_size: usize,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> ClientResult<usize> {
        let encoding_config = self
            .client
            .encoding_config()
            .get_for_type(metadata.metadata().encoding_type());
        let primary_sliver_size = encoding_config
            .sliver_size_for_blob::<Primary>(
                u64::try_from(blob_size).expect("blob size should be valid"),
            )
            .map_err(|_| {
                ClientError::from(ClientErrorKind::ByteRangeReadError(
                    "blob too large to determine sliver size".to_string(),
                ))
            })?
            .get() as usize;
        Ok(primary_sliver_size)
    }

    /// Retrieves slivers needed for the byte range.
    async fn retrieve_slivers_for_range(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_indices: &[SliverIndex],
        certified_epoch: Epoch,
    ) -> ClientResult<Vec<SliverData<Primary>>> {
        let start_time = Instant::now();

        let mut slivers = self
            .client
            .retrieve_slivers_retry_committees::<Primary>(
                metadata,
                sliver_indices,
                certified_epoch,
                self.config.max_retrieve_slivers_attempts,
                self.config.timeout,
            )
            .await?;

        let elapsed = start_time.elapsed();
        tracing::debug!("Retrieved {} slivers in {:?}", slivers.len(), elapsed);

        // Sort slivers by index to process them in order
        slivers.sort_by_key(|s| s.index.get());

        Ok(slivers)
    }
}

/// Calculates the end byte position of the byte range, and returns an error if it is out of
/// bounds.
fn calculate_and_validate_end_byte_position(
    start_byte_position: usize,
    byte_length: NonZeroUsize,
    blob_size: usize,
) -> ClientResult<usize> {
    let end_byte_position = start_byte_position
        .checked_add(byte_length.get())
        .ok_or_else(|| {
            ClientError::from(ClientErrorKind::ByteRangeReadInputError(
                "byte range overflow".to_string(),
            ))
        })?;

    if end_byte_position > blob_size {
        return Err(ClientError::from(ClientErrorKind::ByteRangeReadInputError(
            format!(
                "byte range out of bounds: requested \
                    {start_byte_position}-{end_byte_position}, blob size is {blob_size}",
            ),
        )));
    }

    Ok(end_byte_position)
}

/// Calculates the sliver indices that contain the byte range, and the start byte position of
/// the first sliver that we need to read from.
///
/// The input should be validated using `calculate_and_validate_end_byte_position` before calling
/// this function.
fn calculate_sliver_indices(
    primary_sliver_size: usize,
    start_byte_position: usize,
    byte_length: NonZeroUsize,
) -> ClientResult<(usize, Vec<SliverIndex>)> {
    tracing::debug!(
        %primary_sliver_size,
        %start_byte_position,
        %byte_length,
        "calculating sliver indices");

    // Calculate which slivers contain the byte range

    // First sliver that contains start_byte_position
    let start_sliver_index = start_byte_position / primary_sliver_size;

    // Each systematic (primary) sliver contains primary_sliver_size bytes of the original blob
    // Note that we subtract 1 from the end byte position because we want to include the last
    // byte in the last sliver.
    let end_byte_position = start_byte_position + byte_length.get() - 1;

    // Last sliver that contains end_byte_position
    let end_sliver_index = end_byte_position / primary_sliver_size;

    // We need to retrieve enough slivers for decoding (n_systematic)
    // Prioritize slivers starting from start_sliver_index
    let sliver_indices: Vec<SliverIndex> = (start_sliver_index..=end_sliver_index)
        .map(|idx| SliverIndex::new(u16::try_from(idx).expect("sliver index should be valid")))
        .collect();

    tracing::debug!(
        "byte range {}-{} spans slivers {}-{}, requesting {} slivers",
        start_byte_position,
        end_byte_position,
        start_sliver_index,
        end_sliver_index,
        sliver_indices.len()
    );

    let new_start_byte_position = start_byte_position - start_sliver_index * primary_sliver_size;

    Ok((new_start_byte_position, sliver_indices))
}

/// Constructs the requested data from the slivers and copy the data directly into the
/// final result buffer.
fn construct_requested_data_from_slivers(
    slivers: &[SliverData<Primary>],
    new_start_byte_position: usize,
    byte_length: NonZeroUsize,
) -> ClientResult<Vec<u8>> {
    // Copy data directly into the final result buffer, skipping unnecessary bytes
    let mut result = Vec::with_capacity(byte_length.get());
    let mut bytes_to_skip = new_start_byte_position;
    let mut bytes_remaining = byte_length.get();

    for sliver in slivers {
        let data = sliver.symbols.data();

        // Skip entire sliver if we haven't reached the start position yet
        if bytes_to_skip >= data.len() {
            return Err(ClientError::from(ClientErrorKind::ByteRangeReadError(
                format!(
                    "start byte position should be within the first read sliver. start \
                        position: {}, sliver size: {}",
                    new_start_byte_position,
                    data.len(),
                ),
            )));
        }

        // Copy the needed bytes directly into result
        let start_in_sliver_inclusive = bytes_to_skip;
        let end_in_sliver_exclusive = (start_in_sliver_inclusive + bytes_remaining).min(data.len());

        // Copy only the needed bytes directly into result
        result.extend_from_slice(&data[start_in_sliver_inclusive..end_in_sliver_exclusive]);

        bytes_remaining -= end_in_sliver_exclusive - start_in_sliver_inclusive;

        // After the first sliver, we don't need to skip any more bytes.
        bytes_to_skip = 0;

        // Early exit if we've collected all needed bytes
        if bytes_remaining == 0 {
            break;
        }
    }

    if bytes_remaining > 0 {
        return Err(ClientError::from(ClientErrorKind::ByteRangeReadError(
            format!(
                "requested byte range is larger than the retrieved slivers: requested {}-{}, \
                retrieved slivers size is {}",
                new_start_byte_position,
                new_start_byte_position + byte_length.get(),
                slivers
                    .iter()
                    .map(|s| s.symbols.data().len())
                    .sum::<usize>(),
            ),
        )));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use walrus_test_utils::{Result, param_test};

    use super::*;

    param_test! {
        test_calculate_sliver_indices-> Result: [
            read_middle: (1024, 100, 200, 1, 0, 100),
            read_start: (1024, 0, 500, 1, 0, 0),
            read_end: (500, 250, 250, 1, 0, 250),
            read_two_cross: (1000, 800, 400, 2, 0, 800),
            read_multiple: (1000, 2500, 3000, 4, 2, 500),
            read_boundary: (1000, 1999, 2, 2, 1, 999),
            read_cross_subsequent_sliver: (500, 450, 75, 2, 0, 450),
            read_exact_multiple_slivers: (1000, 1000, 2000, 2, 1, 0),
            read_last_byte_of_first_sliver: (1000, 999, 1, 1, 0, 999),
            small_sliver: (100, 450, 75, 2, 4, 50),
        ]
    }
    fn test_calculate_sliver_indices(
        sliver_size: usize,
        start: usize,
        length: usize,
        expected_count: usize,
        expected_first_sliver_index: u16,
        expected_offset: usize,
    ) -> Result {
        let (new_start, indices) = calculate_sliver_indices(
            sliver_size,
            start,
            NonZeroUsize::new(length).expect("length must be non zero"),
        )?;

        assert_eq!(indices.len(), expected_count, "sliver count mismatch");
        assert_eq!(
            indices[0].get(),
            expected_first_sliver_index,
            "first sliver mismatch"
        );
        assert_eq!(new_start, expected_offset, "offset mismatch");
        Ok(())
    }

    /// Helper function to create a sliver with sequential byte values starting from a given value.
    fn create_sequential_sliver(
        sliver_index: u16,
        start_value: u8,
        size: usize,
    ) -> SliverData<Primary> {
        let data: Vec<u8> = (0..size)
            .map(|i| start_value.wrapping_add(u8::try_from(i).expect("byte value should be valid")))
            .collect();
        SliverData::new(
            data,
            NonZeroU16::new(1).unwrap(),
            SliverIndex::new(sliver_index),
        )
    }

    param_test! {
        test_construct_requested_data_from_slivers -> Result: [
            full_sliver: (1, 100, 0, 100, 0, 99),
            middle_range: (1, 100, 50, 30, 50, 79),
            last_byte: (1, 100, 99, 1, 99, 99),
            first_byte: (1, 100, 0, 1, 0, 0),
            near_end: (1, 100, 80, 15, 80, 94),
            cross_two_middle: (2, 100, 50, 100, 50, 149),
            exact_boundary: (2, 100, 0, 150, 0, 149),
            single_byte_at_end: (2, 100, 99, 1, 99, 99),
            two_bytes_crossing: (2, 100, 99, 2, 99, 100),
            cross_three: (3, 100, 80, 140, 80, 219),
            last_fifty_and_first_fifty: (2, 100, 50, 100, 50, 149),
            multiple_slivers: (10, 10, 7, 73, 7, 79),
            multiple_exact_slivers: (5, 10, 2, 18, 2, 19),
            single_byte_sliver: (10, 1, 0, 10, 0, 9),
        ]
    }
    fn test_construct_requested_data_from_slivers(
        num_slivers: usize,
        sliver_size: usize,
        offset: usize,
        byte_length: usize,
        expected_first_byte: u8,
        expected_last_byte: u8,
    ) -> Result {
        let slivers: Vec<_> = (0..num_slivers)
            .map(|i| {
                create_sequential_sliver(
                    u16::try_from(i).expect("sliver index should be valid"),
                    u8::try_from(i * sliver_size).expect("byte value should be valid"),
                    sliver_size,
                )
            })
            .collect();
        let result = construct_requested_data_from_slivers(
            &slivers,
            offset,
            NonZeroUsize::new(byte_length).expect("byte length must be non zero"),
        )?;

        assert_eq!(result.len(), byte_length);
        assert_eq!(result[0], expected_first_byte);
        assert_eq!(result[byte_length - 1], expected_last_byte);
        Ok(())
    }

    #[test]
    fn test_construct_data_continuity() -> Result {
        // Verify data is continuous across sliver boundaries
        let slivers = vec![
            create_sequential_sliver(0, 0, 100),
            create_sequential_sliver(1, 100, 100),
            create_sequential_sliver(2, 200, 100),
        ];
        let result = construct_requested_data_from_slivers(
            &slivers,
            50,
            NonZeroUsize::new(200).expect("byte length must be non zero"),
        )?;

        // Check continuity at boundaries
        assert_eq!(result[49], 99); // Last byte of first sliver
        assert_eq!(result[50], 100); // First byte of second sliver
        assert_eq!(result[149], 199); // Last byte of second sliver
        assert_eq!(result[150], 200); // First byte of third sliver
        Ok(())
    }

    /// Tests that an error is returned if the requested byte range is larger than the retrieved \
    /// slivers.
    #[test]
    fn test_construct_requested_data_from_slivers_error() -> Result {
        let slivers = vec![
            create_sequential_sliver(0, 0, 100),
            create_sequential_sliver(1, 100, 100),
            create_sequential_sliver(2, 200, 100),
        ];
        let result = construct_requested_data_from_slivers(
            &slivers,
            50,
            NonZeroUsize::new(1000).expect("byte length must be non zero"),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .kind()
                .to_string()
                .contains("requested byte range is larger than the retrieved slivers")
        );

        Ok(())
    }
}
