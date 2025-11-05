#![allow(missing_docs)]
use crate::encoding::{
    DataTooLargeError,
    EncodingConfigEnum,
    EncodingFactory as _,
    Primary,
    Secondary,
    utils,
};

pub mod symbol_array;
pub mod symbol_file;

#[cfg(feature = "test-utils")]
pub mod test_blob;

// const MAX_BUFFER_SIZE_HINT: usize = 64 << 20; // 64 MiB
// const MIN_ROWS_IN_BUFFER_IN_INGRESS: usize = 2;
//
// struct SymbolBufWriter {
//     inner: Vec<u8>,
// }
//
// impl SymbolBufWriter {
//     fn write(&mut self, buf: &[u8]) {
//         todo!()
//     }
//
//     fn flush(&mut self) {
//         todo!()
//     }
// }
//
// struct DataIngressPhase {
//     params: BlobParameters,
//     config: EncodingConfigEnum,
//     data_remaining: usize,
//     max_buffer_size: usize,
//
//     source_tracker: TrackSourceData,
//     symbol_buffer: SymbolTile,
//     // file: SymbolFile,
// }

/// Defines a region in the expansion.
///
///
/// ```text
/// ┌──────────┬─────┐
/// │     S    │ SRE │
/// ├──────────┼─────┘
/// │          │
/// │   SCE    │  R3 │
/// │          │
/// └──────────┘ ─ ─ ┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    Source,
    /// The region of the recovery symbols from the expansion of the source columns.
    SourceColumnExpansion,
    /// The region of the recovery symbols from the expansion of the source rows.
    SourceRowExpansion,
    /// The region corresponding to all primary slivers: `Source` and `SourceColumnExpansion`.
    Primary,
    /// The region corresponding to all secondary slivers: `Source` and `SourceRowExpansion`.
    Secondary,
}

#[derive(Debug, Clone, Copy)]
pub struct BlobExpansionParameters {
    pub n_shards: usize,
    pub n_source_primary_slivers: usize,
    pub source_columns: usize,
    pub symbol_size: usize,
    pub blob_size: usize,
}

impl BlobExpansionParameters {
    pub fn new(blob_size: usize, config: EncodingConfigEnum) -> Result<Self, DataTooLargeError> {
        let symbol_size = utils::compute_symbol_size_from_usize(
            blob_size,
            config.source_symbols_per_blob(),
            config.encoding_type().required_alignment(),
        )?;
        let source_rows: usize = config.n_source_symbols::<Primary>().get().into();
        let source_columns = config.n_source_symbols::<Secondary>().get().into();
        let n_shards = config.n_shards().get().into();

        Ok(Self {
            n_shards,
            n_source_primary_slivers: source_rows,
            source_columns,
            symbol_size: symbol_size.get().into(),
            blob_size,
        })
    }

    pub fn row_count(&self, region: Region) -> usize {
        let n_rows_source = self.n_source_primary_slivers;
        match region {
            Region::Source | Region::Secondary | Region::SourceRowExpansion => n_rows_source,
            Region::SourceColumnExpansion => self.n_shards - n_rows_source,
            Region::Primary => self.n_shards,
        }
    }

    pub fn column_count(&self, region: Region) -> usize {
        let n_columns_source = self.source_columns;
        match region {
            Region::Source | Region::Primary | Region::SourceColumnExpansion => self.source_columns,
            Region::SourceRowExpansion => self.n_shards - n_columns_source,
            Region::Secondary => self.n_shards,
        }
    }

    pub fn row_bytes(&self, region: Region) -> usize {
        self.column_count(region) * self.symbol_size
    }

    pub fn column_bytes(&self, region: Region) -> usize {
        self.row_count(region) * self.symbol_size
    }

    pub fn region_bytes(&self, region: Region) -> usize {
        self.row_bytes(region) * self.row_count(region)
    }
}
//
// impl DataIngressPhase {
//     pub fn new(config: EncodingConfigEnum, blob_size: usize) -> Result<Self, DataTooLargeError> {
//         todo!()
//         // let symbol_size = utils::compute_symbol_size_from_usize(
//         //     blob_size,
//         //     config.source_symbols_per_blob(),
//         //     config.encoding_type().required_alignment(),
//         // )?;
//         // // let n_rows = config.n_source_symbols::<Primary>().get().into();
//         // // let n_columns = config.n_source_symbols::<Secondary>().get().into();
//         // let n_shards = config.n_shards().get().into();
//
//         // let memory_required_per_row = n_shards * usize::from(symbol_size.get());
//         // // For the data ingress phase, we have a single encoder, so reserve some memory for that.
//         // let available_buffer_memory = MAX_BUFFER_SIZE_HINT
//         //     .saturating_sub(encoder_memory_per_row(symbol_size.get().into(), n_shards));
//
//         // let n_buffer_rows = cmp::max(
//         //     (available_buffer_memory as f64 / memory_required_per_row as f64).floor() as usize,
//         //     MIN_ROWS_IN_BUFFER_IN_INGRESS,
//         // );
//         // let buffer = Vec::with_capacity(n_buffer_rows * memory_required_per_row);
//
//         // Ok(Self {
//         //     rows_flushed: 0,
//         //     max_buffer_size: buffer.len(),
//         //     buffer,
//         // })
//     }
//
//     pub fn add_data(&mut self, data: &[u8]) -> Result<Option<()>, Box<dyn Error>> {
//         todo!()
//         // // Add data to the tile, we add the symbols for a row, then expand, then add symbols for the
//         // // subsequent row. When the tile becomes full OR we're at the end of the data stream, we flush.
//         // let mut is_source_complete = false;
//
//         // for event in self.source_tracker.add_source_data(data)? {
//         //     let mut flush_buffer = false;
//
//         //     match event {
//         //         SourceRowEvent::SourceRowData(row_data) => self.symbol_buffer.write(row_data),
//         //         SourceRowEvent::EndOfSourceRow => {
//         //             // Encode the source row and write the encoded bytes into our buffer.
//         //             // If this fills the buffer, then flush the buffer to disk and clear it in
//         //             // preparation for further data.
//
//         //             let row = self.symbol_buffer.last_row();
//         //             let (source_bytes, _) = row.split_at(self.params.source_row_length_bytes());
//
//         //             // TODO(jsmith): The encoder here keeps a copy of the row, and returns symbols
//         //             // in a vector, both of which are uncessary allocations for this use case.
//         //             let encoder = ReedSolomonEncoder::new(
//         //                 source_bytes,
//         //                 self.config.n_source_symbols::<Secondary>(),
//         //                 self.config.n_shards(),
//         //             )?;
//         //             for symbol in encoder.encode_all_repair_symbols() {
//         //                 self.symbol_buffer.write(&symbol);
//         //             }
//
//         //             flush_buffer = self.symbol_buffer.is_full();
//         //         }
//         //         SourceRowEvent::EndOfSource => {
//         //             flush_buffer = !self.symbol_buffer.is_empty();
//         //             is_source_complete = true;
//         //         }
//         //     }
//
//         //     if flush_buffer {
//         //         // self.file.add_section_a_tile(&mut self.symbol_buffer)?;
//         //         self.symbol_buffer.clear();
//         //     }
//         // }
//
//         // if is_source_complete {
//         //     self.encode_remaining_symbols()
//         // } else {
//         //     Ok(None)
//         // }
//     }
//
//     fn encode_remaining_symbols(&self) -> Result<Option<()>, Box<dyn Error>> {
//         todo!()
//     }
// }
//
// /// Estimates the amount of memory required for the ReedSolomon encoder for a single sliver.
// const fn encoder_memory_per_row(symbol_size: usize, n_shards: usize) -> usize {
//     // The default reed-solomon encoder stores symbols as a multiple of 64 bytes.
//     const BYTE_MULTIPLE_PER_SYMBOL: usize = 64;
//     symbol_size.next_multiple_of(BYTE_MULTIPLE_PER_SYMBOL) * n_shards
// }
//
//
// // ------------------------------------------------------------------------------------------
// // Utility to track the source data provided.
// //
//
// /// State-machine that tracks the file's source data in terms of its position when
// /// laid out in rows for encoding, and whether a row and the data should be complete.
// #[derive(Debug)]
// struct TrackSourceData {
//     /// Common parameters pertaining to the blob.
//     params: BlobParameters,
//     /// The current source-row that is being filled.
//     current_row: usize,
//     /// The current byte-position within the row.
//     current_row_position: usize,
// }
//
// impl TrackSourceData {
//     /// Records the receipt of the data and returns an iterator of events indicating whether the
//     /// data should be added to the current row, when a row has ended, and when the data has ended.
//     pub fn add_source_data<'a, 'b>(
//         &'a mut self,
//         data: &'b [u8],
//     ) -> Result<AddSourceData<'a, 'b>, TooMuchDataError> {
//         let excess_data = data.len().saturating_sub(self.remaining_required());
//
//         if excess_data > 0 {
//             Err(TooMuchDataError(excess_data))
//         } else {
//             Ok(AddSourceData {
//                 data: Some(data),
//                 state: self,
//             })
//         }
//     }
//
//     fn remaining_required(&self) -> usize {
//         todo!()
//     }
// }
//
// #[derive(Debug, thiserror::Error)]
// #[error("more data than expected was provided: {0}")]
// struct TooMuchDataError(usize);
//
// struct AddSourceData<'a, 'b> {
//     state: &'a mut TrackSourceData,
//     // INV: The total amount of data iterated is never more than the blob size.
//     /// The data being split into rows and events.
//     ///
//     /// None indicates the end of the iterator, which is distinct from the data being empty.
//     /// The iterator may still emit EndOfRow and EndOfData events with empty data.
//     data: Option<&'b [u8]>,
// }
//
// impl<'a, 'b> Iterator for AddSourceData<'a, 'b> {
//     type Item = SourceRowEvent<'b>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         let Some(data) = self.data.as_mut() else {
//             return None;
//         };
//
//         let source_row_length = self.state.params.symbol_size * self.state.params.source_rows;
//         assert!(
//             self.state.current_row <= self.state.params.source_rows,
//             "current row index must indicate or a row or end"
//         );
//         assert!(
//             self.state.current_row_position <= source_row_length,
//             "current row position index must indicate a byte in a row or end"
//         );
//
//         if self.state.current_row_position == source_row_length {
//             self.state.current_row += 1;
//             self.state.current_row_position = 0;
//             return Some(SourceRowEvent::EndOfSourceRow);
//         }
//
//         if self.state.current_row == self.state.params.source_rows {
//             assert!(
//                 self.state.current_row_position == 0,
//                 "row index should not increment for end-of-data row"
//             );
//             assert!(
//                 data.is_empty(),
//                 "the total amount of data must be exactly the blob size"
//             );
//             self.data = None;
//             return Some(SourceRowEvent::EndOfSource);
//         }
//
//         let bytes_remaining_in_row = source_row_length - self.state.current_row_position;
//         assert!(
//             self.state.current_row < source_row_length && bytes_remaining_in_row > 0,
//             "at least 1 row is incomplete given the above conditions"
//         );
//
//         match data.split_off(..bytes_remaining_in_row.min(data.len())) {
//             Some(&[]) => {
//                 // A previous iteration consumed the data but did not result in end-of-data.
//                 self.data = None;
//                 None
//             }
//             Some(non_empty) => {
//                 self.state.current_row_position += non_empty.len();
//                 Some(SourceRowEvent::SourceRowData(non_empty))
//             }
//             None => unreachable!("`min(_, data.len())` ensures a valid range"),
//         }
//     }
// }
//
// /// An event indicating that the data is part of the current row, if the row has ended,
// /// or if the source file has ended.
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// enum SourceRowEvent<'a> {
//     SourceRowData(&'a [u8]),
//     EndOfSourceRow,
//     EndOfSource,
// }
