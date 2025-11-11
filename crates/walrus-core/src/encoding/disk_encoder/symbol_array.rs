#![allow(missing_docs)]
use core::{cmp, ops::DerefMut};
use std::{boxed::Box, vec};

pub(super) const UNSPECIFIED_DIMENSION: usize = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayOrder {
    RowMajor,
    ColumnMajor,
}

pub type WriteOrder = ArrayOrder;

#[derive(Debug)]
pub struct SymbolArray2d<T> {
    /// The number of rows in this 2d array.
    n_rows: usize,
    /// The number of columns in this 2d array.
    n_columns: usize,
    /// The layout of the array, either row or column major order.
    ///
    /// If [`ArrayOrder::RowMajor`], then the data in the buffer is laid out with rows sequential.
    /// If [`ArrayOrder::ColumnMajor`], then columns are laid out sequentially.
    layout_order: ArrayOrder,
    /// The size of each symbol in bytes.
    symbol_size_bytes: usize,
    /// Buffer containing symbol data.
    buffer: T,
}

impl SymbolArray2d<Box<[u8]>> {
    pub fn new(shape: (usize, usize), symbol_size: usize, layout: ArrayOrder) -> Self {
        assert_ne!(shape.0, UNSPECIFIED_DIMENSION, "rows must be specified");
        assert_ne!(shape.1, UNSPECIFIED_DIMENSION, "columns must be specified");
        assert_ne!(symbol_size, 0, "`symbol_size` cannot be zero");

        let buffer_size_bytes = shape.0 * shape.1 * symbol_size;
        let buffer = vec![0u8; buffer_size_bytes].into_boxed_slice();

        Self::from_buffer(buffer, shape, symbol_size, layout)
    }
}

impl<T> SymbolArray2d<T>
where
    T: DerefMut<Target = [u8]>,
{
    pub fn from_buffer(
        buffer: T,
        shape: (usize, usize),
        symbol_size: usize,
        layout: ArrayOrder,
    ) -> Self {
        assert_ne!(
            shape,
            (UNSPECIFIED_DIMENSION, UNSPECIFIED_DIMENSION),
            "at least 1 dimension must be specified"
        );
        assert_ne!(symbol_size, 0, "`symbol_size` cannot be zero");
        assert_eq!(
            buffer.len() % symbol_size,
            0,
            "buffer length must be a multiple of a symbol size"
        );

        let (mut n_rows, mut n_columns) = shape;
        let buffer_symbol_capacity = buffer.len() / symbol_size;

        if n_rows != UNSPECIFIED_DIMENSION && n_columns != UNSPECIFIED_DIMENSION {
            // Dimensions are fully specified.
            assert_eq!(
                n_rows * n_columns,
                buffer_symbol_capacity,
                "buffer must have capacity to store total number of symbols"
            );
        } else {
            // At least 1 dimension is unspecified (zero), so adding them gives the specified dim.
            let specified_dim = n_rows + n_columns;

            assert_eq!(
                buffer_symbol_capacity % specified_dim,
                0,
                "buffer must have capacity to store whole multiples of the specified dimension"
            );

            let calculated_dim = buffer_symbol_capacity / specified_dim;
            if n_rows == UNSPECIFIED_DIMENSION {
                n_rows = calculated_dim;
            } else {
                n_columns = calculated_dim;
            }
        }

        Self {
            n_rows,
            n_columns,
            symbol_size_bytes: symbol_size,
            layout_order: layout,
            buffer,
        }
    }

    pub fn get_mut(&mut self, row: usize, column: usize) -> &mut [u8] {
        let flat_index = match self.layout_order {
            ArrayOrder::RowMajor => row * self.n_columns + column,
            ArrayOrder::ColumnMajor => column * self.n_rows + row,
        };
        let start = flat_index * self.symbol_size_bytes;
        let end = start + self.symbol_size_bytes;

        &mut self.buffer.deref_mut()[start..end]
    }

    /// Shrink to the new shape, which must have a capacity at most the prior capacity.
    pub fn shrink_to(&mut self, shape: (usize, usize)) {
        assert!(shape.0 != 0 && shape.1 != 0, "both axes must be non-zero");
        assert!(
            shape.0 * shape.1 <= self.n_rows * self.n_columns,
            "specified shape must be a smaller capacity"
        );
        self.n_rows = shape.0;
        self.n_columns = shape.1;
    }

    pub fn capacity_bytes(&self) -> usize {
        self.n_rows * self.n_columns * self.symbol_size_bytes
    }
}

impl<T> AsRef<[u8]> for SymbolArray2d<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn as_ref(&self) -> &[u8] {
        let end = self.capacity_bytes();
        &self.buffer[..end]
    }
}

#[derive(Debug)]
pub struct SymbolArray2dCursor<T = Box<[u8]>> {
    inner: SymbolArray2d<T>,
    write_order: ArrayOrder,
    symbol_offset: usize,
    bytes_written_in_symbol: usize,
}

impl<T> SymbolArray2dCursor<T>
where
    T: DerefMut<Target = [u8]>,
{
    pub fn new(inner: SymbolArray2d<T>, write_order: ArrayOrder) -> Self {
        Self {
            inner,
            write_order,
            symbol_offset: 0,
            bytes_written_in_symbol: 0,
        }
    }

    fn current_symbol_mut(&mut self) -> &mut [u8] {
        let (row, column) = match self.write_order {
            ArrayOrder::RowMajor => (
                self.symbol_offset / self.inner.n_columns,
                self.symbol_offset % self.inner.n_columns,
            ),
            ArrayOrder::ColumnMajor => (
                self.symbol_offset % self.inner.n_rows,
                self.symbol_offset / self.inner.n_rows,
            ),
        };

        self.inner.get_mut(row, column)
    }

    fn write_transposed(&mut self, mut data: &[u8]) -> std::io::Result<usize> {
        if self.is_full() {
            return Ok(0);
        }
        assert!(
            self.bytes_written_in_symbol < self.inner.symbol_size_bytes,
            "byte offset must be kept less than `symbol_size` by advancing `symbol_offset`"
        );

        let mut written = 0;

        while !data.is_empty() && !self.is_full() {
            let byte_offset_in_symbol = self.bytes_written_in_symbol;
            let symbol_buffer = &mut self.current_symbol_mut()[byte_offset_in_symbol..];
            let amount_to_copy = cmp::min(data.len(), symbol_buffer.len());

            symbol_buffer[..amount_to_copy].copy_from_slice(&data[..amount_to_copy]);

            data = &data[amount_to_copy..];

            self.bytes_written_in_symbol += amount_to_copy;
            if self.bytes_written_in_symbol == self.inner.symbol_size_bytes {
                self.symbol_offset += 1;
                self.bytes_written_in_symbol = 0;
            }

            written += amount_to_copy;
        }

        Ok(written)
    }

    pub fn is_full(&self) -> bool {
        let total_symbols = self.inner.n_rows * self.inner.n_columns;
        self.symbol_offset == total_symbols
    }

    /// Returns a reference to the underlying [`SymbolArray2d`].
    pub fn get_ref(&self) -> &SymbolArray2d<T> {
        &self.inner
    }

    fn write_in_order(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        unimplemented!()
    }

    pub fn capacity_bytes(&self) -> usize {
        self.inner.capacity_bytes()
    }

    pub fn clear(&mut self) {
        self.symbol_offset = 0;
        self.bytes_written_in_symbol = 0;
    }

    pub fn clear_and_shrink_to(&mut self, shape: (usize, usize)) {
        self.clear();
        self.inner.shrink_to(shape);
    }
}

impl<T> std::io::Write for SymbolArray2dCursor<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.write_order == self.inner.layout_order {
            self.write_in_order(buf)
        } else {
            self.write_transposed(buf)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn cursor_write() {
        let array = SymbolArray2d::new((3, 5), 1, ArrayOrder::ColumnMajor);
        let mut cursor = SymbolArray2dCursor::new(array, ArrayOrder::RowMajor);

        let written = cursor
            .write(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])
            .unwrap();

        assert_eq!(written, 15);
        assert!(cursor.is_full());
        assert_eq!(
            cursor.get_ref().as_ref(),
            [0, 5, 10, 1, 6, 11, 2, 7, 12, 3, 8, 13, 4, 9, 14]
        );
    }

    #[test]
    fn cursor_write_2() {
        let array = SymbolArray2d::new((3, 5), 2, ArrayOrder::ColumnMajor);
        let mut cursor = SymbolArray2dCursor::new(array, ArrayOrder::RowMajor);

        let written = cursor
            .write(&[
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29,
            ])
            .unwrap();

        assert_eq!(written, 30);
        assert!(cursor.is_full());
        assert_eq!(
            cursor.get_ref().as_ref(),
            [
                0, 1, 10, 11, 20, 21, 2, 3, 12, 13, 22, 23, 4, 5, 14, 15, 24, 25, 6, 7, 16, 17, 26,
                27, 8, 9, 18, 19, 28, 29
            ]
        );
    }
}

//     pub fn new(shape: (usize, usize), symbol_size: usize, order: ArrayOrder) -> Self {
//         assert!(shape.0 != 0 && shape.1 != 0, "both axes must be non-zero");
//         assert_ne!(symbol_size, 0, "`symbol_size` cannot be zero");
//         let mut this = Self {
//             shape,
//             symbol_size,
//             order,
//             buffer: Box::default(),
//         };
//         this.buffer = alloc::vec![0; this.capacity_bytes()].into_boxed_slice();
//         this
//     }
//
//     /// Clears then resizes the buffer to the specified rows and columns.
//     ///
//     /// If the buffer must grows then it is either extended or reallocated to the new size. If the
//     /// buffer must shrink, then the memory is first deallocated and then reallocated such that no
//     /// excess capacity is held.
//     pub fn clear_and_reshape(&mut self, shape: (usize, usize), order: ArrayOrder) {
//         assert!(shape.0 != 0 && shape.1 != 0, "both axes must be non-zero");
//         let original_capacity = self.capacity_bytes();
//
//         self.shape = shape;
//         let required_buffer_capacity = self.capacity_bytes();
//
//         if original_capacity > required_buffer_capacity {
//             // Free the prior buffer.
//             self.buffer = Vec::default();
//         } else {
//             // Just clear it to avoid any data copy on reservation.
//             self.buffer.clear();
//         };
//         self.buffer.reserve_exact(required_buffer_capacity);
//         self.order = order;
//     }
//
//     #[must_use = "must use the returned count of bytes written"]
//     fn write(&mut self, data: &[u8]) -> usize {
//         let (writeable, _) = data.split_at(cmp::min(data.len(), self.remaining_capacity()));
//         self.buffer.extend(writeable);
//         assert!(self.buffer.len() <= self.capacity_bytes());
//         writeable.len()
//     }
//
//     fn remaining_capacity(&self) -> usize {
//         self.capacity_bytes() - self.buffer.len()
//     }
//
//     fn last_major_axis(&self) -> &[u8] {
//         let length = self.buffer.len() % self.major_axis_capacity_bytes();
//         let start = self.buffer.len() - length;
//
//         &self.buffer[start..(start + length)]
//     }
//
//     fn transpose(&self) {
//         todo!()
//     }
//
//     fn major_axis_capacity_bytes(&self) -> usize {
//         self.shape.1 * self.symbol_size
//     }
//
//     fn capacity_bytes(&self) -> usize {
//         self.major_axis_capacity_bytes() * self.shape.0
//     }
//
//     fn is_full(&self) -> bool {
//         self.remaining_capacity() == 0
//     }
//
//     fn clear(&mut self) {
//         self.buffer.clear();
//     }
//
//     fn is_empty(&self) -> bool {
//         self.buffer.is_empty()
//     }
//
//     fn filled_row_count(&self) -> usize {
//         self.buffer.len() / self.major_axis_capacity_bytes()
//     }
//
//     fn is_only_full_rows(&self) -> bool {
//         self.buffer.len() % self.major_axis_capacity_bytes() == 0
//     }
// }
//
