#![allow(missing_docs)]
use core::ops::DerefMut;

pub(super) const UNSPECIFIED_DIMENSION: usize = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArrayOrder {
    RowMajor,
    ColumnMajor,
}

#[derive(Debug)]
pub(super) struct SymbolArray2d<T> {
    /// The shape of the array.
    shape: (usize, usize),
    /// The size of each symbol in the tile.
    symbol_size: usize,
    /// The layout order of the array.
    order: ArrayOrder,
    /// Buffer containing symbol data.
    buffer: T,
}

impl<T> SymbolArray2d<T>
where
    T: DerefMut<Target = [u8]>,
{
    pub fn from_buffer(
        buffer: T,
        mut shape: (usize, usize),
        symbol_size: usize,
        order: ArrayOrder,
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

        let buffer_symbol_capacity = buffer.len() / symbol_size;

        if shape.0 != UNSPECIFIED_DIMENSION && shape.1 != UNSPECIFIED_DIMENSION {
            // Dimensions are fully specified.
            assert_eq!(
                shape.0 * shape.1,
                buffer_symbol_capacity,
                "buffer must have capacity to store total number of symbols"
            );
        } else {
            // At least 1 dimension is unspecified (zero).
            let specified_dim = shape.0 + shape.1;
            assert_eq!(
                buffer_symbol_capacity % specified_dim,
                0,
                "buffer must have capacity to store whole multiples of the specified dimension"
            );

            let calculated_dim = buffer_symbol_capacity / specified_dim;
            shape = match shape {
                (UNSPECIFIED_DIMENSION, specified_dim) => (calculated_dim, specified_dim),
                (specified_dim, UNSPECIFIED_DIMENSION) => (specified_dim, calculated_dim),
                _ => unreachable!("handled in the if branch"),
            };
        }

        Self {
            shape,
            symbol_size,
            order,
            buffer,
        }
    }

    /// Changes the layout of the 2D array.
    ///
    /// This changes how the data is laid-out, but not the semantics of the data (the rows before
    /// and after the change are the same, just possibly not contiguous).
    pub fn change_layout(&mut self, order: ArrayOrder) {
        if self.order == order {
            return;
        }

        self.order == order;
        self.transpose();
    }

    fn transpose(&mut self) {
        todo!()
    }
}

impl<T> AsRef<[u8]> for SymbolArray2d<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.buffer.deref()
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
