use raptorq::ObjectTransmissionInformation;

use super::MAX_SYMBOL_SIZE;

/// Creates a new [`ObjectTransmissionInformation`] for the given `symbol_size`.
///
/// # Panics
///
/// Panics if the `symbol_size` is larger than [`MAX_SYMBOL_SIZE`][`super::MAX_SYMBOL_SIZE`].
pub(super) fn get_transmission_info(symbol_size: usize) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(
        0,
        symbol_size
            .try_into()
            .expect("`symbol_size` must not be larger than `MAX_SYMBOL_SIZE`"),
        0,
        1,
        1,
    )
}

/// Computes the correct symbol size given the number of source symbols, `symbols_count`, and the
/// data length, `data_length`. Returns `None` if the computed symbol size is larger than
/// [`MAX_SYMBOL_SIZE`][`super::MAX_SYMBOL_SIZE`].
pub(super) fn compute_symbol_size(data_length: usize, symbols_count: usize) -> Option<usize> {
    let result = (data_length - 1) / symbols_count + 1;
    if result > MAX_SYMBOL_SIZE {
        None
    } else {
        Some(result)
    }
}
