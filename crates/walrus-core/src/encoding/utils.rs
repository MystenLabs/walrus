use raptorq::ObjectTransmissionInformation;

pub(super) fn get_transmission_info(symbol_size: u16) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(0, symbol_size, 0, 1, 1)
}

pub(super) fn compute_symbol_size(data_length: usize, symbols_count: usize) -> u16 {
    // TODO(mlegner): add proper error handling
    ((data_length - 1) / symbols_count + 1) as u16
}
