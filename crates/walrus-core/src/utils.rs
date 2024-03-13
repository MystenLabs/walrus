// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities used throughout Walrus.

/// Concatenate multiple string constants.
///
/// Based on the [const_str] crate. If more complex functionality is needed,
/// then consider including that crate.
///
/// [const_str]: https://docs.rs/const-str/latest/const_str/
#[macro_export]
macro_rules! concat_const_str {
    ($($str:expr),+ $(,)?) => {{
        const STRS: &[&str] = &[$($str),+];
        const OUTPUT_LENGTH: usize = {
            let mut output_length = 0;
            let mut i = 0;

            while i < STRS.len() {
                output_length += STRS[i].as_bytes().len();
                i += 1;
            }
            output_length
        };
        const OUTPUT: [u8; OUTPUT_LENGTH] = {
            let mut output = [0u8; OUTPUT_LENGTH];
            let mut output_index = 0;
            let mut str_index = 0;
            let mut byte_index = 0;

            while str_index < STRS.len() {
                let current_bytes = STRS[str_index].as_bytes();
                while byte_index < current_bytes.len() {
                    output[output_index] = current_bytes[byte_index];
                    byte_index += 1;
                    output_index += 1;
                }
                str_index += 1;
            }
            output
        };

        // Safety: inputs are all strings, so output should be valid utf8
        unsafe { core::str::from_utf8_unchecked(&OUTPUT) }
    }};
}
