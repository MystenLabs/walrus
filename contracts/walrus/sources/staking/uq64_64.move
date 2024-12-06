// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO: remove this once we have a higher resolution fixed point number in the stdlib (#835)
module walrus::uq64_64;

const EDivisionByZero: u64 = 0;
const EQuotientTooLarge: u64 = 1;

/// Minimal high-precision fixed point implementation until it's available in std.
public struct UQ64_64(u128) has copy, drop;

/// Create a fixed-point value from a quotient specified by its numerator and denominator.
/// Aborts if the denominator is zero.
public fun from_quotient(numerator: u64, denominator: u64): UQ64_64 {
    assert!(denominator != 0, EDivisionByZero);

    // Scale the numerator to have 128 fractional bits and the denominator to have 64 fractional
    // bits, so that the quotient will have 64 fractional bits.
    let scaled_numerator = numerator as u256 << 128;
    let scaled_denominator = denominator as u256 << 64;
    let quotient = scaled_numerator / scaled_denominator;

    // This check should always succeed since the numerator is a u64.
    assert!(quotient <= std::u128::max_value!() as u256, EQuotientTooLarge);
    UQ64_64(quotient as u128)
}

/// Greater than. Returns `true` if and only if `a > b`.
public fun gt(a: &UQ64_64, b: &UQ64_64): bool {
    a.0 > b.0
}

#[test_only]
public fun from_raw(raw_value: u128): UQ64_64 {
    UQ64_64(raw_value)
}
