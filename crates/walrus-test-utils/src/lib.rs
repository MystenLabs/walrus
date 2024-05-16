// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities shared between various crates.

use rand::{rngs::StdRng, seq::SliceRandom, RngCore, SeedableRng};
use tempfile::TempDir;

/// A result type useful in tests, that wraps any error implementation.
pub type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Macro for creating parametrized *synchronous* tests.
///
/// The `param_test!` macro accepts the name of an existing function, followed by a list of case
/// names and their arguments. It expands to a module with a `#[test]` function for each of the
/// cases. Each test case calls the existing, named function with their provided arguments.
///
/// In addition to function arguments, you can also use this with generic functions by providing a
/// list of type parameters.
///
/// See [`async_param_test`] for a similar macro that works with `async` function.
///
/// # Examples
///
/// Calling a simple test function can be done as follows:
///
/// ```
/// # use walrus_test_utils::param_test;
/// #
/// param_test! {
///     test_sum: [
///         positive_sums: (10, 7, 17),
///         negative_sums: (-5, -3, -8)
///     ]
/// }
/// fn test_sum(lhs: i32, rhs: i32, sum: i32) {
///     assert_eq!(lhs + rhs, sum);
/// }
/// ```
///
/// Additionally, test functions can also have return types, such as a [`Result`]:
///
/// ```
/// # use std::error::Error;
/// # use walrus_test_utils::param_test;
/// #
/// param_test! {
///     test_parses -> Result<(), Box<dyn Error>>: [
///         positive: ("21", 21),
///         negative: ("-17", -17),
///     ]
/// }
/// fn test_parses(to_parse: &str, expected: i32) -> Result<(), Box<dyn Error>> {
///     assert_eq!(expected, to_parse.parse::<i32>()?);
///     Ok(())
/// }
/// ```
///
/// You can also use this with generic functions:
///
/// ```
/// # use walrus_test_utils::param_test;
/// #
/// param_test! {
///     test_generic: [
///         comparison_u32: <u32>(10, 7, false),
///         comparison_i32: <i32>(-5, -5, true),
///     ]
/// }
/// fn test_generic<T: std::fmt::Debug + PartialEq>(lhs: T, rhs: T, equal: bool) {
///     assert_eq!(lhs == rhs, equal);
/// }
/// ```
///
/// Finally, attributes such as as `#[ignore]` may be added to individual tests:
///
/// ```
/// # use std::error::Error;
/// # use walrus_test_utils::param_test;
/// #
/// param_test! {
///     test_parses -> Result<(), Box<dyn Error>>: [
///         #[ignore] positive: ("21", 21),
///         negative: ("-17", -17),
///     ]
/// }
/// fn test_parses(to_parse: &str, expected: i32) -> Result<(), Box<dyn Error>> {
///     assert_eq!(expected, to_parse.parse::<i32>()?);
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! param_test {
    ($func_name:ident -> $return_ty:ty: [
        $( $(#[$outer:meta])* $case_name:ident:
            $(<$($type_args:ty),+>)?( $($args:expr),* $(,)? ) ),+$(,)?
    ]) => {
        mod $func_name {
            use super::*;

            $(
                #[test]
                $(#[$outer])*
                fn $case_name() -> $return_ty {
                    $func_name$(::<$($type_args),+>)?($($args),*)
                }
            )*
        }
    };
    ($func_name:ident: [
        $( $(#[$outer:meta])* $case_name:ident:
            $(<$($type_args:ty),+>)?( $($args:expr),* $(,)? ) ),+$(,)?
    ]) => {
        param_test!(
            $func_name -> ():
            [ $( $(#[$outer])* $case_name: $(<$($type_args),+>)?( $($args),* ) ),+ ]
        );
    };
}

/// Macro for creating parametrized *asynchronous* tests.
///
/// This macro behaves similarly to the [`param_test`] macro, however it must be used with an
/// `async` function. For convenience, the macro expands the test cases with the `#[tokio::test]`
/// attribute. If specifying any additional attributes to any test case, it is necessary to
/// re-specify the `#[tokio::test]` macro for *every* test case.
///
/// In contrast to [`param_test`], this does not currently support type parameters.
///
/// See [`param_test`] for more information and examples.
#[macro_export]
macro_rules! async_param_test {
    ($func_name:ident -> $return_ty:ty: [
        $( $(#[$outer:meta])+ $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        mod $func_name {
            use super::*;

            $(
                $(#[$outer])+
                async fn $case_name() -> $return_ty {
                    $func_name($($args),+).await
                }
            )*
        }
    };
    ($func_name:ident: [
        $( $(#[$outer:meta])+ $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        async_param_test!( $func_name -> (): [ $( $(#[$outer])+ $case_name: ($($args),+) ),* ] );
    };

    ($func_name:ident: [
        $( $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        async_param_test!( $func_name -> (): [ $( #[tokio::test] $case_name: ($($args),+) ),* ] );
    };
    ($func_name:ident -> $return_ty:ty: [
        $( $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        async_param_test!(
            $func_name -> $return_ty: [ $( #[tokio::test] $case_name: ( $($args),+ ) ),* ]
        );
    }
}

/// A wrapper for a type along with a temporary directory on which it depends.
#[derive(Debug)]
pub struct WithTempDir<T> {
    /// The wrapped inner type.
    pub inner: T,
    /// The temporary directory that is kept alive.
    pub temp_dir: TempDir,
}

impl<T> WithTempDir<T> {
    /// Convert a `WithTempDir<T>` to a `WithTempDir<U>` by applying the provided
    /// function to the inner value, while maintaining the temporary directory.
    pub fn map<U, F>(self, f: F) -> WithTempDir<U>
    where
        F: FnOnce(T) -> U,
    {
        WithTempDir {
            inner: f(self.inner),
            temp_dir: self.temp_dir,
        }
    }
}

impl<T> AsRef<T> for WithTempDir<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> AsMut<T> for WithTempDir<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

/// Asserts that two sequences that implement [`std::iter::IntoIterator`], and whose items
/// implement [`Ord`] are equal, irrespective of ordering.
#[macro_export]
macro_rules! assert_unordered_eq {
    ($lhs:expr, $rhs:expr) => {
        let mut lhs: Vec<_> = $lhs.into_iter().collect();
        let mut rhs: Vec<_> = $rhs.into_iter().collect();

        lhs.sort();
        rhs.sort();

        assert_eq!(lhs, rhs);
    };
    ($lhs:expr, $rhs:expr, $f:expr) => {
        let mut lhs: Vec<_> = $lhs.into_iter().collect();
        let mut rhs: Vec<_> = $rhs.into_iter().collect();

        lhs.sort_by_key($f);
        rhs.sort_by_key($f);

        assert_eq!(lhs, rhs);
    };
}

/// Gets a random subset of `count` elements of `data` in an arbitrary order using the provided RNG.
///
/// If the `data` has fewer elements than `count` the original number of elements is returned.
pub fn random_subset_from_rng<T>(
    data: impl IntoIterator<Item = T>,
    mut rng: &mut impl RngCore,
    count: usize,
) -> impl Iterator<Item = T> {
    let mut data: Vec<_> = data.into_iter().collect();
    data.shuffle(&mut rng);
    data.into_iter().take(count)
}

/// Gets a random subset of `count` elements of `data` in an arbitrary order.
///
/// Uses a newly generated RNG with fixed seed. If the `data` has fewer elements than `count` the
/// original number of elements is returned.
pub fn random_subset<T>(
    data: impl IntoIterator<Item = T>,
    count: usize,
) -> impl Iterator<Item = T> {
    random_subset_from_rng(data, &mut StdRng::seed_from_u64(42), count)
}

/// Creates a byte vector of length `data_length` filled with random data using the provided RNG.
pub fn random_data_from_rng(data_length: usize, rng: &mut impl RngCore) -> Vec<u8> {
    let mut result = vec![0u8; data_length];
    rng.fill_bytes(&mut result);
    result
}

/// Creates a byte vector of length `data_length` filled with random data.
///
/// Uses a newly generated RNG with fixed seed.
pub fn random_data(data_length: usize) -> Vec<u8> {
    random_data_from_rng(data_length, &mut StdRng::seed_from_u64(42))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    param_test! {
        test_with_no_return: [
            case1: (true, 1, 1),
            case2: (false, 3, 4)
        ]
    }
    fn test_with_no_return(bool_arg: bool, usize_arg: usize, u32_arg: u32) {
        assert_eq!(bool_arg, Ok(usize_arg) == usize::try_from(u32_arg));
    }

    param_test! {
        test_with_return -> Result<(), Box<dyn Error>>: [
            case1: ("5", 5),
            case2: ("7", 7)
        ]
    }
    fn test_with_return(to_parse: &str, parsed: usize) -> Result<(), Box<dyn Error>> {
        let result: usize = to_parse.parse()?;
        assert_eq!(parsed, result);
        Ok(())
    }

    async_param_test! {
        async_test_with_return -> Result<(), Box<dyn Error>>: [
            case1: ("5", 5),
            case2: ("7", 7)
        ]
    }
    async fn async_test_with_return(to_parse: &str, parsed: usize) -> Result<(), Box<dyn Error>> {
        let result: usize = to_parse.parse()?;
        assert_eq!(parsed, result);
        Ok(())
    }
}
