// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Test utilities shared between various crates.

use std::{fs, future::Future, path::Path};

use anyhow::ensure;
use rand::{rngs::StdRng, seq::SliceRandom, RngCore, SeedableRng};
use tempfile::TempDir;

/// A result type useful in tests, that wraps any error implementation.
pub type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
        $crate::param_test!(
            $func_name -> ():
            [ $( $(#[$outer])* $case_name: $(<$($type_args),+>)?( $($args),* ) ),+ ]
        );
    };
}

/// Macro for creating parametrized *asynchronous* tests.
///
/// This macro behaves similarly to the [`param_test`] macro, however it must be used with an
/// `async` function. For convenience, the macro expands the test cases with the `#[tokio::test]`
/// attribute.
///
/// In contrast to [`param_test`], this does not currently support type parameters, but it does
/// support adding parameters to all cases in addition to individual cases. For example, the
/// following applies `#[tokio::test(start_paused = true)]` to all the test cases, but `#[ignore]`
/// to only the `positive` test case.
///
/// ```ignored
/// # use std::error::Error;
/// # #[cfg(test)]
/// # use walrus_test_utils::async_param_test;
/// #
/// async_param_test! {
///     #[tokio::test(start_paused = true)]
///     test_parses -> Result<(), Box<dyn Error>>: [
///         #[ignore] positive: ("21", 21),
///         negative: ("-17", -17),
///     ]
/// }
/// async fn test_parses(to_parse: &str, expected: i32) -> Result<(), Box<dyn Error>> {
///     tokio::time::resume(); // Panics if not paused.
///     assert_eq!(expected, to_parse.parse::<i32>()?);
///     Ok(())
/// }
/// ```
///
/// See [`param_test`] for more information and examples.
#[macro_export]
macro_rules! async_param_test {
    // Macro uses 'internal rules' to avoid any difficulty with imports. Additionally, we use
    // 'TT Bundling' to pass parameters through multiple calls as a single unit.
    //
    // See https://danielkeep.github.io/tlborm/book/README.html for more information.
    (@expand_return_type ()) => { () };
    (@expand_return_type ($return_type:ty)) => { $return_type };
    (@merge_attributes (), (), $body:tt) => {
        async_param_test!(@expand_test_case (#[tokio::test]), $body);
    };
    (@merge_attributes (), ($(#[$case:meta])+), $body:tt) => {
        async_param_test!(@expand_test_case ($(#[$case])+), $body);
    };
    (@merge_attributes ($(#[$outer:meta])+), ($(#[$case:meta])*), $body:tt) => {
        async_param_test!(@expand_test_case ($(#[$outer])+ $(#[$case])*), $body);
    };
    (@expand_test_case ($(#[$outer:meta])*), ($($body:tt)+)) => { $(#[$outer])* $($body)+ };
    (@group_inputs $shared_meta:tt, $func_name:ident, $return_group:tt, [
        $($(#[$case_meta:meta])* $case_name:ident: ($($args:expr),+)),+$(,)?
    ]) => {
        mod $func_name {
            use super::*;

            $(
                async_param_test!{
                    @merge_attributes $shared_meta, ($(#[$case_meta])*),
                    (
                        async fn $case_name() -> async_param_test!(
                            @expand_return_type $return_group
                        ) {
                            $func_name($($args),+).await
                        }
                    )
                }
            )*
        }
    };
    ($(#[$outer:meta])* $func_name:ident $(-> $return_ty:ty)?: $cases:tt) => {
        async_param_test!(
            @group_inputs ($(#[$outer])*), $func_name, ($($return_ty)?), $cases
        );
    };
}

/// Macro for creating parametrized *simtest* tests.
///
/// Note that this macro reuses [`async_param_test`] macro, but use `#[walrus_simtest]` instead of
/// `#[tokio::test]` attribute to each test case.
#[macro_export]
macro_rules! simtest_param_test {
    ($func_name:ident: [
        $( $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        async_param_test!(
            $func_name -> (): [ $( #[walrus_simtest] $case_name: ($($args),+) ),* ]
        );
    };
    ($func_name:ident -> $return_ty:ty: [
        $( $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        async_param_test!(
            $func_name -> $return_ty: [ $( #[walrus_simtest] $case_name: ( $($args),+ ) ),* ]
        );
    };
    (#[tokio::test(start_paused = true)] $func_name:ident -> $return_ty:ty: [
        $( $case_name:ident: ( $($args:expr),+ ) ),+$(,)?
    ]) => {
        async_param_test!(
            $func_name -> $return_ty: [
                $( #[tokio::test(start_paused = true)] $case_name: ( $($args),+ ) ),*
            ]
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

    /// Convert a `WithTempDir<T>` to a `WithTempDir<U>` by applying the provided
    /// fallible function to the inner value, while maintaining the temporary directory.
    pub fn and_then<U, F, E>(self, f: F) -> std::result::Result<WithTempDir<U>, E>
    where
        F: FnOnce(T) -> std::result::Result<U, E>,
    {
        Ok(WithTempDir {
            inner: f(self.inner)?,
            temp_dir: self.temp_dir,
        })
    }

    /// Convert a `WithTempDir<T>` to a `WithTempDir<U>` by applying the provided
    /// async function to the inner value, while maintaining the temporary directory.
    pub async fn map_async<U, F, Fut>(self, f: F) -> WithTempDir<U>
    where
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = U>,
    {
        WithTempDir {
            inner: f(self.inner).await,
            temp_dir: self.temp_dir,
        }
    }

    /// Convert a `WithTempDir<T>` to a `WithTempDir<U>` by applying the provided
    /// fallible async function to the inner value, while maintaining the temporary
    /// directory.
    pub async fn and_then_async<U, F, Fut, E>(self, f: F) -> std::result::Result<WithTempDir<U>, E>
    where
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = std::result::Result<U, E>>,
    {
        Ok(WithTempDir {
            inner: f(self.inner).await?,
            temp_dir: self.temp_dir,
        })
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

/// Gets a random subset of `count` elements of `data` in an arbitrary order.
///
/// Uses a newly generated RNG with fixed seed. If the `data` has fewer elements than `count` the
/// original number of elements is returned.
pub fn random_subset<T>(
    data: impl IntoIterator<Item = T>,
    count: usize,
) -> impl Iterator<Item = T> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut data: Vec<_> = data.into_iter().collect();
    data.shuffle(&mut rng);
    data.into_iter().take(count)
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

/// Creates a list of deterministically generated random bytearray of
/// length `data_length` with different seeds.
pub fn random_data_list(data_length: usize, count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| random_data_from_rng(data_length, &mut StdRng::seed_from_u64(i as u64)))
        .collect()
}

/// Creates a NonZero value from a literal with a compile-time check disallowing zero.
#[macro_export]
macro_rules! nonzero {
    (0) => {
        compile_error!("cannot create a NonZero with value zero")
    };
    ($value:literal) => {
        NonZero::new($value).expect("is non-zero")
    };
}

/// Overwrites a file with the provided content and returns an error if the file is not equal to the
/// original content.
pub fn overwrite_file_and_fail_if_not_equal(
    path: impl AsRef<Path>,
    content: impl AsRef<str>,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    let content = content.as_ref();
    let original = fs::read_to_string(path)?;
    fs::write(path, content)?;
    ensure!(
        // Compare lines so this also works on Windows where line endings are different.
        original.lines().eq(content.lines()),
        "file {} was out of sync; was updated automatically",
        path.display()
    );
    Ok(())
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
        async_sum_no_return: [
            case1: (2, 3, 5),
            case2: (7, 4, 11)
        ]
    }
    async fn async_sum_no_return(lhs: usize, rhs: usize, total: usize) {
        assert_eq!(lhs + rhs, total);
    }

    async_param_test! {
        async_sum_return -> Result<(), Box<dyn Error>>: [
            case1: (2, 3, 5),
            case2: (7, 4, 11)
        ]
    }
    async fn async_sum_return(lhs: usize, rhs: usize, total: usize) -> Result<(), Box<dyn Error>> {
        assert_eq!(lhs + rhs, total);
        Ok(())
    }

    async_param_test! {
        #[tokio::test(start_paused = true)]
        async_sum_no_return_with_shared_meta: [
            case1: (2, 3, 5),
            case2: (7, 4, 11)
        ]
    }
    async fn async_sum_no_return_with_shared_meta(lhs: usize, rhs: usize, total: usize) {
        #[cfg(not(msim))]
        tokio::time::resume(); // Panics if not paused.
        assert_eq!(lhs + rhs, total);
    }

    async_param_test! {
        #[tokio::test(start_paused = true)]
        async_sum_return_with_shared_meta -> Result<(), Box<dyn Error>>: [
            case1: (2, 3, 5),
            case2: (7, 4, 11)
        ]
    }
    async fn async_sum_return_with_shared_meta(
        lhs: usize,
        rhs: usize,
        total: usize,
    ) -> Result<(), Box<dyn Error>> {
        #[cfg(not(msim))]
        tokio::time::resume(); // Panics if not paused.
        assert_eq!(lhs + rhs, total);
        Ok(())
    }

    async_param_test! {
        async_sum_return_with_individual_meta -> Result<(), Box<dyn Error>>: [
            #[tokio::test] #[ignore = "testing that attribute is applied"] case1: (2, 3, 5),
            case2: (7, 4, 11)
        ]
    }
    async fn async_sum_return_with_individual_meta(
        lhs: usize,
        rhs: usize,
        total: usize,
    ) -> Result<(), Box<dyn Error>> {
        assert_eq!(lhs + rhs, total);
        Ok(())
    }

    async_param_test! {
        #[tokio::test(start_paused = true)]
        async_sum_return_with_individual_and_shared_meta -> Result<(), Box<dyn Error>>: [
            #[ignore = "testing that attribute is applied"] case1: (2, 3, 5),
            case2: (7, 4, 11)
        ]
    }
    async fn async_sum_return_with_individual_and_shared_meta(
        lhs: usize,
        rhs: usize,
        total: usize,
    ) -> Result<(), Box<dyn Error>> {
        #[cfg(not(msim))]
        tokio::time::resume(); // Panics if not paused.
        assert_eq!(lhs + rhs, total);
        Ok(())
    }
}
