#[test_only]
module subsidies::subsidies_tests;

use subsidies::subsidies;

const ENotImplemented: u64 = 0;

#[test]
fun test_subsidies() {}

#[test, expected_failure(abort_code = ::subsidies::subsidies_tests::ENotImplemented)]
fun test_subsidies_fail() {
    abort ENotImplemented
}
