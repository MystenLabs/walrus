# Walrus Performance Tests

Runs performance tests from `scripts/k6` with different parameter combinations against a Walrus
deployment.

## Overview

This crate contains *integration tests* that target a running Walrus cluster, such as on localhost
or in CI. The primary use-case is for running these tests in CI or on the performance testbed.

## Listing tests

The tests can be listed with

```bash
cargo nextest list --features=k6-tests --package walrus-performance-tests --profile=performance-test
```

The command to run the test intentionally requires several flags to be set: The
`--features=k6-tests` is required as the tests are gated behind a feature flag to ensure that people
do not unintentionally run them during `cargo test`; the `--package` flag is required as the package
is not listed as a default package of the crate, to avoid it being unintentionally targeted; and
the `--profile` flag is required as the default nextest profile has been configured to filter out
these tests. The profile `performance-test` includes only these tests and ensures that they run
sequentially and in order (publisher tests before aggregator tests).

## Running tests

Tests can be run with

```bash
cargo nextest run --features=k6-tests --package walrus-performance-tests --profile=performance-test
```

Add `--no-capture` flag to see live output from the running tests.
