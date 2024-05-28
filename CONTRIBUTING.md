# How to Contribute

## GitHub flow

We generally follow the [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow) in our project. In
a nutshell, this requires the following steps to contribute:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) (only required if
   you don't have write access to the repository).
1. [Create a feature branch](https://docs.github.com/en/get-started/quickstart/github-flow#create-a-branch).
1. [Make changes and create a
   commit](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-and-pushing-changes)
1. Push your changes to GitHub and [create a pull
   request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request) (PR);
   note that we enforce a particular style for the PR titles, see [below](#commit-messages).
1. Wait for maintainers to review your changes and, if necessary, revise your PR.
1. When all requirements are met, a reviewer or the PR author (if they have write permissions) can merge the PR.

## Commit messages

To ensure a consistent Git history (from which we can later easily generate changelogs automatically), we always squash
commits when merging a PR and enforce that all PR titles comply with the [conventional-commit
format](https://www.conventionalcommits.org/en/v1.0.0/). For examples, please take a look at our [commit
history](https://github.com/MystenLabs/walrus/commits/main).

## Pre-commit hooks

We have CI jobs running for every PR to test and lint the repository. You can install Git pre-commit hooks to ensure
that these check pass even *before pushing your changes* to GitHub. To use this, the following steps are required:

1. Install [Rust](https://www.rust-lang.org/tools/install).
1. [Install pre-commit](https://pre-commit.com/#install) using `pip` or your OS's package manager.
1. Run `pre-commit install` in the repository.

After this setup, the code will be checked, reformatted, and tested whenever you create a Git commit.

You can also use a custom pre-commit configuration if you wish:

1. Create a file `.custom-pre-commit-config.yaml` (this is set to be ignored by Git).
1. Run `pre-commit install -c .custom-pre-commit-config.yaml`.

## Tests

The majority of our code is covered by automatic unit and integration tests which you can run
through `cargo test`.

Integration and end-to-end tests are excluded by default when running `cargo test` as they depend on
additional packages and take longer to run. You can run these test as follows:

```sh
cargo test -- --ignored
```

### Test coverage

We would like to cover as much code as possible with tests. Ideally you would add unit tests for all code you
contribute. To analyze test coverage, we use [Tarpaulin](https://crates.io/crates/cargo-tarpaulin). You can install and
run the tool as follows:

```sh
cargo install cargo-tarpaulin
cargo tarpaulin --out html
```

This creates a file `tarpaulin-report.html`, which shows you coverage statistics as well as which individual lines are
or aren't covered by tests. Other valid output formats are `json`, `stdout`, `xml`, and `lcov`.

The configuration file for Tarpaulin is [.tarpaulin.toml](./.tarpaulin.toml).

### Running a local Walrus testbed

You can conveniently deploy and interact with a local Walrus testbed as described [here](README.md#run-a-local-walrus-testbed).

## Benchmarks

We run micro-benchmarks for encoding, decoding, and authentication using
[Criterion.rs](https://bheisler.github.io/criterion.rs/book/criterion_rs.html). These benchmarks are not run
automatically in our pipeline as there is an [explicit advice against doing
this](https://bheisler.github.io/criterion.rs/book/faq.html#how-should-i-run-criterionrs-benchmarks-in-a-ci-pipeline).

You can run the benchmarks by calling `cargo bench` from the project's root directory. Criterion will output some data
to the command line and also generate HTML reports including plots; the root file is located at
[`target/criterion/report/index.html].

Criterion automatically compares the results from multiple runs. To check if your code changes improve or worsen the
performance, run the benchmarks first on the latest `main` branch and then again with your code changes or explicitly
set and use baselines with `--set-baseline` and `--baseline`. See the [Criterion
documentation](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html#baselines) for further
details.

### Profiling

To get quick insights into where the program spends most of its time, you can use the [flamegraph
tool](https://github.com/flamegraph-rs/flamegraph). After installing with `cargo install flamegraph`, you can run
binaries, tests, or benchmarks and produce SVG outputs. For example to analyze the `blob_encoding` benchmark, you can
run the following:

```sh
CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --root --bench blob_encoding --open
```

See [the documentation](https://github.com/flamegraph-rs/flamegraph) for further details and options.

## Signed commits

We appreciate it if you configure Git to [sign your
commits](https://gist.github.com/troyfontaine/18c9146295168ee9ca2b30c00bd1b41e).
