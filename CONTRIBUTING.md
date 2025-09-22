# How to Contribute

## Important note

> [!IMPORTANT]
> We highly appreciate contributions, but **simple typo fixes (e.g., minor spelling errors,
> punctuation changes, or trivial rewording) will be ignored** unless they significantly improve
> clarity or fix a critical issue. If you are unsure whether your change is substantial enough,
> consider opening an issue first to discuss it.

If you want to make a substantial contribution, please first make sure a corresponding issue exists.
Then contact the Walrus maintainers through that issue to check if anyone is already working on this
and to discuss details and design choices before starting the actual implementation.

## GitHub flow

Before contributing, please read the [important note above](#important-note).

We generally follow the [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow)
in our project. In a nutshell, this requires the following steps to contribute:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/contributing-to-projects)
   (only required if you don't have write access to the repository).
1. [Create a feature branch](https://docs.github.com/en/get-started/quickstart/github-flow#create-a-branch).
1. [Make changes and create a
   commit](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-and-pushing-changes).
1. Push your changes to GitHub and [create a pull
   request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request)
   (PR); note that we enforce a particular style for the PR titles, see [below](#commit-messages).
1. Wait for maintainers to review your changes and, if necessary, revise your PR.
1. When all requirements are met, a reviewer or the PR author (if they have write permissions) can
   merge the PR.

## Conventions

To keep our code clean, readable, and maintainable, we strive to follow various conventions. These
are described in detail in the following subsections. Note that some but not all of them are
enforced by our CI pipeline and our [pre-commit hooks](#pre-commit-hooks).

### Error handling and panics

We *do not* use `unwrap` in production code; `unwrap` should only be used in tests, benchmarks, or
similar code. If possible, code should be rewritten such that neither `unwrap` nor `expect` is
needed. If this is not possible or cumbersome, but we *know for sure* that a value *cannot* be
`None` or `Err`, use `expect` with an explanation why it cannot fail.

Otherwise, handle these values explicitly, using `Option` or `Result` return types if needed.
Furthermore, if a function can panic under certain conditions, prefer an explicit `panic!` and make
sure to document this in the function's docstring, see also [below](#documentation).

### Type conversions

[Type cast expressions](https://doc.rust-lang.org/reference/expressions/operator-expr.html#type-cast-expressions)
with ` as `, especially on numeric types, can sometimes have unwanted and unexpected semantics,
including silent truncation, wrapping, or loss of precision. Consequently, we recommend to instead
use `from`/`into` or, if that is not available, `try_from`/`try_into` with [proper error
handling](#error-handling-and-panics) for type conversions.

### Logging

We use [tracing](https://docs.rs/tracing/latest/tracing/) for logging within our crates. Please add
reasonable [spans](https://docs.rs/tracing/latest/tracing/#spans) and [logging
events](https://docs.rs/tracing/latest/tracing/#events) to your code with appropriate [logging
level](https://docs.rs/tracing/latest/tracing/struct.Level.html). In addition, please consider the
following conventions:

- Log entries generally start with a lowercase letter and do not end in a full stop. You can however
  use commas and semicolons.
- Prefer including additional data as [*metadata fields*](https://docs.rs/tracing/latest/tracing/#recording-fields)
  instead of including them in the message. Use the shorthand form whenever possible. Only include
  variables directly in the string if they are necessary to create a useful message in the first
  place.
- In async code, generally use [`instrument`](https://docs.rs/tracing-attributes/latest/tracing_attributes/attr.instrument.html)
  attributes instead of manually creating and entering spans as this automatically handles await
  points correctly.

### Naming

We generally follow the [naming conventions of the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/naming.html).
In addition, please consider the following recommendations:

- All components should have descriptive names that match their purpose.
- The larger the scope of a component, the more expressive its name should be.
- Choose full words over abbreviations. The only exceptions are very frequent and common
  abbreviations like `min`, `max`, `id`.
- The only situation in which very short or even single-letter names are acceptable is for
  parameters of very short closures.

### File names

We use the "modern" naming convention where a module `some_module` is called `some_module.rs` and
its submodules are in the directory `some_module/`, see the [corresponding page in the Rust
reference](https://doc.rust-lang.org/reference/items/modules.html#module-source-filenames).

### Commit messages

To ensure a consistent Git history (from which we can later easily generate changelogs
automatically), we always squash commits when merging a PR and enforce that all PR titles comply
with the [conventional-commit format](https://www.conventionalcommits.org/en/v1.0.0/). For examples,
please take a look at our [commit history](https://github.com/MystenLabs/walrus/commits/main).

### Documentation

Make sure all public structs, enums, functions, etc. are covered by docstrings. Docstrings for
private or `pub(crate)` components are appreciated as well but not enforced.

In general, we follow the [guidelines about documentation in the Rust
documentation](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html#documenting-components).
In particular, please adhere to the following conventions:

- All docstrings are written as full sentences, starting with a capital letter.
- The first line should be a short sentence summarizing the component. Details should be described
  after an empty line.
- If a function can panic, this *must* be documented in a `# Panics` section in the docstring.
- An `# Examples` section is often useful and can simultaneously serve as [documentation
  tests](https://doc.rust-lang.org/rustdoc/write-documentation/documentation-tests.html).
- Docstrings should be [cross-linked](https://doc.rust-lang.org/rustdoc/write-documentation/linking-to-items-by-name.html)
  whenever it makes sense.
- Module docstrings should be inside the respective module file with `//!` (instead of at the module
  inclusion location).

Additionally, if you made any user-facing changes, please adjust our documentation under
[docs/book](./docs/book/).

### Formatting

#### Rust code formatting

We use a few unstable formatting options of Rustfmt. Unfortunately, these can only be used with a
stable toolchain when specified via the `--config` command-line option. This is done in
[CI](.github/workflows/code.yml) and in our [pre-commit hooks](.pre-commit-config.yaml) (see also
[above](#pre-commit-hooks)).

If your editor supports reading `rust-analyzer` preferences from `.vscode/settings.json`, you may want
to add the following configuration to that file to setup autoformatting. Note that this repo ignores
`.vscode/*` to allow you to further customize your workspace settings.

```json
{
  "rust-analyzer.rustfmt.extraArgs": [
    "--config",
    "group_imports=StdExternalCrate,imports_granularity=Crate,imports_layout=HorizontalVertical"
  ]
}
```

Also make sure you use the correct version of Rustfmt. See
[`rust-toolchain.toml`](rust-toolchain.toml) for the current version. This also impacts other checks,
for example Clippy.

#### Move code formatting

We use the `@mysten/prettier-plugin-move` npm package to format Move code. If you're using VSCode,
you can install the [Move Formatter](https://marketplace.visualstudio.com/items?itemName=mysten.prettier-move)
extension. The formatter is also run automatically in the [pre-commit hooks](#pre-commit-hooks).

To use it as a stand-alone tool, we recommend installing it globally (requires NodeJS and npm):

```sh
npm i -g prettier @mysten/prettier-plugin-move
```

The Move formatter can then be run manually by executing:

```sh
prettier-move --write <path-to-move-file-or-folder>
```

## Pre-commit hooks

We have CI jobs running for every PR to test and lint the repository. You can install Git pre-commit
hooks to ensure that these check pass even *before pushing your changes* to GitHub. To use this, the
following steps are required:

1. Install [Rust](https://www.rust-lang.org/tools/install).
1. Install [nextest](https://nexte.st/).
1. [Install pre-commit](https://pre-commit.com/#install) using `pip` or your OS's package manager.
1. Run `pre-commit install` in the repository.

After this setup, the code will be checked, reformatted, and tested whenever you create a Git commit.

You can also use a custom pre-commit configuration if you wish:

1. Create a file `.custom-pre-commit-config.yaml` (this is set to be ignored by Git).
1. Run `pre-commit install -c .custom-pre-commit-config.yaml`.

## Tests

The majority of our code is covered by automatic unit and integration tests which you can run
through `cargo test` or `cargo nextest run` (requires [nextest](https://nexte.st/)).

Integration and end-to-end tests are excluded by default when running `cargo nextest` as they depend
on additional packages and take longer to run. These tests can either be run as follows:

```sh
cargo nextest run --run-ignored ignored-only # run *only* ignored tests
cargo nextest run --run-ignored all # run *all* tests
```

### External test cluster

Integration tests that require a running Sui test cluster can use an external cluster. This requires
a one-time setup:

```sh
CLUSTER_CONFIG_DIR="$PWD/target/sui-start"
mkdir "$CLUSTER_CONFIG_DIR"
sui genesis -f --with-faucet --working-dir "$CLUSTER_CONFIG_DIR"
```

For running tests, start the external cluster with `sui start`, set the environment variable
`SUI_TEST_CONFIG_DIR` to the configuration directory, and run the tests using `cargo test --
--ignored`:

```sh
CLUSTER_CONFIG_DIR="$PWD/target/sui-start"
SUI_CONFIG_DIR="$CLUSTER_CONFIG_DIR" sui start&
SUI_PID=$!
SUI_TEST_CONFIG_DIR="$CLUSTER_CONFIG_DIR" cargo test -- --ignored
```

This runs the tests with the newest contract version.

After the tests have completed, you can stop the cluster:

```sh
kill $SUI_PID
```

Note that it is currently not possible to use an external cluster with `cargo nextest`.

### Running a local Walrus testbed

In addition to publicly deployed Walrus systems, you can deploy a Walrus testbed on your local
machine for manual testing. All you need to do is run the script `scripts/local-testbed.sh`. See
`scripts/local-testbed.sh -h` for further usage information.

The script generates configuration that you can use when running the Walrus client and prints the
path to that configuration file.

In addition, one can spin up a local grafana instance to visualize the metrics collected by the
storage nodes. This can be done via `cd docker/grafana-local; docker compose up`. This should work
with the default storage node configuration.

Note that while the Walrus storage nodes of this testbed run on your local machine, the Sui devnet
is used by default to deploy and interact with the contracts. To run the testbed fully locally, simply
[start a local network with `sui start --with-faucet --force-regenesis`](https://docs.sui.io/guides/developer/getting-started/local-network)
(requires `sui` to be v1.28.0 or higher) and specify `localnet` when starting the Walrus testbed.

### Simtests

We use simulation testing to ensure that the Walrus system keeps working under various failure
scenarios. The tests are in the `walrus-simtest` and `walrus-service` crates, with most of the
necessary plumbing primarily in `walrus-service`.

To run simulation tests, first install the `cargo simtest` tool:

```sh
./scripts/simtest/install.sh
```

You can then run all simtests with

```sh
cargo simtest
```

See [further information about the simtest framework](https://github.com/MystenLabs/sui/tree/main/crates/sui-simulator#how-to-run-sim-tests).

## Benchmarks

We run micro-benchmarks for encoding, decoding, and authentication using
[Criterion.rs](https://bheisler.github.io/criterion.rs/book/criterion_rs.html). These benchmarks are
not run automatically in our pipeline as there is an [explicit advice against doing
this](https://bheisler.github.io/criterion.rs/book/faq.html#how-should-i-run-criterionrs-benchmarks-in-a-ci-pipeline).

You can run the benchmarks by calling `cargo bench` from the project's root directory. Criterion
will output some data to the command line and also generate HTML reports including plots; the root
file is located at [`target/criterion/report/index.html].

Criterion automatically compares the results from multiple runs. To check if your code changes
improve or worsen the performance, run the benchmarks first on the latest `main` branch and then
again with your code changes or explicitly set and use baselines with `--set-baseline` and
`--baseline`. See the [Criterion
documentation](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html#baselines)
for further details.

### Profiling

To get quick insights into where the program spends most of its time, you can use the [flamegraph
tool](https://github.com/flamegraph-rs/flamegraph). After installing with `cargo install
flamegraph`, you can run binaries, tests, or benchmarks and produce SVG outputs. For example to
analyze the `blob_encoding` benchmark, you can run the following:

```sh
CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --root --bench blob_encoding --open
```

See [the documentation](https://github.com/flamegraph-rs/flamegraph) for further details and options.

## Signed commits

We appreciate it if you configure Git to [sign your
commits](https://gist.github.com/troyfontaine/18c9146295168ee9ca2b30c00bd1b41e).
