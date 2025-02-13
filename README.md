# Walrus

A decentralized blob store using [Sui](https://github.com/MystenLabs/sui) for coordination and governance.

## Documentation

General Walrus documentation is available at [docs.walrus.site](https://docs.walrus.site), which is
built from the [MystenLabs/walrus-docs](https://github.com/MystenLabs/walrus-docs) repository. That
repository also contains usage examples for Walrus.

Our encoding system, which we call *Red Stuff*, is described in detail in
[docs/red-stuff.md](docs/red-stuff.md).

All our code is covered by Rust docs. You can build these and open them in your web browser by
running the following:

```sh
cargo doc --workspace --open # add `--no-deps` to prevent building docs of all dependencies
```

## Repository structure

This repository contains all Walrus-related code, tools, and documentation:

- [`contracts`](contracts) contains all smart contracts used by Walrus for coordination and governance.
- [`crates`](crates) contains all Rust crates related to Walrus including binaries for storage nodes
  and clients. See [below](#rust-crates) for further information about those.
- [`docker`](docker) contains Dockerfiles and docker-compose setups for building and running Walrus.
- [`docs`](docs) contains high-level technical and design documentation about Walrus.
- [`scripts`](docs) contains tools used for evaluating and testing the code. In particular, this
  contains a script to run a local testbed, see [below](#run-a-local-walrus-testbed).
- [`testnet-contracts`](testnet-contracts) contains the Walrus contracts deployed for Walrus Testnet.

### Rust crates

Our Rust code is split into several crates with different responsibilities. The main code for Walrus
is contained in the following crates:

- [walrus-core](crates/walrus-core/) contains core types and functionality, including encoding and
  authentication mechanisms.
- [walrus-sdk](crates/walrus-sdk/) contains (client) interactions with storage nodes.
- [walrus-service](crates/walrus-service/) contains client and server functionality including the
  storage backend. This crate also contains all main binaries, in particular `walrus` and
  `walrus-node`.
- [walrus-sui](crates/walrus-sui/) contains all types and interactions with the Sui smart contracts.
- [walrus-utils](crates/walrus-utils/) contains utility functions used in multiple crates.

The following crates contain additional tools that are not part of the main functionality of Walrus
and more extensive tests:

- [walrus-e2e-tests](crates/walrus-e2e-tests/) contains end-to-end tests, some of which are also
  run as simulation tests.
- [walrus-orchestrator](crates/walrus-orchestrator/) contains tools to deploy and benchmark
  distributed Walrus networks. This crate is not a default member of the workspace and therefore
  needs to be built explicitly by adding `-p walrus-orchestrator` or `--workspace` to the cargo
  commands.
- [walrus-proc-macros](crates/walrus-proc-macros/) contains procedural macros used in the other
  crates, notably to define simulation tests.
- [walrus-proxy](crates/walrus-proxy/) contains a metrics proxy that authenticates storage nodes and
  collects metrics from them. This crate is not a default member of the workspace and therefore
  needs to be built explicitly by adding `-p walrus-proxy` or `--workspace` to the cargo commands.
- [walrus-simtest](crates/walrus-simtest/) contains simulation tests to ensure that Walrus works
  correctly for all interleavings of concurrent operations and in the presence of crashes.
- [walrus-stress](crates/walrus-stress/) contains a stress client, which is used to put load on
  storage nodes.
- [walrus-test-utils](crates/walrus-test-utils/) contains test macros and other utilities used in
  the other crates.

## Using the Walrus client

The `walrus` binary can be used to interact with Walrus as a client. To use it, you need a Walrus
configuration and a Sui wallet; both of which are automatically generated when [running a local
testbed](#run-a-local-walrus-testbed). If you want to interact with a publicly deployed Walrus
system, you need to obtain the system information and set up a wallet manually.

In general, you can build and run the Walrus client as follows:

```sh
cargo run --bin walrus -- <commands and arguments>
```

Detailed usage information is available through the `--help` flag:

```sh
cargo run --bin walrus -- --help
```

Further information is available in the [public
documentation](https://docs.walrus.site/usage/interacting.html).

## Run a local Walrus testbed

In addition to publicly deployed Walrus systems, you can deploy a Walrus testbed on your local
machine for manual testing. All you need to do is run the script `scripts/local-testbed.sh`. See
`scripts/local-testbed.sh -h` for further usage information.

The script generates configuration that you can use when [running the Walrus
client](#using-the-walrus-client) and prints the path to that configuration file.

In addition, one can spin up a local grafana instance to visualize the metrics collected by the
storage nodes. This can be done via `cd docker/grafana-local; docker compose up`. This should work
with the default storage node configuration.

Note that while the Walrus storage nodes of this testbed run on your local machine, the Sui devnet
is used by default to deploy and interact with the contracts. To run the testbed fully locally, simply
[start a local network with `sui start --with-faucet --force-regenesis`](https://docs.sui.io/guides/developer/getting-started/local-network)
(requires `sui` to be v1.28.0 or higher) and specify `localnet` when starting the Walrus testbed.

## Hardware requirements

- We assume that this code is executed on at least 32-bit hardware; concretely, we assume that a `u32` can be converted
  safely into a `usize`.
- Servers are assumed to use a 64-bit architecture (or higher); concretely, `usize` has at least 64 bits.
- When a client is executed on a 32-bit architecture, it may panic for blobs above a certain size. Given sufficient
  physical memory (4 GiB), it is generally possible to encode or decode blobs smaller than 500 MiB on 32-bit
  architectures.

## Contributing

If you observe a bug or want to request a feature, please search for an existing
[issue](https://github.com/MystenLabs/walrus/issues) on this topic and, if none exists, create a new one. If you would
like to contribute code directly (which we highly appreciate), please familiarize yourself with our [contributing
workflow](./CONTRIBUTING.md).

## License

This project is licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or
<https://www.apache.org/licenses/LICENSE-2.0>).
