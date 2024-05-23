# Walrus

A decentralized blob store using [Sui](https://github.com/MystenLabs/sui) for coordination and governance.

## Documentation

A high-level description of the whole system is available in
[docs/devnet-public/README.md](docs/devnet-public/README.md) and further documents linked therein.

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
- [`docs`](docs) contains high-level technical and design documentation about Walrus.
- [`scripts`](docs) contains tools used for evaluating and testing the code. In particular, this
  contains a script to run a local testbed, see [below](#run-it-locally).

### Rust crates

Our Rust code is split into several crates with different responsibilities:

- [walrus-core](crates/walrus-core/) contains core types and functionality, including encoding and
  authentication mechanisms.
- [walrus-sui](crates/walrus-sui/) contains all types and interactions with the Sui smart contracts.
- [walrus-sdk](crates/walrus-sdk/) contains (client) interactions with storage nodes.
- [walrus-service](crates/walrus-service/) contains client and server functionality including the
  storage backend. This crate also contains all binaries, in particular `walrus` and `walrus-node`.
- [walrus-test-utils](crates/walrus-test-utils/) contains test macros and other utilities used in
  the other crates.
- [walrus-orchestrator](crates/walrus-orchestrator/) contains tools to deploy and benchmark
  distributed Walrus networks.

## Run it locally

In addition to publicly deployed Walrus systems, you can deploy a Walrus testbed on your local
machine for manual testing. All you need to do is run the script `scripts/local-testbed.sh`. See
`scripts/local-testbed.sh -h` for further usage information.

Note that while the Walrus storage nodes of this testbed run on your local machine, the Sui devnet
is used by default to deploy and interact with the contracts. To run the testbed fully locally, simply
[start a `sui-test-validator`](https://docs.sui.io/guides/developer/getting-started/local-network)
and specify `localnet` when starting the Walrus testbed.

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
