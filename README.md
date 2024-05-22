# Walrus

A decentralized blob store using [Sui](https://github.com/MystenLabs/sui) for coordination and governance.

## Walrus overview

A description of the whole system is available in
[docs/devnet-public/README.md](docs/devnet-public/README.md) and further documents linked therein.

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
