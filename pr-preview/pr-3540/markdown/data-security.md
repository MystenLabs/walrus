> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus provides decentralized storage for application and user data. All data stored on
Walrus is public and can be accessed by anyone. Walrus natively provides data availability
and integrity guarantees. It does not provide confidentiality.

> **Caution**
>
> Blob IDs are not secrets. Anyone with a blob ID can fetch the blob. Do not upload data to
> Walrus that you are not willing to make public.
## Data availability

The encoding mechanisms applied by Walrus guarantee that blobs can be written and remain
available as long as 2/3 of the shards are operated by storage nodes that act honestly.
After data is written, reads are possible even if as few as 1/3 of the nodes are available.

Each blob has a point of availability (PoA) observable through an event on Sui. Before the
PoA, you are responsible for ensuring blob availability and upload to Walrus. After the
PoA, Walrus is responsible for maintaining blob availability for the full storage period.

If a blob is incorrectly encoded, storage nodes can produce an inconsistency proof. Reads
for blob IDs with inconsistency proofs return `None`. Correctly stored blobs cannot have
inconsistency proofs generated for them.

You can learn more in the [whitepaper](/walrus.pdf) and in the [Walrus fundamentals
documentation](/docs/system-overview/core-concepts).

## Data integrity

Walrus guarantees that any data read corresponds to what the user who uploaded the data
intended. Because the encoding is done by the client, it is possible that this encoding is
incorrect, either by mistake or on purpose. This causes some subtleties, which are
described in the [encoding documentation](/docs/system-overview/red-stuff).

## Data confidentiality

Walrus does not provide native encryption for data. By default, all blobs stored in Walrus
are public and discoverable by everyone. To keep data confidential, encrypt it on the
client before you store it, for example with [Seal](https://github.com/MystenLabs/seal), and
treat the blob ID as public. Encryption is your responsibility. Walrus stores and serves
whatever bytes you upload.

## Data sovereignty and residency

Walrus does not let you choose which storage nodes or geographic regions hold your data.
When you store a blob, Walrus erasure-codes it into slivers and distributes them across all
shards, and a smart contract on Sui assigns shards to storage nodes. There
is no mechanism to pin a blob to a specific node, region, or jurisdiction.

Because every blob is public and its placement is not controllable, do not store personal or
regulated data on Walrus in the clear. If you must handle such data, encrypt it before
storing it, keep the plaintext and any keys off Walrus, and store only what you are willing
to make permanently public. For the distribution and encoding details, see
[Core Concepts](/docs/system-overview/core-concepts) and [Red Stuff](/docs/system-overview/red-stuff).

## Nautilus: Secure and verifiable offchain computation

Nautilus is a framework for secure and verifiable offchain computation on Sui. It enables
you to delegate sensitive or resource-intensive tasks to a self-managed trusted execution
environment (TEE) while using smart contract verification to preserve trust onchain.

Use Nautilus for hybrid apps that require private data handling, complex computations, or
integration with external Web2 systems. The framework makes computations
tamper-resistant, isolated, and cryptographically verifiable.

Nautilus currently supports self-managed AWS Nitro Enclave TEEs. You can verify AWS-signed
enclave attestations onchain using Move smart contracts. See the [GitHub
repository](https://github.com/MystenLabs/nautilus) for the reproducible build template.

### Use cases

- **Trusted oracles:** Process offchain data from Web2 services or decentralized storage
  platforms like Walrus in a tamper-resistant way.
- **AI agents:** Securely run AI models for inference or execute agentic workflows while
  providing data and model provenance onchain.
- **DePIN solutions:** Enable private data computation in IoT and supply chain networks.
- **Fraud prevention:** Secure order matching, settlement, and multi-party computations for
  DEXs and layer 2 solutions.
- **Identity management:** Provide onchain verifiability for decentralized governance with
  proof of tamper resistance.

To get started, see [Using
Nautilus](https://docs.sui.io/concepts/cryptography/nautilus/using-nautilus).