{/* https://linear.app/mysten-labs/issue/DOCS-655/data-security */}

Walrus provides decentralized storage for application and user data. All data stored on Walrus is public and can be accessed by anyone. While there are some data availability and integrity guarantees provided by Walrus natively, use cases that require data confidentially should use additional encryption mechanisms such as [Seal](https://seal-docs.wal.app/) and [Nautilus](https://docs.sui.io/concepts/cryptography/nautilus).

## Data availability

The encoding mechanisms applied by Walrus guarantee that blobs can be written and remain available as long as 2/3 of the shards are operated by storage nodes that act honestly. Once data is written, reads are possible even if as few as 1/3 of the nodes are available. 

Each blob has a point of availability (PoA) observable through an event on Sui. Before the PoA, you are responsible for ensuring blob availability and upload to Walrus. After the PoA, Walrus is responsible for maintaining blob availability for the full storage period.

If a blob is incorrectly encoded, storage nodes can produce an inconsistency proof. Reads for blob IDs with inconsistency proofs return `None`. Correctly stored blobs cannot have inconsistency proofs generated for them.

You can learn more in the [whitepaper](/walrus.pdf) and in the [design properties documentation](/docs/design/properties).

## Data integrity

Walrus guarantees that any data read corresponds to what the user who uploaded the data intended. As the encoding is done by the client, it is possible that this encoding is incorrect, either by mistake or on purpose. This causes some subtleties, which are described in the [encoding documentation](/docs/design/encoding).

## Seal: Data confidentially and access control 

Walrus does not provide native encryption for data. By default, all blobs stored in Walrus are public and discoverable by everyone. If your use case needs encryption or access control, you need to secure data before uploading to Walrus.

You can use any encryption and access-control mechanism you prefer. However, if you want on-chain access control, [Seal](https://seal-docs.wal.app/) is the most powerful and straightforward option. 

Seal allows you to encrypt data using threshold encryption, where no single party holds the full decryption key. You can define on-chain access policies that determine who can decrypt the data and under what conditions and store encrypted content on Walrus while keeping decryption logic verifiable and flexible.

Seal was designed to integrate seamlessly with Walrus and is recommended for any use cases involving:

- Sensitive off-chain content, for example, user documents, game assets, or private messages.

- Time-locked or token-gated data.

- Data shared between trusted parties or roles.

To get started, refer to the [Seal SDK](https://www.npmjs.com/package/@mysten/seal).

## Nautilus: Secure and verifiable off-chain computation

Nautilus is a framework for secure and verifiable off-chain computation on Sui. It enables you to delegate sensitive or resource-intensive tasks to a self-managed trusted execution environment (TEE) while using smart contract verification to preserve trust on-chain.

Use Nautilus for hybrid dApps that require private data handling, complex computations, or integration with external Web2 systems. The framework ensures computations are tamper-resistant, isolated, and cryptographically verifiable.

Nautilus currently supports self-managed AWS Nitro Enclave TEEs. You can verify AWS-signed enclave attestations on-chain using Move smart contracts. See the [GitHub repo](https://github.com/MystenLabs/nautilus) for the reproducible build template.

### Use cases

- **Trusted oracles**: Process off-chain data from Web2 services or decentralized storage platforms like Walrus in a tamper-resistant way.

- **AI agents**: Securely run AI models for inference or execute agentic workflows while providing data and model provenance on-chain.

- **DePIN solutions**: Enable private data computation in IoT and supply chain networks.

- **Fraud prevention**: Secure order matching, settlement, and multi-party computations for DEXs and layer-2 solutions.

- **Identity management**: Provide on-chain verifiability for decentralized governance with proof of tamper resistance.

To get started, see the [Using Nautilus documentation](https://docs.sui.io/concepts/cryptography/nautilus/using-nautilus).