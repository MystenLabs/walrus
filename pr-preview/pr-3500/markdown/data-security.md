> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus provides decentralized storage for application and user data. All data stored on Walrus is public and can be accessed by anyone. While Walrus natively provides some data availability and integrity guarantees, use cases that require data confidentiality should use additional encryption mechanisms such as [Seal](https://seal-docs.wal.app/) and [Nautilus](https://docs.sui.io/concepts/cryptography/nautilus).

> **Caution**
>
> Blob IDs are not secrets. Anyone with a blob ID can fetch the blob, so encrypt private data before uploading it to Walrus.
## Data availability

The encoding mechanisms applied by Walrus guarantee that blobs can be written and remain available as long as 2/3 of the shards are operated by storage nodes that act honestly. After data is written, reads are possible even if as few as 1/3 of the nodes are available.

Each blob has a point of availability (PoA) observable through an event on Sui. Before the PoA, you are responsible for ensuring blob availability and upload to Walrus. After the PoA, Walrus is responsible for maintaining blob availability for the full storage period.

If a blob is incorrectly encoded, storage nodes can produce an inconsistency proof. Reads for blob IDs with inconsistency proofs return `None`. Correctly stored blobs cannot have inconsistency proofs generated for them.

You can learn more in the [whitepaper](/walrus.pdf) and in the [Walrus fundamentals documentation](/docs/system-overview/core-concepts).

## Data integrity

Walrus guarantees that any data read corresponds to what the user who uploaded the data intended. Because the encoding is done by the client, it is possible that this encoding is incorrect, either by mistake or on purpose. This causes some subtleties, which are described in the [encoding documentation](/docs/system-overview/red-stuff).

## Seal: Data confidentiality and access control

Walrus does not provide native encryption for data. By default, all blobs stored in Walrus are public and discoverable by everyone. If your use case needs encryption or access control, you need to secure data before uploading to Walrus.

You can use any encryption and access-control mechanism you prefer. If you want onchain access control, [Seal](https://seal-docs.wal.app/) is the most powerful and straightforward option.

Seal allows you to encrypt data using threshold encryption, where no single party holds the full decryption key. You can define onchain access policies that determine who can decrypt the data and under what conditions, and store encrypted content on Walrus while keeping decryption logic verifiable and flexible.

Seal integrates seamlessly with Walrus and is recommended for any use cases involving:

- Sensitive off-chain content, for example, user documents, game assets, or private messages
- Time-locked or token-gated data
- Data shared between trusted parties or roles
- Agent state and memory that an autonomous agent persists between runs

> **Walk through it end to end**
>
> The [Encrypting data with Seal](/docs/seal-encryption-tutorial) tutorial has working TypeScript that encrypts a message, stores the ciphertext on Walrus, reads it back, and decrypts it.
### Encrypting agent state

A common pattern for AI agents is to keep working memory or accumulated state durable across runs by storing it on Walrus. Because Walrus blobs are public, encrypt that state with Seal before storing it, and gate decryption with a `seal_approve` policy that only the agent's identity (or its operator) satisfies. The agent encrypts and uploads at the end of a run, then reads and decrypts at the start of the next one, keeping the state confidential while it lives on Walrus.

### Seal concepts

A few terms appear throughout the Seal documentation and the tutorial:

- **Access policy.** A Move function named `seal_approve` (or `seal_approve_*`) that decides who may decrypt. Seal key servers evaluate it against current onchain state before releasing decryption shares.
- **Allowlist.** A common policy where a shared object holds a list of authorized addresses. It suits private documents and invitational sharing. Other patterns include token gating and time locks.
- **Policy ID (identity).** The byte string a piece of data is encrypted under. It must begin with the policy's namespace (for an allowlist, the allowlist object's ID) so the policy's prefix check passes.
- **Key servers and threshold.** Seal splits the decryption key across key servers; the threshold is how many must each return a share to decrypt. No single server can decrypt on its own.

These are summarized here only as orientation. For the authoritative definitions, the full API, and the security model, see [Using Seal](https://seal-docs.wal.app/UsingSeal).

> **Seal transactions need an explicit sender**
>
> When building a programmable transaction block (PTB) for Seal policy checks, set the transaction sender before passing the PTB to Seal. The sender must match the address that is requesting decryption access.
> 
> ```ts
tx.setSender(address);
```
> 
> If the sender is missing or does not match, decryption can fail with `Transaction was not signed by the correct sender`.
### Troubleshooting

- **Decryption needs to be online.** Decryption is not an offline operation: the client must reach the Seal key servers and the relevant Sui network so the policy can be evaluated and shares retrieved. Plan for network access wherever you decrypt.
- **`Requested package is not supported`.** The key servers do not recognize your package on the network they serve. Confirm the package is published on the same network as the key servers, and that decryption targets that same network. Seal key servers are network-specific: a Testnet key server does not serve keys for a Mainnet package, and vice versa.
- **`Transaction was not signed by the correct sender`.** Set the PTB sender with `tx.setSender()` and ensure it matches the address requesting access (see the tip above).

To get started, follow the [Encrypting data with Seal](/docs/seal-encryption-tutorial) tutorial or refer to the [Seal SDK](https://www.npmjs.com/package/@mysten/seal).

## Nautilus: Secure and verifiable off-chain computation

Nautilus is a framework for secure and verifiable off-chain computation on Sui. It enables you to delegate sensitive or resource-intensive tasks to a self-managed trusted execution environment (TEE) while using smart contract verification to preserve trust onchain.

Use Nautilus for hybrid apps that require private data handling, complex computations, or integration with external Web2 systems. The framework ensures computations are tamper-resistant, isolated, and cryptographically verifiable.

Nautilus currently supports self-managed AWS Nitro Enclave TEEs. You can verify AWS-signed enclave attestations onchain using Move smart contracts. See the [GitHub repository](https://github.com/MystenLabs/nautilus) for the reproducible build template.

### Use cases

- **Trusted oracles:** Process off-chain data from Web2 services or decentralized storage platforms like Walrus in a tamper-resistant way.
- **AI agents:** Securely run AI models for inference or execute agentic workflows while providing data and model provenance onchain.
- **DePIN solutions:** Enable private data computation in IoT and supply chain networks.
- **Fraud prevention:** Secure order matching, settlement, and multi-party computations for DEXs and layer 2 solutions.
- **Identity management:** Provide onchain verifiability for decentralized governance with proof of tamper resistance.

To get started, see [Using Nautilus](https://docs.sui.io/concepts/cryptography/nautilus/using-nautilus).