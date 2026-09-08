> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

An AI agent is only as trustworthy as the memory it acts on. If memory can be silently altered, or if the service holding it can rewrite history or lock you out, the agent's decisions cannot be trusted. Walrus Memory gives agents memory that is both **persistent** (it outlives any single process or provider) and **verifiable** (you can confirm the stored data is intact and who controls it).

Verifiability here rests on three independent properties. None of them requires you to trust the relayer.

## Content-addressed writes

Every write to Walrus produces a **blob ID derived from the stored bytes**. The identifier is a function of the content, so the same bytes always produce the same blob ID, and any change to the bytes produces a different one. A blob ID that still resolves is therefore proof that the stored bytes were not altered after the write.

Each memory you store carries its own blob ID. The result of `rememberAndWait` includes a `blob_id`, and every hit returned by `recall` carries the `blob_id` of the memory it matched, so you always have a durable handle to the exact revision that was stored.

> **Note**
>
> A blob ID addresses the **Seal ciphertext** that Walrus stores, not your plaintext. It proves the encrypted bytes are intact and unchanged. Confidentiality comes from Seal encryption. Content-addressing provides integrity and immutability on top of it.
Because a new version is a new blob with a new ID, the chain of blob IDs over time is an immutable lineage of revisions. For the versioning pattern built on this property, see [Versioned datasets](/walrus-memory/sdk/versioned-datasets).

## Onchain-enforced ownership

Your memory is anchored to a `MemWalAccount` object on Sui. Ownership of that account, and the authorization of the delegate keys that can act on it, are enforced onchain by the smart contract, not by the relayer. A compromised or malicious relayer cannot change who owns an account or forge delegate permissions. It can serve requests, but it cannot rewrite the ownership record.

This is what makes the memory portable and censorship-resistant: control follows the onchain account, so you can move to a different relayer, or self-host one, without losing ownership. For the full enforcement model, see [Ownership and access](/walrus-memory/fundamentals/concepts/ownership-and-access) and the [data flow and security model](/walrus-memory/fundamentals/architecture/data-flow-security-model).

## Decentralized durability

The encrypted blobs live on Walrus, which stores data across a decentralized network with no single point of failure. Walrus is the permanent record: the relayer keeps a vector index for fast search, but that index is a cache. If it is lost, `restore` rebuilds it by reading your blobs back from Walrus, decrypting, and re-indexing. The memory survives the loss of the relayer's database because the source of truth is onchain and on Walrus, not in the relayer. For how blobs are written, funded, and read back, see [How storage works](/walrus-memory/fundamentals/architecture/how-storage-works).

## Verify it yourself

You can check all three properties yourself. Inspect each one directly:

- **Availability:** Call `health()` to confirm the relayer is reachable before you rely on it. This is an unauthenticated liveness check, not a validation of your credentials.
- **Integrity:** Read the `blob_id` off a `recall` result. It is a content-addressed handle to the exact stored ciphertext, so a blob ID that resolves confirms that encrypted blob is intact and unchanged. This is blob-level integrity, and the plaintext inside is protected by Seal encryption, not by content-addressing.
- **Ownership:** Take your `MemWalAccount` object ID and open it in a Sui explorer. The account, its owner, and its delegate authorizations are public onchain state that anyone can inspect, independent of the relayer.

> **Note**
>
> A dedicated `verify()` helper that reconstructs a memory from its onchain blob object is on the roadmap. Until it ships, verifiability comes from the three properties above: content-addressed blob IDs, onchain-enforced ownership you can inspect on a Sui explorer, and durability backed by Walrus. It is a property of the architecture, not a single SDK call.
## References

- [Versioned datasets](/walrus-memory/sdk/versioned-datasets)
- [Ownership and access](/walrus-memory/fundamentals/concepts/ownership-and-access)
- [Data flow and security model](/walrus-memory/fundamentals/architecture/data-flow-security-model)
- [How storage works](/walrus-memory/fundamentals/architecture/how-storage-works)
- [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop)