> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Agents built on EVM, Base, Virtuals, or any other stack can use Walrus Memory. The runtime your agent executes in and the chain it transacts on are independent of where its memory lives.

Walrus Memory stores memories as encrypted blobs on Walrus and records ownership in a Sui account. Your agent authenticates with an Ed25519 delegate key that the account owner registers on that account. An agent that also signs Ethereum transactions, holds an ERC-20 balance, or runs inside a Virtuals runtime uses Walrus Memory the same way as any other agent.

## Requirements

Your agent needs two things, whatever its language or chain:

1. **An Ed25519 delegate key registered on a Walrus Memory account.** The account owner registers the public key onchain, which grants the agent permission to read and write that account's memory. See [Ownership and Delegates](/walrus-memory/fundamentals/concepts/ownership-and-access).
2. **Network access to a relayer.** The relayer performs the encryption, storage, and indexing work, so your agent needs no Sui node, Walrus node, or WAL tokens.

Your agent needs no Sui wallet of its own, no bridge, no wrapped token, and no change to how it transacts on its own chain.

## Use an SDK

For agents in TypeScript or Python, the SDK signs each request, builds and caches the Seal session key, and polls asynchronous jobs for you:

[Source: guides/agent-runtimes.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/agent-runtimes.md)

```ts
import { MemWal } from "@mysten-incubation/memwal";

const memwal = MemWal.create({
  key: process.env.MEMWAL_PRIVATE_KEY,
  accountId: process.env.MEMWAL_ACCOUNT_ID,
  // Mainnet relayer. Use https://relayer-staging.memory.walrus.xyz for Testnet.
  serverUrl: "https://relayer.memory.walrus.xyz",
  namespace: "agent-memory",
});

await memwal.health();
await memwal.remember("The user prefers dark mode.");
const hits = await memwal.recall({ query: "user preferences", limit: 5 });
```

To load credentials from the environment, validate connectivity at boot, and handle credential errors, see [Headless SDK Setup](/walrus-memory/sdk/headless-setup). For the write-confirm-recall cycle, see the [Agent Storage Loop](/walrus-memory/sdk/agent-storage-loop).

## Sign requests directly

For agents in a language with no SDK, such as Go, Rust, or Elixir, call the relayer API directly. Every authenticated route takes the same signed headers, so your agent needs an Ed25519 signer, SHA-256, and an HTTP client.

Build this canonical message and sign it:

[Source: guides/agent-runtimes.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/agent-runtimes.md)

```text
{timestamp}.{method}.{path_and_query}.{body_sha256}.{nonce}.{account_id}
```

Send the result as hex in these headers:

| **Header** | **Value** |
|---|---|
| `x-public-key` | Hex-encoded Ed25519 public key, 32 bytes |
| `x-signature` | Hex-encoded Ed25519 signature of the message above, 64 bytes |
| `x-timestamp` | Unix timestamp in seconds, valid for five minutes |
| `x-nonce` | UUID v4, which the relayer records for replay protection |
| `x-account-id` | The account object ID. Official SDKs always send it and include it in the signed message |

The relayer verifies the signature, then resolves the owner by looking up your public key in the account's onchain delegate keys. For every route, request shape, and response shape, see the [Relayer API Reference](/walrus-memory/relayer/api-reference).

### Reads need a Seal credential too

Signed headers authenticate the caller, but they do not decrypt anything. The routes that return stored memories, `/api/recall`, `/api/ask`, and `/api/restore`, also need a Seal credential, and the relayer rejects the call without one:

[Source: guides/agent-runtimes.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/agent-runtimes.md)

```text
SEAL credential required (x-seal-session, x-delegate-key, or SERVER_SUI_PRIVATE_KEY)
```

Supply it one of three ways:

| **Credential** | **How it works** |
|---|---|
| `x-seal-session` | A base64 exported Seal `SessionKey`. The official SDKs build, cache, and send this, and it is the path to follow for new clients |
| `x-delegate-key` | The legacy delegate private key header. Deprecated, and it hands the relayer your key |
| Server fallback | A relayer configured with `SERVER_SUI_PRIVATE_KEY` decrypts on its own key, so the client sends neither header |

A client that sends only the five signed headers authenticates successfully and then fails to decrypt, which is the failure to expect if recall returns nothing usable. Building a `SessionKey` outside the SDK means implementing the Seal client flow yourself, so plan for that work before choosing the direct path for reads. Writes through `/api/remember` do not need it.

Confirm the agent reaches the relayer before you debug signing, using the unauthenticated health route:

[Source: guides/agent-runtimes.md](https://github.com/MystenLabs/MemWal/blob/dev/docs/guides/agent-runtimes.md)

```sh
$ curl -sS "$MEMWAL_RELAYER_URL/health"
```

## Limits of cross-chain support

Walrus Memory does not do the following. A design that assumes otherwise does not work:

1. Walrus Memory never mirrors, wraps, or relays a memory onto another chain. Ownership records live on Sui.
2. An Ethereum address cannot own a memory account. The delegate key is Ed25519, and the account is a Sui object.
3. Walrus charges storage in WAL. Your agent neither holds nor spends it when a relayer fronts that cost.

To link an EVM contract to a memory account, model the mapping in your own application: store it in your contract or database, and have your agent present the matching delegate key. For how the embed, store, and recall loop works underneath, see [How AI Agent Memory Works](/walrus-memory/fundamentals/concepts/how-agent-memory-works).