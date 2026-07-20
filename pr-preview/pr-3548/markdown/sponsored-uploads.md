> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Most users do not hold SUI or WAL, and most apps do not want to ask them to. To build an app where your backend pays for Walrus storage and users upload without managing a wallet, you need to choose among several patterns. The sections below compare those patterns, show a reference architecture, and document the operational pitfalls teams hit when a single wallet funds many uploads.

For the step-by-step operator instructions, see [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher), [Use the Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher), and [Operate an Upload Relay](/docs/operator-guide/upload-relay).

## Sponsored compared with walletless

These two properties are independent, and most apps want both:

- **Sponsored:** Your app pays the SUI and WAL cost of a store, not the user. You sponsor by operating a funded wallet that signs and pays for the store.
- **Walletless:** The user never creates or connects a Sui wallet. Your app authenticates the user through its own mechanism, such as a username and password, and does the onchain work for them.

Walrus has no built-in gas-sponsorship primitive that lets a user sign a store while a separate sponsor pays the gas. Sponsorship in Walrus means your backend holds and funds the wallet that performs the store. To make the user the owner of the stored blob, your backend stores the blob, then sends the resulting `Blob` object to the user's address.

## What does storage cost?

Every store spends real resources, so sponsorship always means one of your wallets pays for the following onchain steps:

1. **Reserve storage:** The store reserves storage space for a number of epochs, paid in WAL.
2. **Register the blob:** The store registers the blob ID onchain. This Sui transaction costs SUI gas.
3. **Certify the blob:** After the slivers reach a quorum of storage nodes, the store certifies the blob onchain. This second Sui transaction costs SUI gas.

There is no free storage tier. Sponsorship changes only who pays, not whether WAL and SUI are spent. Keep both a WAL balance and a SUI balance funded on whichever wallet performs the store. Where a network configures an onchain storage subsidy, the subsidy can offset part of the WAL cost, but it does not change who signs the store or who pays the SUI gas.

For the full onchain lifecycle, see [Operations](/docs/system-overview/operations).

## Choose a pattern

| Pattern | Who signs onchain | Who pays | Walletless for the user | Best for |
| --- | --- | --- | --- | --- |
| Backend publisher | Publisher wallet | App | Yes | Server backends that accept raw bytes over HTTP and pay for storage |
| Upload relay | Client (or the app's signer) | Client, app, or relay policy | Only if the app manages signing | Browser and mobile clients that cannot open many node connections |
| Direct SDK | The signer your code configures | App wallet or signer | Yes, if the backend holds the signer | Apps that integrate Walrus directly and control signing in code |

### Backend publisher

A [publisher](/docs/operator-guide/publishers/operating-publisher) is an HTTP service that accepts raw blob bytes and performs the entire store with its own funded wallet. This is the most direct sponsored and walletless pattern: the user's device never touches Sui.

The publisher exposes `PUT /v1/blobs` with query parameters that control the resulting blob, including `epochs`, `permanent`, `deletable`, and `send_object_to`. To hand ownership to a walletless user, pass their Sui address as `send_object_to`. The publisher then stores the blob and sends the resulting `Blob` object to that address. See [Storing blobs with the HTTP API](/docs/http-api/storing-blobs).

On Mainnet, never run an open, unauthenticated publisher, because anyone who reaches it can spend your wallet's SUI and WAL. Put an authentication and cost-control boundary in front of it. The [Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher) verifies a JWT on each request and can cap the epochs and blob size a token is allowed to store. The [Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide) covers running one safely.

### Upload relay

An [upload relay](/docs/operator-guide/upload-relay) lets a client avoid opening a connection to every storage node, which makes it the right fit for browsers and low-powered devices. The relay distributes slivers and returns a confirmation certificate, but it does not perform onchain operations: the client still registers the blob before the upload and certifies it after.

Because the client does the onchain work, the upload relay is walletless only when your app manages the signing and funding for the user, for example through a backend signer. A paid relay also requires a tip, which the client's payment transaction provides. See the [tip mechanism](/docs/system-overview/relay#tip-mechanism) for how to configure and pay the tip.

### Direct SDK

If your backend already runs application code, you can integrate a Walrus SDK and keep the funded signer in your backend. This gives you the most control over batching, retries, and error handling, but you implement the flow yourself. See [Software Development Kits (SDKs) and Other Tools](/docs/typescript-sdk/sdks).

## Reference architecture: app backend sponsors storage

A common production pattern puts an authenticated backend in front of a funded publisher:

1. The client authenticates to your backend with your own credentials, not a wallet.
2. Your backend applies its policy, such as per-user quotas, allowed blob sizes, and storage duration, then forwards the bytes to your publisher.
3. The publisher stores the blob with its funded wallet and, if you pass `send_object_to`, sends the resulting `Blob` object to the user's address.
4. Your backend returns the blob ID and any object ID to the client.

Teams commonly run the publisher on a cloud host behind an edge or gateway layer that terminates TLS, authenticates requests, and rate-limits abusive callers before any request reaches the funded wallet. The authenticated publisher's JWT flow fits directly into this boundary.

## Own and hand off the blob

Decide up front who owns the stored blob and for how long:

- **Ownership:** If you do not specify a destination, the publisher keeps the resulting `Blob` object, which returns to the operator's main wallet. Pass `send_object_to=<address>` to transfer ownership to a walletless user's address at store time, or `share=true` to wrap it as a shared blob.
- **Deletable compared with permanent:** Newly stored blobs are deletable by default. The owner can delete a deletable blob to reclaim storage; a permanent blob cannot. Choose with `deletable=true` or `permanent=true`. See [Deletable and permanent blobs](/docs/http-api/storing-blobs#deletable-and-permanent-blobs).
- **Renewal:** Storage lasts for the epochs you paid for. Decide whether your backend renews blobs before they expire, or whether that responsibility passes to the user along with the object.

## Operational pitfalls

The issues below are the ones teams most often hit when one wallet funds many uploads.

### Do not sign many concurrent stores from one wallet

A single Sui address cannot safely sign many transactions at once, because concurrent transactions contend for the same owned gas and storage objects and can lock or equivocate them. The publisher solves this by creating internal **sub-wallets** funded from the main wallet, so parallel requests each use a distinct address. By default it creates 8 sub-wallets; tune this with `--n-clients` and persist them with `--sub-wallets-dir`. See [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher).

If you build your own uploader instead of using the publisher, replicate this: give each concurrent worker its own funded address rather than sharing one.

### Keep resource reuse disabled when a wallet is shared

The publisher store API accepts a `reuse_resources` parameter that defaults to `false`. When it is `false`, each store registers fresh storage and blob resources. When it is `true`, the store tries to reuse storage or blob objects the wallet already owns.

Reusing owned resources saves gas for a single-wallet workflow. But when concurrent stores share a funding wallet, reuse lets two in-flight uploads contend for the same owned objects, which can leave a blob registered but never certified. If you see stores that hang in a registered-but-not-certified state on a shared wallet, keep `reuse_resources` at its default of `false` so every upload registers its own resources.

### Keep the funding wallets topped up

A store fails if the signing wallet runs out of SUI or WAL. When you run a publisher, the main wallet automatically refills its sub-wallets, but the main wallet still needs replenishing. Monitor both the SUI and WAL balances and alert before they run dry. Size your WAL balance for the epochs and volume you store, and your SUI balance for the two transactions each store performs.

## References

- [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher)
- [Use the Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher)
- [Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide)
- [Operate an Upload Relay](/docs/operator-guide/upload-relay)
- [Storing blobs with the HTTP API](/docs/http-api/storing-blobs)
- [Choose your upload path](/docs/getting-started#choose-your-upload-path)