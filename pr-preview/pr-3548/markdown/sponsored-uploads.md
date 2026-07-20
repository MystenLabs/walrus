> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Most end users do not hold SUI or WAL, and most apps do not want to ask them to. This page describes how to build an app where your backend pays for Walrus storage, and where users upload without managing a wallet themselves. It compares the available patterns, shows a reference architecture, and documents the operational pitfalls that teams hit when a single wallet funds many uploads.

This is an architecture guide. For the step-by-step operator instructions it links to, see [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher), [Use the Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher), and [Operate an Upload Relay](/docs/operator-guide/upload-relay).

## Sponsored compared with walletless

These two properties are independent, and most apps want both:

- **Sponsored** means your app pays the SUI and WAL cost of a store, rather than the end user. You sponsor by operating a funded wallet that signs and pays for the store.
- **Walletless** means the end user never creates or connects a Sui wallet. Your app authenticates the user through its own mechanism, such as a username and password, and performs the onchain work for them.

Walrus does not have a built-in gas-sponsorship primitive that lets an end user sign a store while a separate sponsor pays the gas. Sponsorship in Walrus means your backend holds and funds the wallet that performs the store. When you want the user to end up owning the stored blob, the backend stores the blob and then sends the resulting `Blob` object to the user's address.

## Understand what a store costs

Every store spends real resources, so sponsorship always means one of your wallets pays for the following onchain steps:

1. **Reserve storage.** The store reserves storage space for a number of epochs. This is paid in WAL.
2. **Register the blob.** The store registers the blob ID onchain. This is a Sui transaction and costs SUI gas.
3. **Certify the blob.** After the slivers reach a quorum of storage nodes, the store certifies the blob onchain. This is a second Sui transaction and costs SUI gas.

There is no free storage tier. "Sponsored" only changes who pays; it does not remove the WAL and SUI cost. Keep both a WAL balance and a SUI balance funded on whichever wallet performs the store. Where a network configures an onchain storage subsidy, it can offset part of the WAL cost, but it does not change who signs the store or who pays the SUI gas.

For the full onchain lifecycle, see [Operations](/docs/system-overview/operations).

## Choose a pattern

| Pattern | Who signs onchain | Who pays | Walletless for the user | Best for |
| --- | --- | --- | --- | --- |
| Backend publisher | Publisher wallet | App | Yes | Server backends that accept raw bytes over HTTP and pay for storage |
| Upload relay | Client (or the app's signer) | Client, app, or relay policy | Only if the app manages signing | Browser and mobile clients that cannot open many node connections |
| Direct SDK | The signer your code configures | App wallet or signer | Yes, if the backend holds the signer | Apps that integrate Walrus directly and control signing in code |

### Backend publisher

A [publisher](/docs/operator-guide/publishers/operating-publisher) is an HTTP service that accepts raw blob bytes and performs the entire store with its own funded wallet. This is the most direct sponsored and walletless pattern: the user's device never touches Sui.

The publisher exposes `PUT /v1/blobs` with query parameters that control the resulting blob, including `epochs`, `permanent`, `deletable`, and `send_object_to`. To hand ownership to a walletless user, pass their Sui address as `send_object_to`; the publisher stores the blob and sends the resulting `Blob` object to that address. See [Storing blobs with the HTTP API](/docs/http-api/storing-blobs).

On Mainnet, never run an open, unauthenticated publisher, because anyone who reaches it can spend your wallet's SUI and WAL. Put an authentication and cost-control boundary in front of it. The [Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher) verifies a JWT on each request and can cap the epochs and blob size a token is allowed to store, and the [Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide) covers running one safely.

### Upload relay

An [upload relay](/docs/operator-guide/upload-relay) removes the need for a client to open a connection to every storage node, which makes it the right fit for browsers and low-powered devices. The relay distributes slivers and returns a confirmation certificate, but it does not perform onchain operations: the client still registers the blob before the upload and certifies it after.

Because the client does the onchain work, the upload relay is walletless only when your app manages the signing and funding for the user, for example through a backend signer. A paid relay also requires a tip, which the client's payment transaction provides. See the [tip mechanism](/docs/system-overview/relay#tip-mechanism) for how the tip is configured and paid.

### Direct SDK

If your backend already runs application code, you can integrate a Walrus SDK and keep the funded signer in your backend. This gives you the most control over batching, retries, and error handling, at the cost of implementing the flow yourself. See [Software Development Kits (SDKs) and Other Tools](/docs/typescript-sdk/sdks).

## Reference architecture: app backend sponsors storage

A common production shape puts an authenticated backend in front of a funded publisher:

1. The client authenticates to your backend with your own credentials, not a wallet.
2. Your backend applies its policy, such as per-user quotas, allowed blob sizes, and storage duration, then forwards the bytes to your publisher.
3. The publisher stores the blob with its funded wallet and, if you pass `send_object_to`, sends the resulting `Blob` object to the user's address.
4. Your backend returns the blob ID, and any object ID, to the client.

Teams commonly run the publisher on a cloud host behind an edge or gateway layer that terminates TLS, authenticates requests, and rate-limits abusive callers before any request reaches the funded wallet. The authenticated publisher's JWT flow fits directly into this boundary.

## Own and hand off the blob

Decide up front who owns the stored blob and for how long:

- **Ownership.** If you do not specify a destination, the publisher keeps the resulting `Blob` object, which returns to the operator's main wallet. Pass `send_object_to=<address>` to transfer ownership to a walletless user's address at store time, or `share=true` to wrap it as a shared blob.
- **Deletable compared with permanent.** Newly stored blobs are deletable by default. A deletable blob can be deleted by its owner to reclaim storage; a permanent blob cannot. Choose with `deletable=true` or `permanent=true`. See [Deletable and permanent blobs](/docs/http-api/storing-blobs#deletable-and-permanent-blobs).
- **Renewal.** Storage lasts for the epochs you paid for. Decide whether your backend renews blobs before they expire, or whether that responsibility passes to the user along with the object.

## Operational pitfalls

The issues below are the ones teams most often hit when one wallet funds many uploads.

### Do not sign many concurrent stores from one wallet

A single Sui address cannot safely sign many transactions at once, because concurrent transactions contend for the same owned gas and storage objects and can lock or equivocate them. The publisher solves this by creating internal **sub-wallets** funded from the main wallet, so parallel requests each use a distinct address. By default it creates 8 sub-wallets; tune this with `--n-clients` and persist them with `--sub-wallets-dir`. See [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher).

If you build your own uploader instead of using the publisher, replicate this: give each concurrent worker its own funded address rather than sharing one.

### Keep resource reuse disabled when a wallet is shared

The publisher store API accepts a `reuse_resources` parameter that defaults to `false`. When it is `false`, each store registers fresh storage and blob resources. When it is `true`, the store tries to reuse storage or blob objects the wallet already owns.

Reusing owned resources saves gas for a single-wallet workflow, but under concurrent stores that share a funding wallet it lets two in-flight uploads contend for the same owned objects, which can leave a blob registered but never certified. If you see stores that hang in a registered-but-not-certified state on a shared wallet, keep `reuse_resources` at its default of `false` so every upload registers its own resources.

### Keep the funding wallets topped up

A store fails if the signing wallet runs out of SUI or WAL. When you run a publisher, the main wallet automatically refills its sub-wallets, but the main wallet still needs replenishing, so monitor both the SUI and WAL balances and alert before they run dry. Size your WAL balance for the epochs and volume you store, and your SUI balance for the two transactions each store performs.

## Related pages

- [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher)
- [Use the Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher)
- [Mainnet Publisher Production Guide](/docs/operator-guide/publishers/mainnet-production-guide)
- [Operate an Upload Relay](/docs/operator-guide/upload-relay)
- [Storing blobs with the HTTP API](/docs/http-api/storing-blobs)
- [Choose your upload path](/docs/getting-started#choose-your-upload-path)