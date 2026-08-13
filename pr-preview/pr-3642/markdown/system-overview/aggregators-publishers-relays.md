> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus exposes three HTTP services. Callers send requests to the wrong one more often than they make any other endpoint mistake. An aggregator reads. A publisher and an upload relay both write, but they differ in who holds the wallet and who pays.

## Compare the three services

| **Service** | **Direction** | **Who pays for storage** | **Who owns the blob** | **Caller needs a wallet** |
| --- | --- | --- | --- | --- |
| Aggregator | Read | Nobody, reads cost nothing | Not applicable | No |
| Publisher | Write | The publisher, from its own wallet | The publisher, unless the caller passes `send_object_to` | No |
| Upload relay | Write | The client | The client, always | Yes |

The wallet column decides most integrations. A caller that cannot hold SUI and WAL needs a publisher. A caller that holds a wallet but cannot open hundreds of connections, such as a browser, needs a relay.

## Aggregator

An aggregator serves stored blobs over HTTP. It performs no Sui onchain actions and therefore consumes no gas, which makes it the cheapest component to run and the safest to expose publicly.

Read a blob by blob ID at `/v1/blobs/<blobId>`, or by Sui object ID at `/v1/blobs/by-object-id/<objectId>`. Reading by object ID also returns the stored attributes as HTTP headers, so prefer it when the content type matters. See [Reading Blobs](/docs/http-api/reading-blobs).

## Publisher

A publisher accepts a blob over HTTP PUT at `/v1/blobs` and does the onchain work itself: it registers the blob, certifies it, and pays the SUI gas and WAL storage cost from its own wallet. The caller sends bytes and needs no wallet at all.

That convenience is also the operational cost. Because the publisher pays, running one publicly on Mainnet means funding strangers' uploads, so Walrus runs no public unauthenticated publisher there. On Mainnet, run your own and authenticate it. See [Operate a Publisher](/docs/operator-guide/publishers/operating-publisher) and [Use the Authenticated Publisher](/docs/operator-guide/publishers/auth-publisher).

Pass `send_object_to` with a Sui address to transfer the created `Blob` object to the caller instead of leaving it with the publisher.

## Upload relay

An upload relay solves a different problem: a browser or mobile client cannot open enough connections to send slivers to every shard. The client sends the blob and a tip to the relay, the relay encodes the blob and distributes the slivers to storage nodes, collects the confirmations, and returns a certificate.

The client then registers, certifies, and pays on Sui itself, and keeps ownership of the blob throughout. A relay never pays for storage, which is why it can run as a public service where a publisher cannot.

A relay exposes its tip configuration at `/v1/tip-config` and accepts blobs at `/v1/blob-upload-relay`. See [Operate an Upload Relay](/docs/operator-guide/upload-relay).

## Pick a service

Match the constraint you actually have:

- Your caller reads blobs: use an aggregator.
- Your caller cannot hold SUI or WAL: use a publisher, and on Mainnet run and authenticate your own.
- Your caller holds a wallet but runs in a browser or on mobile: use an upload relay.
- Your caller runs server-side and holds a funded wallet: skip all three and write through the [TypeScript SDK](/docs/typescript-sdk/sdks), which talks to storage nodes directly.

For the patterns that combine these when your app pays on behalf of its users, see [Sponsored and Walletless Uploads](/docs/sponsored-uploads).

## Endpoints

The [Network Reference](/docs/network-reference#aggregators-and-publishers) maintains the current Mainnet and Testnet endpoints for all three services, along with the community-operated list. Read the values there rather than copying them, because operators change.

Most public aggregators and publishers limit requests to 10 MiB. To move larger blobs, run your own or use the CLI. See [Large Uploads](/docs/large-uploads).

## References

- [Network Reference](/docs/network-reference)
- [Reading Blobs](/docs/http-api/reading-blobs)
- [Storing Blobs](/docs/http-api/storing-blobs)
- [Sponsored and Walletless Uploads](/docs/sponsored-uploads)
- [Public Aggregators and Publishers](/docs/system-overview/public-aggregators-and-publishers)