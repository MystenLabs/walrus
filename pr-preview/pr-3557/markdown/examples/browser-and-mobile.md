> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Client apps that run in a browser or on a mobile device cannot open the many connections a direct
store needs. A blob is split into slivers that go to every storage node, so an unprivileged client
would have to manage dozens of parallel uploads and collect a confirmation from each node. The two
building blocks that make client-side storage practical are an upload relay for writes and a
CDN-backed aggregator for reads. This page shows the full round trip with the Walrus TypeScript SDK,
and the points where a mobile app differs from a browser app.

For the upload path tradeoffs, see [Choose your upload path](/docs/getting-started#choose-your-upload-path).
For a deployed reference app, open [relay.wal.app](https://relay.wal.app) or read the full
[source code](https://github.com/MystenLabs/walrus-sdk-relay-example-app). For an annotated
walkthrough of that app, see [Walrus Relay](/docs/examples/walrus-relay).

## Why client apps use a relay

An upload relay accepts a single request, encodes the blob, distributes the slivers to the storage
nodes, and returns one certificate. Asset management onchain still happens on the client, so the
client keeps control of registration, payment, and certification while the relay absorbs the
network fan-out. Browsers and mobile devices benefit the most, since both have limited bandwidth and
cannot hold many outbound connections open. See the [upload relay](/docs/system-overview/relay)
overview for the design, and [Operate an Upload Relay](/docs/operator-guide/upload-relay) for the
endpoints and tip mechanism.

> **Tip**
>
> Mysten Labs runs public upload relays you can point a client at directly:
> 
> - **Testnet:** `https://upload-relay.testnet.walrus.space`
> - **Mainnet:** `https://upload-relay.mainnet.walrus.space`
> 
> These are also listed in the [Network Reference](/docs/network-reference#upload-relays).
## Configure the client

Create a `WalrusClient` with an `uploadRelay` configuration. The `host` points at the relay, and
`sendTip` sets the maximum tip your client is willing to pay a paid relay. A free relay reports
`no_tip` and the client pays only the onchain storage fee.

```ts
import { getFullnodeUrl, SuiClient } from "@mysten/sui/client";
import { WalrusClient } from "@mysten/walrus";

export const suiClient = new SuiClient({
  url: getFullnodeUrl("testnet"),
});

export const walrusClient = new WalrusClient({
  network: "testnet",
  suiClient,
  uploadRelay: {
    host: "https://upload-relay.testnet.walrus.space",
    sendTip: {
      max: 1_000,
    },
    timeout: 600_000,
  },
});
```

## Store a file through the relay

The SDK exposes the relay store as a `writeFilesFlow` with four steps. Each onchain step returns a
transaction for the connected wallet to sign, so the user keeps custody of the keys and pays the
fees. The flow is the same in a browser and in a mobile app.

```ts
import { WalrusFile } from "@mysten/walrus";

// 1. Wrap the file contents and start the flow.
const files = [
  WalrusFile.from({
    contents: new Uint8Array(await file.arrayBuffer()),
    identifier: file.name,
    tags: { contentType: file.type },
  }),
];

const flow = walrusClient.writeFilesFlow({ files });

// 2. Encode the blob locally.
await flow.encode();

// 3. Register the blob onchain. Sign and execute the returned transaction.
const registerTx = flow.register({
  epochs: 3,
  deletable: true,
  owner: account.address,
});
const { digest } = await signAndExecuteTransaction({ transaction: registerTx });

// 4. Upload the encoded data to the storage nodes through the relay.
await flow.upload({ digest });

// 5. Certify the blob onchain, then read back the stored file metadata.
const certifyTx = flow.certify();
await signAndExecuteTransaction({ transaction: certifyTx });

const storedFiles = await flow.listFiles();
// storedFiles[0] carries the blob ID and the Sui object ID you read back later.
```

After `listFiles` returns, confirm the blob is durable before you depend on it. See
[Verify blob availability before acting](/docs/walrus-client/verifying-availability).

## Browser and mobile differences

The flow above runs unchanged in any JavaScript runtime. Two parts of the surrounding app differ by
platform.

- **Reading file bytes.** A browser reads bytes from a `File` with `arrayBuffer`. A React Native app
  reads them from the device file system or image picker and converts the result to a `Uint8Array`
  before calling `WalrusFile.from`.
- **Signing transactions.** A browser app signs the `register` and `certify` transactions through a
  wallet connector. A mobile app signs through its wallet integration, such as a deep-linked wallet
  or an embedded signer. The transactions the flow produces are identical either way.

> **Caution**
>
> Blobs stored on Walrus are public and readable by anyone. Encrypt sensitive data before you store
> it, for example with [Seal](/docs/data-security). This applies equally to browser and mobile clients.
## Read through a CDN-backed aggregator

Reads are plain HTTPS GET requests, so a browser or mobile client reads a blob with `fetch` and no
SDK. Put a CDN in front of an aggregator so repeated reads of popular blobs are served from a nearby
edge cache, which matters most for mobile clients on slow or metered networks. Set the aggregator
base URL from an endpoint in the [Network Reference](/docs/network-reference#aggregators-and-publishers).

Read by blob ID:

```ts
const res = await fetch(`${AGGREGATOR}/v1/blobs/${blobId}`);
const bytes = new Uint8Array(await res.arrayBuffer());
```

Read by Sui object ID when you want the stored HTTP headers, such as `content-type`:

```ts
const res = await fetch(`${AGGREGATOR}/v1/blobs/by-object-id/${objectId}`);
```

> **Reading right after upload**
>
> A CDN-fronted aggregator might briefly serve a cached `404` from before the blob propagated to the
> edge. If your app just certified the blob, retry the read with backoff rather than treating the
> first `404` as missing. See [Reading blobs right after upload](/docs/troubleshooting/reading-blobs-after-upload).
## Related

- [Walrus Relay](/docs/examples/walrus-relay): annotated source for the deployed browser example.
- [Upload relay](/docs/system-overview/relay): how the relay batches, retries, and certifies.
- [Reading blobs over HTTP](/docs/http-api/reading-blobs): the full aggregator read API.
- [Verify blob availability before acting](/docs/walrus-client/verifying-availability): confirm
  durability before depending on a blob.