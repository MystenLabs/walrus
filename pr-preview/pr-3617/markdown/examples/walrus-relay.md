> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

The Walrus upload relay lets browser apps store blobs without opening a connection to every storage
node: the client sends a single request to the relay, and the relay encodes the blob, distributes
the slivers to the storage node committee, and returns an availability certificate. The following
example combines the relay with the Walrus TypeScript SDK into a complete React web application:
users connect a Sui wallet, upload files through the public Testnet upload relay, and see each
uploaded blob rendered on the page.

See also:

- [Deployed instance](https://relay.wal.app): try the app in your browser.
- [Source code on GitHub](https://github.com/MystenLabs/walrus-sdk-relay-example-app): the complete
  application.
- [Browser and Mobile Apps](/docs/examples/browser-and-mobile): the same store flow as standalone
  SDK code, with the differences for mobile clients.
- [Upload Relay](/docs/system-overview/relay): when to use a relay and how it batches, retries, and
  certifies uploads.
- [Operate an Upload Relay](/docs/operator-guide/upload-relay): the relay's endpoints, tip
  mechanism, and configuration.

## Prerequisites

- Node.js 18 or later and the `pnpm` package manager.
- A Sui browser wallet with Testnet SUI and WAL. The wallet signs the transactions that register
  and certify each blob and pay for its storage.
- No API key. The app points at the public Testnet upload relay that Mysten Labs runs at
  `https://upload-relay.testnet.walrus.space`, listed in the
  [Network Reference](/docs/network-reference#upload-relays).

## Run the app locally

1. Clone the repository and install the dependencies:

   ```sh
   git clone https://github.com/MystenLabs/walrus-sdk-relay-example-app.git
   cd walrus-sdk-relay-example-app
   pnpm install
   ```

2. Start the development server:

   ```sh
   pnpm dev
   ```

3. Open `http://localhost:5173` in your browser, connect your wallet, and upload a file.

## Key files

Three files carry the Walrus-specific logic. The rest of the repository is standard React
scaffolding: components, hooks, and styling.

### Walrus client

`src/lib/walrus.ts` creates a `SuiClient` for the Testnet full node and a `WalrusClient` with an
`uploadRelay` configuration. The `host` points at the relay, `sendTip.max` caps the tip (in MIST)
the client agrees to pay a paid relay, and the generous `timeout` accommodates large uploads. A
relay advertises its required tip through its `/v1/tip-config` endpoint, and a free relay reports
`no_tip`.

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-sdk-relay-example-app/blob/main/src/lib/walrus.ts -->

To target Mainnet instead, set `network: "mainnet"`, create the `SuiClient` for the Mainnet full
node, and point `uploadRelay.host` at `https://upload-relay.mainnet.walrus.space`.

### Network configuration

`src/networkConfig.ts` registers the Testnet and Mainnet full node URLs with dapp-kit's
`createNetworkConfig`, which backs the wallet connection UI:

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-sdk-relay-example-app/blob/main/src/networkConfig.ts -->

### Application UI

`src/App.tsx` renders the upload page: a `FileUpload` component that drives the store flow and a
gallery of the blobs uploaded in the current session. The `handleUploadComplete` callback prepends
each newly stored blob to the list:

<!-- ImportContent: GitHub source — resolve at export time or visit https://github.com/MystenLabs/walrus-sdk-relay-example-app/blob/main/src/App.tsx -->

## Upload flow

The `useWalrusUpload` hook in
[`src/hooks/useWalrusUpload.ts`](https://github.com/MystenLabs/walrus-sdk-relay-example-app/blob/main/src/hooks/useWalrusUpload.ts)
orchestrates the store through the SDK's `writeFilesFlow` in four steps:

1. **Encode:** Wrap the selected file with `WalrusFile.from` and call `flow.encode()`, which
   computes the blob metadata locally in the browser.
2. **Register:** `flow.register()` returns a transaction that registers the blob onchain with its
   storage parameters and sends the relay tip. The connected wallet signs and executes it.
3. **Upload:** `flow.upload({ digest })` sends the file data to the relay in a single request. The
   relay distributes the slivers to the storage nodes and collects a confirmation certificate.
4. **Certify:** `flow.certify()` returns a transaction that certifies the blob on Sui. After the
   wallet executes it, `flow.listFiles()` returns the blob ID and Sui object ID of the stored file.

Because the user's own wallet registers, pays for, and certifies the blob, the user keeps ownership
of the resulting Sui object; the relay only fans out the data. For the same flow as copy-pasteable
standalone code, see
[Browser and Mobile Apps](/docs/examples/browser-and-mobile#store-a-file-through-the-relay).

## Troubleshooting

- **The SDK fails to load in your own Vite app:** Exclude the SDK's WASM package from Vite's
  dependency optimization in `vite.config.ts`:

  ```ts
  optimizeDeps: {
    exclude: ["@mysten/walrus-wasm"],
  },
  ```

- **The relay rejects an upload:** A paid relay only accepts uploads whose registration transaction
  includes the advertised tip. Make sure the client's `sendTip.max` covers the tip that the relay
  reports through `/v1/tip-config`.
- **`Cannot read properties of undefined (reading 'buffer')`:** `WalrusFile.from` expects a
  `Uint8Array` for `contents`, not a `File`, `Blob`, or raw `ArrayBuffer`. Read the bytes first
  with `new Uint8Array(await file.arrayBuffer())`.