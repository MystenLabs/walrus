> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Official SDKs from Mysten Labs, community-maintained SDKs, and community tools cover the main ways to build on Walrus: the TypeScript SDK for full client-side control, HTTP-based SDKs in other languages, and explorers for inspecting blobs and operators.

## SDKs maintained by Mysten Labs

Mysten Labs has built and published a [Walrus TypeScript SDK](https://sdk.mystenlabs.com/walrus), which supports a wide variety of operations. See also the related [examples](https://github.com/MystenLabs/ts-sdks/tree/main/packages/walrus/examples).

Install the SDK together with the Sui TypeScript SDK from npm:

```sh
$ npm install @mysten/walrus @mysten/sui
```

Create a `WalrusClient` by selecting a network and passing a `SuiClient`:

```ts
import { getFullnodeUrl, SuiClient } from '@mysten/sui/client';
import { WalrusClient } from '@mysten/walrus';

const suiClient = new SuiClient({
  url: getFullnodeUrl('mainnet'),
});

const walrusClient = new WalrusClient({
  network: 'mainnet',
  suiClient,
});
```

The SDK bundles the package and object IDs for each network, so selecting a network applies the correct values automatically. To configure a custom or pinned deployment, pass the system and staking object IDs from the [Network Reference](/docs/network-reference#system-and-staking-object-ids). For a complete browser upload flow through an upload relay, see the [browser and mobile apps example](/docs/examples/browser-and-mobile), and for SDK error-handling patterns, see [error handling](/docs/troubleshooting/error-handling).

The Walrus repository also contains a Rust SDK (the [`walrus-sdk` crate](https://github.com/MystenLabs/walrus/tree/main/crates/walrus-sdk)), which the Walrus CLI itself builds on. The Walrus core team continues to develop it.

## Community-maintained SDKs

Besides these official SDKs, community teams maintain third-party SDKs that interact with the [HTTP API](/docs/http-api/storing-blobs) exposed by Walrus aggregators and publishers:

- [Walrus Go SDK](https://github.com/namihq/walrus-go) (maintained by the Nami Cloud team)

- [Walrus PHP SDK](https://github.com/suicore/walrus-sdk-php) (maintained by the Suicore team)

- [Walrus Python SDK](https://github.com/standard-crypto/walrus-python) (maintained by the Standard Crypto team)

Mysten Labs does not maintain these SDKs, so evaluate them before depending on them in production.

## Explorers

The [Walruscan](https://walruscan.com/) blob explorer, built and maintained by the Staketab team, supports exploring blobs, blob events, operators, and more. It also supports staking operations.

See the [Awesome Walrus repository](https://github.com/MystenLabs/awesome-walrus?tab=readme-ov-file#visualization) for more visualization tools.

## Other tools

The community builds many other tools for visualization, monitoring, and more. For a full list, see the [Awesome Walrus repository](https://github.com/MystenLabs/awesome-walrus).