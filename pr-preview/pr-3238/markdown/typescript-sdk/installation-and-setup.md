The [Walrus TypeScript SDK](https://sdk.mystenlabs.com/walrus) is built and maintained by Mysten Labs as an extension of the Sui TypeScript SDK. It works directly with the Walrus network's storage nodes or the Walrus upload relay. Without an upload relay, reading and writing blobs require around 2200 and 335 requests, respectively. The upload relay significantly reduces the number of requests required to write a blob, though reading still requires a significant number of requests.

For many use cases, publishers and aggregators are the better choice. The Walrus TypeScript SDK is useful when users must pay for their own storage directly, or when the application needs to interface with the Walrus network without an intermediary.

## Create a client

To use Walrus TypeScript SDK, create a `Client` from Sui TypeScript SDK and extend it with the Walrus SDK:

```typescript

const client = new SuiGrpcClient({
	network: 'testnet',
	baseUrl: 'https://fullnode.testnet.sui.io:443',
}).$extend(walrus());
```

## Connect to a different network

Walrus TypeScript SDK includes the package and object IDs for connecting to the Walrus Testnet. To connect to Mainnet or another network, manually configure the SDK with a different set of package and object IDs:

```typescript

const client = new SuiGrpcClient({
	network: 'testnet',
	baseUrl: 'https://fullnode.testnet.sui.io:443',
}).$extend(
	walrus({
		packageConfig: {
			systemObjectId: '0x98ebc47370603fe81d9e15491b2f1443d619d1dab720d586e429ed233e1255c1',
			stakingPoolId: '0x20266a17b4f1a216727f3eef5772f8d486a9e3b5e319af80a5b75809c035561d',
		},
	}),
);
```

## Customize fetch behavior

Depending on your environment, you might want to customize how data is fetched, for example by setting custom timeouts, rate limits, or retry logic:

```typescript

const client = new SuiGrpcClient({
	network: 'testnet',
	baseUrl: 'https://fullnode.testnet.sui.io:443',
}).$extend(
	walrus({
		storageNodeClientOptions: {
			fetch: (url, options) => {
				console.log('fetching', url);
				return fetch(url, options);
			},
			timeout: 60_000,
		},
	}),
);
```