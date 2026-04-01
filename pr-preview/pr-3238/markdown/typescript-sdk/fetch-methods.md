By default, the Walrus TypeScript SDK uses the global `fetch` for your environment's default runtime. This imposes no concurrency limits and uses default timeouts and behavior defined by your runtime.

To customize how the SDK makes requests, provide a custom `fetch` method through `storageNodeClientOptions`:

```ts

const client = new SuiGrpcClient({
	network: 'testnet',
	baseUrl: 'https://fullnode.testnet.sui.io:443',
}).$extend(
	walrus({
		storageNodeClientOptions: {
			timeout: 60_000,
			fetch: (url, init) => {
				// Some casting may be required because undici types may not exactly match the @node/types types
				return fetch(url as RequestInfo, {
					...(init as RequestInit),
					dispatcher: new Agent({
						connectTimeout: 60_000,
					}),
				}) as unknown as Promise<Response>;
			},
		},
	}),
);
```