> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Memory runs on the [Cloudflare Workers](https://developers.cloudflare.com/workers/) runtime, but the edge environment differs from Node.js in a few ways that affect bundling and reliability. This guide covers the configuration and patterns that make it work cleanly.

## Required configuration

The SDK relies on the Node.js `crypto` built-in, and `@mysten/seal` and `@mysten/sui` pull in more Node APIs. Enable the Node.js compatibility flag in your `wrangler.toml`:

```toml wrangler.toml
compatibility_flags = ["nodejs_compat"]
```

Without `nodejs_compat` the build fails outright: `wrangler deploy` stops with `Could not resolve "crypto"` because the SDK calls `await import("crypto")` internally. Adding the flag resolves it.

## Bundle size

Even the default `MemWal` client requires `@mysten/seal` and `@mysten/sui` as peer dependencies (it builds a Seal session key on the client to authenticate each request), so it pulls in a sizeable dependency graph. A Worker bundling just the default client comes out to roughly **1.2 MB raw (around 225 KB gzipped)**, measured with `wrangler deploy --dry-run` against `@mysten/sui` 2.x. That is comfortably within the Workers size limit, but worth knowing:

- A single incompatible peer version can break the build for the **entire** Worker, not just the memory feature. Pin your `@mysten/*` versions and treat the memory dependency as a unit.
- The default `@mysten-incubation/memwal` entry point is the lightest to bundle. The relayer handles embeddings and Walrus storage server-side, so the client only needs `@mysten/seal` and `@mysten/sui` (both required peers). The `@mysten-incubation/memwal/manual` entry point additionally pulls in `@mysten/walrus` and client-side encryption and upload, adding more to the bundle. Prefer the default entry point on Workers unless you specifically need the manual flow.

## Crash isolation with dynamic import

To keep a Walrus Memory bundling or runtime issue from taking down the rest of your agent, load it behind a defensive dynamic import and feature-flag it off when config is missing or the client fails to come up. This way the app keeps serving requests even if memory is unavailable for a cycle:

```ts
let memwal: any = null;

try {
  const mod = await import("@mysten-incubation/memwal");
  memwal = mod.MemWal.create({ key, accountId, serverUrl, namespace });
  await memwal.health();
} catch (e) {
  console.log("MemWal unavailable, degrading gracefully:", e);
  memwal = null; // app keeps running without memory this cycle
}
```

Guard your read and write paths on `memwal` being non-null, and the rest of the Worker stays healthy regardless of the memory layer's state.

> **Note**
>
> The dynamic `import()` also keeps the heavy dependency graph out of your Worker's cold-start critical path until memory is actually needed.
## Next steps

Continue with the client reference and relayer options used in the examples above:

- [Walrus Memory client](/walrus-memory/sdk/usage/memwal): the default relayer-backed client used above
- [Public relayer](/walrus-memory/relayer/public-relayer): managed Mainnet and Testnet relayer endpoints
- [API Reference](/walrus-memory/sdk/api-reference): full method signatures and config fields