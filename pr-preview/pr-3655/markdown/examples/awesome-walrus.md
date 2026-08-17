> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus is a storage layer, and the products in this section show what teams build on top of it. Use them directly, or read their source as reference implementations for your own project.

## Built on Walrus

First-party products with Walrus as their storage layer.

### Walrus Memory

[Walrus Memory](https://docs.wal.app/walrus-memory/) gives AI agents portable, verifiable long-term memory. It stores encrypted memories on Walrus as durable blobs and retrieves them with semantic search; Sui smart contracts enforce ownership onchain. It ships a TypeScript SDK, a Python SDK, an MCP server for agent clients, and a hosted playground.

- App: [memory.walrus.xyz](https://memory.walrus.xyz)
- Source: [github.com/MystenLabs/MemWal](https://github.com/MystenLabs/MemWal)
- Docs: [docs.wal.app/walrus-memory](https://docs.wal.app/walrus-memory/)

### Walrus Sites

[Walrus Sites](/docs/sites) hosts static web sites entirely onchain: Sui stores the ownership and resource index, Walrus stores the files, and portals serve the result over standard HTTP. The documentation site you are reading is itself a Walrus Site.

## Community directory

The community maintains [Awesome Walrus](https://github.com/MystenLabs/awesome-walrus), a categorized directory of ecosystem projects: SDKs and client libraries, infrastructure and tooling, and applications built on Walrus. Open a pull request there to add your project.

## Reference implementations

- [Walrus Relay example](/docs/examples/walrus-relay): A deployed browser app storing blobs through the public upload relay, with annotated source.
- [SDK examples](https://github.com/MystenLabs/ts-sdks/tree/main/packages/walrus/examples): Runnable TypeScript SDK snippets maintained alongside the SDK.