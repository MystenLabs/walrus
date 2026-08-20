> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

- [Aggregators, Publishers, and Upload Relays](/docs/system-overview/aggregators-publishers-relays): What each Walrus HTTP service does, which direction of traffic it handles, who pays for storage, and which endpoint to call, so you can tell an aggregator from a publisher from an upload relay.
- [Available Networks](/docs/system-overview/available-networks): Overview of Walrus networks including Mainnet and Testnet configurations, parameters, and setup instructions.
- [Caching Hot Reads](/docs/system-overview/caching): Guidance for caching frequently read Walrus blobs, including caching aggregators, CDN fronting, immutability guarantees, and pitfalls to avoid.
- [Walrus Fundamentals](/docs/system-overview/core-concepts): Technical reference for Walrus fundamentals, including architecture, data storage, and data retrieval.
- [Operations](/docs/system-overview/operations): Developer guide to Walrus operations for blob management.
- [Public Aggregators and Publishers](/docs/system-overview/public-aggregators-and-publishers): Find public Walrus aggregator and publisher endpoints and use them to read and store blobs over HTTP without running your own client.
- [Batch Storage with Quilt](/docs/system-overview/quilt): Comprehensive guide to Walrus Quilt for batch storage of multiple small blobs with cost optimization and metadata management.
- [RedStuff Encoding Example](/docs/system-overview/red-stuff-details): Step-by-step worked example of RedStuff encoding showing primary and secondary sliver creation with matrix illustrations.
- [RedStuff Properties and Parameters](/docs/system-overview/red-stuff-parameters): RedStuff encoding properties, Walrus-specific parameters, blob size limits, sliver-to-shard mapping, sliver authentication, and metadata overhead.
- [RedStuff Recovery Example](/docs/system-overview/red-stuff-recovery): Worked example of RedStuff sliver recovery after shard failure, showing how primary and secondary slivers are reconstructed from other shards.
- [RedStuff Encoding Algorithm](/docs/system-overview/red-stuff): Learn how the RedStuff encoding algorithm works in Walrus, including erasure coding, RaptorQ fountain codes, sliver encoding, recovery, and blob metadata.
- [Upload Relay](/docs/system-overview/relay): Learn how the Walrus upload relay simplifies blob uploads by batching requests, handling retries, and generating certificates on behalf of your application.
- [Storage Costs](/docs/system-overview/storage-costs): Comprehensive guide to Walrus storage costs including fixed USD-denominated pricing, WAL tokens, SUI gas fees, and cost optimization strategies.
- [System Constraints & Considerations](/docs/system-overview/system-constraints): Storage limits, cost considerations, memory requirements, and other constraints to consider when building on Walrus.
- [View System Information](/docs/system-overview/view-system-info): Use the walrus info command to view Walrus system parameters, storage node details, epoch information, and current storage costs.
- [WAL Tokenomics FAQ](/docs/system-overview/wal-tokenomics-faq): Answers to common questions about the WAL token, covering fee flows, staking and re-delegation, rewards, slashing, and what burning a blob does.