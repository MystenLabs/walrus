# Software development kits (SDKs) and other tools

Mysten Labs has built and published a [Walrus TypeScript SDK](https://sdk.mystenlabs.com/walrus),
which supports a wide variety of operations. See also the related
[examples](https://github.com/MystenLabs/ts-sdks/tree/main/packages/walrus/examples).

The Walrus core team is also actively working on a Rust SDK for Walrus, which will be made available
some time after the Mainnet launch.

Besides these official SDKs, there also exist a few unofficial third-party SDKs for interacting
with the [HTTP API](./web-api.md#http-api-usage) exposed by Walrus aggregators and publishers:

- [Walrus Go SDK](https://github.com/namihq/walrus-go) (maintained by the *Nami Cloud* team)
- [Walrus PHP SDK](https://github.com/suicore/walrus-sdk-php) (maintained by the *Suicore* team)

For data security, use the [TypeScript SDK](https://www.npmjs.com/package/@mysten/seal) for Seal.
It provides threshold encryption and onchain access control for decentralized data protection.
Also, refer to [Data security](../dev-guide/data-security.md) for details.

Finally, there is [Tusky](https://docs.tusky.io/about/about-tusky), a complete data storage platform
built on Walrus, including encryption, HTTP APIs, sharing capabilities, and more.
Tusky maintains its own [TypeScript SDK](https://github.com/tusky-io/ts-sdk).



### Community contributed Tools and Resources
**[Awesome Walrus](https://github.com/MystenLabs/awesome-walrus)** is a collection of tools, libraries, and resources that have been developed and shared by the Walrus user and developer community. In addition to the official tools mentioned earlier, there are also community-driven tools that help streamline development and management within the Walrus ecosystem, making it more efficient and user-friendly.

> ⚠️ This warning icon means that the following tools may not work properly at this time. Please check these tools carefully.

#### Visualization
- Brightlystake - Online dashboards show state's of operators and shards
  - [Walrus Operators Dashboard](https://walrus-stats.brightlystake.com) - [Shards Dashboard](https://walrus-stats.brightlystake.com/shard-owners) - [Further Information](details/brightly-stake.md)
  - Load balanced SUI RPC with geo affinity enabled [https://lb-sui-testnet.brightlystake.com:443](https://lb-sui-testnet.brightlystake.com:443)
- [Walrus Grafana Tools](https://github.com/bartosian/walrus-tools) - A collection of Grafana tools for the Walrus ecosystem monitoring.
- [Walrus Endpoint Latency Dashboard](https://walrus-latency.nodeinfra.com) - Monitors the latency of public aggregator endpoints of Walrus.
- [Walrus ChainViz](https://walrus.chainviz.io) - ChainViz is an interactive explorer for the Walrus network, providing a comprehensive view of decentralized storage. It features a 3D globe for visualizing the network, live monitoring of nodes, aggregators, and publishers, as well as advanced filtering and search capabilities.

#### Cli Tools
- [Morsa](https://gitlab.com/blockscope-net/walrus-morsa) - A storage node monitoring CLI tool that alerts via PD, Slack, Discord and TG.
- [walrus-completion](https://github.com/StakinOfficial/walrus-completion) - A bash, zsh and Python completion for Walrus CLI.

#### Walrus Sites
- Walrus Sites GA - Reusable GitHub Action for deploying Walrus Sites
  - [GitHub](https://github.com/zktx-io/walrus-sites-ga) - [Marketplace](https://github.com/marketplace/actions/walrus-sites-ga) - [Examples](https://github.com/zktx-io/walrus-sites-ga-example) - [Further Information](details/walrus_sites_ga.md)

#### Operator Tooling
- [Walrus Aggregator cache config](https://gist.github.com/DataKnox/983d834202e235dc25e9f5ae69e6c2fb) - Steps to configure the Walrus Aggregator Cache with NGinx and LetsEncrypt.
- [Walrus Monitoring Tools by Chainode Tech](https://github.com/Chainode/Walrus-Tools) - It enables monitoring of your Walrus Storage Node, Publisher, Aggregator, and the underlying hardware.
- [Walrus Ansible Deployment](https://github.com/imperator-co-org/walrus-ansible) - Ansible playbook for deploying a walrus node: Storage, Aggregator & Publisher with Docker-Compose.
- [Walrus Faucet](https://faucet.stakepool.dev.br/walrus) - A public faucet for developers who need test tokens in the Walrus ecosystem.
- [Walrus Commission Claim](https://github.com/suicore/operator-tools) - Enables operators to claim commission using Sui Wallet-compatible wallets, including: zkLogin wallets, Hardware wallets, Passphrase wallets.
