# Walrus Documentation

**Walrus is a decentralized storage network** that provides affordable, highly available storage for unstructured data like files, images, and videos. Built for the AI era, Walrus uses advanced erasure coding to deliver robust, Byzantine fault-tolerant storage.

```admonish tip title="Ready to get started?"
**New to Walrus?** Jump to our [Quick Start Guide](./usage/quickstart.md) to store your first blob in 10 minutes.

**Experienced with Sui?** Go to the [Setup Guide](./usage/setup.md) for full installation and configuration options.
```

```admonish tip title="Fun fact"
You're viewing these docs from Walrus right now! See [Walrus Sites](./walrus-sites/intro.md) to learn how to build decentralized websites.
```

## Key Features

- **💰 Cost-Efficient Storage** - Advanced erasure coding keeps costs ~5x the blob size, much cheaper than full replication while more robust than subset storage

- **⛓️ Sui Integration** - Uses Sui blockchain for coordination and payments; storage and blobs are Sui objects that smart contracts can interact with

- **🌐 Flexible Access** - Command-line interface (CLI), SDKs, and HTTP APIs work seamlessly with traditional caches and CDNs

- **🔒 Highly Available** - Byzantine fault-tolerant design ensures your data stays accessible even when storage nodes fail

- **⚡ Decentralized** - No single point of failure; operated by a committee of storage nodes using delegated proof of stake

```admonish danger title="Public access"
**All blobs stored in Walrus are public and discoverable by all.** Don't store secrets or private data without encryption. See [Data Security](./dev-guide/data-security.md) for guidance.
```

## Documentation Overview

### 🚀 [Getting Started](./usage/started.md)
New to Walrus? Start with our [Quick Start Guide](./usage/quickstart.md) to install Walrus and store your first blob in ~10 minutes.

### 📖 [Usage Guides](./usage/interacting.md)
Learn to use the CLI, HTTP API, and SDKs. Includes setup instructions, command reference, and examples.

### 🌐 [Walrus Sites](./walrus-sites/intro.md)
Build fully decentralized websites hosted on Walrus. These docs are hosted on Walrus!

### 🔧 [Developer Guide](./dev-guide/dev-guide.md)
Architecture details, storage costs, Sui integration, and advanced usage patterns.

### ⚙️ [Operator Guide](./operator-guide/operator-guide.md)
Run your own storage nodes, aggregators, or publishers.

### 📰 [Dev Blog](./blog/00_intro.md)
Announcements, updates, and insights from the Walrus team.

### 📚 [Glossary](./glossary.md)
Key terminology explained.

## Sources

This documentation is built using [mdBook](https://rust-lang.github.io/mdBook/) from source files in
<https://github.com/MystenLabs/walrus>. Please report or fix any errors you find in this
documentation in that GitHub project.
