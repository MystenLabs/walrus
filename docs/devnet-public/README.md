# The Walrus Decentralized Blob Storage System

Welcome to the GitHub repository for Walrus, a decentralized storage and availability protocol
designed specifically for large binary files, or "blobs". Walrus focuses on providing a robust
solution for storing unstructured content on decentralized storage nodes while ensuring high
availability and reliability even in the presence of Byzantine faults.

## Features

- **Storage and Retrieval**: Walrus supports storage operations to write and read blobs. It also
  allows anyone to prove that a blob has been stored and is available for retrieval at a later
  time.

- **Cost Efficiency**: By utilizing advanced error correction coding, Walrus maintains storage
  costs at approximately five times the size of the stored blobs. And encoded parts of each blob
  are stored on each storage node. This is significantly more cost-effective compared to
  traditional full replication methods. And much more robust against failures compared to
  protocols that only store each blob on a subset of storage nodes.

- **Integration with Sui Blockchain**: Walrus leverages the Sui chain for coordination, attesting
  availability and payments. Storage space can be owned as a resource on Sui, split, merged, and
  transferred. Blob storage is represented using storage objects on Sui, and smart contracts can
  check whether a blob is available and for how long.

- **Flexible Access**: Users can interact with Walrus through a Command-Line Interface (CLI),
  Software Development Kits (SDKs), and web2 HTTP technologies. Walrus is designed to work well
  with traditional caches and Content Distribution networks (CDNs), while ensuring all operations
  can also be run using local tools to maximize decentralization.

## Architecture and Operations

Walrus's architecture ensures that content remains accessible and retrievable even when many
storage nodes are unavailable or malicious. Under the hood it uses modern error correction
techniques based on fast linear fountain codes, augmented to ensure resilience against Byzantine
faults, and a dynamically changing set of storage nodes. The core of Walrus remains simple, and
storage node management and blob certification leverages Sui smart contracts.

The following design documents are available:
- [Walrus Technical Overview](./overview.md) provides an overview of the objectives, security
  properties and architecture of the Walrus system.
- [Walrus Glossary](./glossary.md) defines key terms used throughout the project.

Walrus is committed to providing a reliable and cost-effective solution for large-scale blob
storage, making it an ideal choice for applications requiring decentralized, affordable, durable,
and accessible data storage.

## Quick Start

Coming soon ...