# Walrus: Decentralized Blob Storage System

Welcome to the GitHub repository for Walrus, a decentralized storage and availability layer designed specifically for managing large binary files, or "blobs". This system focuses on providing a robust solution for storing unstructured content on decentralized storage nodes while ensuring high availability and reliability even in the presence of Byzantine faults.

## Features

- **Storage and Retrieval**: Walrus supports a variety of operations including blob storage, retrieval, and verification of availability.

- **Cost Efficiency**: By utilizing advanced error correction coding, Walrus maintains storage costs at approximately five times the size of the stored blobs. This is significantly more cost-effective compared to traditional full replication methods.

- **Integration with Sui Blockchain**: Walrus leverages the Sui chain for coordination, attesting availability and financial transactions, storing only the metadata on Sui or its history to streamline operations.

- **Flexible Access**: Users can interact with Walrus through a command-line interface (CLI), software development kits (SDKs), and web2 HTTP technologies.

## Architecture and Operations

Walrus's architecture ensures that content remains accessible and retrievable even when many storage nodes are unavailable or malicious. We provide detailed discussions on the mechanisms behind storage, retrieval, and ensuring availability. Walrus is compatible with traditional CDN setups that can utilize Walrus caches.

Looking ahead, we are exploring enhancements such as storage attestation based on challenges to validate the presence or availability of stored blobs. Moreover, Walrus will accommodate periodic payments for ongoing storage. Additionally, Walrus supports light-nodes that contribute to the network by storing smaller parts of blobs and assisting in recovery processes.
The system includes minimal governance features that permit storage nodes to transition between different storage epochs.

The following design documents are available:
- [Walrus Architecture](./architecture.md) provides an overview of the objectives, security properties and architecture of the Walrus system.
- [Walrus Glossary](./glossary.md) defines key terms used throughout the project.

Walrus is committed to providing a reliable and cost-effective solution for large-scale blob storage, making it an ideal choice for applications requiring decentralized, affordable, durable, and accessible data storage.