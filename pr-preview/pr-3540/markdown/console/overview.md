> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus Console is the developer-first interface for interacting with the Walrus network. It gives you a hosted place to store, manage, and serve data without running your own Walrus client or handling wallets and tokens directly. You sign in, get an API key, and upload your first file in minutes, then manage everything from one place.

> **Info**
>
> Walrus Console is available on Mainnet through a closed, invite-only beta. Some capabilities described in this documentation are planned to ship at general availability or later, and the structure might change before GA. Availability is called out per feature below.
## How Console relates to Walrus

Walrus is the underlying protocol: a decentralized network that stores data as blobs, coordinated onchain through Sui. You can use Walrus directly through the CLI and SDKs, which give you low-level control over blobs, storage epochs, and payments.

Walrus Console sits on top of that protocol as a managed developer surface. It handles the parts that are otherwise manual: account and wallet provisioning, storage payment, metadata, and a dashboard and API for organizing your data. Use the CLI and SDKs when you want direct protocol access, and use Console when you want a hosted, managed experience.

## Core concepts: spaces, buckets, and files

Console organizes your data in three levels.

A **space** is the top-level container tied to your account. Console creates a **Personal Space** for you automatically when you sign up. A space tracks how much storage you have used against your storage cap. Team Spaces, which let a group share storage under one managed API key, arrive at general availability.

A **bucket** is a named container inside a space that holds files. Every bucket has a visibility setting. In the current beta, all buckets are private and encrypted with [Seal](/docs/data-security); public buckets are planned for a later release.

A **file** is an individual object stored inside a bucket. Uploads are asynchronous: you upload a file, then poll its status until Console confirms it is stored on Walrus. Each file can carry metadata you define, which you can use later to search and organize your data.

## Encryption and privacy

> **Warning**
>
> On Walrus, stored blobs are public by default. Anyone who has a blob ID can read the bytes. Do not rely on obscurity for sensitive data.
Console private buckets solve this by encrypting every file client-side with [Seal](/docs/data-security) before upload. Console stores ciphertext only and never sees your plaintext or your decryption keys. Setting up a private bucket uses a short reserve, sign, and finalize handshake that provisions the bucket's Seal access policy onchain. Encryption on upload and decryption on download both happen on your machine.

## Asset types

Console is built around a single navigation shell that treats your data as typed assets. Files, memory, and datasets are all first-class asset types managed the same way.

**Files** are general-purpose objects you upload and retrieve. This is the asset type available in the current beta.

**Memory** refers to Walrus Memory namespaces, the portable memory layer for AI agents. At GA you can browse, search, and manage your agent memory and renew its storage directly in Console, without touching the SDK.

**Datasets** are published collections with metadata and an access model. You can set a dataset to public, time-gated, or perpetually gated access, and later list it on the Walrus Marketplace.

## Accounts and sign-in

You sign in through Google, with Apple joining soon, using Sui [zkLogin](https://docs.sui.io/concepts/cryptography/zklogin) capabilities. Console derives a Sui address from your identity and silently provisions a Pearl wallet (the embedded wallet Console manages for you), so you do not manage private keys or hold tokens to get started.

Identities from different providers map to separate accounts. Console shows an account-separation notice the first time you sign in so it is clear which identity owns which data.

You own your data. Console does not migrate data you previously stored on Walrus outside the product; you re-upload it.

## API keys and roles

You mint API keys in the Console web app, choosing a `read_write` or `read_only` role for each. Console shows the full key (prefixed `hbr_`) once, at creation, and cannot recover it afterward, so store it like a cloud secret access key. For what each role can do, see the [API reference](./api-reference).

When you create an encrypted-capable key, Console also returns a service private key (prefixed `suiprivkey1`). You keep this locally and use it to sign the transaction that finalizes a private bucket and to authenticate decrypt sessions with Seal. It does not need a token balance.

### Connect AI clients with the MCP server

Console publishes an open-source [MCP](https://modelcontextprotocol.io/) server that exposes file and bucket operations as tools for AI clients. You connect Claude Code, Cursor, or any MCP-compliant client using your existing API key, with no separate credential. This is available in beta.

## Storage, epochs, and renewal

Walrus storage is time-bound. You pay to store data for a number of storage epochs, and data expires when its storage runs out. Console manages epochs and payment for you rather than asking you to track them by hand.

After GA, storage renews automatically for wallets that stayed active, meaning at least one Walrus transaction within a recent activity window. Active developers keep their data without manual renewal, and dormant accounts expire naturally.

## Billing and the free tier

Console keeps a perpetual free tier so new developers are not paywalled. A free storage cap, tentatively 5 GB, is planned pending analysis of real Mainnet usage. Usage-based billing for reads and egress, along with a paid top-up path, follows at and after GA. Console manages WAL token handling on your behalf.

## What is available in beta

Capabilities roll out in phases. The current beta is a subset of the full product.

| Capability | Availability |
| --- | --- |
| Google sign-in, Personal Space | Beta |
| Apple sign-in | Soon |
| Private, Seal-encrypted buckets and file upload or download | Beta |
| API keys and the MCP server | Beta |
| Memory and datasets as asset types | GA |
| Mainnet billing and free tier | GA |
| Auto-renewal, existing-blob discovery, Team Spaces, Marketplace sync | After GA |