# Getting started with Walrus

By the end of this Getting Started deep dive, we want you to be able to

- Explain what a blob is.
- Explain the difference between testnet and mainnet.
- Create a Sui wallet and locate its balances.
- Use the Sui testnet "faucet" to get coins for use in testing Walrus.
- Convert SUI coins to WAL coins on testnet.
- Explain what Walrus "epochs" are.
- Store blobs on Walrus.
- Read blobs from Walrus.
- Extend the lifetime of a blob.
- Delete a blob.
- Understand the fundamentals of public data on Walrus and know where to look for more information.

## Blobs

Walrus is all about blobs. A blob is like a file on your computer. It's an array of bytes that has a
fixed size. Just like your local file system, Walrus is indifferent to the type of data being stored. It could be text, images, videos, source code, etc. To Walrus it's just a "blob" of data.

To store a blob on Walrus, you upload it to the Walrus network. From then on you (and others) can refer to it by its `blob_id`. Blobs are immutable which means their contents - once written - cannot change. Blobs are generally *owned* by an [Address](https://www.notion.so/Getting-started-with-Walrus-27d6d9dcb4e980b7b6d3e92b6982bb87?pvs=21). Blobs have a finite lifetime but can be extended indefinitely.

TK figure out the right info-architecture transition here.

<aside>
💡 Walrus depends on Sui.
</aside>

## Sui

Because Walrus depends on Sui, it's important to digress a bit from our focus on Walrus to make sure you're up to speed on some details about Sui.

### Testnet and Mainnet

Sui and Walrus are both decentralized distributed systems. In practice this means they are made up of a bunch of independent servers communicating and collectively establishing shared state. A group of servers together is called a network.

Sui and Walrus each have multiple corresponding networks.

- **Testnet** is a sandbox-like network where coins are free and software packages can be built, tested, and debugged without fear of losing money or losing valuable data.
- **Mainnet** is the production environment where coins are typically *not* free, and folks depend on various package behaviors.

When you are getting started, it's best to play around in **Testnet** until you get the hang of the
various tools and the larger ecosystem.

### Addresses and Accounts

Sui has the concept of Addresses and Accounts. Think of them like a mailbox and a key. These are important for Walrus because when you store blobs on Walrus, they will be bound to an Object on Sui, that is owned by an Address, potentially the Address of your Account.

### Address

A **unique location** on the blockchain, like a mailbox number. Anyone can see it and send things to
it (like cryptocurrency or digital items). It's just a public identifier - a string of letters and numbers like `0x1234abcd...`

- A 32-byte identifier (displayed as 64 hex characters with `0x` prefix)
- Represents a location on the blockchain that can own objects
- Derived from a public key using a hash function

### Account

An address **plus the key to access it**. If you have the private key (like a mailbox key), you can
actually use what's stored at that address - send money, move items, etc.

Another simple analogy would be that an **address** is like your email address (public, anyone can
send you stuff), and an **account** is like your email address + the password (you can read and send emails).

You can have an address without controlling it, but an account means you have both the address and the ability to use it.

- An **account** is the combination of an address and its controlling private key
- Represents actual ownership and control
- The private key holder can sign transactions for that address

### Sui Objects

Sui is a blockchain that supports programmability at a [fundamental level](https://docs.sui.io/concepts/transactions/prog-txn-blocks). Almost everything on Sui is an object. This includes Walrus Blobs. Importantly, objects within Sui are subject to
[ownership](https://docs.sui.io/concepts/object-ownership) rules. For the purpose of this deep dive, we'll only look at "Address-Owned" objects, but be aware that there are other types of ownership.

**Object Ownership**: Objects on Sui are owned by addresses, not accounts. The distinction matters for:

- Shared objects (no single controlling account)
- Immutable objects (no account can modify)
- Address-owned objects (controlled by whoever has the private key)

### Sui Transactions

Changes to objects on Sui happen through the use of transactions. We won't say too much about
transactions, except that they are a low-level primitive that Sui uses to change data on the blockchain. These transactions are *signed* by accounts on behalf of addresses and result in objects being created, updated, transferred, and sometimes destroyed. Learn more about
[transactions](https://docs.sui.io/concepts/transactions).

### Walrus and Sui

Sui allows its users to store information in and emit events onto the blockchain. Walrus leverages these capabilities to track Blobs, their respective owners, and their lifetimes, as well as many other aspects of the Walrus network that we can get into later. For now, let's move on to getting our hands dirty with the tools.

## Setting up Sui

TK continue from here
######################
