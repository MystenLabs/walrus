# Data security

Walrus provides decentralized storage for application and user data.
Walrus ensures availability and integrity but does not provide native encryption for data.
By default, all blobs stored in Walrus are public and discoverable by all.
If your app needs encryption or access control, secure data before uploading to Walrus.

## Securing data with Seal

Use [Seal](https://github.com/MystenLabs/seal) for encryption and onchain access control.

Seal allows you to:

- Encrypt data using threshold encryption, where no single party holds the full decryption key.
- Define onchain access policies that determine who can decrypt the data and under what conditions.
- Store encrypted content on Walrus while keeping decryption logic verifiable and flexible.

Seal integrates naturally with Walrus and is recommended for any use case involving:

- Sensitive offchain content (e.g., user documents, game assets, private messages)
- Time-locked or token-gated data
- Data shared between trusted parties or roles

To get started, refer to the [Seal SDK](https://www.npmjs.com/package/@mysten/seal).
