# Data security

Walrus provides decentralized storage for application and user data. While Walrus ensures availability and integrity through its architecture, it does not natively encrypt data. By default, all blobs stored in Walrus are public and discoverable by all. If your application requires encryption or fine-grained access control, you should secure your data before uploading it to Walrus.

## Securing data with Seal

To enable end-to-end encryption and programmable access control, you can use [Seal](https://github.com/MystenLabs/seal) - a decentralized secrets management service.

Seal allows you to:
- Encrypt data using threshold encryption, where no single party holds the full decryption key.
- Define onchain access policies that determine who can decrypt the data and under what conditions.
- Store encrypted content on Walrus while keeping decryption logic verifiable and flexible.

Seal integrates naturally with Walrus and is recommended for any use case involving:
- Sensitive offchain content (e.g., user documents, game assets, private messages)
- Time-locked or token-gated data
- Data shared between trusted parties or roles

To get started, refer to the [Seal SDK](https://www.npmjs.com/package/@mysten/seal) or the [documentation](https://github.com/MystenLabs/seal) for encrypting data before storing it on Walrus.
