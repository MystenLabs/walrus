The Sui Archival application demonstrates how Sui blockchain checkpoint data can be archived on Walrus in a reliable, deterministic, and resilient manner.

The application is accessible at https://walrus-sui-archival.wal.app/

The application's code is [open source and available on GitHub](https://github.com/MystenLabs/walrus-sui-archival).

## How it works

The application polls data from Sui by subscribing to checkpoint sources such as the ingestion framework. It continuously receives live checkpoints as they are created. Once the application obtains a checkpoint, it creates a checkpoint blob based on a deterministic algorithm. The application then uploads blobs to Walrus. The application stores blob metadata in its local database. When blobs are close to expiration, the system automatically extends their lifetime to ensure continuous availability.

Additional technical details can be found in the [application's documentation](https://walrus-sui-archival.wal.app/tech/).

The application uses the following code for the main archival functionality:

[View the application's full code on GitHub](https://github.com/MystenLabs/walrus-sui-archival).