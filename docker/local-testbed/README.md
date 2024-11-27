# Walrus Network Docker Compose Setup

Tested on M2 MacOS 15.1 with Docker v27.3.1.

This setup launches:

- A Sui Network with 4 validators, 1 fullnode, and 1 faucet.
- 4 Walrus Storage Nodes.

## How to Run

Make sure you're under `walrus/docker/local-testbed`

1. `./build-local-image.sh`
   1. only if you want to run containers built from local repo
   2. takes about 10 min on M2 Pro Macbook
   3. be sure assign 16GB+ to docker process for image building process
2. `docker compose up` (the latest Docker versions have deprecated `docker-compose`).

## How to Interact with the Network

1. Use `docker ps` to view all running containers. Look for containers with the images:

   - `mysten/walrus-service:<VERSION>` or `local-testbed_walrus-service:<VERSION>`
   - `mysten/sui-tools:mainnet`

2. To access a container’s shell:

   - Run `docker exec -it dryrun-node-0/1/2/3 bash`.
   - Each Walrus node's configuration file is located at `/opt/walrus/outputs/dryrun-node-[0|1|2|3].yaml`.
   - Each storage node contains Sui wallet information for all network participants (storage nodes and admin).

3. To obtain more WAL tokens for testing:

   - Shell into a container and run:
     - `sui client faucet --url http://sui-localnet:9123/gas` to get more Sui tokens.
     - `walrus get-wal <amount_in_mist>` (e.g., `500000000000` for 500 WAL).

4. To store data on the Walrus Network:

   - Shell into any storage node container and run (all of them are pre-configured with a `walrus` client):
     - `walrus store <file>` to upload data.
     - Similarly, use `walrus` commands to retrieve stored data.

5. Optionally, you can expose port 9185 from any storage node container and interact with the REST API from you local development environment.

## What Happens Behind the Scenes

1. Each `docker compose up` starts a fresh Sui network.
2. Walrus contracts are deployed to the new Sui network.
3. Walrus dry-run configurations are generated, along with Sui wallets for each storage node.
4. Local Sui tokens are obtained.
5. Local Sui tokens are exchanged for Walrus tokens.
6. Storage nodes are initialized and started.

## Disclaimer

1. This project is in its early stages. A fixed version of the Walrus image and contracts is used for testing.
   We are working on a process to regularly update the image version, enabling users to test the latest features.
2. The Sui network uses the `mainnet` image label. At the time of testing, it was an alias for `mainnet-v1.37.3`.
3. Walrus publisher and aggregator features are currently not supported but are under development.
