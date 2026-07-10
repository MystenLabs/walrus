> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

To view a website deployed using Walrus Sites, you must use a Sites Portal. A portal retrieves site resources from Walrus and their corresponding Sui objects before serving the site in your browser. You can browse any Walrus Site deployed on Mainnet or Testnet using a Sites Portal.

Use Docker to deploy a Sites Portal locally. There are no Testnet portals hosted for public good, and Mainnet sites must use a SuiNS domain name to be resolved through Walrus Foundation's public Mainnet portal.

- [x] Install [Docker](https://docs.docker.com/get-docker/).

- [x] [Install and configure the `site-builder`](/docs/sites/getting-started/installing-the-site-builder).

Clone the `walrus-sites` repository and check out the stable branch:

```sh
$ git clone https://github.com/MystenLabs/walrus-sites.git
$ cd walrus-sites
$ git checkout mainnet
```

Copy the template configuration file for your target network and rename it to `portal-config.yaml`:

```sh
$ cp portal/server/portal-config.mainnet.example.yaml portal/server/portal-config.yaml
```

Run the Docker container:

```bash
docker run \
  -it \
  --rm \
  -v $(pwd)/portal/server/portal-config.yaml:/portal-config.yaml:ro \
  -e PORTAL_CONFIG=/portal-config.yaml \
  -p 3000:3000 \
  mysten/walrus-sites-server-portal:mainnet-v2.8.0
```

The portal Docker image version must match your `site-builder` version. Run the following command to get the version tag:

```bash
$ site-builder -V | awk '{ print $2 }' | awk -F - '{ printf("v%s\n", $1) }'
```

Be sure that version tag matches the version in `mysten/walrus-sites-server-portal:mainnet-v2.8.0`.

Once the Docker container is running, open your browser and navigate to the following URL:

```
http:/localhost:3000
```

### Local development

This method requires `bun`. Check whether `bun` is installed:

```sh
$ bun --version
```

If `bun` is not installed, run the following command:

```sh
$ curl -fsSL https://bun.sh/install | bash
```

Install the dependencies:

```sh
$ git clone https://github.com/MystenLabs/walrus-sites.git
$ cd walrus-sites/portal
$ bun install
```

To run a server-side portal, copy the example [`portal-config.yaml`](/docs/sites/portals/mainnet-testnet) for your target network and start the server. The server portal runs at `localhost:3000`.

```sh
# Mainnet
$ cp server/portal-config.mainnet.example.yaml server/portal-config.yaml

# Testnet
$ cp server/portal-config.testnet.example.yaml server/portal-config.yaml

$ bun run server
```

To run a service-worker portal, copy the example [`.env.local`](/docs/sites/portals/mainnet-testnet#environment-variable-overrides) for your target network and start the worker. The service-worker portal runs at `localhost:8080`.

```sh
# Mainnet
$ cp worker/.env.mainnet.example worker/.env.local

# Testnet
$ cp worker/.env.testnet.example worker/.env.local

$ bun run build:worker
$ bun run worker
```