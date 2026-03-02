This page walks you through the full initial setup of a Walrus storage node, from system
preparation to registration and first startup.

:::warning

If you deviate from the standard setup below (user, directories, ports), make sure to adjust later
steps accordingly. Some Walrus and Sui configuration files include absolute paths, so simply moving
files without adjusting those might cause issues.

:::

<Tabs>
<TabItem value="prereq" label="Prerequisites">

- [x] **OS:** Linux (Ubuntu 24.04 recommended), x86_64 with AVX2 and SSSE3 support (available on
  most CPUs less than 10 years old)
- [x] **Storage:** A large disk partition for blob storage, mounted at `/opt/walrus/db`
- [x] **Network:** A public IP address with a DNS name pointing to it
- [x] **Initial system setup**

    1. Create a `walrus` user and group:

        ```sh
        sudo useradd -s /bin/bash walrus
        ```

    2. Create the directory structure:

        ```sh
        sudo install -d -o walrus -g walrus -m 0755 \
          /opt/walrus /opt/walrus/config /opt/walrus/config/tls /opt/walrus/bin /opt/walrus/db
        ```

    3. Mount the large storage partition to `/opt/walrus/db` and ensure it persists across reboots
       (for example, by adding an entry in `/etc/fstab`).

    4. Configure the firewall. Open port **9185** (storage node) and **80** (automatic certificate
       renewal). If you plan to run an aggregator with a reverse proxy, also open **443**:

        ```sh
        for PORT in 80 443 9185; do
          sudo iptables -A INPUT -p tcp --dport $PORT -j ACCEPT
        done
        ```

        Make the firewall configuration persistent across reboots:

        ```sh
        sudo apt-get install iptables-persistent
        sudo iptables-save
        ```

        Verify the rules: `sudo iptables -L INPUT -n`

    :::tip

    You can test your firewall setup by running `nc -l <port>` on the Walrus host and
    `echo test | nc <hostname> <port>` from a different machine. You do not need to open the metrics
    port (9184).

    :::

</TabItem>
</Tabs>

## TLS setup {#tls-setup}

The storage node handles TLS directly. If you deploy a reverse proxy in front of the storage node,
you **must** disable TLS termination on the proxy or ensure it uses the same key as the storage
node.

:::info

You can use any tool to obtain and renew certificates. Just ensure you generate a key of the
correct type (ECDSA secp256r1) in the correct format (PKCS8) and use the full certificate chain in
PEM format.

:::

The following steps use [certbot](https://certbot.eff.org) to request and manage certificates.

1. Install certbot using snap from the **edge** channel (needed for
   [PKCS#8 key format](https://github.com/certbot/certbot/issues/10131)):

```sh
sudo snap install --classic certbot --channel=edge
```

2. Set the `SERVER_NAME` environment variable to the node's public hostname:

```sh
SERVER_NAME=  # your node's public hostname

# Verify: the external IP should match where the DNS name resolves
curl ifconfig.me; echo
dig +short $SERVER_NAME
```

3. Save the following deploy hook script at `/opt/walrus/bin/tls-deploy-hook.sh`. Certbot uses this
   to copy certificates to the storage node's TLS directory after issuance or renewal:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Absolute directory in which to place the TLS certificate and private key.
WALRUS_TLS_DIR="$1"
# The directory from which to copy the issued/renewed certificates.
# The variable $RENEWED_LINEAGE is automatically populated by certbot.
TLS_SOURCE_DIR="$RENEWED_LINEAGE"

# Remove the previous certificate files
rm -f "$WALRUS_TLS_DIR/cert.pem" "$WALRUS_TLS_DIR/chain.pem" \
      "$WALRUS_TLS_DIR/fullchain.pem" "$WALRUS_TLS_DIR/privkey.pem"
# Copy the newly issued/renewed certificate and key into the walrus TLS directory.
/usr/bin/env cp -rL "$TLS_SOURCE_DIR"/* "$WALRUS_TLS_DIR/"
# Change the owner from root to walrus
/usr/bin/env chown walrus:walrus "$WALRUS_TLS_DIR"/*
```

4. Set the correct permissions:

```sh
sudo chown walrus /opt/walrus/bin/tls-deploy-hook.sh
sudo chmod u=rwx,g=rx,o=rx /opt/walrus/bin/tls-deploy-hook.sh
```

5. Perform a dry-run of the certificate request:

```sh
sudo certbot certonly --standalone \
  --reuse-key --key-type ecdsa --elliptic-curve=secp256r1 \
  --domain $SERVER_NAME --cert-name walrus-storage-node \
  --deploy-hook="/opt/walrus/bin/tls-deploy-hook.sh /opt/walrus/config/tls" \
  --dry-run
```

6. If successful, re-run the command without `--dry-run` to obtain the actual certificate.

After this, certbot automatically renews the certificate before it expires, and the node picks up
changes to the file. You can verify the setup:

```sh
# Check the renewal configuration
cat /etc/letsencrypt/renewal/walrus-storage-node.conf

# Check the renewal timer is active
sudo systemctl status snap.certbot.renew.timer
```

:::tip

If you need to manually re-run the deploy hook, use:
`sudo certbot reconfigure --cert-name walrus-storage-node --run-deploy-hooks`

:::

## Install jemalloc {#jemalloc}

Install [jemalloc](https://jemalloc.net/) for improved memory allocation performance:

```sh
sudo apt-get install libjemalloc2
```

This is configured through `LD_PRELOAD` in the systemd service file below.

:::info

The library path `/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` is for x86_64 Ubuntu. If you use a
different architecture, adjust the path accordingly.

:::

## systemd service file {#systemd}

Create a systemd service at `/etc/systemd/system/walrus-node.service`:

```ini
[Unit]
Description=Walrus Node

[Service]
User=walrus
Environment=RUST_BACKTRACE=1
Environment=RUST_LOG=info
Environment="LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
ExecStart=/opt/walrus/bin/walrus-node run --config-path /opt/walrus/config/walrus-node.yaml
Restart=always
TimeoutStopSec=300

LimitNOFILE=4294967296

[Install]
WantedBy=multi-user.target
```

:::info

`TimeoutStopSec=300` gives the node up to 5 minutes to shut down gracefully, including terminating open connections and flushing the database. If you find this to be too short, you can increase it.

:::

## Download binaries and generate configuration {#binaries}

Run all the following commands as the `walrus` user:

```sh
sudo su walrus
```

### Set environment variables

<TabItem label="Mainnet" value="mainnet">

```sh
NETWORK=mainnet
```

</TabItem>
<TabItem label="Testnet" value="testnet">

```sh
NETWORK=testnet
```

</TabItem>

Set the server name (as configured in the [TLS setup](#tls-setup)):

```sh
SERVER_NAME=  # your node's public hostname
```

### Download binaries

Download the `walrus-node` and `walrus` binaries:

```sh
cd /opt/walrus/bin
for BIN in walrus walrus-node; do
  curl -L "https://storage.googleapis.com/mysten-walrus-binaries/$BIN-$NETWORK-latest-ubuntu-x86_64" -o $BIN
  chmod 0755 $BIN
done
```

Verify the versions match the latest
[release](https://github.com/MystenLabs/walrus/releases) for the target network:

```sh
./walrus-node -V
./walrus -V
```

Alternatively, you can download the binaries directly from the
[GitHub releases page](https://github.com/MystenLabs/walrus/releases).

:::tip

For signed binaries and verification, see [Verifying Signed Binaries](/docs/operator-guide/signed-binaries).

:::

### Create the client configuration

```sh
CLIENT_CONFIG=/opt/walrus/config/client_config.yaml
curl "https://docs.wal.app/setup/client_config_$NETWORK.yaml" -o $CLIENT_CONFIG
```

### Generate the node configuration

The `walrus-node setup` command generates the node configuration and a Sui wallet. Key options:

- **`--node-capacity`**: Use the capacity that you can dedicate to the Walrus database. Accepts
  values like `3.14TB`, `2.718TiB`, and similar units.
- **`--sui-network`**: The Sui full node URL used to read objects and send transactions. Requires a
  Sui full node that can sustain at least 10 requests per second for checkpoint data.
- **`--sui-rpc`**: The Sui RPC URL used specifically for reading transactions during event
  processing. See the [FAQ](/docs/operator-guide/storage-node-faq#sui-rpc) for the distinction.
  Can be the same as `--sui-network` or a separate endpoint.
- **`--checkpoint-bucket`**: URL for checkpoint-based transaction reading as a fallback.
- **`--additional-rpc-endpoints`**: Additional Sui RPC endpoints for redundancy (can be specified
  multiple times).
- **`--storage-price`** and **`--write-price`**: Your voting parameters for the storage price per
  MiB and epoch, and write price per MiB (one-time fee). The currency can be specified with the
  `--price-currency` flag (defaults to FROST). Check with the Walrus Foundation for the current
  recommended values.
- **`--commission-rate`**: Commission rate in basis points (100 bp = 1%). See the
  [FAQ](/docs/operator-guide/storage-node-faq#commission) for details. The default value of 6000 bp
  (60%) is the the maximum commission rate if you intend to receive staking from the Walrus
  Foundation.
- **`--metrics-push-url`**: URL for pushing Prometheus metrics. The Walrus Foundation provides
  a metrics endpoint for committee members.
- **`--image-url`**, **`--project-url`**, **`--description`**: Optional metadata about your node.

All of these options can also be changed in the generated config file after setup.

:::info

Operators who set up their node in coordination with the Walrus Foundation or Mysten Labs can get
access to a dedicated Sui full node. Otherwise, you need to run your own or use a third-party RPC
provider. If you run your own Sui RPC node, make sure to always keep it up-to-date and **not**
enable aggressive pruning (all data must be kept for at least one week on Mainnet or two days on
Testnet).

:::

Extract the system and staking object IDs from the client configuration file downloaded earlier:

```sh
SYSTEM_OBJECT=$(awk '/^system_object:/ {print $2}' $CLIENT_CONFIG)
STAKING_OBJECT=$(awk '/^staking_object:/ {print $2}' $CLIENT_CONFIG)
```

Set the remaining variables and run the setup command:

```sh
NODE_CAPACITY=   # for example, 3.14TB or 2.718TiB
NODE_NAME=""     # an arbitrary string identifying your node; include your entity name
PUBLIC_PORT=9185 # change if you deviate from defaults or use a reverse proxy
SUI_RPC_URL=     # URL of a Sui full node for the target network

/opt/walrus/bin/walrus-node setup \
  --sui-network "$SUI_RPC_URL" \
  --config-directory /opt/walrus/config \
  --storage-path /opt/walrus/db \
  --sui-rpc "$SUI_RPC_URL" \
  --system-object "$SYSTEM_OBJECT" \
  --staking-object "$STAKING_OBJECT" \
  --node-capacity "$NODE_CAPACITY" \
  --public-host $SERVER_NAME \
  --public-port $PUBLIC_PORT \
  --name "$NODE_NAME" \
  --network-key-path /opt/walrus/config/tls/privkey.pem \
  --certificate-path /opt/walrus/config/tls/fullchain.pem \
  --metrics-push-url "https://walrus-metrics-$NETWORK.mystenlabs.com/publish/metrics" \
  --checkpoint-bucket "https://checkpoints.$NETWORK.sui.io" \
  --force
```

:::info

Run `walrus-node setup --help` for a full description of all options.

:::

After setup, review the generated configuration file at `/opt/walrus/config/walrus-node.yaml` and
verify IP addresses, DNS names, port numbers, and file paths. You can edit the file directly or
re-run the `setup` command.

### Wallet configuration

The `walrus-node setup` command creates a Sui wallet at `/opt/walrus/config/sui_config.yaml` with
the private key in `/opt/walrus/config/sui.keystore`. You can replace these with a different wallet,
but note that you might have to adjust absolute paths in `sui_config.yaml`.

:::warning

Do not reuse any keys, wallets, or other secrets from Testnet or anywhere else. Each network
deployment should use freshly generated credentials.

:::

## Registration and startup {#registration}

1. **Fund the wallet.** Send SUI to the wallet address shown during setup. 1 SUI is sufficient for
   registration, but the node needs additional SUI for ongoing operation. A recommended initial
   balance is ~20 SUI.

2. **Register the node:**

```sh
/opt/walrus/bin/walrus-node register --config-path /opt/walrus/config/walrus-node.yaml
```

:::warning

Registration must be run during initial setup. It creates the on-chain records for your node.

:::

3. **Set up commission and governance authorization.** Designate a secure wallet address for
   receiving commission and authorizing governance operations. See
   [Commission and Governance](/docs/operator-guide/commission-governance) for details.

4. **Start the node:**

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now walrus-node.service
```

5. **Verify the node is running:**

```sh
sudo systemctl status walrus-node.service
journalctl -efu walrus-node
```

:::info

Before the node is selected for the committee, you might see error messages like
`"message":"unable to push metrics","error":"metrics push failed: [403 Forbidden]"`. This is
expected, as your node is not part of the committee yet and must be specifically allowlisted to
send metrics.

:::

6. **Check the health endpoint** (ideally from a different machine to also verify firewall setup):

```sh
curl https://<public-address>:9185/v1/health | jq
```

This should return a 200 status with a non-empty JSON payload. The `nodeStatus` should be
`Standby`. You can also check `https://<public-address>:9185/v1/api` in a browser to verify
the TLS certificate is set up correctly.

7. **Verify the on-chain key** using the `walrus health` command. This performs a cryptographic
   check that the node uses the key registered on chain:

```sh
/opt/walrus/bin/walrus --config /opt/walrus/config/client_config.yaml health --node-id <YOUR_NODE_ID>
```