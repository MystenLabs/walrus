This guide covers migrating a Walrus storage node to new hardware. It assumes you run the node (and optionally an aggregator and publisher) on the same host.

<Tabs>
<TabItem value="prereq" label="Prerequisites">

- [x] New host set up following the [Storage Node Setup](/docs/operator-guide/storage-node-setup)
  guide (through the [TLS setup](/docs/operator-guide/storage-node-setup#tls-setup) and
  [systemd service](/docs/operator-guide/storage-node-setup#systemd) sections. Do **not** run
  `walrus-node setup` or `register` on the new host.)
- [x] SSH access from the new host to the old host

</TabItem>
</Tabs>

## First transfer

To reduce downtime, start by transferring all data while the old host is still running.

##### Step 1: Log into the new host as the `walrus` user.

##### Step 2: Set the environment variables for the old host.

```sh
OLD_HOST=  # hostname or IP of the old host
OLD_USER=  # SSH user on the old host
```

##### Step 3: Verify you can SSH into the old host from the new host.

##### Step 4: Transfer the `/opt/walrus` directory.

```sh
rsync -avz --progress $OLD_USER@$OLD_HOST:/opt/walrus/ /opt/walrus
```

## Second transfer and migration

##### Step 1: Stop all services on the old host.

Wait for each command to finish before proceeding.

```sh
sudo systemctl stop walrus-node.service
sudo systemctl stop walrus-aggregator.service   # if applicable
sudo systemctl stop walrus-publisher.service     # if applicable
```

:::caution

Stopping the node cleanly is critical. Running `rsync` against a database that is still being written to might result in a corrupted copy.

:::

##### Step 2: Run a second `rsync` to synchronize changes.

Use `--delete` to synchronize any changes made after the first transfer:

```sh
rsync -avz --delete --progress $OLD_USER@$OLD_HOST:/opt/walrus/ /opt/walrus
```

##### Step 3: Update DNS records.

While the sync is running, update your DNS records to point to the new host.

##### Step 4: Download the latest binaries.

Download the latest `walrus` and `walrus-node` binaries (see [Download binaries](/docs/operator-guide/storage-node-setup#binaries)).

##### Step 5: Verify transferred data.

Verify the contents of `/opt/walrus/config` and `/opt/walrus/wallets` (if you run a publisher) match the old host. For `/opt/walrus/db`, check that the directory structure and file sizes are consistent with the old host.

##### Step 6: Update the hostname (if changed).

If you use a new domain name for the node, update `public_host` in `/opt/walrus/config/walrus-node.yaml`.

##### Step 7: Start all services on the new host.

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now walrus-node.service
sudo systemctl enable --now walrus-aggregator.service   # if applicable
sudo systemctl enable --now walrus-publisher.service     # if applicable
```

##### Step 8: Migrate the reverse proxy (if applicable).

If you had a reverse proxy (for example, nginx) on the old host, migrate that configuration as well and start the proxy.

## Verify the migration

##### Step 1: Check the health endpoint.

Run the following command from a different machine:

```sh
curl https://PUBLIC_ADDRESS:9185/v1/health | jq
```

The response should show `"nodeStatus": "Active"` and a `persisted` events count in the order of tens of millions.

##### Step 2: Verify the on-chain key.

```sh
/opt/walrus/bin/walrus --config /opt/walrus/config/client_config.yaml health --node-id YOUR_NODE_ID
```

This performs the same authentication check that other nodes and the Walrus client use.

##### Step 3: Verify the event count.

If the persisted events count is less than 10 million, something is wrong with the database. Stop the node and check permissions, and compare the directory structure in `/opt/walrus/db` against the old host.

:::caution

Do not start the node with an empty database. A full sync is an expensive operation that you should only use in emergencies. Reach out to the core team on Discord before wiping the database or starting with an empty one.

:::

If the database is corrupted, try the following steps in order:

1. Run `rsync` again from the old host.
1. If the corruption is in `/opt/walrus/db/events` or `/opt/walrus/db/event_blob_writer`, delete that directory. It rebuilds automatically.
1. If the main database is corrupted, try running `/opt/walrus/bin/walrus-node db-tool repair-db --db-path /opt/walrus/db`.

##### Step 4: (Optional) Test your aggregator.

```sh
curl https://AGGREGATOR_ADDRESS/v1/blobs/BLOB_ID
```

##### Step 5: (Optional) Test your publisher.

```sh
curl -X PUT https://PUBLISHER_ADDRESS/v1/blobs -d "test"
```

##### Step 6: Clean up the old host.

Only after verifying everything works, clean up the old host by removing `/opt/walrus`.