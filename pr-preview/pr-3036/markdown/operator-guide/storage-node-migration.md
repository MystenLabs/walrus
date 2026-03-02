This guide covers migrating a Walrus storage node to new hardware. It assumes you run the node
(and optionally an aggregator and publisher) on the same host.

<Tabs>
<TabItem value="prereq" label="Prerequisites">

- [x] New host set up following the [Storage Node Setup](/docs/operator-guide/storage-node-setup)
  guide (through the [TLS setup](/docs/operator-guide/storage-node-setup#tls-setup) and
  [systemd service](/docs/operator-guide/storage-node-setup#systemd) sections — do **not** run
  `walrus-node setup` or `register` on the new host)
- [x] SSH access from the new host to the old host

</TabItem>
</Tabs>

## First transfer

To reduce downtime, start by transferring all data while the old host is still running.

1. Log into the new host as the `walrus` user.

2. Set the environment variables for the old host:

```sh
OLD_HOST=  # hostname or IP of the old host
OLD_USER=  # SSH user on the old host
```

3. Verify you can SSH into the old host from the new host.

4. Transfer the `/opt/walrus` directory:

```sh
rsync -avz --progress $OLD_USER@$OLD_HOST:/opt/walrus/ /opt/walrus
```

## Second transfer and migration

1. **Stop all services on the old host.** Wait for each command to finish before proceeding.

```sh
sudo systemctl stop walrus-node.service
sudo systemctl stop walrus-aggregator.service   # if applicable
sudo systemctl stop walrus-publisher.service     # if applicable
```

:::warning

Stopping the node cleanly is critical. Running `rsync` against a database that is still being
written to might result in a corrupted copy.

:::

2. Run a second `rsync` with `--delete` to synchronize any changes since the first transfer:

```sh
rsync -avz --delete --progress $OLD_USER@$OLD_HOST:/opt/walrus/ /opt/walrus
```

3. While this is running, update your DNS records to point to the new host.

4. Download the latest `walrus` and `walrus-node` binaries (see
   [Download binaries](/docs/operator-guide/storage-node-setup#binaries)).

5. Verify the contents of `/opt/walrus/config`, `/opt/walrus/db`, and `/opt/walrus/wallets`
   (if you run a publisher) match the old host.

6. If you use a new domain name for the node, update `public_host` in
   `/opt/walrus/config/walrus-node.yaml`.

7. Start all services on the new host:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now walrus-node.service
sudo systemctl enable --now walrus-aggregator.service   # if applicable
sudo systemctl enable --now walrus-publisher.service     # if applicable
```

8. If you had a reverse proxy (for example, nginx) on the old host, migrate that configuration as
   well and start the proxy.

## Verify the migration

1. **Check the health endpoint** (ideally from a different machine):

```sh
curl https://<public-address>:9185/v1/health | jq
```

The response should show `"nodeStatus": "Active"` and a `persisted` events count in the order of
tens of millions.

2. **Verify the on-chain key:**

```sh
/opt/walrus/bin/walrus --config /opt/walrus/config/client_config.yaml health --node-id <YOUR_NODE_ID>
```

This performs the same authentication check that other nodes and the Walrus client use.

3. If the persisted events count is less than 10 million, something is wrong with the database.
   Stop the node and check permissions, and compare the directory structure in `/opt/walrus/db`
   against the old host.

:::warning

Do not start the node with an empty database. A full sync is an expensive operation that should
only be used in emergencies. Reach out to the core team on Discord before wiping the database or
starting with an empty one. If the database is corrupted, try the following steps in order:

1. Run `rsync` again from the old host.
2. If the corruption is in `/opt/walrus/db/events` or `/opt/walrus/db/event_blob_writer`, you can
   delete that directory — it is rebuilt automatically.
3. If the main database is corrupted, try running
   `/opt/walrus/bin/walrus-node db-tool repair-db --db-path /opt/walrus/db`.

:::

4. (Optional) Test your aggregator: `curl https://<aggregator-address>/v1/blobs/<BLOB_ID>`

5. (Optional) Test your publisher: `curl -X PUT https://<publisher-address>/v1/blobs -d "test"`

6. **Only after verifying everything works**, you can clean up the old host by removing
   `/opt/walrus`.