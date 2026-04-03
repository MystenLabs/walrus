This page covers the ongoing operation of your Walrus storage node after [initial setup](/docs/operator-guide/storage-nodes/storage-node-setup).

## Important data to back up

Back up the `/opt/walrus/config` directory. For database backups, see the [Backup and Restore Guide](/docs/operator-guide/storage-nodes/backup-restore-guide).

## Key metrics and alerts

The following metrics are the most important for monitoring node health. Set up alerts based on the severity levels below.

#### Critical (page-worthy)

- `walrus_event_processor_latest_downloaded_checkpoint`: This value should continuously increase. Alert if it shows no progress for more than 30 minutes.

#### Needs attention during business hours

- `walrus_event_cursor_progress{state="highest_finished"}`: This value should continuously increase. Alert if there is no progress for more than 30 minutes. Contact the Walrus team if this happens.

- `uptime`: Frequent and repeated node restarts over a 30-minute period indicate an issue. Contact the Walrus team if this happens.

#### Operational alerts

- `walrus_sui_balance_mist`: Warn if the balance drops below 2 SUI (2,000,000,000 MIST). Escalate if it drops below 1 SUI. Ensure the node wallet is sufficiently funded.
- `http_server_tls_certificate_not_after_seconds`: This metric monitors TLS certificate expiration. Set up an alert to warn before the certificate expires so you have time to renew it.

#### General guidance

Check the logs for warnings or errors if any of these metrics stall:

```sh
$ journalctl -efu walrus-node
```

Other metrics like `walrus_storage_confirmations_issued_total` should also increase regularly, but they depend on user activity.

## Update your node

To update your node:

##### Step 1: Stop services.

Stop the node service (and aggregator or publisher if running on the same host):

```sh
$ sudo systemctl stop walrus-node.service
$ sudo systemctl stop walrus-aggregator.service  # if applicable
$ sudo systemctl stop walrus-publisher.service   # if applicable
```

##### Step 2: Download new binaries.

Download the new `walrus-node` and `walrus` binaries to `/opt/walrus/bin`.

##### Step 3: Start the services again.

```sh
$ sudo systemctl start walrus-node.service
$ sudo systemctl start walrus-aggregator.service  # if applicable
$ sudo systemctl start walrus-publisher.service   # if applicable
```

:::info

You are generally expected to upgrade within 24 hours of a new release. In emergency situations, immediate action is appreciated. Subscribe to the [Walrus release calendar](https://calendar.google.com/calendar/u/0/embed?src=c_97763fcda7894da7ddcd68595a797397b9b4294b69603a52e30d4fa0c3fee2bb@group.calendar.google.com) to stay informed about upcoming releases.

:::

## Repair the database

If the node database becomes corrupted (for example, after an unclean shutdown), you can attempt a repair:

```sh
$ /opt/walrus/bin/walrus-node db-tool repair-db --db-path /opt/walrus/db
```

If the repair is unsuccessful, restore from a backup. See the [Backup and Restore Guide](/docs/operator-guide/storage-nodes/backup-restore-guide).

## Update onchain parameters

To modify node parameters (capacity, voting parameters, metadata, and others), edit the `/opt/walrus/config/walrus-node.yaml` file. The node automatically picks up changes and updates onchain information. See the [Storage Node FAQ on TLS](/docs/operator-guide/storage-nodes/storage-node-faq#tls) for details on how automatic configuration updates work.

Avoid changing the node name, keys, and network address unless necessary because this causes some friction in the network.

## Community monitoring tools

Several community members have created tools for monitoring Walrus services. These tools are listed on [awesome-walrus](https://github.com/MystenLabs/awesome-walrus).

:::caution

The Walrus team does not provide or officially support community tools.

:::