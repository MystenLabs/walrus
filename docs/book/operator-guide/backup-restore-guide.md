# Backup and restore guide

## Overview

Walrus storage nodes provide backup and restore functionality for the primary database containing
blob data. This guide covers configuration requirements, operational procedures, and best practices
for both automated and manual backup processes, as well as restore operations.

## Prerequisites

- Storage node must be running with appropriate permissions to create backups
- Sufficient disk space for backup storage (recommended: separate physical volume)
- Unix/Linux operating system with support for Unix domain sockets
- `walrus` user account with appropriate permissions

### Local administration socket configuration

The backup system communicates with running storage nodes through a Unix domain socket. To enable
this functionality:

1. **Configure the administration socket path** in your node configuration file:

   ```yaml
   admin_socket_path: /opt/walrus/admin.socket
   ```

1. **Restart the storage node** to initialize the socket:

   ```bash
   sudo systemctl restart walrus-node.service
   ```

1. **Verify socket creation**:

   ```bash
   ls -la /opt/walrus/admin.socket
   ```

```admonish warning
All administrative operations must be executed under the `walrus` user context to ensure proper
permissions and security.
```

## Automated periodic backups

Storage nodes support scheduled automatic backups through checkpoint configuration. Add the
following configs to your node configuration:

```yaml
checkpoint_config:
  # Directory where backups will be stored
  db_checkpoint_dir: /opt/walrus/checkpoints

  # Number of backups to retain (oldest will be deleted)
  max_db_checkpoints: 2

  # Backup frequency (example: 4-hour interval)
  db_checkpoint_interval:
    secs: 14400  # 4 hours in seconds
    nanos: 0

  # Sync in-memory data to disk before creating a bakcup
  sync: true

  # Maximum concurrent backup operations
  max_background_operations: 1

  # Enable/disable automated backups
  periodic_db_checkpoints: true
```

```admonish tip
To disable automated backups, set `periodic_db_checkpoints: false` in your configuration.
```

## Manual backup creation

Create on-demand backups using the `local-admin` command:

```bash
# Basic backup command
sudo -u walrus walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket \
  checkpoint create \
  --path /opt/walrus/backups/manual-backup-name
```

```admonish tip
The backup operation runs in the background within the storage node. Once the backup creation is
initialized, the process continues independently even if the command-line interface is terminated.
```

## List Available Backups

```bash
sudo -u walrus walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket \
  checkpoint list \
  --path /opt/walrus/checkpoints
```

**Sample output:**

```console
Backups:
Backup ID: 1, Size: 85.9 GB, Files: 1055, Created: 2025-07-02T00:25:48Z
Backup ID: 2, Size: 86.2 GB, Files: 1058, Created: 2025-07-02T04:25:52Z
```

## Restore from a backup

```admonish warning
Do not copy backup directories directly to the storage node's data path. The restore tool must be
used to properly reconstruct the database from checkpoint files. Directly copied content cannot be
recognized by the storage engine.
```

Follow these steps to restore from a backup:

1. **Stop the storage node service**:

   ```bash
   sudo systemctl stop walrus-node.service

   # Verify the service is stopped
   sudo systemctl status walrus-node.service
   ```

1. **Optional: Backup current database**

   ```bash
   # Assuming the Walrus storage path is: `storage_path: /opt/walrus/db`
   sudo -u walrus cp -r /opt/walrus/db /opt/walrus/db.backup.$(date +%Y%m%d-%H%M%S)
   ```

   This command saves:
   - Main database files
   - Events database (`/opt/walrus/db/events/`)
   - Event blob data (`/opt/walrus/db/event_blob_writer/`)

1. **Clear existing data** (if performing a clean restore):

   Remove all existing database files to ensure a clean restore,

   ```bash
   # Assuming the Walrus storage path is: `storage_path: /opt/walrus/db`
   sudo -u walrus rm -rf /opt/walrus/db/*
   ```

   This command removes:
   - Main database files
   - Events database (`/opt/walrus/db/events/`)
   - Event blob data (`/opt/walrus/db/event_blob_writer/`)

1. **Restore main database**:

   ```admonish warning
   The restore process can take significant time depending on database size. Run the restore command
   in a persistent session using `tmux` or `screen` to prevent interruption if your connection drops.
   ```

   ```bash
   # Restore from specific checkpoint
   sudo -u walrus walrus-node \
     restore \
     --db-checkpoint-path /opt/walrus/checkpoints \
     --db-path /opt/walrus/db \
     --checkpoint-id 2

   # Or restore from latest checkpoint (omit --checkpoint-id)
   sudo -u walrus walrus-node \
     restore \
     --db-checkpoint-path /opt/walrus/checkpoints \
     --db-path /opt/walrus/db
   ```

1. **Start the storage node**:

   ```bash
   sudo systemctl start walrus-node.service

   # Monitor startup logs
   sudo journalctl -u walrus-node.service -f
   ```

The storage node will begin downloading and replaying events. This process may take some time before
the node transitions to `Active` state.
