# Backup and Restore Guide

## Overview

Storage nodes provide comprehensive backup functionality for the primary database containing blob
data. This document details the configuration and operational procedures for both automated and
manual backup processes.

## Prerequisites

### Local Administration Socket Configuration

The backup system interfaces with running storage nodes through a Unix domain socket. To enable
this functionality:

1. Configure the `admin_socket_path` parameter in the node configuration file:

```yaml
...
admin_socket_path: /opt/walrus/admin.socket
...
```

1. Restart the storage node service to initialize the socket
1. Ensure all administrative operations are executed under the `walrus` user context

## Backup Procedures

### Automated Periodic Backups

Storage nodes support scheduled automatic backups through the following configuration parameters:

```yaml
checkpoint_config:
  db_checkpoint_dir: /opt/walrus/checkpoints  # Backup storage directory
  max_db_checkpoints: 2                       # Retention policy (number of backups)
  db_checkpoint_interval:                     # Backup frequency
    secs: 14400                               # 4-hour interval (in seconds)
    nanos: 0
  sync: true                                  # Force filesystem synchronization
  max_background_operations: 1                # Concurrent operation limit
  periodic_db_checkpoints: true               # Enable automated backups
```

```admonish warning
**Important Requirements:**
- The `walrus` user must possess read/write permissions for the specified `db_checkpoint_dir`
- Backup operations require an active storage node instance accessible via the administration
  socket
```

### Manual Backup Creation

Execute manual backups using the `local-admin` command interface:

```bash
sudo -u walrus walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket checkpoint create \
  --path /opt/walrus/my-checkpoint
```

```admonish warning
**Best Practice:** Store backups on a separate physical disk or storage volume to ensure data
recovery capabilities in the event of primary storage failure.
```

### Backup Inventory Management

To enumerate existing backups within a specified directory:

```bash
sudo -u walrus ./walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket checkpoint list \
  --path /opt/walrus/backup

# Sample Output:
# Backups:
# Backup ID: 1, Size: 85.9 GB, Files: 1055, Created: 2025-07-02T00:25:48Z
```

## Restoration Procedures

### Database Recovery Process

To restore a storage node from a backup checkpoint:

1. **Terminate the storage node service:**

```bash
sudo systemctl stop walrus-node.service
```

1. **Execute the restoration procedure:**

```bash
sudo -u walrus ./walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket restore \
  --db-checkpoint-path /opt/walrus/backup/ \
  --db-path /opt/walrus/db/ \
  --checkpoint-id 1
```

```admonish tip
**Note:** If the `--checkpoint-id` parameter is omitted, the system will automatically select the
most recent checkpoint available in the specified directory.
```
