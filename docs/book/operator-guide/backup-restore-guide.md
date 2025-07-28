# Backup and Restore Guide

## Overview

Walrus storage nodes provide comprehensive backup and restore functionality for the primary database
containing blob data. This guide covers configuration requirements, operational procedures, and best
practices for both automated and manual backup processes.

## Prerequisites

### System Requirements

- Storage node must be running with appropriate permissions to create backups
- Sufficient disk space for backup storage (recommended: separate physical volume)
- Unix/Linux operating system with support for Unix domain sockets
- `walrus` user account with appropriate permissions

### Local Administration Socket Configuration

The backup system communicates with running storage nodes through a Unix domain socket. To enable
this functionality:

1. **Configure the administration socket path** in your node configuration file:

   ```yaml
   # /opt/walrus/node-config.yaml
   admin_socket_path: /opt/walrus/admin.socket
   ```

1. **Restart the storage node** to initialize the socket:

   ```bash
   sudo systemctl restart walrus-node.service
   ```

1. **Verify socket creation**:

   ```bash
   sudo -u walrus ls -la /opt/walrus/admin.socket
   ```

```admonish warning
All administrative operations must be executed under the `walrus` user context to ensure proper
permissions and security.
```

## Backup Configuration

### Automated Periodic Backups

Storage nodes support scheduled automatic backups through checkpoint configuration. Add the
following parameters to your node configuration:

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

  # Force filesystem synchronization after backup
  sync: true

  # Maximum concurrent backup operations
  max_background_operations: 1

  # Enable/disable automated backups
  periodic_db_checkpoints: true
```

### Directory Permissions

Ensure proper permissions for backup directories:

```bash
# Create backup directory
sudo mkdir -p /opt/walrus/checkpoints

# Set ownership and permissions
sudo chown walrus:walrus /opt/walrus/checkpoints
sudo chmod 750 /opt/walrus/checkpoints
```

## Backup Operations

### Manual Backup Creation

Create on-demand backups using the `local-admin` command:

```bash
# Basic backup command
sudo -u walrus walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket \
  checkpoint create \
  --path /opt/walrus/backups/manual-$(date +%Y%m%d-%H%M%S)
```

```admonish tip title="Best Practices"
1. **Use descriptive names**: Include timestamps in backup paths for easy identification
2. **Separate storage**: Store backups on different physical disks or network storage
4. **Off-site copies**: Consider replicating critical backups to remote locations
```

### Monitoring Backup Status

#### List Available Backups

```bash
sudo -u walrus walrus-node local-admin \
  --socket-path /opt/walrus/admin.socket \
  checkpoint list \
  --path /opt/walrus/checkpoints
```

**Sample Output:**

``` console
Backups:
Backup ID: 1, Size: 85.9 GB, Files: 1055, Created: 2025-07-02T00:25:48Z
Backup ID: 2, Size: 86.2 GB, Files: 1058, Created: 2025-07-02T04:25:52Z
```

## Restoration Procedures

### Pre-Restoration Checklist

Before initiating a restore:

1. **Verify backup availability** and integrity
1. **Ensure sufficient disk space** for the restored database

### Database Recovery Process

Follow these steps to restore from a backup:

1. **Stop the storage node service**:

   ```bash
   sudo systemctl stop walrus-node.service

   # Verify the service is stopped
   sudo systemctl status walrus-node.service
   ```

1. **Optional: Backup current database**

   ```bash
   # Create safety backup of current state
   sudo -u walrus cp -r /opt/walrus/db /opt/walrus/db.backup.$(date +%Y%m%d-%H%M%S)
   ```

1. **Execute the restoration**:

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
