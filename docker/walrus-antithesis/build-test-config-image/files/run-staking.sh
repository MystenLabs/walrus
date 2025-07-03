#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

## -----------------------------------------------------------------------------
## Prepare the environment
## -----------------------------------------------------------------------------

HOSTNAME=$(hostname)

# create directories
mkdir -p /root/.sui/sui_config
mkdir -p /root/.config/walrus
mkdir -p /opt/walrus/outputs

# copy from deploy outputs so that we can use `sui client` directly, otherwise, we don't really need
# this copying.
cp /opt/walrus/outputs/staking.yaml /root/.sui/sui_config/client.yaml
cp /opt/walrus/outputs/staking.keystore /root/.sui/sui_config/sui.keystore
cp /opt/walrus/outputs/staking.aliases /root/.sui/sui_config/sui.aliases

echo "Disk space usage:"
df -h

# copy binaries
cp /root/sui_bin/sui /usr/local/bin/
cp /opt/walrus/bin/walrus /usr/local/bin/

echo "WAL balance"
while ! sui client balance; do
    echo "Failed to get balance, retrying in 5 seconds..."
    sleep 5
done

echo "starting staking client"
## -----------------------------------------------------------------------------
## Start the node
## -----------------------------------------------------------------------------
RUST_BACKTRACE=full RUST_LOG=walrus=debug,info /opt/walrus/bin/walrus-stress \
    --config-path /opt/walrus/outputs/client_config_staking.yaml \
    --sui-network "http://10.0.0.20:9000;http://10.0.0.20:9123/gas" \
    staking
