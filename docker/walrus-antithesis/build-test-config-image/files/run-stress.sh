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

# copy from deploy outputs
# TODO(zhewu): here we are using the admin wallet to run the stress client, which is not ideal.
# We need to create several client wallets for the stress test so that we can have more than one
# stress client running concurrently.
cp /opt/walrus/outputs/sui_admin.yaml /root/.sui/sui_config/client.yaml
cp /opt/walrus/outputs/sui_admin.keystore /root/.sui/sui_config/sui.keystore
cp /opt/walrus/outputs/sui_admin.aliases /root/.sui/sui_config/sui.aliases

# extract object IDs from the deploy outputs
SYSTEM_OBJECT=$(grep "system_object" /opt/walrus/outputs/deploy | awk '{print $2}')
STAKING_OBJECT=$(grep "staking_object" /opt/walrus/outputs/deploy | awk '{print $2}')
EXCHANGE_OBJECT=$(grep "exchange_object" /opt/walrus/outputs/deploy | awk '{print $2}')

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

echo "starting stress client"
## -----------------------------------------------------------------------------
## Start the node
## -----------------------------------------------------------------------------
RUST_BACKTRACE=1 RUST_LOG=info /opt/walrus/bin/walrus-stress \
    --config-path /opt/walrus/outputs/client_config.yaml \
    --write-load 10 \
    --read-load 10 \
    --n-clients 2 \
    --sui-network "http://10.0.0.20:9000;http://10.0.0.20:9123/gas" \
    --wallet-path /root/.sui/sui_config/client.yaml \
    --gas-refill-period-millis 60000
