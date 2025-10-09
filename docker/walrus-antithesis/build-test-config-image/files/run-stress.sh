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
cp /opt/walrus/outputs/stress.yaml /root/.sui/sui_config/client.yaml
cp /opt/walrus/outputs/stress.keystore /root/.sui/sui_config/sui.keystore
cp /opt/walrus/outputs/stress.aliases /root/.sui/sui_config/sui.aliases


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
RUST_BACKTRACE=full RUST_LOG=info /opt/walrus/bin/walrus-stress \
    --config-path /opt/walrus/outputs/client_config_stress.yaml \
    --sui-network "http://10.0.0.20:9000;http://10.0.0.20:9123/gas" \
    single-client \
    --target-requests-per-minute 10 \
    --check-read-result true \
    --max-blobs-in-pool 100 \
    poisson-size --lambda 1024 --size-multiplier 10 \
    poisson-store-length --store-lambda 3
