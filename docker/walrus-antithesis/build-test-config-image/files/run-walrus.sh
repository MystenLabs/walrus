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
cp /opt/walrus/outputs/${HOSTNAME}-sui.yaml /root/.sui/sui_config/client.yaml
cp /opt/walrus/outputs/${HOSTNAME}.aliases /root/.sui/sui_config/sui.aliases

# extract object IDs from the deploy outputs
SYSTEM_OBJECT=$(grep "system_object" /opt/walrus/outputs/deploy | awk '{print $2}')
STAKING_OBJECT=$(grep "staking_object" /opt/walrus/outputs/deploy | awk '{print $2}')
EXCHANGE_OBJECT=$(grep "exchange_object" /opt/walrus/outputs/deploy | awk '{print $2}')

echo "Disk space usage:"
df -h

# copy binaries
cp /root/sui_bin/sui /usr/local/bin/
cp /opt/walrus/bin/walrus /usr/local/bin/

cat <<EOF >/root/.config/walrus/client_config.yaml
system_object: ${SYSTEM_OBJECT}
staking_object: ${STAKING_OBJECT}
exchange_objects: [${EXCHANGE_OBJECT}]
EOF

# get some sui tokens with retry
while ! sui client faucet --url http://10.0.0.20:9123/gas; do
    echo "Failed to get SUI tokens, retrying in 5 seconds..."
    sleep 5
done
sleep 3

echo "Exchange for WAL tokens (500 WAL)"
# exchange for WAL tokens (500 WAL) with retry
while ! walrus get-wal --amount 500000000000; do
    echo "Failed to get WAL tokens, retrying in 5 seconds..."
    sleep 5
done

echo "WAL balance"
while ! sui client balance; do
    echo "Failed to get balance, retrying in 5 seconds..."
    sleep 5
done

echo "starting walrus node"
## -----------------------------------------------------------------------------
## Start the node
## -----------------------------------------------------------------------------
RUST_BACKTRACE=full RUST_LOG=walrus=debug,info /opt/walrus/bin/walrus-node run --config-path /opt/walrus/outputs/${HOSTNAME}.yaml
