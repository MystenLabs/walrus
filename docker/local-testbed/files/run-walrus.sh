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

# copy binaries
cp /root/sui_bin/sui /usr/local/bin/
cp /opt/walrus/bin/walrus /usr/local/bin/

cat <<EOF >/root/.config/walrus/client_config.yaml
system_object: ${SYSTEM_OBJECT}
staking_object: ${STAKING_OBJECT}
exchange_objects: [${EXCHANGE_OBJECT}]
EOF

# get some sui tokens
sui client faucet --url http://sui-localnet:9123/gas
sleep 3

# exchange for WAL tokens (500 WAL)
walrus get-wal --amount 500000000000
sui client balance

## -----------------------------------------------------------------------------
## Start the node
## -----------------------------------------------------------------------------
/opt/walrus/bin/walrus-node run --config-path /opt/walrus/outputs/${HOSTNAME}.yaml
