#!/bin/bash

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
STACKING_OBJECT=$(grep "staking_object" /opt/walrus/outputs/deploy | awk '{print $2}')
EXCHANGE_OBJECT=$(grep "exchange_object" /opt/walrus/outputs/deploy | awk '{print $2}')

# copy binaries
cp /root/sui_bin/sui /usr/local/bin/
cp /opt/walrus/bin/walrus /usr/local/bin/

# copy walrus client config
cp /root/walrus-client-config.yaml /root/.config/walrus/client_config.yaml
# replace <SYSTEM_OBJECT> and <STACKING_OBJECT> with the actual object IDs from the deploy outputs
sed -i "s/<SYSTEM_OBJECT>/${SYSTEM_OBJECT}/g" /root/.config/walrus/client_config.yaml
sed -i "s/<STACKING_OBJECT>/${STACKING_OBJECT}/g" /root/.config/walrus/client_config.yaml
sed -i "s/<EXCHANGE_OBJECT>/${EXCHANGE_OBJECT}/g" /root/.config/walrus/client_config.yaml

# get some sui tokens
sui client faucet --url http://sui-localnet:9123/gas
sleep 3

# exchange for WAL tokens (500 WAL)
walrus get-wal -a 500000000000
sui client balance

## -----------------------------------------------------------------------------
## Register the node
## -----------------------------------------------------------------------------
# /opt/walrus/bin/walrus-node register --config-path /opt/walrus/config/walrus-node.yaml

## -----------------------------------------------------------------------------
## Start the node
## -----------------------------------------------------------------------------
/opt/walrus/bin/walrus-node run --config-path /opt/walrus/outputs/${HOSTNAME}.yaml
