#!/bin/bash

rm -rf walrus-docs
git clone https://github.com/MystenLabs/walrus-docs.git
cp -r walrus-docs/contracts /opt/walrus

cd /opt/walrus

rm -rf /opt/walrus/outputs/*

/opt/walrus/bin/walrus-deploy deploy-system-contract \
  --working-dir /opt/walrus/outputs \
  --sui-network 'http://sui-localnet:9000;http://sui-localnet:9123/gas' \
  --n-shards 10 \
  --host-addresses 10.0.0.10 10.0.0.11 10.0.0.12 10.0.0.13 \
  --storage-price 5 \
  --write-price 1 \
  --epoch-duration 1h > /opt/walrus/outputs/deploy

/opt/walrus/bin/walrus-deploy generate-dry-run-configs \
  --working-dir /opt/walrus/outputs \
  --use-legacy-event-provider
