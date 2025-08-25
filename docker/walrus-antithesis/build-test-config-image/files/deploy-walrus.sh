#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

die() {
  echo "$0: error: $*" >&2
  exit 1
}

# use EPOCH_DURATION to set the epoch duration, default is 10 minutes in antithesis test.
EPOCH_DURATION=${EPOCH_DURATION:-10m}

cd /opt/walrus || die "/opt/walrus does not exist?"

rm -rf /opt/walrus/outputs/*
ls -al /opt/walrus/contracts

echo "Deploying system contract"
/opt/walrus/bin/walrus-deploy deploy-system-contract \
  --working-dir /opt/walrus/outputs \
  --contract-dir /opt/walrus/contracts \
  --do-not-copy-contracts \
  --sui-network 'http://10.0.0.20:9000;http://10.0.0.20:9123/gas' \
  --n-shards 10 \
  --host-addresses 10.0.0.10 10.0.0.11 10.0.0.12 10.0.0.13 \
  --storage-price 5 \
  --write-price 1 \
  --with-wal-exchange \
  --epoch-duration "$EPOCH_DURATION" >/opt/walrus/outputs/deploy \
  || die "Failed to deploy system contract"

echo "Generating dry run configs"

RUST_LOG=walrus=debug,info /opt/walrus/bin/walrus-deploy generate-dry-run-configs \
  --working-dir /opt/walrus/outputs \
  --extra-client-wallets stress,staking \
  --admin-wallet-path /opt/walrus/outputs/sui_admin.yaml \
  --sui-amount 1000000000000 \
  --sui-client-request-timeout 90s \
  || die "Failed to generate dry-run configs"
