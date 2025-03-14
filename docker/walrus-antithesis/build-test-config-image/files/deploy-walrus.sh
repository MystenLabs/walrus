#!/bin/bash
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

# use EPOCH_DURATION to set the epoch duration, default is 10 minutes in antithesis test.
EPOCH_DURATION=${EPOCH_DURATION:-10m}

cd /opt/walrus

rm -rf /opt/walrus/outputs/*
ls -al /opt/walrus/contracts

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
  --epoch-duration $EPOCH_DURATION >/opt/walrus/outputs/deploy

/opt/walrus/bin/walrus-deploy generate-dry-run-configs \
  --working-dir /opt/walrus/outputs
