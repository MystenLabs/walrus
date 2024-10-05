# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

#!/bin/bash

./target/debug/walrus-node generate-config \
        --storage-path ./db \
        --protocol-key-pair-path ./test_protocol.key \
        --network-key-pair-path ./test_network.key \
        --sui-rpc https://fullnode.devnet.sui.io:443 \
        --system-object 0xd06f23668d1d337df633b5b647c3462b0a5bc588f5f2f5a6b6c130015a9712cc \
        --staking-object 0xba4df210f3bfffe862717cea7d4ca3bd05677ad6cadfd4f98116852b53bb17b0 \
        --wallet-config ./sui_client.yaml \
        --storage-price 10 \
        --write-price 5 \
        --node-capacity 250000000000 \
        --metrics-address 127.0.0.1:9188 \
        --rest-api-address 127.0.0.1:9189 \
        --name "walrus-node" \
        --config-path ./node_config.yaml
