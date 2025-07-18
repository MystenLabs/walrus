#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0


# List of contract directories to process
contracts=(
  wal
  wal_exchange
  walrus
  subsidies
  walrus_subsidies
)

for contract in "${contracts[@]}"; do
    toml_file="/contracts/${contract}/Move.toml"
    sed 's|^Sui = { git = "https://github.com/MystenLabs/sui.git", subdir = "crates/sui-framework/packages/sui-framework".*|Sui = { local = "/opt/sui/crates/sui-framework/packages/sui-framework" }|' \
        < "$toml_file" > "${toml_file}.new"
    mv "${toml_file}.new" "$toml_file"
done
