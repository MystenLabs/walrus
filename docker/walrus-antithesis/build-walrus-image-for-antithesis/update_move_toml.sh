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
    # Replace git-based Sui dependencies with local dependencies
    # Pattern: <package> = { git = <url>, subdir = <dir>, rev = <rev> }
    # Replacement: <package> = { local = "/opt/sui/<dir>" }
    perl -i -pe 's{
        (\w+)\s*=\s*\{\s*
        git\s*=\s*"https://github\.com/MystenLabs/sui\.git"\s*,\s*
        subdir\s*=\s*"([^"]+)"\s*,\s*
        rev\s*=\s*"[^"]+"\s*
        \}
    }{$1 = { local = "/opt/sui/$2" }}gx' "$toml_file"

    # Delete the Move.lock. Under the new package management the loader resolves
    # dependencies from the lockfile's `[pinned.testnet]` entries, which still point
    # `std`/`sui` at git sources and trigger a network `git clone` that fails in the
    # sealed antithesis environment. Removing the lockfile forces the loader to repin
    # from the manifest above, which now references the local `/opt/sui` copy.
    rm -f "/contracts/${contract}/Move.lock"
done
