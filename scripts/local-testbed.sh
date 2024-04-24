# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
#!/bin/bash

set -euo pipefail

trap ctrl_c INT
function ctrl_c() {
    tmux kill-server
    exit 0
}

(tmux kill-server || true) 2>/dev/null

committee_size=${1:-4} # Default value of 4 if no argument is provided
if ! [ "$committee_size" -gt 0 ] 2>/dev/null; then
    echo "Invalid argument: $1 is not a valid positive integer."
    echo "Usage: $0 [<committee_size>] [<shards>]"
    exit 1
fi
shards=${2:-10} # Default value of 10 if no argument is provided
if ! [ "$shards" -ge "$committee_size" ] 2>/dev/null; then
    echo "Invalid argument: $2 is not an integer greater than or equal to 'committee_size'."
    echo "Usage: $0 [<committee_size>] [<shards>]"
    exit 1
fi

# Set working directory
working_dir="./working_dir"

# Print configs
echo Generating configuration...
cargo run --bin walrus-node -- generate-dry-run-configs \
--working-dir $working_dir --committee-size $committee_size --n-shards $shards

# Spawn nodes
for i in $(seq -w 0 $((committee_size-1))); do
    tmux new -d -s "n$i" \
    "cargo run --bin walrus-node -- run \
    --config-path $working_dir/dryrun-node-$i.yaml --cleanup-storage \
    |& tee $working_dir/n$i.log"
done

echo "\nSpawned $committee_size nodes in separate tmux sessions handing a total of $shards shards."

# Instructions to run a client
cat << EOF

To store a file (e.g., the README.md) on the testbed, use the following command:
$ cargo run --bin client -- --config working_dir/client_config.yaml store README.md

You can then read the stored file by running the following (replacing "\$BLOB_ID" by the blob ID \
returned by the store operation):
$ cargo run --bin client -- --config working_dir/client_config.yaml read \$BLOB_ID
EOF

while true; do
    sleep 1
done
