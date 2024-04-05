# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

#!/bin/bash

trap ctrl_c INT
function ctrl_c() {
    tmux kill-server
    exit 0
}

(tmux kill-server || true) 2>/dev/null

if [ -n "$1" ]; then
    if [ "$1" -eq "$1" ] 2>/dev/null; then
        committee_size=$1
    else
        echo "Invalid argument: $1 is not a valid integer."
        echo "Usage: $0 [<committee_size>] [<shards>]"
        exit 1
    fi
else
    committee_size=4 # Default value if no argument is provided
fi

if [ -n "$2" ]; then
    if [ "$2" -eq "$2" ] 2>/dev/null; then
        shards=$2
    else
        echo "Invalid argument for shards: $2 is not a valid integer."
        echo "Usage: $0 [<committee_size>] [<shards>]"
        exit 1
    fi
else
    shards=10 # Default value if no argument is provided
fi

# Set working directory
working_dir="./working_dir"

# Print configs
cargo run --bin walrus-node -- generate-dry-run-configs \
--working-dir $working_dir --committee-size $committee_size --total-shards $shards

# Spawn nodes
for i in $(seq 0 $((committee_size-1))); do
    tmux new -d -s "n$i" \
    "cargo run --bin walrus-node -- run \
    --config-path $working_dir/dryrun-node-$i.yaml --cleanup-storage \
    from-client-config --client-config-path $working_dir/client_config.yaml \
    --storage-node-index $i |& tee $working_dir/n$i.log"
done

echo "Spawned $committee_size nodes in separate tmux sessions handing a total of $shards shards."

# Instructions to run a client
echo "\nTo run a client, use the following command:"
echo "$ echo \"123456\" > blob.txt"
echo "$ cargo run --bin client -- --config working_dir/client_config.yaml store blob.txt"

while true; do
    sleep 1
done
