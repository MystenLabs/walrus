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

for i in $(seq 0 $((committee_size-1))); do
    tmux new -d -s "n$i" \
    "cargo run --bin walrus-node -- \
    dry-run --committee-size $committee_size --total-shards $shards --index $i \
    |& tee n$i.log"
done

echo "Spawned $committee_size nodes in separate tmux sessions handing a total of $shards shards."

# Instructions to run a client
echo "\nTo run a client, use the following command:"
echo "$ echo \"123456\" > blob.txt"
echo "$ cargo run --bin client -- --config working_dir/client_config.yaml store --file blob.txt"

while true; do
    sleep 1
done
