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

function usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "OPTIONS:"
    echo "  -c <committee_size>   Specify number of storage nodes (default: 4)"
    echo "  -s <n_shards>         Specify number of shards (default: 10)"
    echo "  -n <network>          Specify network to generate configs for (default: devnet)"
    echo "  -e                    Use existing config"
    echo "  -h                    Print this usage message"
}



function run_node() {
    cmd="cargo run --bin walrus-node -- run \
    --config-path $working_dir/$1.yaml \
    ${2:-} \
    |& tee $working_dir/$1.log"
    echo $cmd
    tmux new -d -s "$1" "$cmd"
}

existing=false
committee_size=4 # Default value of 4 if no argument is provided
shards=10 # Default value of 4 if no argument is provided
network=devnet

while getopts ":n:c:she" arg; do
    case "${arg}" in
        n)
            network=${OPTARG}
            ;;
        c)
            committee_size=${OPTARG}
            ;;
        s)
            shards=${OPTARG}
            ;;
        e)
            existing=true
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
    esac
done

if ! [ "$committee_size" -gt 0 ] 2>/dev/null; then
    echo "Invalid argument: $committee_size is not a valid positive integer."
    usage
    exit 1
fi

if ! [ "$shards" -ge "$committee_size" ] 2>/dev/null; then
    echo "Invalid argument: $shards is not an integer greater than or equal to 'committee_size'."
    usage
    exit 1
fi

networks=("devnet", "testnet", "localnet")
if ! [[ ${networks[@]} =~ $network ]]; then
    echo "Invalid argument: $network is not a valid network (${networks[@]})."
    usage
    exit 1
fi


# Set working directory
working_dir="./working_dir"

if ! $existing; then
    # Generate configs
    echo Generating configuration...
    cargo run --bin walrus-node -- generate-dry-run-configs \
    --working-dir $working_dir --committee-size $committee_size --n-shards $shards \
    --sui-network $network
    # Spawn nodes
    for i in $(seq -w 0 $((committee_size-1))); do
        run_node "dryrun-node-$i" "--cleanup-storage"
    done

    echo "Spawned $committee_size nodes with a total of $shards shards in separate tmux sessions."
else
    i=0
    for config in $( ls $working_dir/dryrun-node-*.yaml ); do
        node_name=$(basename -- "$config")
        node_name="${node_name%.*}"
        run_node $node_name
        ((i++))
    done

    echo "\nSpawned $i existing nodes in separate tmux sessions."
fi



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
