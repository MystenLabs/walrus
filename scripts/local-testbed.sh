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

nets=("devnet", "testnet", "localnet")

function usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "OPTIONS:"
    echo "  -c <committee_size>   Number of storage nodes (default: 4)"
    echo "  -s <n_shards>         Number of shards (default: 10)"
    echo "  -n <network>          Network (${nets[@]}; default: devnet) to generate configs for"
    echo "  -d <duration>         Set the length of the epoch (in human readable format, e.g., '60s', default: 1h)"
    echo "  -e                    Use existing config"
    echo "  -h                    Print this usage message"
}

function run_node() {
    cmd="./target/debug/walrus-node run --config-path $working_dir/$1.yaml ${2:-} \
        |& tee $working_dir/$1.log"
    echo $cmd
    tmux new -d -s "$1" "$cmd"
}


existing=false
committee_size=4 # Default value of 4 if no argument is provided
shards=10 # Default value of 4 if no argument is provided
network=devnet
epoch_duration=1h

while getopts "n:c:s:d:eh" arg; do
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
        d)
            epoch_duration=${OPTARG}
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

if ! [[ ${nets[@]} =~ $network ]]; then
    echo "Invalid argument: $network is not a valid network (${nets[@]})."
    usage
    exit 1
fi


echo Building walrus-node and walrus-deploy binaries...
cargo build --bin walrus-node --bin walrus-deploy

# Set working directory
working_dir="./working_dir"

# Derive the ip addresses for the storage nodes
ips=" "
for i in $(seq 1 $committee_size); do
    ips+="127.0.0.1 "
done

# Initialize cleanup to be empty
cleanup=

if ! $existing; then
    # Cleanup
    rm -f $working_dir/dryrun-node-*.yaml
    cleanup="--cleanup-storage"

    # Deploy system contract
    echo Deploying system contract...
    cargo run --bin walrus-deploy -- deploy-system-contract \
    --working-dir $working_dir --sui-network $network --n-shards $shards --host-addresses $ips \
    --storage-price 5 --write-price 1 --epoch-duration $epoch_duration

    # Generate configs
    echo Generating configuration...
    cargo run --bin walrus-deploy -- generate-dry-run-configs \
    --working-dir $working_dir --use-legacy-event-provider
fi

i=0
for config in $( ls $working_dir/dryrun-node-*[0-9].yaml ); do
    node_name=$(basename -- "$config")
    node_name="${node_name%.*}"
    run_node $node_name $cleanup
    ((i++))
done

echo "\nSpawned $i nodes in separate tmux sessions."

echo "\nClient configuration stored at working_dir/client_config.yaml."
echo "See README.md for further information on the Walrus client."

while true; do
    sleep 1
done
