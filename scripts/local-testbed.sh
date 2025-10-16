#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

trap ctrl_c INT

join_by() {
  delim_save="$1"
  delim=""
  shift
  str=""
  for arg in "$@"; do
    str="$str$delim$arg"
    delim="$delim_save"
  done
  echo "$str"
}

kill_tmux_sessions() {
  { tmux ls || true; } | { grep -o "dryrun-node-\d*" || true; } | xargs -rn1 tmux kill-session -t
}

ctrl_c() {
  kill_tmux_sessions
  exit 0
}

kill_tmux_sessions

usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "OPTIONS:"
  echo "  -b <database_url>     Specify a backup database url (ie: postgresql://postgres:postgres@localhost/postgres, default: none)"
  echo "  -c <committee_size>   Number of storage nodes (default: 4)"
  echo "  -d <duration>         Set the length of the epoch (in human readable format, e.g., '60s', default: 1h)"
  echo "  -e                    Use existing config"
  echo "  -f                    Tail the logs of the nodes (default: false)"
  echo "  -g                    Enable garbage collection (blob info cleanup and data deletion)"
  echo "  -h                    Print this usage message"
  echo "  -l <rust_log>         Set RUST_LOG environment variable for all nodes (default: info)"
  echo "  -n <network>          Sui network to generate configs for (default: devnet)"
  echo "  -s <n_shards>         Number of shards (default: 10)"
  echo "  -t                    Use testnet contracts"
  echo "  -a <ip_address>       Specify the IP address that is used for all nodes (default: 127.0.0.1)"
}

run_node() {
  cmd="RUST_LOG=$rust_log ./target/release/walrus-node run --config-path $working_dir/$1.yaml ${2:-} \
    |& tee -a $working_dir/$1.log"
  echo "Running within tmux: '$cmd'..."
  tmux new -d -s "$1" "$cmd"
}


backup_database_url=
committee_size=4 # Default value of 4 if no argument is provided
epoch_duration=1h
network=devnet
rust_log=info # Default RUST_LOG level
shards=10 # Default value of 4 if no argument is provided
tail_logs=false
use_existing_config=false
contract_dir="./contracts"
host_address="127.0.0.1"
enable_garbage_collection=false

while getopts "b:c:d:efghl:n:s:ta:" arg; do
  case "${arg}" in
    f)
      tail_logs=true
      ;;
    g)
      enable_garbage_collection=true
      ;;
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
      use_existing_config=true
      ;;
    b)
      backup_database_url=${OPTARG}
      ;;
    t)
      contract_dir="./testnet-contracts"
      ;;
    a)
      host_address=${OPTARG}
      ;;
    h)
      usage
      exit 0
      ;;
    l)
      rust_log=${OPTARG}
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

if $use_existing_config; then
  echo "$0: Using existing config"
else
  echo "$0: Using network: $network"
  echo "$0: Using committee_size: $committee_size"
  echo "$0: Using shards: $shards"
  echo "$0: Using epoch_duration: $epoch_duration"
  echo "$0: Using RUST_LOG: $rust_log"
  echo "$0: Using backup_database_url: $backup_database_url"
  echo "$0: Using garbage collection: $enable_garbage_collection"
fi


if ! $use_existing_config; then
  if [[ -n "$backup_database_url" ]]; then
    echo "Reverting database migrations to ensure walrus-backup is starting fresh... [backup_database_url=$backup_database_url]"
    diesel migration --database-url "$backup_database_url" revert --all ||:
    diesel migration --database-url "$backup_database_url" run

    # shellcheck disable=SC2207
    schema_files=( $(git ls-files '**/schema.rs') )

    # Cleanup the output of the diesel migration. (Annoying by-product of limited diesel support for licenses and formatting.)
    pre-commit run licensesnip --files "${schema_files[@]}" 1>/dev/null 2>&1 ||:
    pre-commit run cargo-fmt --files "${schema_files[@]}" 1>/dev/null 2>&1 ||:
  fi
fi


features=( deploy )
binaries=( walrus walrus-node walrus-deploy )
if [[ -n "$backup_database_url" ]]; then
  features+=( backup )
  binaries+=( walrus-backup )
fi

echo "Building $(join_by ', ' "${binaries[@]}") binaries..."
# shellcheck disable=SC2046
cargo build \
  --release \
  $(printf -- "--bin %s " "${binaries[@]}") \
  --features "$(join_by , "${features[@]}")"

# Set working directory
working_dir="./working_dir"

# Derive the ip addresses for the storage nodes
ips=( )
for node_count in $(seq 1 "$committee_size"); do
  ips+=( "${host_address}" )
done

# Initialize cleanup to be empty
cleanup=

if ! $use_existing_config; then
  # Cleanup
  find contracts -name 'build' -type d -exec rm -rf {} +
  rm -f $working_dir/dryrun-node-*.yaml
  rm -f $working_dir/dryrun-node-*.log
  cleanup="--cleanup-storage"

  # Deploy system contract
  echo Deploying system contract...
  ./target/release/walrus-deploy deploy-system-contract \
    --working-dir $working_dir \
    --sui-network "$network" \
    --n-shards "$shards" \
    --host-addresses "${ips[@]}" \
    --storage-price 5 \
    --write-price 1 \
    --epoch-duration "$epoch_duration" \
    --contract-dir "$contract_dir" \
    --with-wal-exchange

  # Generate configs
  generate_dry_run_args=( --working-dir "$working_dir" )
  if [[ -n "$backup_database_url" ]]; then
    generate_dry_run_args+=( --backup-database-url "$backup_database_url" )
  fi
  echo "Generating configuration [${generate_dry_run_args[*]}]..."
  ./target/release/walrus-deploy generate-dry-run-configs "${generate_dry_run_args[@]}"

  echo "
event_processor_config:
  adaptive_downloader_config:
  max_workers: 2
  initial_workers: 2" | \
      tee -a $working_dir/dryrun-node-*[0-9].yaml >/dev/null

  # Add garbage collection configuration if enabled
  if $enable_garbage_collection; then
    echo "
db_config:
  global:
    experimental_use_optimistic_transaction_db: true
garbage_collection:
  enable_blob_info_cleanup: true
  enable_data_deletion: true" | \
        tee -a $working_dir/dryrun-node-*[0-9].yaml >/dev/null
  fi
fi

node_count=0
#
# shellcheck disable=SC2045
for config in $( ls $working_dir/dryrun-node-*[0-9].yaml ); do
  node_name=$(basename -- "$config")
  node_name="${node_name%.*}"
  run_node "$node_name" "$cleanup"
  ((node_count++))
done

echo "
Spawned $node_count nodes in separate tmux sessions. (See \`tmux ls\` for the list of tmux sessions.)

Client configuration stored at '$working_dir/client_config.yaml'.
See README.md for further information on the Walrus client."

if $tail_logs; then
  tail -F "$working_dir"/dryrun-node-*.log | grep --line-buffered --color -Ei "ERROR|CRITICAL|^"
else
  echo "Press Ctrl+C to stop the nodes."
  while (( 1 )); do
    sleep 120
  done
fi
