#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

RUN=0
RUNS=2
COMMITTEE_SIZE=4
SHARDS=20
NUM_SERVERS_TO_CRASH=1
NETWORK=devnet
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
WORKING_DIR=./working_dir/crash_recovery_testing
RUN_DIR=$WORKING_DIR/$TIMESTAMP
SUI_CLI="sui client"
WALRUS_CLI="cargo run --bin walrus"
WALRUS_NODE="cargo run --bin walrus-node"
BLOBS_PER_RUN=5
BLOB_IDS=()

function log(){
    echo "[$(date +'%T')] $1"
}

# Generate a random file of size $1 MB
function generate() {
    local size=$1
    local filename=$2

    if [[ -z "$size" || -z "$filename" ]]; then
        log "cannot generate a file for unspecified size/filename"
        return 1
    fi
    log "Generating file of size $size MB..."
    dd if=/dev/urandom of="$filename" bs=1M count="$size" iflag=fullblock > "${RUN_DIR}"/"${RUN}"/log/err 2>&1
}

# Run a node
function run_node() {
    cmd="${WALRUS_NODE} run --config-path $1"
    log "$cmd"
    $cmd > "${RUN_DIR}"/"${RUN}"/log/err 2>&1 &
    PID=$!
    sleep 5
    log "ensuring that storage node with pid ${PID} is running..."
    ps -p ${PID} > "${RUN_DIR}"/"${RUN}"/log/err
}

# Start storage nodes
function start_storage_nodes() {
    for i in $(seq 0 $((COMMITTEE_SIZE - 1))); do
        sed 's|storage_path.*|storage_path: \
        '"${RUN_DIR}/${RUN}/data/storage-node-${i}"'|' \
        "${RUN_DIR}/dryrun-node-${i}.yaml" > "${RUN_DIR}/${RUN}/storage-node-${i}.yaml"
        run_node "${RUN_DIR}/${RUN}/storage-node-${i}.yaml"
    done
    log "servers are up and running"
}

# Prepare client, get gas from faucet, etc
function prepare_client() {
    cmd="${SUI_CLI} --client.config ${RUN_DIR}/sui_client.yaml faucet \
    --url https://faucet.devnet.sui.io/v1/gas"
    $cmd > "${RUN_DIR}"/"${RUN}"/log/err 2>&1
    log "waiting for coin"
    sleep 10
    cmd="${SUI_CLI} --client.config ${RUN_DIR}/sui_client.yaml gas"
    $cmd > "${RUN_DIR}"/"${RUN}"/log/err 2>&1
    log "client is ready"
}

# Store one file
function store_file() {
    file=$1
    log "Storing blob $file..."
    cmd="${WALRUS_CLI} -- --config $RUN_DIR/client_config.yaml store $file"
    log "$cmd"
    blob_id=$($cmd 2>&1 | grep "Blob ID" | awk '{print $3}')
    log "Stored blob id: $blob_id"
    mkdir -p "${RUN_DIR}/data/$blob_id"
    mv "$file" "${RUN_DIR}/data/$blob_id/original"
    BLOB_IDS+=("$blob_id")
}

# Kill the server
kill_server(){
    # Receive the value of kill signal eg 9,4
    local SIG=$1
    # Receive the process ID to be killed
    local MPID=$2
    log "killing server with pid: $MPID"
    { kill -"$SIG" "${MPID}" && wait "${MPID}"; } 2> "${RUN_DIR}"/log/err || true
}

# Kill some servers
function kill_servers() {
    local num_servers=$1
    local KILLPID
    KILLPID=$(ps -ef | grep "${WORKING_DIR}" | grep -v grep | \
    awk '{print $2}' | tr '\n' ' ' || true)
    IFS=' ' read -r -a KILLPID_ARRAY <<< "$KILLPID"
    # Check if there are any processes to kill
    if [ ${#KILLPID_ARRAY[@]} -eq 0 ]; then
        log "No processes to kill."
        return 0
    fi
    local SHUFFLED
    SHUFFLED=($(printf "%s\n" "${KILLPID_ARRAY[@]}" | sort -R))
    # Adjust num_servers to not exceed the array length
    if [ "$num_servers" -gt "${#SHUFFLED[@]}" ]; then
        num_servers=${#SHUFFLED[@]}
    fi
    for i in "${SHUFFLED[@]:0:$num_servers}"
    do
        kill_server 9 "$i"
    done
}

crash_and_store() {
    log "Ensuring there are no relevant walrus servers running..."
    kill_servers $(ps -ef | grep "${WORKING_DIR}" | grep -v grep | wc -l)
    log "Generating new RUN workdir ${RUN_DIR}/${RUN}..."
    mkdir -p "${RUN_DIR}"/"${RUN}"/log/
    mkdir -p "${RUN_DIR}"/"${RUN}"/data/

    if [[ ${RUN} -gt 1  ]]; then
        # shellcheck disable=SC2004
        log "Copying datadir from RUN ${RUN_DIR}/$((${RUN}-1)) into ${RUN_DIR}/${RUN}..."
        cp -fr "${RUN_DIR}"/$((${RUN}-1))/data/ "${RUN_DIR}"/"${RUN}"/data 2>&1
    fi

    log "Starting storage nodes..."
    start_storage_nodes

    log "Crashing $NUM_SERVERS_TO_CRASH storage nodes..."
    kill_servers $NUM_SERVERS_TO_CRASH

    log "Preparing client..."
    prepare_client

    log "Storing blobs..."

    # Store files
    for i in $(seq 1 $BLOBS_PER_RUN); do
        generate 1 "$RUN_DIR/${RUN}/data/blob$i"
        store_file "$RUN_DIR/${RUN}/data/blob$i"
    done
}

function read_files() {
    # Read all the files back and compare
    log "Reading all files back and comparing..."
    for blob_id in "${BLOB_IDS[@]}"; do
        # Get all network addresses
        host_addresses=()
        for i in $(seq 0 $((COMMITTEE_SIZE - 1))); do
            host_addresses+=("
            $(grep "rest_api_address:" "${RUN_DIR}/${RUN}/storage-node-${i}.yaml" | \
            awk '{print $2}' | tr -d '"')")
        done
        for host_address in "${host_addresses[@]}"; do
            get_blob_confirmation "$blob_id" "$host_address"
            if [ $? -ne 0 ]; then
                echo "Error getting confirmations for blob $blob_id"
                return 1
            fi
        done
        log "Got storage confirmations for blob $blob_id from all nodes"
        log "Reading blob $blob_id..."
        cmd="${WALRUS_CLI} -- --config $RUN_DIR/client_config.yaml read $blob_id \
        --out ${RUN_DIR}/data/${blob_id}/downloaded"
        log "$cmd"
        $cmd > "${RUN_DIR}"/"${RUN}"/log/err 2>&1
        if [ ! -f "${RUN_DIR}/data/${blob_id}/downloaded" ]; then
            log "Blob $blob_id not found in storage after read"
            exit 1
        fi
        diff "${RUN_DIR}"/data/"${blob_id}/original" "${RUN_DIR}"/data/"${blob_id}"/downloaded
        log "Blob $blob_id read successfully"
    done
}

function get_blob_confirmation() {
    blob_id=$1
    host_address=$2
    #/v1/blobs/{blob_id}/confirmations
    cmd="curl -s -X GET https://${host_address}/v1/blobs/${blob_id}/confirmation --insecure"
    log "$cmd"
    output=$($cmd)
    log "Blob confirmation: $output"
    if echo "$output" | grep -q "success"; then
        return 0
    fi
    return 1
}

function store_files() {
    ips=" "
    for i in $(seq 1 $COMMITTEE_SIZE); do
        ips+="127.0.0.1 "
    done

    # Deploy system contract
    log "Deploying system contract..."
    $WALRUS_NODE -- deploy-system-contract \
    --working-dir "$RUN_DIR" --sui-network $NETWORK --n-shards $SHARDS --host-addresses $ips > "${RUN_DIR}"/log/err 2>&1

    # Generate configs
    log "Generating configuration..."
    $WALRUS_NODE -- generate-dry-run-configs --working-dir "$RUN_DIR" > "${RUN_DIR}"/log/err 2>&1

    # Start actual crash recovery testing
    log "Starting crash recovery testing iterations..."
    for i in $(seq 1 ${RUNS}); do
        RUN=$i
        log "====== RUN #${RUN} ======"
        crash_and_store
    done
}

function crash_recovery() {
    mkdir -p "${RUN_DIR}/log"
    RUN_DIR=$( realpath "${RUN_DIR}" )
    WORKING_DIR=$( realpath "${WORKING_DIR}" )
    store_files
    kill_servers $(ps -ef | grep "${WORKING_DIR}" | grep -v grep | wc -l)
    for i in $(seq 0 $(($COMMITTEE_SIZE - 1))); do
        run_node "${RUN_DIR}/${RUN}/storage-node-${i}.yaml"
    done
    attempt=0
    while [ $attempt -lt 10 ]; do
        read_files
        if [ $? -eq 0 ]; then
            log "Crash recovery test passed."
            exit 0
        fi
        attempt=$((attempt + 1))
        sleep 10
    done
    exit 1
}

crash_recovery
