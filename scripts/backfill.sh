#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# This script runs the blob backfill from a backup store on GCS.
#
# The script will spawn multiple processes in pairs. In each pair, one process pull the blobs from
# GCS into a subdirectory based on the blob's prefix, and the other process encodes the blobs and
# pushes the slivers to the node to backfill.
#
# Before running the script, set the node IDs for the storage nodes to be backfilled, and the name
# of the GCS bucket in which the backup is stored.

root_dir=/tmp/backfill
all_blobs_file="$root_dir"/all-blobs.txt
walrus_bin="$root_dir"/bin/walrus
node_ids="0xCHANGE_NODE_ID"
gcs_bucket=add-gcs-bucket  # E.g., walrus-backup-mainnet

# Validate that the node_ids variable has been updated from its placeholder value.
if [ "$node_ids" = "0xCHANGE_NODE_ID" ]; then
  echo "Error: The node_ids variable is still set to the placeholder value '0xCHANGE_NODE_ID'."
  echo "Please replace it with the actual node IDs before running the script."
  exit 1
fi

# Validate that the GCS bucket name has been set
if [ "$gcs_bucket" = "add-gcs-bucket" ]; then
  echo "Error: The GCS bucket name must be set before running the script."
  echo "Please replace the placeholder value 'add-gcs-bucket' with the actual bucket name."
  exit 1
fi

run_pull_with_prefix() {
  prefix="$1"
  dir="$root_dir"/"$prefix"
  mkdir -p "$dir" ||:
  backfill_dir="$dir"/staging
  log_file="$dir"/log_pull.txt

  echo "Running read job with prefix $prefix"
  echo "Logging to $log_file"
  echo
  <$all_blobs_file \
    RUST_BACKTRACE=1 \
    RUST_LOG=info \
    "$walrus_bin" \
    pull-archive-blobs \
      --prefix "$prefix" \
      --pulled-state "$dir"/pulled-state.txt \
      --gcs-bucket $gcs_bucket \
      --backfill-dir "$backfill_dir" \
    |& tee -a "$log_file" \
    >/dev/null
  }

run_backfill_with_prefix() {
  prefix="$1"
  dir="$root_dir"/"$prefix"
  mkdir -p "$dir" ||:

  backfill_dir="$dir"/staging
  log_file="$dir"/log_push.txt
  echo "Running backfill job with prefix $prefix"
  echo "Logging to $log_file"
  echo

  RUST_LOG=info \
    RUST_BACKTRACE=1 \
    "$walrus_bin" \
    blob-backfill \
      --backfill-dir "$backfill_dir" \
      --pushed-state "$dir"/pushed-state.txt \
      $node_ids \
    |& tee -a "$log_file" \
    >/dev/null
}

run_backfill_pair_with_prefix() {
  prefix="$1"
  run_pull_with_prefix "$prefix" &
  run_backfill_with_prefix "$prefix" &
}

for prefix in {{a..z},{A..Z},{0..9},-,_}; do
  run_backfill_pair_with_prefix "$prefix"
done
