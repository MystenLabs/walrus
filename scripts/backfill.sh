#!/bin/bash
root_dir=/tmp/backfill
all_blobs_file="$root_dir"/all-blobs.txt
walrus_bin="$root_dir"/bin/walrus
node_ids="0x1173be0a597b193938b392bf9a6cab78d09b7faccde2883683758b2d8dc21e30"

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
      --gcs-bucket walrus-backup-mainnet \
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
