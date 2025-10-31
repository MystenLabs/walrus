#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

error() {
  echo "$0: error: $1" >&2
}

note() {
  echo "$0: note: $1" >&2
}

die() {
  echo "$0: error: $1" >&2
  exit 1
}

usage() {
  echo "Usage: $0 -f <file> -s <size> [-k] [-- <walrus store args>...]"
  echo ""
  echo "Split a file into chunks and store them using walrus store."
  echo ""
  echo "OPTIONS:"
  echo "  -f <file>             Input file to split (required)"
  echo "  -s <size>             Chunk size (e.g., 10M, 100K, 1G) (required)"
  echo "  -k                    Keep chunks after successful completion (default: delete)"
  echo "  -h                    Print this usage message"
  echo "  --                    Delimiter for walrus store arguments"
  echo ""
  echo "EXAMPLES:"
  echo "  $0 -f large_file.txt -s 10M -- --epochs 5"
  echo "  $0 -f video.mp4 -s 100M -k -- --epochs max --force"
  echo ""
  echo "The chunks will be named: basename_0.ext, basename_1.ext, etc."
  echo "Chunks are deleted on success by default, kept on failure for retry/idempotency."
}

file=""
chunk_size=""
delete_on_success=true
walrus_args=()

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f)
      file="$2"
      shift 2
      ;;
    -s)
      chunk_size="$2"
      shift 2
      ;;
    -k)
      delete_on_success=false
      shift
      ;;
    -h)
      usage
      exit 0
      ;;
    --)
      shift
      walrus_args=("$@")
      break
      ;;
    *)
      error "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

# Validate required arguments
if [[ -z "$file" ]]; then
  error "input file (-f) is required"
  usage
  exit 1
fi

if [[ -z "$chunk_size" ]]; then
  error "chunk size (-s) is required"
  usage
  exit 1
fi

if [[ ! -f "$file" ]]; then
  die "file not found: $file"
fi

# Extract basename and extension
file_basename=$(basename "$file")
file_name="${file_basename%.*}"
file_ext="${file_basename##*.}"

# Handle case where file has no extension
if [[ "$file_name" == "$file_ext" ]]; then
  file_ext=""
else
  file_ext=".$file_ext"
fi

# Create temp directory for chunks
temp_dir=$(mktemp -d -t walrus-chunks-XXXXXX)
note "splitting $file into chunks of size $chunk_size in $temp_dir..." >&2

# Split the file into chunks with numeric suffixes
split -b "$chunk_size" "$file" "$temp_dir/chunk_"

# Rename chunks to the desired format: basename_i.ext
chunk_files=()
i=0
for chunk in "$temp_dir"/chunk_*; do
  if [[ "$file_ext" == "" ]]; then
    new_name="$temp_dir/${file_name}_${i}"
  else
    new_name="$temp_dir/${file_name}_${i}${file_ext}"
  fi
  mv "$chunk" "$new_name"
  chunk_files+=("$new_name")
  ((i++))
done

note "created ${#chunk_files[@]} chunks"

# Display the chunks
for chunk in "${chunk_files[@]}"; do
  note "  - $(basename "$chunk")"
done

# Call walrus store with all chunks
note "running: walrus store ${walrus_args[*]} ${chunk_files[*]}"

if walrus store "${walrus_args[@]}" "${chunk_files[@]}"; then
  note "✓ walrus store completed successfully"

  if $delete_on_success; then
    note "deleting chunks..."
    rm -rf "$temp_dir"
    note "✓ chunks deleted"
  else
    note "keeping chunks in: $temp_dir"
  fi

  exit 0
else
  exit_code=$?
  error "✗ walrus store failed with exit code: $exit_code"
  error "keeping chunks for retry in: $temp_dir"
  exit $exit_code
fi
