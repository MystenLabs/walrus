#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
# This script allows creating unsigned transactions for operations that need to be signed
# by a Walrus multisig address.
# Intended to be used using the github workflow defined in
# `../.github/workflows/create-tx-for-multisig.yml`

GAS_OBJECT_ID=""
TX_TYPE=""
BASE_SUBSIDY_FROST=""
PER_SHARD_SUBSIDY_FROST=""
SUBSIDY_RATE_BASIS_POINTS=""
WAL_AMOUNT_FROST=""
WAL_COIN_OBJECT_ID=""

# Addresses
WALRUS_ADMIN="0x62a69ba94e191634841cc4d196e70ec3e4667fc78013ae6d7405a0c593b39f1e"
WALRUS_OPS="0x23eb7ccbbb4a21afea8b1256475e255b3cd84083ca79fa1f1a9435ab93d2b71b"
WALRUS_SUBSIDIES_FUNDER="0xa09dd2e4c0f68a0e52c89f40e818be2ee33523b8600e28a4cc7285a0ae2b9821"

# Object IDs
WALRUS_SUBSIDIES_UPGRADE_CAP="0x0f73338243cc49e217d49ecfad806fa80f3ef48357e9b12614d8e57027fa0a75"
WALRUS_SUBSIDIES_ADMIN_CAP="0xd62d3d5b43dae752464afa2a8354fe52e0c31619778e8ab88c2e9a37c8349d04"


usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "OPTIONS:"
  echo "  -t <tx_type> Transaction type, mandatory ('upgrade-walrus-subsidies', 'set-walrus-subsidy-rates', 'add-wal-to-subsidies')"
  echo "  -g <obj_id>  Gas object ID, defaults to gas object with highest balance"
  echo "  -b <base_subsidy_frost> Base subsidy in FROST (only for set-walrus-subsidy-rates)"
  echo "  -s <per_shard_subsidy_frost> Per shard subsidy in FROST (only for set-walrus-subsidy-rates)"
  echo "  -r <subsidy_rate_basis_points> Subsidy rate in basis points (only for set-walrus-subsidy-rates)"
  echo "  -w <wal_amount_frost> Amount of WAL in FROST to add (only for add-wal-to-subsidies)"
  echo "  -c <wal_coin_object_id> WAL coin object ID to use (only for add-wal-to-subsidies, defaults to merging all WAL coins)"
}

# List all WAL coin object IDs for the given address, sorted by balance descending.
WAL_PKG_ADDR="356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59"

all_wal_coins_for_addr() {
  ADDRESS=$1
  sui client objects "$ADDRESS" --json \
    | jq -r --arg wa "$WAL_PKG_ADDR" \
      'def to_hex: (. / 16 | floor) as $hi | (. % 16) as $lo
        | "0123456789abcdef"[$hi:$hi+1] + "0123456789abcdef"[$lo:$lo+1];
      def bcs_u64: . as $a | reduce range(8) as $i (0; . + ($a[$i] * pow(256; $i)));
      [.[] | select(.data.Move.type_ | type == "object"
        and .Coin.struct.address == $wa and .Coin.struct.module == "wal")
      | { id: ("0x" + (.data.Move.contents[0:32] | map(to_hex) | join(""))),
          balance: (.data.Move.contents[32:40] | bcs_u64) }]
      | sort_by(-.balance) | .[].id'
}

gas_obj_for_addr() {
  ADDRESS=$1
  if [[ -z $GAS_OBJECT_ID ]]; then
    sui client gas "$ADDRESS" --json | jq -r 'max_by(.mistBalance) | .gasCoinId'
  else
    echo "$GAS_OBJECT_ID"
  fi
}

gas_budget_for_addr() {
  ADDRESS=$1
  GAS_COIN=$2
  sui client gas "$ADDRESS" --json \
    | jq -r --arg g "$GAS_COIN" '.[] | select(.gasCoinId == $g) | .mistBalance'
}

subsidies_pkg() {
  sui client object "$WALRUS_SUBSIDIES_UPGRADE_CAP" --json \
    | jq -r 'def to_hex: (. / 16 | floor) as $hi | (. % 16) as $lo
        | "0123456789abcdef"[$hi:$hi+1] + "0123456789abcdef"[$lo:$lo+1];
      "0x" + (.data.Move.contents[32:64] | map(to_hex) | join(""))'
}

subsidies_object() {
  sui client object "$WALRUS_SUBSIDIES_ADMIN_CAP" --json \
    | jq -r 'def to_hex: (. / 16 | floor) as $hi | (. % 16) as $lo
        | "0123456789abcdef"[$hi:$hi+1] + "0123456789abcdef"[$lo:$lo+1];
      "0x" + (.data.Move.contents[32:64] | map(to_hex) | join(""))'
}

upgrade_walrus_subsidies() {
  GAS=$(gas_obj_for_addr "$WALRUS_ADMIN")
  GAS_BUDGET=$(gas_budget_for_addr "$WALRUS_ADMIN" "$GAS")
  CONTRACT_DIR=mainnet-contracts/walrus_subsidies
  sui client \
    upgrade \
    --gas "$GAS" \
    --gas-budget "$GAS_BUDGET" \
    --upgrade-capability "$WALRUS_SUBSIDIES_UPGRADE_CAP" \
    "$CONTRACT_DIR" \
    --serialize-unsigned-transaction
}

set_walrus_subsidy_rates() {
  GAS=$(gas_obj_for_addr "$WALRUS_OPS")
  GAS_BUDGET=$(gas_budget_for_addr "$WALRUS_OPS" "$GAS")
  WALRUS_SUBSIDIES_PKG=$(subsidies_pkg)
  WALRUS_SUBSIDIES_OBJECT=$(subsidies_object)

  CMD="sui client ptb --gas-coin @$GAS --gas-budget $GAS_BUDGET"
  if [[ -n $BASE_SUBSIDY_FROST ]]; then
    CMD="$CMD --move-call $WALRUS_SUBSIDIES_PKG::walrus_subsidies::set_base_subsidy \
    @$WALRUS_SUBSIDIES_OBJECT @$WALRUS_SUBSIDIES_ADMIN_CAP $BASE_SUBSIDY_FROST"
  fi
  if [[ -n $PER_SHARD_SUBSIDY_FROST ]]; then
    CMD="$CMD --move-call $WALRUS_SUBSIDIES_PKG::walrus_subsidies::set_per_shard_subsidy \
    @$WALRUS_SUBSIDIES_OBJECT @$WALRUS_SUBSIDIES_ADMIN_CAP $PER_SHARD_SUBSIDY_FROST"
  fi
  if [[ -n $SUBSIDY_RATE_BASIS_POINTS ]]; then
    CMD="$CMD --move-call $WALRUS_SUBSIDIES_PKG::walrus_subsidies::set_system_subsidy_rate \
    @$WALRUS_SUBSIDIES_OBJECT @$WALRUS_SUBSIDIES_ADMIN_CAP $SUBSIDY_RATE_BASIS_POINTS"
  fi
  $CMD --serialize-unsigned-transaction
}


add_wal_to_subsidies() {
  if [[ -z $WAL_AMOUNT_FROST && -z $WAL_COIN_OBJECT_ID ]]; then
    echo "Error: specify WAL amount in FROST (-w), a WAL coin object ID (-c), or both" >&2
    exit 1
  fi
  GAS=$(gas_obj_for_addr "$WALRUS_SUBSIDIES_FUNDER")
  GAS_BUDGET=$(gas_budget_for_addr "$WALRUS_SUBSIDIES_FUNDER" "$GAS")
  WALRUS_SUBSIDIES_PKG=$(subsidies_pkg)
  WALRUS_SUBSIDIES_OBJECT=$(subsidies_object)

  if [[ -n $WAL_AMOUNT_FROST ]]; then
    # Build a PTB that merges all WAL coins (if needed), splits the amount, and calls add_coin.
    CMD=(sui client ptb --gas-coin "@$GAS" --gas-budget "$GAS_BUDGET")

    if [[ -n $WAL_COIN_OBJECT_ID ]]; then
      # Use the specific coin provided.
      PRIMARY_COIN="$WAL_COIN_OBJECT_ID"
    else
      # Fetch all WAL coins and merge them into one.
      mapfile -t WAL_COINS < <(all_wal_coins_for_addr "$WALRUS_SUBSIDIES_FUNDER")
      if [[ ${#WAL_COINS[@]} -eq 0 ]]; then
        echo "Error: no WAL coins found for $WALRUS_SUBSIDIES_FUNDER" >&2
        exit 1
      fi
      PRIMARY_COIN="${WAL_COINS[0]}"
      if [[ ${#WAL_COINS[@]} -gt 1 ]]; then
        MERGE_ARGS=$(printf ", @%s" "${WAL_COINS[@]:1}")
        MERGE_ARGS="[${MERGE_ARGS:2}]"
        CMD+=(--merge-coins "@$PRIMARY_COIN" "$MERGE_ARGS")
      fi
    fi

    CMD+=(--split-coins "@$PRIMARY_COIN" "[$WAL_AMOUNT_FROST]")
    CMD+=(--assign coin)
    CMD+=(--move-call "$WALRUS_SUBSIDIES_PKG::walrus_subsidies::add_coin" "@$WALRUS_SUBSIDIES_OBJECT" coin.0)
    "${CMD[@]}" --serialize-unsigned-transaction
  else
    # No amount specified; send the full coin directly.
    sui client \
      ptb \
      --gas-coin @"$GAS" \
      --gas-budget "$GAS_BUDGET" \
      --move-call "$WALRUS_SUBSIDIES_PKG"::walrus_subsidies::add_coin \
        @"$WALRUS_SUBSIDIES_OBJECT" @"$WAL_COIN_OBJECT_ID" \
      --serialize-unsigned-transaction
  fi
}

while getopts "t:g:b:s:r:w:c:h" arg; do
  case "${arg}" in
    t)
      TX_TYPE=${OPTARG}
      ;;
    g)
      GAS_OBJECT_ID=${OPTARG}
      ;;
    b)
      BASE_SUBSIDY_FROST=${OPTARG}
      ;;
    s)
      PER_SHARD_SUBSIDY_FROST=${OPTARG}
      ;;
    r)
      SUBSIDY_RATE_BASIS_POINTS=${OPTARG}
      ;;
    w)
      WAL_AMOUNT_FROST=${OPTARG}
      ;;
    c)
      WAL_COIN_OBJECT_ID=${OPTARG}
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

case "$TX_TYPE" in
  upgrade-walrus-subsidies)
    upgrade_walrus_subsidies
    ;;
  set-walrus-subsidy-rates)
    set_walrus_subsidy_rates
    ;;
  add-wal-to-subsidies)
    add_wal_to_subsidies
    ;;
  "")
    echo "Error: The transaction type must be specified" >&2
    exit 1
    ;;
  *)
    echo "Error: Invalid transaction type \"$TX_TYPE\"" >&2
    exit 1
esac
