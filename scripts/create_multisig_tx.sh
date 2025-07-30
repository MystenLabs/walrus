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

# Addresses
WALRUS_ADMIN="0x62a69ba94e191634841cc4d196e70ec3e4667fc78013ae6d7405a0c593b39f1e"
WALRUS_OPS="0x23eb7ccbbb4a21afea8b1256475e255b3cd84083ca79fa1f1a9435ab93d2b71b"

# Object IDs
WALRUS_SUBSIDIES_UPGRADE_CAP="0x0f73338243cc49e217d49ecfad806fa80f3ef48357e9b12614d8e57027fa0a75"
WALRUS_SUBSIDIES_ADMIN_CAP="0xd62d3d5b43dae752464afa2a8354fe52e0c31619778e8ab88c2e9a37c8349d04"


usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "OPTIONS:"
  echo "  -t <tx_type> Transaction type, mandatory ('upgrade-walrus-subsidies', 'set-walrus-subsidy-rates')"
  echo "  -g <obj_id>  Gas object ID, defaults to gas object with highest balance"
  echo "  -b <base_subsidy_frost> Base subsidy in FROST (only for set-walrus-subsidy-rates)"
  echo "  -s <per_shard_subsidy_frost> Per shard subsidy in FROST (only for set-walrus-subsidy-rates)"
  echo "  -r <subsidy_rate_basis_points> Subsidy rate in basis points (only for set-walrus-subsidy-rates)"
}

gas_obj_for_addr() {
  ADDRESS=$1
  if [[ -z $GAS_OBJECT_ID ]]
  then
    sui client gas $ADDRESS  --json | jq -r 'max_by(.mistBalance) | .gasCoinId'
  else
    echo $GAS_OBJECT_ID
  fi
}

upgrade_walrus_subsidies() {
  GAS=$(gas_obj_for_addr $WALRUS_ADMIN)
  GAS_BUDGET=$(sui client object $GAS --json | jq -r '.content.fields.balance')
  CONTRACT_DIR=mainnet-contracts/walrus_subsidies
  sui client \
    upgrade \
    --gas $GAS \
    --gas-budget $GAS_BUDGET \
    --upgrade-capability $WALRUS_SUBSIDIES_UPGRADE_CAP \
    $CONTRACT_DIR \
    --serialize-unsigned-transaction
}

set_walrus_subsidy_rates() {
  GAS=$(gas_obj_for_addr $WALRUS_OPS)
  GAS_BUDGET=$(sui client object $GAS --json | jq -r '.content.fields.balance')
  WALRUS_SUBSIDIES_PKG=$(sui client object $WALRUS_SUBSIDIES_UPGRADE_CAP --json | jq -r '.content.fields.package' )
  WALRUS_SUBSIDIES_OBJECT=$(sui client object $WALRUS_SUBSIDIES_ADMIN_CAP --json | jq -r '.content.fields.subsidies_id' )

  CMD="sui client \
    ptb \
    --gas-coin @$GAS \
    --gas-budget $GAS_BUDGET"
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


while getopts "t:g:b:s:r:h" arg; do
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
  "")
    echo "Error: The transaction type must be specified" >&2
    exit 1
    ;;
  *)
    echo "Error: Invalid transaction type \"$TX_TYPE\"" >&2
    exit 1
esac
