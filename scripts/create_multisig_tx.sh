#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
# This script allows creating unsigned transactions for operations that need to be signed
# by a Walrus multisig address.
# Intended to be used using the github workflow defined in
# `../.github/workflows/create-tx-for-multisig.yml`

GAS_OBJECT_ID=""
TX_TYPE=""

# Addresses
WALRUS_ADMIN="0x62a69ba94e191634841cc4d196e70ec3e4667fc78013ae6d7405a0c593b39f1e"
WALRUS_OPS="0x23eb7ccbbb4a21afea8b1256475e255b3cd84083ca79fa1f1a9435ab93d2b71b"

# Object IDs
SUBSIDIES_ADMIN_CAP="0x7cd09be8545e524217e6f35e5c306b64e3545594879dc2590e024f42bce439c6"
SUBSIDIES_UPGRADE_CAP="0x632e10712d32b0851a1109d5a7f09680d11c74ffa5ba50eef7f14d85385cb615"
WALRUS_SUBSIDIES_UPGRADE_CAP="0x0f73338243cc49e217d49ecfad806fa80f3ef48357e9b12614d8e57027fa0a75"
WALRUS_SUBSIDIES_ADMIN_CAP="0xd62d3d5b43dae752464afa2a8354fe52e0c31619778e8ab88c2e9a37c8349d04"


usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "OPTIONS:"
  echo "  -t <tx_type> Transaction type, mandatory ('upgrade-walrus-subsidies', 'transfer-subsidies-funds')"
  echo "  -g <obj_id>  Gas object ID, defaults to gas object with highest balance"
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

transfer_subsidies_funds() {
  GAS=$(gas_obj_for_addr $WALRUS_OPS)
  GAS_BUDGET=$(sui client object $GAS --json | jq -r '.content.fields.balance')
  SUBSIDIES_PACKAGE=$(sui client object $SUBSIDIES_UPGRADE_CAP --json | jq -r '.content.fields.package')
  SUBSIDIES_OBJECT=$(sui client object $SUBSIDIES_ADMIN_CAP --json | jq -r '.content.fields.subsidies_id' )
  WALRUS_SUBSIDIES_PKG=$(sui client object $WALRUS_SUBSIDIES_UPGRADE_CAP --json | jq -r '.content.fields.package' )
  WALRUS_SUBSIDIES_OBJECT=$(sui client object $WALRUS_SUBSIDIES_ADMIN_CAP --json | jq -r '.content.fields.subsidies_id' )
  sui client \
    ptb \
    --gas-coin @$GAS \
    --gas-budget $GAS_BUDGET \
    --move-call $SUBSIDIES_PACKAGE::subsidies::withdraw_balance \
    @$SUBSIDIES_OBJECT @$SUBSIDIES_ADMIN_CAP \
    --assign balance \
    --move-call $WALRUS_SUBSIDIES_PKG::walrus_subsidies::add_balance \
    @$WALRUS_SUBSIDIES_OBJECT balance \
    --serialize-unsigned-transaction
}

while getopts "t:g:h" arg; do
  case "${arg}" in
    t)
      TX_TYPE=${OPTARG}
      ;;
    g)
      GAS_OBJECT_ID=${OPTARG}
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
  transfer-subsidies-funds)
    transfer_subsidies_funds
    ;;
  "")
    echo "Error: The transaction type must be specified" >&2
    exit 1
    ;;
  *)
    echo "Error: Invalid transaction type \"$TX_TYPE\"" >&2
    exit 1
esac
