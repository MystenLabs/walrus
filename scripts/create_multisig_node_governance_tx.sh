#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
# This script creates an unsigned transaction that calls governance-authorized functions
# from a multisig wallet that is the governance-authorized address for a Walrus storage node.
# Intended to be used using the github workflow defined in
# `../.github/workflows/create-tx-for-multisig-node-governance.yml`

UPGRADE_MANAGER_OBJECT_ID=""
NODE_ID=""
MULTISIG_WALLET_ADDRESS=""
DIGEST_BASE64=""
# Staking object ID default to mainnet
STAKING_OBJECT_ID="0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904"
GAS_OBJECT_ID=""

usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "OPTIONS:"
  echo "  -u <upgrade_manager_object_id>  UpgradeManager object ID (required)"
  echo "  -n <node_id>                    Storage node ID (required)"
  echo "  -m <multisig_wallet_address>    Multisig wallet address (required, transaction sender)"
  echo "  -d <package_digest_base64>      Base64-encoded package digest (required)"
  echo "  -s <staking_object_id>          Staking object ID (defaults to mainnet $STAKING_OBJECT_ID)"
  echo "  -g <gas_object_id>              Gas object ID (defaults to highest balance gas object owned by the multisig)"
}

while getopts "u:n:m:d:s:g:h" arg; do
  case "${arg}" in
    u)
      UPGRADE_MANAGER_OBJECT_ID=${OPTARG}
      ;;
    n)
      NODE_ID=${OPTARG}
      ;;
    m)
      MULTISIG_WALLET_ADDRESS=${OPTARG}
      ;;
    d)
      DIGEST_BASE64=${OPTARG}
      ;;
    s)
      if [[ -n ${OPTARG} ]]; then
        STAKING_OBJECT_ID=${OPTARG}
      fi
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

if [[ -z $UPGRADE_MANAGER_OBJECT_ID ]]; then
  echo "Error: -u <upgrade_manager_object_id> is required" >&2
  exit 1
fi
if [[ -z $NODE_ID ]]; then
  echo "Error: -n <node_id> is required" >&2
  exit 1
fi
if [[ -z $MULTISIG_WALLET_ADDRESS ]]; then
  echo "Error: -m <multisig_wallet_address> is required" >&2
  exit 1
fi
if [[ -z $DIGEST_BASE64 ]]; then
  echo "Error: -d <package_digest_base64> is required" >&2
  exit 1
fi

gas_obj() {
  if [[ -z $GAS_OBJECT_ID ]]; then
    sui client gas "$MULTISIG_WALLET_ADDRESS" --json | jq -r 'max_by(.mistBalance) | .gasCoinId'
  else
    echo "$GAS_OBJECT_ID"
  fi
}

gas_budget() {
  GAS_COIN=$1
  sui client gas "$MULTISIG_WALLET_ADDRESS" --json \
    | jq -r --arg g "$GAS_COIN" '.[] | select(.gasCoinId == $g) | .mistBalance'
}

# Resolves the current Walrus package ID from the UpgradeCap held by the UpgradeManager.
# Uses the Sui gRPC `LedgerService.GetObject` API and reads the parsed Move struct from
# `Object.json`. `sui client object --json` no longer surfaces parsed Move struct fields
# on recent CLI versions, so we go to the fullnode directly.
walrus_pkg() {
  local rpc grpc_endpoint
  rpc=$(sui client envs --json | jq -r --arg a "$(sui client active-env)" '.[0][] | select(.alias == $a) | .rpc')
  # Strip scheme; gRPC is served on the same host:port as JSON-RPC via HTTP/2 ALPN.
  grpc_endpoint=${rpc#https://}
  grpc_endpoint=${grpc_endpoint#http://}
  grpcurl \
    -d "{\"object_id\":\"$UPGRADE_MANAGER_OBJECT_ID\",\"read_mask\":{\"paths\":[\"json\"]}}" \
    "$grpc_endpoint" sui.rpc.v2.LedgerService/GetObject \
    | jq -r '.object.json.cap.package'
}

# Decodes the base64 digest and renders it as a Move `vector<u8>` literal.
digest_vector() {
  local bytes
  bytes=$(echo -n "$DIGEST_BASE64" | base64 -d | xxd -p -c 1 | awk '{print "0x" $0 "u8"}' | paste -sd, -)
  if [[ -z $bytes ]]; then
    echo "Error: failed to decode base64 digest" >&2
    exit 1
  fi
  echo "vector[$bytes]"
}

GAS=$(gas_obj)
if [[ -z $GAS || $GAS == "null" ]]; then
  echo "Error: no gas coin found for multisig address $MULTISIG_WALLET_ADDRESS" >&2
  exit 1
fi
GAS_BUDGET=$(gas_budget "$GAS")
WALRUS_PKG=$(walrus_pkg)
if [[ -z $WALRUS_PKG || $WALRUS_PKG == "null" ]]; then
  echo "Error: failed to resolve walrus package from upgrade manager $UPGRADE_MANAGER_OBJECT_ID" >&2
  exit 1
fi
DIGEST=$(digest_vector)

CMD=(sui client ptb)
CMD+=(--gas-coin "@$GAS")
CMD+=(--gas-budget "$GAS_BUDGET")
CMD+=(--move-call "$WALRUS_PKG::auth::authenticate_sender")
CMD+=(--assign auth)
CMD+=(
  --move-call "$WALRUS_PKG::upgrade::vote_for_upgrade"
  "@$UPGRADE_MANAGER_OBJECT_ID"
  "@$STAKING_OBJECT_ID"
  auth
  "@$NODE_ID"
  "$DIGEST"
)
"${CMD[@]}" --serialize-unsigned-transaction
