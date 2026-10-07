#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
# This script creates an unsigned transaction that upgrades the Walrus system contract with the
# `EmergencyUpgradeCap` (`authorize_emergency_upgrade`, upgrade, `commit_upgrade`), sent by the
# address that owns the cap (on Mainnet, the Walrus admin multisig).
# Intended to be used using the github workflow defined in
# `../.github/workflows/create-tx-for-multisig-walrus-upgrade.yml`

set -o pipefail

NETWORK="mainnet"
SENDER=""
WALRUS_DEPLOY="./target/release/walrus-deploy"
WALLET_PATH="$HOME/.sui/sui_config/client.yaml"

usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "OPTIONS:"
  echo "  -n <network>        Network, 'mainnet' or 'testnet' (defaults to $NETWORK)"
  echo "  -s <sender>         Transaction sender, the owner of the EmergencyUpgradeCap (defaults to the network's cap owner)"
  echo "  -d <walrus_deploy>  Path to the walrus-deploy binary (defaults to $WALRUS_DEPLOY)"
  echo "  -w <wallet_path>    Path to a Sui client config for the network, used for RPC only (defaults to $WALLET_PATH)"
}

while getopts "n:s:d:w:h" arg; do
  case "${arg}" in
    n)
      NETWORK=${OPTARG}
      ;;
    s)
      SENDER=${OPTARG}
      ;;
    d)
      WALRUS_DEPLOY=${OPTARG}
      ;;
    w)
      WALLET_PATH=${OPTARG}
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

case "$NETWORK" in
  mainnet)
    CONTRACT_DIR=mainnet-contracts/walrus
    CAP_OWNER="0x62a69ba94e191634841cc4d196e70ec3e4667fc78013ae6d7405a0c593b39f1e"
    UPGRADE_MANAGER_OBJECT_ID="0xc42868ad4861f22bd1bcd886ae1858d5c007458f647a49e502d44da8bbd17b51"
    STAKING_OBJECT_ID="0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904"
    SYSTEM_OBJECT_ID="0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2"
    ;;
  testnet)
    CONTRACT_DIR=testnet-contracts/walrus
    CAP_OWNER="0x181816cd2efb860628385e8653b37260d0d065c844803b23852799cc19ee2c28"
    UPGRADE_MANAGER_OBJECT_ID="0xc768e475fd1527b7739884d7c3a3d1bc09ae422dfdba6b9ae94c1f128297283c"
    STAKING_OBJECT_ID="0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3"
    SYSTEM_OBJECT_ID="0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af"
    ;;
  *)
    echo "Error: Invalid network \"$NETWORK\"" >&2
    exit 1
esac

if [[ -z $SENDER ]]; then
  SENDER=$CAP_OWNER
fi

# The Move compiler prints its progress to stdout, so only the last line is the transaction.
"$WALRUS_DEPLOY" emergency-upgrade \
  --serialize-unsigned \
  --sender "$SENDER" \
  --wallet-path "$WALLET_PATH" \
  --contract-dir "$CONTRACT_DIR" \
  --upgrade-manager-object-id "$UPGRADE_MANAGER_OBJECT_ID" \
  --staking-object-id "$STAKING_OBJECT_ID" \
  --system-object-id "$SYSTEM_OBJECT_ID" \
  | tail -n 1
