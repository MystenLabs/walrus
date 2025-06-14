# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

"""
A simple script that uses the Sui CLI to merge all the SUI coins in the wallet
into a single coin.
"""

import subprocess
import json
from argparse import ArgumentParser


# The maximum number of coins that can be paid in a single operation.
MAX_NUM_COINS = 256
# The gas budget for the pay-sui command.
GAS_BUDGET = 100_000_000


def parse_args():
    """Parses the command line arguments.

    Only needed to show the help message.
    """
    parser = ArgumentParser(
        description="Merge all SUI coins in the wallet into a single coin. \
            Uses the address and the network that are set in the Sui CLI. \
            As a result, the wallet will contain 2 SUI coins.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip the confirmation.",
    )
    return parser.parse_args()


def get_sui_coins() -> list[str]:
    """Gets the object IDs of all the SUI coins owned by the wallet."""
    result = subprocess.run(
        ["sui", "client", "gas", "--json"],
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    return [coin["gasCoinId"] for coin in data]


def get_wallet_address() -> str:
    """Gets the address of the wallet."""
    result = subprocess.run(
        ["sui", "client", "active-address"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def get_wallet_env() -> str:
    """Gets the network of the wallet."""
    result = subprocess.run(
        ["sui", "client", "active-env"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def pay_sui_to_smash(coins: list[str], recipient: str) -> None:
    """Merges the SUI coins in the list into a single coin.

    Uses the gas-smashing property of the pay-sui command.  Assumes that the
    list of coins contains less than `MAX_NUM_COINS` elements.
    """
    assert (
        len(coins) <= MAX_NUM_COINS
    ), "The list of coins contains more than `MAX_NUM_COINS` elements."

    cmd = [
        "sui",
        "client",
        "pay-all-sui",
        "--recipient",
        recipient,
        "--input-coins",
        *coins,
        # TODO: This should be estimated by the CLI. Remove when it is fixed.
        "--gas-budget",
        str(GAS_BUDGET),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def merge_all_coins(recipient: str) -> None:
    """Merges all the SUI coins in the wallet into a single coin."""
    coins = get_sui_coins()
    if len(coins) > MAX_NUM_COINS:
        print(
            f"Found {len(coins)} SUI coins to merge. \
                The operation will require multiple transactions."
        )

    while len(coins) > 1:
        coins_to_merge = coins[:MAX_NUM_COINS]
        print(f"Merging {len(coins_to_merge)} SUI coins in a batch.")
        pay_sui_to_smash(coins_to_merge, recipient)
        coins = get_sui_coins()

    print("All coins merged.")


if __name__ == "__main__":
    args = parse_args()
    address = get_wallet_address()
    env = get_wallet_env()
    print(f"Merging all the SUI coins for the wallet address: {address} on {env}.")

    # Early exit if the wallet already contains a single SUI coin.
    coins = get_sui_coins()
    if len(coins) == 1:
        print("The wallet already contains a single SUI coin.")
        exit()

    # Prompt the user for confirmation.
    if not args.yes:
        print("Continue? (y/n)")
        if input() != "y":
            print("Aborting. Use the -y --yes flag to skip the confirmation.")
            exit()

    merge_all_coins(address)
