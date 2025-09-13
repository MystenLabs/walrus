# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

import base64
import os
from pathlib import Path
from itertools import product

# Configure these paths to match your system
FULL_NODE_URL = "https://fullnode.testnet.sui.io:443"
PATH_TO_WALRUS = "../CONFIG/bin/walrus"
# TODO: Fix the path below to point to your walrus config file. By default, it is assumed to be in
# ~/.config/walrus/client_config.yaml.
PATH_TO_WALRUS_CONFIG = str(Path.home() / ".config" / "walrus" / "client_config.yaml")


def default_configuration_paths():
    walrus_config_file_names = ["client_config.yaml", "client_config.yml"]
    directories = [Path(".")]

    if xdg_config_dir := os.environ.get("XDG_CONFIG_HOME"):
        directories.append(Path(xdg_config_dir))

    if home_dir := Path.home():
        directories.append(home_dir / ".config" / "walrus")
        directories.append(home_dir / ".walrus")

    return [
        directory / file_name
        for directory, file_name in product(directories, walrus_config_file_names)
    ]


def num_to_blob_id(blob_id_num):
    extracted_bytes = []
    for i in range(32):
        extracted_bytes += [blob_id_num & 0xFF]
        blob_id_num = blob_id_num >> 8
    assert blob_id_num == 0
    blob_id_bytes = bytes(extracted_bytes)
    encoded = base64.urlsafe_b64encode(blob_id_bytes)
    return encoded.decode("ascii").strip("=")


if __name__ == "__main__":
    # A test case for the num_to_blob_id function
    blob_id_num = (
        46269954626831698189342469164469112511517843773769981308926739591706762839432
    )
    blob_id_base64 = "iIWkkUTzPZx-d1E_A7LqUynnYFD-ztk39_tP8MLdS2Y"
    assert num_to_blob_id(blob_id_num) == blob_id_base64
