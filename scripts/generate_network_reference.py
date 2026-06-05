# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

"""Generate the volatile sections of the Network Reference documentation page.

The Network Reference page (docs/content/network-reference.mdx) is the single
canonical place that documents Walrus endpoints, package IDs, object IDs, epoch
durations, and configuration constants. Those values drift over time, so the
volatile blocks are generated from source-of-truth files instead of being
hand-maintained:

    * setup/client_config.yaml
        system / staking / exchange object IDs, RPC URLs, shard counts, and the
        maximum number of epochs ahead, for both Mainnet and Testnet.
    * mainnet-contracts/<pkg>/Published.toml
        Mainnet package IDs (the `original-id` field) for the WAL token, the
        Walrus system package, and the subsidies package.
    * scripts/network_reference_data.yaml
        curated values that have no other machine-readable source: public
        endpoints, Walrus Sites package IDs, production epoch durations, and
        token-unit constants. That file also records the page owner and the
        refresh cadence. It lives next to this script (rather than under
        docs/data/) because docs/data/*.yaml is auto-published as static JSON by
        the docs build, and this curated subset is not the full reference.

Each generated region in the page is delimited by HTML/MDX comment markers:

    {/* GENERATED:START id=<block-id> */}
    ...generated content...
    {/* GENERATED:END id=<block-id> */}

Usage:
    python3 scripts/generate_network_reference.py            # rewrite the page
    python3 scripts/generate_network_reference.py --check    # fail on drift

The --check mode regenerates every block in memory and compares it against the
committed page. It exits non-zero (and prints a diff) when they differ, which is
how CI detects that either a source file changed without regenerating the page,
or the page was edited by hand inside a generated region.

The script intentionally depends only on the Python standard library so it runs
in CI and locally without any package installation.
"""

import argparse
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

PAGE_PATH = REPO_ROOT / "docs" / "content" / "network-reference.mdx"
CLIENT_CONFIG_PATH = REPO_ROOT / "setup" / "client_config.yaml"
DATA_PATH = REPO_ROOT / "scripts" / "network_reference_data.yaml"
MAINNET_CONTRACTS_DIR = REPO_ROOT / "mainnet-contracts"

# Mainnet package directory -> human-readable label shown in the page, in the
# order they should appear.
MAINNET_PACKAGES = [
    ("wal", "WAL token"),
    ("walrus", "Walrus system"),
    ("subsidies", "Subsidies"),
]

SUISCAN_OBJECT_URL = "https://suiscan.xyz/mainnet/object/{id}/tx-blocks"


# --------------------------------------------------------------------------- #
# Minimal parsers (standard library only)
# --------------------------------------------------------------------------- #


def _strip_quotes(value):
    """Remove a single pair of matching surrounding quotes from a scalar."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


_MAPPING_ITEM_RE = re.compile(r"^[\w-]+:(\s|$)")


def parse_yaml(text):
    """Parse the small YAML subset used by the source files.

    Supports block mappings, block sequences of scalars, and block sequences of
    mappings, with `#` comments, blank lines, and single- or double-quoted
    scalars. It does not support flow collections, multi-line scalars, or
    anchors, none of which appear in the files this script reads.
    """
    raw_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        raw_lines.append(line.rstrip())

    pos = 0

    def indent_of(line):
        return len(line) - len(line.lstrip(" "))

    def parse(indent):
        nonlocal pos
        if raw_lines[pos].lstrip().startswith("- "):
            sequence = []
            while pos < len(raw_lines):
                line = raw_lines[pos]
                if indent_of(line) != indent or not line.lstrip().startswith("- "):
                    break
                rest = line.lstrip()[2:]
                if _MAPPING_ITEM_RE.match(rest):
                    # Mapping item: rewrite "- key: ..." as a mapping line one
                    # level deeper, then parse it as a nested mapping.
                    item_indent = indent + 2
                    raw_lines[pos] = " " * item_indent + rest
                    sequence.append(parse(item_indent))
                else:
                    pos += 1
                    sequence.append(_strip_quotes(rest.strip()))
            return sequence

        mapping = {}
        while pos < len(raw_lines):
            line = raw_lines[pos]
            if indent_of(line) != indent or line.lstrip().startswith("- "):
                break
            key, _, value = line.lstrip().partition(":")
            key = key.strip()
            value = value.strip()
            pos += 1
            if value == "":
                mapping[key] = parse(indent + 2)
            else:
                mapping[key] = _strip_quotes(value)
        return mapping

    return parse(indent_of(raw_lines[0])) if raw_lines else {}


def parse_published_toml(text):
    """Return {section: {key: value}} for the simple Published.toml format."""
    sections = {}
    current = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            current = stripped[1:-1]
            sections[current] = {}
        elif "=" in stripped and current is not None:
            key, _, value = stripped.partition("=")
            sections[current][key.strip()] = _strip_quotes(value.strip())
    return sections


# --------------------------------------------------------------------------- #
# Source loading
# --------------------------------------------------------------------------- #


def load_sources():
    """Read every source file and return the data needed to render the page."""
    config = parse_yaml(CLIENT_CONFIG_PATH.read_text())
    contexts = config["contexts"]
    data = parse_yaml(DATA_PATH.read_text())

    packages = {}
    for directory, label in MAINNET_PACKAGES:
        published = parse_published_toml(
            (MAINNET_CONTRACTS_DIR / directory / "Published.toml").read_text()
        )
        original_id = published["published.mainnet"]["original-id"]
        packages[directory] = (label, original_id)

    return {"contexts": contexts, "data": data, "packages": packages}


# --------------------------------------------------------------------------- #
# Block renderers (one per GENERATED id)
# --------------------------------------------------------------------------- #


def _rpc_url(context):
    urls = context["rpc_urls"]
    return urls[0] if isinstance(urls, list) else urls


def render_network_parameters(sources):
    mainnet = sources["contexts"]["mainnet"]
    testnet = sources["contexts"]["testnet"]
    epoch = sources["data"]["epoch_durations"]
    return "\n".join(
        [
            "| **Parameter** | **Mainnet** | **Testnet** |",
            "|---|---|---|",
            "| Sui network | Mainnet | Testnet |",
            f"| Number of shards | {mainnet['n_shards']} | {testnet['n_shards']} |",
            f"| Epoch duration | {epoch['mainnet']} | {epoch['testnet']} |",
            "| Maximum number of epochs for which storage can be bought "
            f"| {mainnet['max_epochs_ahead']} | {testnet['max_epochs_ahead']} |",
        ]
    )


def render_rpc_endpoints(sources):
    mainnet = sources["contexts"]["mainnet"]
    testnet = sources["contexts"]["testnet"]
    return "\n".join(
        [
            "| **Network** | **Sui RPC URL** |",
            "|---|---|",
            f"| Mainnet | `{_rpc_url(mainnet)}` |",
            f"| Testnet | `{_rpc_url(testnet)}` |",
        ]
    )


def render_mainnet_packages(sources):
    rows = ["| **Package** | **ID** |", "|---|---|"]
    for directory, _ in MAINNET_PACKAGES:
        label, package_id = sources["packages"][directory]
        url = SUISCAN_OBJECT_URL.format(id=package_id)
        rows.append(f"| {label} | [`{package_id}`]({url}) |")
    return "\n".join(rows)


def render_sites_packages(sources):
    sites = sources["data"]["sites_packages"]
    return "\n".join(
        [
            "| **Network** | **Walrus Sites package ID** |",
            "|---|---|",
            f"| Mainnet | `{sites['mainnet']}` |",
            f"| Testnet | `{sites['testnet']}` |",
        ]
    )


def render_object_ids(sources):
    mainnet = sources["contexts"]["mainnet"]
    testnet = sources["contexts"]["testnet"]
    return "\n".join(
        [
            "| **Object** | **Network** | **ID** |",
            "|---|---|---|",
            f"| System object | Mainnet | `{mainnet['system_object']}` |",
            f"| Staking object | Mainnet | `{mainnet['staking_object']}` |",
            f"| System object | Testnet | `{testnet['system_object']}` |",
            f"| Staking object | Testnet | `{testnet['staking_object']}` |",
        ]
    )


def render_testnet_exchange_objects(sources):
    objects = sources["contexts"]["testnet"]["exchange_objects"]
    return "\n".join(f"- `{object_id}`" for object_id in objects)


def render_reference_endpoints(sources):
    rows = [
        "| **Service** | **Network** | **Endpoint** |",
        "|---|---|---|",
    ]
    for entry in sources["data"]["reference_endpoints"]:
        rows.append(
            f"| {entry['service']} | {entry['network']} | `{entry['url']}` |"
        )
    return "\n".join(rows)


def render_upload_relays(sources):
    relays = sources["data"]["upload_relays"]
    return "\n".join(
        [
            "| **Network** | **Upload relay endpoint** |",
            "|---|---|",
            f"| Mainnet | `{relays['mainnet']}` |",
            f"| Testnet | `{relays['testnet']}` |",
        ]
    )


def render_token_units(sources):
    rows = [
        "| **Token** | **Smallest unit** | **Conversion** |",
        "|---|---|---|",
    ]
    for entry in sources["data"]["token_units"]:
        rows.append(
            f"| {entry['token']} | {entry['smallest_unit']} | {entry['conversion']} |"
        )
    return "\n".join(rows)


RENDERERS = {
    "network-parameters": render_network_parameters,
    "rpc-endpoints": render_rpc_endpoints,
    "package-ids": render_mainnet_packages,
    "sites-packages": render_sites_packages,
    "object-ids": render_object_ids,
    "exchange-objects": render_testnet_exchange_objects,
    "reference-endpoints": render_reference_endpoints,
    "upload-relays": render_upload_relays,
    "token-units": render_token_units,
}


# --------------------------------------------------------------------------- #
# Page rewriting
# --------------------------------------------------------------------------- #

BLOCK_RE = re.compile(
    r"(?P<start>\{/\* GENERATED:START id=(?P<id>[\w-]+) \*/\}\n)"
    r"(?P<body>.*?)"
    r"(?P<end>\n\{/\* GENERATED:END id=(?P=id) \*/\})",
    re.DOTALL,
)


def render_page(text, sources):
    """Return the page text with every generated block re-rendered.

    Raises a ValueError if the page contains a block whose id has no renderer,
    or if a renderer was never used (an id is missing from the page).
    """
    seen = set()

    def replace(match):
        block_id = match.group("id")
        if block_id not in RENDERERS:
            raise ValueError(
                f"page has GENERATED block id={block_id!r} with no renderer"
            )
        seen.add(block_id)
        content = RENDERERS[block_id](sources)
        return f"{match.group('start')}{content}{match.group('end')}"

    new_text = BLOCK_RE.sub(replace, text)

    missing = set(RENDERERS) - seen
    if missing:
        raise ValueError(
            "no GENERATED block found for renderer id(s): "
            + ", ".join(sorted(missing))
        )
    return new_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the page is up to date instead of rewriting it; "
        "exit non-zero on drift",
    )
    args = parser.parse_args()

    sources = load_sources()
    current = PAGE_PATH.read_text()
    updated = render_page(current, sources)

    relative_page = PAGE_PATH.relative_to(REPO_ROOT)

    if args.check:
        if current != updated:
            diff = difflib.unified_diff(
                current.splitlines(keepends=True),
                updated.splitlines(keepends=True),
                fromfile=f"{relative_page} (committed)",
                tofile=f"{relative_page} (regenerated)",
            )
            sys.stdout.writelines(diff)
            print(
                f"\nerror: {relative_page} is out of date. "
                "Run `python3 scripts/generate_network_reference.py` and commit "
                "the result.",
                file=sys.stderr,
            )
            return 1
        print(f"{relative_page} is up to date.")
        return 0

    if current != updated:
        PAGE_PATH.write_text(updated)
        print(f"updated {relative_page}")
    else:
        print(f"{relative_page} already up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
