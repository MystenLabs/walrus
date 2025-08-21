#!/bin/bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# This script will create a PR to generate a new Sui testnet version.

set -euo pipefail

# check required params
if [[ -z $1 || $# -ne 1 ]]
then
    echo "USAGE: bump_sui_testnet_version.sh <new-tag>"
    exit 1
else
    NEW_TAG=$1
fi

BASE="main"   # <-- always use main as the base branch

# One list with all relevant TOML locations (globs ok)
FILES=(
    "Move.toml"
    "crates/**/Move.toml"
    "contracts/**/Move.toml"
    "apps/**/Move.toml"
    "Cargo.toml"
    "crates/**/Cargo.toml"
    "apps/**/Cargo.toml"
    "docker/**/sui_version.toml"
    "Cargo.toml"
    "contracts/*/Move.toml"
    "testnet-contracts/*/Move.toml"
    "docker/**/sui_version.toml"
)

STAMP="$(date +%Y%m%d%H%M%S)"
BRANCH="chore/bump-sui-${NEW_TAG}-${STAMP}"
git checkout -b "$BRANCH"

shopt -s nullglob globstar
changed=0

# A single sed script with three *independent* substitutions:
# 1) Move.toml Sui framework rev (strict: repo + subdir must match)
# 2) Cargo.toml Sui git deps tag (strict: same inline table contains the Sui repo URL)
# 3) docker/*/sui_version.toml simple key="testnet-vX.Y.Z"
for pat in "${FILES[@]}"; do
for f in $pat; do
    [[ -f "$f" ]] || continue
    before="$(sha1sum "$f" | awk '{print $1}')"

    # Use extended regex, in-place
    sed -E -i \
    -e 's|^([[:space:]]*Sui[[:space:]]*=[[:space:]]*\{[^}]*\
        git[[:space:]]*=[[:space:]]*"https://github.com/MystenLabs/sui\.git"[^}]*\
        subdir[[:space:]]*=[[:space:]]*"crates/sui-framework/packages/sui-framework"\
        [^}]*rev[[:space:]]*=[[:space:]]*")([^"]+)(")|\1'"$NEW_TAG"'\3|' \
    -e 's|^([[:space:]]*[A-Za-z0-9_\-]+[[:space:]]*=[[:space:]]*\
        \{[^}]*git[[:space:]]*=[[:space:]]*"https://github.com/MystenLabs/sui(\.git)?"\
        [^}]*,?[[:space:]]*tag[[:space:]]*=[[:space:]]*")([^"]+)(")|\1'"$NEW_TAG"'\3|' \
    -e 's|^([[:space:]]*[A-Za-z0-9_]+[[:space:]]*=[[:space:]]*")\
        testnet-v[0-9]+\.[0-9]+\.[0-9]+(")|\1'"$NEW_TAG"'\2|' \
    "$f"

    after="$(sha1sum "$f" | awk '{print $1}')"
    if [[ "$before" != "$after" ]]; then
    echo "updated: $f"
    git add "$f"
    changed=1
    fi
done
done

if [[ "$changed" -ne 1 ]]; then
echo "No matching lines changed; nothing to do."
exit 0
fi

# Build & lockfiles
cargo build --locked || cargo build
shopt -s globstar
git add -- Cargo.lock **/Cargo.lock Move.lock **/Move.lock 2>/dev/null || true

# Commit, push, PR
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git commit -m "chore: bump Sui to ${NEW_TAG}"
git push -u origin "$BRANCH"

gh pr create \
--base "$BASE" \
--head "$BRANCH" \
--title "chore: bump Sui to ${NEW_TAG}" \
--reviewer "ebmifa,mlegner,wbbradley" \
--body $'Automated Sui bump with single-pass updater:\n\n\
    1) Move.toml Sui framework rev (repo+subdir strict)\n\
    2) Cargo.toml Sui git deps tag (repo URL strict)\n\
    3) docker/**/sui_version.toml\n\n`cargo build` run to refresh lockfiles.'
