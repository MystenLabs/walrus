#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# This script creates a PR branch and updates Sui testnet versions in
# selected files.

set -Eeuo pipefail

# Check required params.
if [[ -z ${1:-} || $# -ne 1 ]]; then
  echo "USAGE: bump_sui_testnet_version.sh <new-tag>"
  exit 1
else
  NEW_TAG="$1"
fi

# (Loose) sanity check on tag format.
if [[ ! "$NEW_TAG" =~ ^testnet-v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Warning: NEW_TAG '$NEW_TAG' doesn't look like testnet-vX.Y.Z" >&2
fi

STAMP="$(date +%Y%m%d%H%M%S)"
BRANCH="$(gh api user --jq '.login')/bump-sui-${NEW_TAG}-${STAMP}"
git checkout -b "$BRANCH"

# Allow recursive globs.
shopt -s globstar nullglob

# List of relevant TOML locations (globs allowed).
FILES=(
  "contracts/**/Move.toml"
  "docker/walrus-antithesis/sui_version.toml"
  "Cargo.toml"
  "testnet-contracts/**/Move.toml"
)

# Expand patterns into actual file paths.
TARGETS=()
for pat in "${FILES[@]}"; do
  for f in $pat; do
    [[ -f "$f" ]] && TARGETS+=("$f")
  done
done

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No matching files found for update."
  exit 0
else
  echo "Updating testnet tags in:"
  printf '  - %s\n' "${TARGETS[@]}"

  for f in "${TARGETS[@]}"; do
    sed -i -E \
      "s/( = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+/\1${NEW_TAG}/g" "$f"
  done
fi

echo "Running cargo build --release ..."
cargo build --release

# Find all directories that contain a Move.toml and rebuild them.
echo "Regenerating Move.lock files..."
for toml in contracts/**/Move.toml testnet-contracts/**/Move.toml; do
  if [[ -f "$toml" ]]; then
    dir=$(dirname "$toml")
    echo "  -> building $dir"
    (cd "$dir" && sui move build)
  fi
done

echo "Staging all changed files..."
git add -u

# Commit, push, and create PR.
git config user.name "github-actions[bot]"
git config user.email \
  "41898282+github-actions[bot]@users.noreply.github.com"

git commit -m "chore: bump Sui to ${NEW_TAG}"
git push -u origin "$BRANCH"

BODY=$(cat <<EOF
Automated Sui bump with single-pass updater.

This PR bumps Sui testnet tag to ${NEW_TAG}.
EOF
)

PR_URL=$(gh pr create \
  --base main \
  --head "$BRANCH" \
  --title "chore: bump Sui to ${NEW_TAG}" \
  --reviewer "ebmifa,mlegner,wbbradley" \
  --body "Automated Sui bump with single-pass updater." \
  --json url -q .url)

echo "Pull request created: $PR_URL"
