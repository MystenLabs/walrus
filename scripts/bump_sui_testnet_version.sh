#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# This script creates a Sui Testnet Version Bump PR

set -Eeuo pipefail

# Ensure required binaries are available
for cmd in cargo gh sui git; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found in PATH." >&2
    exit 1
  fi
done

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

# Make sure GITHUB_ACTOR is set.
if [[ -z "${GITHUB_ACTOR:-}" ]]; then
  GITHUB_ACTOR="$(git config user.name 2>/dev/null || echo github-actions[bot])"
fi

# Set up branch for changes.
STAMP="$(date +%Y%m%d%H%M%S)"
BRANCH="${GITHUB_ACTOR}/bump-sui-${NEW_TAG}-${STAMP}"
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

# Check if we found any targets.
if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No matching files found for update."
  exit 0
else
  echo "Updating testnet tags in:"
  printf '  - %s\n' "${TARGETS[@]}"

  for f in "${TARGETS[@]}"; do
    sed -i -E \
      "s/(rev = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+/\1${NEW_TAG}/g; \
      s/(tag = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+/\1${NEW_TAG}/g; \
      s/(SUI_VERSION = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+/\1${NEW_TAG}/g" "$f"
  done
fi

# Update Cargo.lock files
echo "Running cargo check ..."
cargo check || true

# Find all directories that contain a Move.toml and generate Move.lock files.
echo "Regenerating Move.lock files..."
for toml in contracts/**/Move.toml testnet-contracts/**/Move.toml; do
  if [[ -f "$toml" ]]; then
    dir=$(dirname "$toml")
    echo "  -> building $dir"
    (cd "$dir" && sui move build)
  fi
done

# Staged all changes
echo "Staging all changed files..."
git add -u . ':!/.github/workflows'

# Commit, push, and create PR.
git config user.name "github-actions[bot]"
git config user.email \
  "41898282+github-actions[bot]@users.noreply.github.com"

# Push branch
git commit -m "chore: bump Sui version to ${NEW_TAG}"
git push -u origin "$BRANCH"

# Generate PR body
BODY=$(cat <<-EOF
This PR updates the Sui testnet version to ${NEW_TAG}
EOF
)

# Create PR
PR_URL=$(gh pr create \
  --base main \
  --head "$BRANCH" \
  --title "chore: bump Sui version to ${NEW_TAG}" \
  --reviewer "MystenLabs/walrus-pe" \
  --body "$BODY" \
  2>&1 | grep -Eo 'https://github.com/[^ ]+')

# Setting the PR to auto merge
gh pr merge --auto --squash --delete-branch "$BRANCH"
echo "Pull request created: $PR_URL"
