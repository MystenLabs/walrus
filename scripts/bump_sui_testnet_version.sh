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

# Escape special sed characters in NEW_TAG for safe substitution.
# This handles &, \, and | (our sed delimiter) which have special meaning in sed replacement.
NEW_TAG_ESCAPED=$(printf '%s' "$NEW_TAG" | sed 's/[&|\]/\\&/g')

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
      "s|(rev = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+|\1${NEW_TAG_ESCAPED}|g; \
      s|(tag = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+|\1${NEW_TAG_ESCAPED}|g; \
      s|(SUI_VERSION = \")testnet-v[0-9]+\.[0-9]+\.[0-9]+|\1${NEW_TAG_ESCAPED}|g" "$f"
  done
fi

# Update Cargo.lock files
echo "Running cargo check ..."
if ! cargo check; then
  echo "Warning: cargo check failed, but continuing to update lock files" >&2
fi

# Find all directories that contain a Move.toml and generate Move.lock files.
echo "Regenerating Move.lock files..."
build_failures=0
for toml in contracts/**/Move.toml testnet-contracts/**/Move.toml; do
  if [[ -f "$toml" ]]; then
    dir=$(dirname "$toml")
    echo "  -> building $dir"
    if ! (cd "$dir" && sui move build); then
      echo "Warning: sui move build failed for $dir" >&2
      ((build_failures++)) || true
    fi
  fi
done
if [[ $build_failures -gt 0 ]]; then
  echo "Warning: $build_failures Move build(s) failed" >&2
fi

# Staged all changes
echo "Staging all changed files..."
git add -u . ':!/.github/workflows'

# Commit, push, and create PR.
git config user.name "github-actions[bot]"
git config user.email \
  "41898282+github-actions[bot]@users.noreply.github.com"

# Push branch
git commit -m "ci: bump Sui testnet version to ${NEW_TAG}"
git push -u origin "$BRANCH"

# Generate PR body
BODY="This PR updates the Sui testnet version to ${NEW_TAG}"

# Create PR
echo "Creating pull request..."
if PR_OUTPUT=$(gh pr create \
  --base main \
  --head "$BRANCH" \
  --title "ci: bump Sui testnet version to ${NEW_TAG}" \
  --reviewer "wbbradley,halfprice,liquid-helium,ebmifa" \
  --body "$BODY" 2>&1); then

  # Extract PR URL from output
  if PR_URL=$(echo "$PR_OUTPUT" | grep -Eo 'https://github.com/[^ ]+'); then
    echo "Successfully created PR: $PR_URL"
  else
    echo "Warning: PR created but could not extract URL from output:"
    echo "$PR_OUTPUT"
    PR_URL="(URL extraction failed)"
  fi
else
  echo "Error: Failed to create pull request:" >&2
  echo "$PR_OUTPUT" >&2
  exit 1
fi

# Setting the PR to auto merge
if ! gh pr merge --auto --squash --delete-branch "$BRANCH"; then
  echo "Warning: Failed to enable auto-merge for PR" >&2
fi

echo "$PR_URL"
