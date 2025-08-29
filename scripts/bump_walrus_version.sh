#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# This script creates a Walrus Version Bump PR

set -euo pipefail
set -x

# Ensure required binaries are available
for cmd in gh git cargo; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found in PATH." >&2
    exit 1
  fi
done

# Get current main version
WALRUS_VERSION=$(sed -nE 's/^version = "([0-9]+\.[0-9]+\.[0-9]+)"/\1/p' ./Cargo.toml)

# Generate the Walrus version bump PR
echo "Generating walrus version bump..."

# Make sure GITHUB_ACTOR is set.
if [[ -z "${GITHUB_ACTOR:-}" ]]; then
  GITHUB_ACTOR="$(whoami 2>/dev/null || echo github-actions[bot])"
fi

# Parse "X.Y.Z" from $WALRUS_VERSION into major/minor/patch, then set NEW_WALRUS_VERSION to "X.(Y+1).Z".
IFS=. read -r major minor patch <<<"$WALRUS_VERSION"; NEW_WALRUS_VERSION="$major.$((minor+1)).$patch"

# Setup new branch for staging
STAMP="$(date +%Y%m%d%H%M%S)"
BRANCH="${GITHUB_ACTOR}/walrus-v${NEW_WALRUS_VERSION}-version-bump-${STAMP}"
git checkout -b "$BRANCH"

# Update the version in Cargo.toml and openrpc.json
sed -i -E "s/^(version = \")[0-9]+\.[0-9]+\.[0-9]+(\"$)/\1${NEW_WALRUS_VERSION}\2/" Cargo.toml

# Cargo check to generate Cargo.lock changes
cargo check || true

# Staged all changes
echo "Staging all changed files..."
git add -A .

# Generate PR body
BODY="Walrus v${NEW_WALRUS_VERSION} Version Bump"

# push branch
git commit -m "$BODY"
git push -u origin "$BRANCH"

# Create PR
PR_URL=$(gh pr create \
  --base main \
  --head "$BRANCH" \
  --title "$BODY" \
  --reviewer "MystenLabs/walrus-pe" \
  --body "$BODY" \
  2>&1 | grep -Eo 'https://github.com/[^ ]+')

echo "Pull request for Walrus v${NEW_WALRUS_VERSION} Version Bump created: $PR_URL"
