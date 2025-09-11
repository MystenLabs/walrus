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

# Extract the current version from Cargo.toml (e.g., "1.2.3" from version = "1.2.3")
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

# Replace the version line in Cargo.toml with the new version number
sed -i -E "s/^(version = \")[0-9]+\.[0-9]+\.[0-9]+(\"$)/\1${NEW_WALRUS_VERSION}\2/" Cargo.toml

# Cargo check to generate Cargo.lock changes
cargo check || true

# Staged all changes
echo "Staging all changed files..."
git add -A .

git config user.name "github-actions[bot]"
git config user.email \
  "41898282+github-actions[bot]@users.noreply.github.com"

# push branch
git commit -m "chore: bump walrus version to v${NEW_WALRUS_VERSION}"
git push -u origin "$BRANCH"

# Generate PR body
BODY="Walrus v${NEW_WALRUS_VERSION} Version Bump"

# Create PR
echo "Creating pull request..."
if PR_OUTPUT=$(gh pr create \
  --base main \
  --head "$BRANCH" \
  --title "chore: bump Walrus version to v${NEW_WALRUS_VERSION}" \
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
gh pr merge --auto --squash --delete-branch "$BRANCH"
