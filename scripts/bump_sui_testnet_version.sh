#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# This script creates a Sui Testnet Version Bump PR

set -Eeuo pipefail

# Resolve sibling scripts relative to this file so the script works from any working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure required binaries are available
for cmd in cargo curl gh sui git; do
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

# Mirror the third-party revs that Sui itself pins at the target tag.
#
# These are raw commit SHAs of *other* repositories, so the testnet-tag substitution above can
# never reach them. Whenever Sui advances one of them, leaving walrus behind produces two
# incompatible copies of the same crate in the dependency graph, and trait impls defined against
# Sui's copy stop applying (for example `TryFrom<&proto::Checkpoint> for Checkpoint`). That broke
# the v1.74.0, v1.75.2, v1.76.0, v1.77.1, v1.79.0, and v1.80.0 bumps, each needing a manual
# follow-up commit, so derive the revs instead of hand-patching them.
SUI_MANIFEST_URL="https://raw.githubusercontent.com/MystenLabs/sui/${NEW_TAG}/Cargo.toml"
echo "Fetching Sui manifest from ${SUI_MANIFEST_URL} ..."
if ! SUI_MANIFEST=$(curl -fsSL "$SUI_MANIFEST_URL"); then
  echo "Error: could not fetch Sui's Cargo.toml for ${NEW_TAG}." >&2
  echo "Check that the tag exists and is pushed." >&2
  exit 1
fi

# Extract the single 40-hex rev that Sui pins for a given dependency repo.
# Errors out unless exactly one distinct rev is found, so an upstream layout change is loud
# rather than silently leaving walrus on a stale rev.
extract_sui_rev() {
  local repo_substring="$1" label="$2" revs
  revs=$(printf '%s\n' "$SUI_MANIFEST" \
    | grep -F "$repo_substring" \
    | grep -oE 'rev = "[0-9a-f]{40}"' \
    | grep -oE '[0-9a-f]{40}' \
    | sort -u) || true  # no match must fall through to the explicit check below, not abort
  if [[ $(printf '%s\n' "$revs" | grep -c .) -ne 1 ]]; then
    echo "Error: expected exactly one ${label} rev in Sui's manifest, found:" >&2
    printf '%s\n' "${revs:-<none>}" | sed 's/^/  /' >&2
    return 1
  fi
  printf '%s' "$revs"
}

SDK_REV=$(extract_sui_rev "sui-rust-sdk" "sui-rust-sdk")
MSIM_REV=$(extract_sui_rev "mysten-sim" "mysten-sim")
echo "Sui ${NEW_TAG} pins sui-rust-sdk=${SDK_REV} mysten-sim=${MSIM_REV}"

# sui-rpc / sui-sdk-types in the workspace manifest.
sed -i -E "/sui-rust-sdk/s|(rev = \")[0-9a-f]{40}|\1${SDK_REV}|g" Cargo.toml

# The tokio / futures-timer patches in the simtest wrapper must track Sui's msim rev.
SIMTEST_SCRIPT="scripts/simtest/cargo-simtest"
if [[ -f "$SIMTEST_SCRIPT" ]]; then
  sed -i -E \
    "/patch\.crates-io\.(tokio|futures-timer)\.rev/s|[0-9a-f]{40}|${MSIM_REV}|g" \
    "$SIMTEST_SCRIPT"
else
  echo "Warning: ${SIMTEST_SCRIPT} not found; skipping msim rev sync" >&2
fi

# Regenerate Cargo.lock.
#
# Cargo treats a version already written in Cargo.lock as a hard pin: if a newly required range
# excludes it, resolution fails outright instead of unlocking the package. The previous `cargo
# check` here swallowed that failure, so the bump could be committed with a Cargo.toml on the new
# tag and a Cargo.lock still on the old one -- which made every Rust CI job fail identically at
# resolution (seen on v1.74.0 and v1.80.0). Unlock offending packages one at a time to keep the
# lock diff minimal, and treat an unresolvable graph as fatal so no inconsistent PR is opened.
echo "Regenerating Cargo.lock ..."
resolve_attempts=0
max_resolve_attempts=5
while true; do
  if resolve_output=$(cargo metadata --format-version 1 2>&1 >/dev/null); then
    break
  fi

  # shellcheck disable=SC2016  # the backticks are literal: cargo quotes crate names with them
  stuck_pkg=$(printf '%s\n' "$resolve_output" \
    | grep -oE 'failed to select a version for `[^`]+`' \
    | head -n 1 \
    | sed -E 's/.*`([^`]+)`.*/\1/') || true  # an unrecognized error must reach the report below

  if [[ -z "$stuck_pkg" ]]; then
    printf '%s\n' "$resolve_output" >&2
    echo "Error: dependency resolution failed for ${NEW_TAG} for a reason other than a lock pin." >&2
    exit 1
  fi

  resolve_attempts=$((resolve_attempts + 1))
  if (( resolve_attempts > max_resolve_attempts )); then
    printf '%s\n' "$resolve_output" >&2
    echo "Error: still unresolved after ${max_resolve_attempts} unlock attempts." >&2
    exit 1
  fi

  echo "  -> unlocking '${stuck_pkg}' (attempt ${resolve_attempts}/${max_resolve_attempts})"
  if ! cargo update -p "$stuck_pkg"; then
    echo "Error: 'cargo update -p ${stuck_pkg}' failed." >&2
    exit 1
  fi
done

# The lock must now agree with the manifests; --locked fails rather than rewriting it.
if ! cargo metadata --locked --format-version 1 >/dev/null 2>&1; then
  echo "Error: Cargo.lock is still inconsistent with Cargo.toml after regeneration." >&2
  exit 1
fi
echo "Cargo.lock is consistent with Cargo.toml."

# A compile check is advisory: source-level API drift needs a human fix, and we still want the PR
# opened so CI reports it. Resolution failures, handled above, are the fatal class.
echo "Running cargo check ..."
if ! cargo check; then
  echo "Warning: cargo check failed; the PR will need manual fixes for API drift" >&2
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

# Push branch using AUTOMERGE_TOKEN so the push comes from the walrus-automerge app
# instead of github-actions[bot]. Pushes made with GITHUB_TOKEN do not trigger CI workflows.
git commit -m "ci: bump Sui testnet version to ${NEW_TAG}"

if [[ -n "${AUTOMERGE_TOKEN:-}" ]]; then
  # Authenticate with the automerge token through an extra header.
  #
  # `tr -d '\n'` is required: `base64` wraps its output at 76 columns, so a long enough token
  # yields a value containing a newline. That newline lands inside the Authorization header, which
  # makes the request malformed and causes GitHub to reset the HTTP/2 stream with "HTTP/2 stream 1
  # was not closed cleanly before end of the underlying stream".
  AUTH_HEADER="$(printf '%s' "x-access-token:${AUTOMERGE_TOKEN}" | base64 | tr -d '\n')"

  # Diagnostic: the token length is not sensitive, and lets us confirm whether a future push
  # failure is caused by an over-long token (see the wrapping note above).
  echo "AUTOMERGE_TOKEN length: ${#AUTOMERGE_TOKEN}" >&2

  # Pass the header through GIT_CONFIG_* rather than `git -c` so the token stays out of the
  # command line, where a process listing would expose it.
  GIT_CONFIG_COUNT=1 \
  GIT_CONFIG_KEY_0="http.https://github.com/.extraheader" \
  GIT_CONFIG_VALUE_0="Authorization: basic ${AUTH_HEADER}" \
    "${SCRIPT_DIR}/git_push_with_retry.sh" -u origin "$BRANCH"
else
  "${SCRIPT_DIR}/git_push_with_retry.sh" -u origin "$BRANCH"
fi

# Generate PR body
BODY="This PR updates the Sui testnet version to ${NEW_TAG}

Revs synced from Sui's manifest at \`${NEW_TAG}\`:
- \`sui-rpc\` / \`sui-sdk-types\`: \`${SDK_REV}\`
- \`mysten-sim\` (tokio / futures-timer patches in \`scripts/simtest/cargo-simtest\`): \`${MSIM_REV}\`"

# Create PR using AUTOMERGE_TOKEN so the `pull_request: opened` event is attributed to the
# walrus-automerge app instead of github-actions[bot]. Events created with GITHUB_TOKEN do not
# trigger CI workflows.
CREATE_TOKEN="${AUTOMERGE_TOKEN:-$GH_TOKEN}"
echo "Creating pull request..."
if PR_OUTPUT=$(GH_TOKEN="$CREATE_TOKEN" gh pr create \
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

# Setting the PR to auto merge.
# Use AUTOMERGE_TOKEN if available so the merge push triggers downstream workflows
# (merges with GITHUB_TOKEN suppress push events to prevent recursive loops).
MERGE_TOKEN="${AUTOMERGE_TOKEN:-$GH_TOKEN}"
if ! GH_TOKEN="$MERGE_TOKEN" gh pr merge --auto --squash --delete-branch "$BRANCH"; then
  echo "Warning: Failed to enable auto-merge for PR" >&2
fi

echo "$PR_URL"
