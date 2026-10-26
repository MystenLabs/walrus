#!/usr/bin/env bash
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0
#
# Detects `cargo deny check advisories` errors and tries to fix them without code changes: first
# with `cargo update` of each flagged crate and then of the crates that depend on it directly, then
# by raising the version requirement of each direct dependency that pulls a still-flagged crate in.
# It keeps a change only if it reduces the flagged advisories and, for a raised requirement, the
# workspace still compiles. Follows Sui's `scripts/cargo-deny-autofix.sh`, which does the first step
# only.
#
# Must be run from the repository root. Requires cargo, cargo-deny, cargo-upgrade (from cargo-edit),
# curl, git, and jq. Compiles upgraded dependencies, so run it without credentials in the
# environment.
#
# Usage: scripts/cargo-deny-autofix.sh
#
# Outputs (in $OUT_DIR, default: a fresh mktemp dir):
#   fixed.md           - one bullet per advisory fixed
#   remaining.md       - one bullet per advisory that could not be fixed
#   failed-upgrades.md - one bullet per direct dependency whose upgrade broke the build
#   changes.patch      - the changes to the manifests and lockfile, if any advisory was fixed
# When $GITHUB_OUTPUT is set, also emits: has_fixes, has_remaining, out_dir.

set -Eeuo pipefail

for cmd in cargo cargo-deny cargo-upgrade curl git jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found in PATH." >&2
    exit 1
  fi
done

OUT_DIR="${OUT_DIR:-$(mktemp -d)}"
mkdir -p "$OUT_DIR"
FIXED_MD="$OUT_DIR/fixed.md"
REMAINING_MD="$OUT_DIR/remaining.md"
FAILED_UPGRADES_MD="$OUT_DIR/failed-upgrades.md"
: >"$FIXED_MD"
: >"$REMAINING_MD"
: >"$FAILED_UPGRADES_MD"

WORK_DIR="$(mktemp -d "$OUT_DIR/work.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

# Prints one line per advisory error: "<advisory id>\t<crate>\t<version>".
flagged_advisories() {
  local json="$WORK_DIR/deny.jsonl"
  if cargo deny --all-features --format json check advisories --hide-inclusion-graph 2>"$json"; then
    return 0
  fi
  local flagged
  flagged=$(jq -r '
    select(.type == "diagnostic" and .fields.severity == "error" and .fields.advisory != null)
    | [.fields.advisory.id] + (.fields.labels[0].span | split(" ") | .[0:2])
    | @tsv' "$json" | sort -u)
  if [[ -z "$flagged" ]]; then
    echo "Error: cargo-deny failed without reporting an advisory:" >&2
    cat "$json" >&2
    return 1
  fi
  echo "$flagged"
}

# Prints the number of flagged advisories.
flagged_count() {
  local flagged
  flagged=$(flagged_advisories)
  grep -c . <<<"$flagged" || true
}

# Prints the crates of the given advisory lines as "<crate>@<version>".
crate_specs() {
  awk -F'\t' 'NF { print $2 "@" $3 }' <<<"$1" | sort -u
}

# Runs the given command and keeps its changes only if they reduce the number of flagged advisories
# and, with `--build` as the first argument, the workspace still compiles. Returns 1 if the changes
# broke the build.
try_change() {
  local build=false status=0 count before="$WORK_DIR/before.patch"
  if [[ "$1" == --build ]]; then
    build=true
    shift
  fi
  git diff --binary >"$before"
  count=$(flagged_count)
  if ! "$@"; then
    # A spec stops matching once an earlier change has moved the same crate.
    echo "Warning: '$*' failed" >&2
  elif cmp -s "$before" <(git diff --binary); then
    return 0
  elif (($(flagged_count) >= count)); then
    echo "Note: '$*' fixes no advisory; reverting" >&2
  elif ! $build || cargo check --workspace --all-features --tests; then
    return 0
  else
    echo "Warning: '$*' breaks the build; reverting" >&2
    status=1
  fi
  git checkout -- .
  if [[ -s "$before" ]]; then
    git apply "$before"
  fi
  return "$status"
}

# Prints the newest non-yanked, non-prerelease release of crate $1 that is semver-compatible with
# version $2, read from the crates.io sparse index.
latest_compatible() {
  local name path
  name=$(tr '[:upper:]' '[:lower:]' <<<"$1")
  case ${#name} in
    1) path="1/$name" ;;
    2) path="2/$name" ;;
    3) path="3/${name:0:1}/$name" ;;
    *) path="${name:0:2}/${name:2:2}/$name" ;;
  esac
  curl -fsSL "https://index.crates.io/$path" | jq -rs --arg v "$2" '
    def parts: split(".") | map(tonumber);
    def compat: if .[0] > 0 then [.[0]] elif .[1] > 0 then [0, .[1]] else . end;
    ($v | parts | compat) as $c
    | [.[] | select(.yanked | not) | .vers | select(test("^[0-9]+\\.[0-9]+\\.[0-9]+$"))
      | select(parts | compat == $c)]
    | max_by(parts) // empty' || true
}

# Updates the given crate to its newest semver-compatible release. `--precise` fails when that
# release needs a newer version of a crate that is already locked (`quinn-proto` 0.11.19 needs
# `rand` 0.10.1 or later while `rand` 0.10.0 is locked), so it reads the blocking crate off cargo's
# error, updates both together, and tries again from the crate's new version. This touches only the
# crates in the conflict chain, unlike `--recursive`.
#
# Adapted from `bump` in Sui's `scripts/cargo-deny-autofix.sh` (MystenLabs/sui#28295), which stops
# at the first version a plain `cargo update` reaches. That is not enough when an intermediate
# release has advisories of its own: a plain update moves `quinn-proto` 0.11.14 only to 0.11.15,
# which RUSTSEC-2026-0341 affects.
update_crate() {
  local name="${1%@*}" version="${1#*@}" target log conflict moved
  target=$(latest_compatible "$name" "$version")
  if [[ -z "$target" ]]; then
    cargo update -p "$1"
    return
  fi
  for _ in 1 2 3 4 5; do
    if [[ "$version" == "$target" ]]; then
      return 0
    fi
    if log=$(cargo update -p "${name}@${version}" --precise "$target" 2>&1); then
      echo "$log" >&2
      return 0
    fi
    echo "$log" >&2
    # shellcheck disable=SC2016 # literal backticks from cargo's message
    conflict=$(sed -n 's/.*previously selected package `\([^ ]*\) v\([^`]*\)`.*/\1@\2/p' <<<"$log" \
      | head -n 1)
    if [[ -z "$conflict" ]]; then
      return 0
    fi
    log=$(cargo update -p "${name}@${version}" -p "$conflict" 2>&1) || true
    echo "$log" >&2
    moved=$(sed -n "s/^ *Updating ${name} v${version} -> v\([^ ]*\).*/\1/p" <<<"$log")
    if [[ -z "$moved" ]]; then
      return 0
    fi
    version="$moved"
  done
}

# Prints the registry crates that depend directly on the given crate.
registry_parents() {
  cargo tree --workspace --all-features --target all -e normal,dev,build -i "$1" --depth 1 \
    --prefix none --format '{p}' \
    | tail -n +2 | sed 's/ (\*)$//' \
    | awk 'NF == 2 { sub(/^v/, "", $2); print $1 "@" $2 }' | sort -u
}

# Updates each flagged crate, then the direct parents of the crates still flagged: a fix can need a
# newer version of a crate further up the tree (`imbl-sized-chunks` needs a newer `imbl`).
update_flagged_crates() {
  local spec parent
  while read -r -u 3 spec; do
    try_change update_crate "$spec" || true
  done 3< <(crate_specs "$1")
  while read -r -u 3 parent; do
    try_change cargo update -p "$parent" || true
  done 3< <(
    crate_specs "$(flagged_advisories)" | while read -r spec; do
      registry_parents "$spec"
    done | sort -u
  )
}

# Prints the registry crates that workspace members depend on directly.
direct_registry_dependencies() {
  cargo metadata --format-version 1 --no-deps \
    | jq -r '.packages[].dependencies[] | select(.source // "" | startswith("registry+")) | .name' \
    | sort -u
}

# Prints the direct registry dependencies whose dependency tree contains the given crate.
upgrade_candidates() {
  comm -12 \
    <(direct_registry_dependencies) \
    <(cargo tree --workspace --all-features --target all -e normal,dev,build -i "$1" \
      --prefix none --format '{p}' | cut -d' ' -f1 | sort -u)
}

# Tries to raise the version requirement on every direct dependency that pulls in one of the flagged
# crates to its newest release.
upgrade_direct_dependencies() {
  local spec dependency
  while read -r -u 3 dependency; do
    # `--recursive false` leaves the lockfile to cargo; cargo-upgrade's own lockfile update fails on
    # yanked crates elsewhere in the lockfile.
    if ! try_change --build cargo upgrade --incompatible --recursive false -p "$dependency"; then
      echo "- \`${dependency}\`" >>"$FAILED_UPGRADES_MD"
    fi
  done 3< <(
    crate_specs "$1" | while read -r spec; do
      upgrade_candidates "$spec"
    done | sort -u
  )
}

# Prints the versions of crate $2 locked in lockfile $1.
locked_versions() {
  awk -v name="\"$2\"" '
    $1 == "name" && $3 == name { found = 1; next }
    found && $1 == "version" { gsub(/"/, "", $3); print $3; found = 0 }' "$1" | sort -u
}

# Prints the given versions that are semver-compatible with version $1, one per line.
compatible_versions() {
  awk -v old="$1" '
    function key(v, p) {
      split(v, p, ".")
      return p[1] > 0 ? p[1] : (p[2] > 0 ? "0." p[2] : v)
    }
    key($0) == key(old)'
}

# Writes one bullet per advisory line of $1 whose advisory and crate version are not in $2,
# formatted like Sui's: "- [<id>](<url>): `<crate>` <old> → <new>".
write_fixed() {
  local id name version new
  while IFS=$'\t' read -r id name version; do
    [[ -n "$id" ]] || continue
    if grep -qF "${id}"$'\t'"${name}"$'\t'"${version}" <<<"$2"; then
      continue
    fi
    new=$(comm -13 <(locked_versions "$WORK_DIR/Cargo.lock.before" "$name") \
      <(locked_versions Cargo.lock "$name") | compatible_versions "$version" | paste -sd, - \
      | sed 's/,/, /g')
    echo "- [${id}](https://rustsec.org/advisories/${id}): \`${name}\` ${version} → ${new:-removed}"
  done <<<"$1" >"$FIXED_MD"
}

# Writes one bullet per advisory line of $1, formatted like Sui's.
write_remaining() {
  local id name version
  while IFS=$'\t' read -r id name version; do
    [[ -n "$id" ]] || continue
    echo "- [${id}](https://rustsec.org/advisories/${id}): \`${name}\` ${version}" \
      "(no fix without code changes; needs a manual upgrade or a \`deny.toml\` ignore)"
  done <<<"$1" >"$REMAINING_MD"
}

cp Cargo.lock "$WORK_DIR/Cargo.lock.before"
echo "==> Checking advisories"
initial=$(flagged_advisories)
remaining=""
if [[ -n "$initial" ]]; then
  echo "==> Updating flagged crates"
  update_flagged_crates "$initial"
  remaining=$(flagged_advisories)
  if [[ -n "$remaining" ]]; then
    echo "==> Upgrading direct dependencies"
    upgrade_direct_dependencies "$remaining"
    remaining=$(flagged_advisories)
  fi
fi

write_fixed "$initial" "$remaining"
write_remaining "$remaining"
if [[ -s "$FIXED_MD" ]]; then
  git diff --binary >"$OUT_DIR/changes.patch"
fi

has_fixes=false
[[ -s "$FIXED_MD" ]] && has_fixes=true
has_remaining=false
[[ -s "$REMAINING_MD" ]] && has_remaining=true

echo "==> Summary"
echo "    Fixed:"
sed 's/^/      /' "$FIXED_MD"
echo "    Remaining (need manual attention):"
sed 's/^/      /' "$REMAINING_MD"
echo "    Upgrades that broke the build:"
sed 's/^/      /' "$FAILED_UPGRADES_MD"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "has_fixes=$has_fixes"
    echo "has_remaining=$has_remaining"
    echo "out_dir=$OUT_DIR"
  } >>"$GITHUB_OUTPUT"
fi
