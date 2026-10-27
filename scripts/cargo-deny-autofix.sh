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
# Advisories ignored in `deny.toml` get the `cargo update` steps too, but not the requirement
# raises, which mostly wait on upstream crates and would cost a compile each week. Ignores whose
# advisory is no longer reported are removed from `deny.toml`.
#
# Must be run from the repository root. Requires cargo, cargo-deny, cargo-upgrade (from cargo-edit),
# curl, git, and jq. Compiles upgraded dependencies, so run it without credentials in the
# environment.
#
# Usage: scripts/cargo-deny-autofix.sh
#
# Outputs (in $OUT_DIR, default: a fresh mktemp dir):
#   fixed.md           - one bullet per advisory fixed, ignored or not
#   remaining.md       - one bullet per advisory that is not ignored and could not be fixed
#   removed-ignores.md - one bullet per ignore removed from `deny.toml`
#   failed-upgrades.md - one bullet per direct dependency whose upgrade broke the build
#   changes.patch      - the changes to the manifests, lockfile, and `deny.toml`, if any
# When $GITHUB_OUTPUT is set, also emits: has_fixes, has_remaining, out_dir.

set -Eeuo pipefail

# The script parses cargo's output, which color codes would break.
export CARGO_TERM_COLOR=never

# Each upgrade attempt compiles the workspace. Stop trying new ones after this many seconds, so the
# job finishes within its timeout and still reports what it fixed.
UPGRADE_DEADLINE=$((SECONDS + 7200))

for cmd in cargo cargo-deny cargo-upgrade curl git jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found in PATH." >&2
    exit 1
  fi
done

# Passing `--config` before the subcommand needs cargo-deny 0.20 or later.
deny_version=$(cargo deny --version | awk '{ print $2 }')
if [[ "$(printf '%s\n' 0.20.0 "$deny_version" | sort -V | head -n 1)" != 0.20.0 ]]; then
  echo "Error: cargo-deny ${deny_version} is too old; this script needs 0.20 or later." >&2
  exit 1
fi

OUT_DIR="${OUT_DIR:-$(mktemp -d)}"
mkdir -p "$OUT_DIR"
FIXED_MD="$OUT_DIR/fixed.md"
REMAINING_MD="$OUT_DIR/remaining.md"
FAILED_UPGRADES_MD="$OUT_DIR/failed-upgrades.md"
REMOVED_IGNORES_MD="$OUT_DIR/removed-ignores.md"
: >"$FIXED_MD"
: >"$REMAINING_MD"
: >"$FAILED_UPGRADES_MD"
: >"$REMOVED_IGNORES_MD"

WORK_DIR="$(mktemp -d "$OUT_DIR/work.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

# Prints the advisory IDs in the `ignore` list of the given cargo-deny config.
ignored_ids() {
  awk '/^ignore = \[/ { in_list = 1; next } in_list && /^\]/ { exit } in_list' "$1" \
    | grep -oE 'RUSTSEC-[0-9]{4}-[0-9]{4}' | sort -u || true
}

# A copy of `deny.toml` without the advisory ignores, so that ignored advisories are flagged too.
NO_IGNORES_CONFIG="$WORK_DIR/deny.toml"
awk '/^ignore = \[/ { print "ignore = []"; skip = 1; next }
  skip && /^\]/ { skip = 0; next }
  !skip' deny.toml >"$NO_IGNORES_CONFIG"
IGNORED_IDS=$(ignored_ids deny.toml)

# Prints one line per advisory error, including ignored ones: "<advisory id>\t<crate>\t<version>".
flagged_advisories() {
  local json="$WORK_DIR/deny.jsonl"
  if cargo deny --all-features --config "$NO_IGNORES_CONFIG" --format json check advisories \
    --hide-inclusion-graph 2>"$json"; then
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
  flagged=$(flagged_advisories) || return 1
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
  local build=false status=0 count after before="$WORK_DIR/before.patch"
  if [[ "$1" == --build ]]; then
    build=true
    shift
  fi
  git diff --binary >"$before"
  if ! count=$(flagged_count); then
    echo "Warning: cargo-deny failed before '$*'; skipping it" >&2
    return 0
  fi
  if ! "$@"; then
    # A spec stops matching once an earlier change has moved the same crate.
    echo "Warning: '$*' failed" >&2
  elif cmp -s "$before" <(git diff --binary); then
    return 0
  elif ! after=$(flagged_count); then
    # For example, a raised requirement that cargo cannot resolve.
    echo "Warning: cargo-deny failed after '$*'; reverting" >&2
  elif ((after >= count)); then
    echo "Note: '$*' fixes no advisory; reverting" >&2
  elif ! $build || cargo check --workspace --all-features --tests; then
    return 0
  else
    echo "Warning: '$*' breaks the build; reverting" >&2
    status=1
  fi
  # Callers run this in a condition, where `set -e` is off; a failed revert must not go unnoticed.
  git checkout -- . || exit 1
  if [[ -s "$before" ]]; then
    git apply "$before" || exit 1
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
      # `--precise` can also fail because a dependent caps the version below the target; a plain
      # update may still reach a fixed release.
      cargo update -p "${name}@${version}" || true
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
    if ((SECONDS > UPGRADE_DEADLINE)); then
      echo "Warning: out of time; skipping the remaining upgrades" >&2
      break
    fi
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
  local id name version added new
  while IFS=$'\t' read -r id name version; do
    [[ -n "$id" ]] || continue
    if grep -qxF "${id}"$'\t'"${name}"$'\t'"${version}" <<<"$2"; then
      continue
    fi
    added=$(comm -13 <(locked_versions "$WORK_DIR/Cargo.lock.before" "$name") \
      <(locked_versions Cargo.lock "$name"))
    # Prefer the version in the same compatible line; an upgraded requirement adds a newer line.
    new=$(compatible_versions "$version" <<<"$added")
    new=$(paste -sd, - <<<"${new:-$added}" | sed 's/,/, /g')
    echo "- [${id}](https://rustsec.org/advisories/${id}): \`${name}\` ${version} → ${new:-removed}"
  done <<<"$1" >"$FIXED_MD"
}

# Prints the advisory lines of $1 whose advisory is not ignored in `deny.toml`.
not_ignored() {
  awk -F'\t' -v ids="$(tr "\n" " " <<<"$IGNORED_IDS")" '
    BEGIN { n = split(ids, list, " "); for (i = 1; i <= n; i++) ignored[list[i]] = 1 }
    NF && !($1 in ignored)' <<<"$1"
}

# Removes the given advisory IDs from the `ignore` list in `deny.toml`, together with their comment
# when no ID under it remains. A line with only `#` ends the comment that introduces the list.
remove_ignores() {
  awk -v ids="$(tr "\n" " " <<<"$1")" '
    function flush() {
      if (!seen_ids || kept != "") printf "%s%s", comments, kept
      comments = ""; kept = ""; seen_ids = 0
    }
    BEGIN { n = split(ids, list, " "); for (i = 1; i <= n; i++) drop[list[i]] = 1 }
    /^ignore = \[/ { in_list = 1; print; next }
    !in_list { print; next }
    /^\]/ { flush(); in_list = 0; print; next }
    /^ *#/ {
      if (seen_ids) flush()
      comments = comments $0 "\n"
      if ($0 ~ /^ *#$/) flush()
      next
    }
    {
      seen_ids = 1
      id = $0
      gsub(/[ ",]/, "", id)
      if (!(id in drop)) kept = kept $0 "\n"
    }' deny.toml >"$WORK_DIR/deny.toml.new"
  mv "$WORK_DIR/deny.toml.new" deny.toml
}

# Removes the ignores whose advisory is no longer reported and lists them.
remove_stale_ignores() {
  local stale id
  stale=$(comm -23 <(echo "$IGNORED_IDS") <(cut -f1 <<<"$1" | sort -u) | grep . || true)
  if [[ -z "$stale" ]]; then
    return 0
  fi
  remove_ignores "$stale"
  while read -r id; do
    echo "- [${id}](https://rustsec.org/advisories/${id}): no longer reported"
  done <<<"$stale" >"$REMOVED_IGNORES_MD"
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
  if [[ -n "$(not_ignored "$remaining")" ]]; then
    echo "==> Upgrading direct dependencies"
    upgrade_direct_dependencies "$(not_ignored "$remaining")"
    remaining=$(flagged_advisories)
  fi
fi

write_fixed "$initial" "$remaining"
write_remaining "$(not_ignored "$remaining")"
remove_stale_ignores "$remaining"
if [[ -s "$FIXED_MD" || -s "$REMOVED_IGNORES_MD" ]]; then
  git diff --binary >"$OUT_DIR/changes.patch"
fi

has_fixes=false
[[ -s "$FIXED_MD" || -s "$REMOVED_IGNORES_MD" ]] && has_fixes=true
has_remaining=false
[[ -s "$REMAINING_MD" ]] && has_remaining=true

echo "==> Summary"
echo "    Fixed:"
sed 's/^/      /' "$FIXED_MD"
echo "    Remaining (need manual attention):"
sed 's/^/      /' "$REMAINING_MD"
echo "    Ignores removed from deny.toml:"
sed 's/^/      /' "$REMOVED_IGNORES_MD"
echo "    Upgrades that broke the build:"
sed 's/^/      /' "$FAILED_UPGRADES_MD"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "has_fixes=$has_fixes"
    echo "has_remaining=$has_remaining"
    echo "out_dir=$OUT_DIR"
  } >>"$GITHUB_OUTPUT"
fi
