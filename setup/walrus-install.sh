#!/bin/sh
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

msg() {
  echo "walrus-install: note: $*"
}

warn() {
  echo "walrus-install: warning: $*" >&2
}

die() {
  echo "walrus-install: error: $*" >&2
  exit 1
}

force=false
install_dir="$HOME"/.local/bin
network=mainnet

while getopts "fi:n:h" opt; do
  case $opt in
    f)
      force=true
      ;;
    h)
      echo "Usage: walrus-install.sh [-f] [-i <install_dir>]"
      exit 0
      ;;
    i)
      install_dir=$OPTARG
      ;;
    n)
      network=$OPTARG
      ;;
    \?)
      die "invalid option: -$OPTARG"
      ;;
  esac
done


# Detect the target OS.
case "$(uname -s)" in
  Darwin)
    case "$(uname -m)" in
      arm64)
        system=macos-arm64
        ;;
      x86_64)
        system=macos-x86_64
        ;;
      *)
        die "unsupported architecture: $(uname -m)"
        ;;
    esac
    ;;
  Linux)
    case "$(uname -m)" in
      aarch64)
        system=ubuntu-aarch64
        ;;
      x86_64)
        system=ubuntu-x86_64
        ;;
      *)
        die "unsupported architecture: $(uname -m)"
        ;;
    esac
    ;;
  *)
    die "unsupported OS: $(uname -s)"
    ;;
esac
test -n "$system" || die "failed to detect the system"

if [ -x "$install_dir"/walrus ]; then
  msg "walrus binary is already installed [path='$install_dir/walrus', existing_version='$("$install_dir"/walrus --version)']"
  if ! $force; then
    die "re-run with the -f flag to re-install"
  else
    msg "force flag is set, overwriting the existing walrus binary"
  fi
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf '"$tmpdir" EXIT

msg "downloading the latest walrus binary [network='$network', system='$system']"
curl \
  -s \
  https://storage.googleapis.com/mysten-walrus-binaries/walrus-"$network"-latest-"$system" \
  -o "$tmpdir"/walrus

# Now, let's install this binary.
mkdir -p "$install_dir" || die "failed to create $install_dir"
cp "$tmpdir"/walrus "$install_dir"/ || die "failed to copy walrus binary to $install_dir"
chmod 0755 "$install_dir"/walrus || die "failed to set permissions on $install_dir/walrus"
msg "walrus binary has been installed to '$install_dir/walrus' [version='$("$install_dir"/walrus --version)']"

resolved_walrus="$(command -v walrus)"
# Check if $install_dir is in $PATH.
if [ -z "$resolved_walrus" ]; then
  msg "the walrus binary is not yet in your PATH. You will need to add '$install_dir' to your PATH."
elif [ "$resolved_walrus" != "$install_dir"/walrus ]; then
  warn "another walrus binary appears to conflict with this installation." \
    "ensure that $install_dir appears before $(dirname "$resolved_walrus") in your PATH."
fi

# Success.
exit 0
