#!/usr/bin/env just --justfile

default: (list)

list:
  just --list

# run tests
test *ARGS:
   cargo nextest run {{ARGS}}

# build docker tools image
build-docker-walrus: (build-docker "walrus")

build-docker-walrus-service: (build-docker "walrus-service")

build-docker-walrus-backup: (build-docker "walrus-backup")

build-docker target:
   nix build .#docker-{{target}} -L --out-link result-docker-{{target}}

# build and load specified docker image
build-load-docker target: (build-docker target)
   docker load < result-docker-{{target}}

# build a cargo target or all
build *ARG:
   mold -run cargo build {{ARG}}

# build nix package
build-nix target:
   nix build .#sui-{{target}} -L --out-link result-{{target}}

# build developer nix package
build-dev-tools: (build-nix "dev-tools")

# clean cargo cache
clean-cache:
   cargo clean

# clean sccache cache directory
[unix]
clean-sccache:
   rm -r $(sccache --show-adv-stats | grep -e "Cache location[[:space:]]*Local disk: " | sed 's/.*: "\(.*\)"/\1/')/* || true

# clean cargo and sccache caches
[confirm]
deep-clean: clean-cache clean-sccache