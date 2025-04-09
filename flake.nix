{
  inputs = {
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    #nixpkgs.url = "nixpkgs/nixos-unstable";
    # we need to wait for https://github.com/NixOS/nixpkgs/pull/387337
    # nixpkgs.url = "github:TomaSajt/nixpkgs?ref=fetch-cargo-vendor-dup";
    # rebased version on master
    nixpkgs.url = "github:poelzi/nixpkgs?ref=fetch-cargo-vendor-dup";

    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      fenix,
      flake-utils,
      nixpkgs,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          # update when toolchain changes
          sha256 = "sha256-X/4ZBHO3iW0fOenQ3foEvscgAPJYl2abspaBThDOukI=";
        };
        # update this hash when dependencies changes
        cargoHash = "sha256-k27v2vWRgjbBLvjL6ErH4K8cLAuqz+weeT1m5csSXHU=";
        # cargoHash = "";

        ##############
        pkgs = nixpkgs.legacyPackages.${system};
        walrusVersion = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
        llvmVersion = pkgs.llvmPackages_14;
      in
      let
        lib = pkgs.lib;
        # we use the llvm toolchain
        stdenv = llvmVersion.stdenv;
        platform = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };
        nativeBuildInputs = with pkgs; [
          git
          pkg-config
          makeWrapper
          llvmVersion.clang
          platform.bindgenHook
          #bintools
          llvmVersion.bintools
        ];
        basePkgs = with pkgs; [
          zstd
        ];
        contractsPkg = pkgs.stdenvNoCC.mkDerivation {
          name = "walrus-contracts";
          version = walrusVersion;

          src = ./contracts;

          installPhase = ''
            mkdir -p $out
            cp -r $src/* $out/
          '';
        };
        configPkg = pkgs.stdenvNoCC.mkDerivation {
          name = "walrus-config";
          version = walrusVersion;

          src = ./setup;

          installPhase = ''
            mkdir -p $out
            cp -r $src/* $out/
          '';
        };

      in
      let
        # builts a sui rust crate
        mkCrate =
          {
            name ? null,
            path ? null,
            bins ? null,
            contracts ? false,
            wrapConfigPkg ? null,            
            extraDeps ? [ ],
            features ? [ ],
            noDefaultFeatures ? false,
            profile ? "release",
            
          }:
          (platform.buildRustPackage {
            pname =
              if !builtins.isNull name then
                name
              else if !builtins.isNull path then
                path
              else
                "sui";
            version = walrusVersion;
            inherit nativeBuildInputs toolchain cargoHash; # platform;

            src = lib.fileset.toSource {
              root = ./.;
              fileset = (
                lib.fileset.unions [
                  ./Cargo.toml
                  ./Cargo.lock
                  ./crates
                  ./contracts
                ]
              );
            };

            cargoBuildFlags =
              (lib.lists.optionals (!builtins.isNull path) [
                "-p"
                path
              ])
              ++ (lib.optionals (!builtins.isNull bins) (
                lib.lists.concatMap (x: [
                  "--bin"
                  x
                ]) bins
              ));
            buildNoDefaultFeatures = noDefaultFeatures;
            buildFeatures = features;

            buildType = profile;

            useFetchCargoVendor = true;

            buildInputs =
              basePkgs
              ++ lib.optionals stdenv.isDarwin (
                with pkgs;
                [
                  darwin.apple_sdk.frameworks.CoreFoundation
                  darwin.apple_sdk.frameworks.CoreServices
                  darwin.apple_sdk.frameworks.IOKit
                  darwin.apple_sdk.frameworks.Security
                  darwin.apple_sdk.frameworks.SystemConfiguration
                ]
              )
              ++ extraDeps
              ++ lib.optional contracts contractsPkg
              ++ lib.optional (!isNull wrapConfigPkg) wrapConfigPkg;

            preBuild = ''
              export GIT_REVISION="${self.rev or self.dirtyRev or "dirty"}";
            '';

            postFixup = if (!isNull wrapConfigPkg) then ''
              if [ -e $out/bin/walrus ]; then
                wrapProgram $out/bin/walrus --set WALRUS_CONFIG ${wrapConfigPkg}/client_config.yaml;
              fi
            '' else "";

            doCheck = true;
            useNextest = true;

            env = {
              ZSTD_SYS_USE_PKG_CONFIG = true;
            };

            outputs = [ "out" ];

            meta = with pkgs.lib; {
              description = "Sui, a next-generation smart contract platform with high throughput, low latency, and an asset-oriented programming model powered by the Move programming language";
              homepage = "https://github.com/mystenLabs/sui";
              changelog = "https://github.com/mystenLabs/sui/blob/${walrusVersion}/RELEASES.md";
              license = with licenses; [
                cc-by-40
                asl20
              ];
              maintainers = with maintainers; [ poelzi ];
              mainProgram = "sui";
            };
          });
        mkDocker =
          {
            name,
            tag ? (self.rev or self.dirtyRev),
            tpkg,
            extraPackages ? [ ],
            cmd ? [ ],
            labels ? { },
            rustLog ? null,
            debug ? false,
            contracts ? false,
          }:
          pkgs.dockerTools.buildImage {
            # pkgs.dockerTools.buildLayeredImage {

            inherit name tag;

            copyToRoot = pkgs.buildEnv {
              name = "image-${name}";
              paths =
                with pkgs.dockerTools;
                [
                  usrBinEnv
                  binSh
                  caCertificates
                  fakeNss
                ]
                ++ [
                  (
                    if debug then
                      # FIXME: why does this not set buildType do debug ?
                      (tpkg.overrideAttrs {
                        buildType = "debug";
                        separateDebugInfo = false;
                      })
                    else
                      tpkg
                  )
                ]
                ++ extraPackages
                ++ lib.optional contracts contractsPkg
                ++ lib.optionals debug [
                  pkgs.bashInteractive
                  pkgs.coreutils
                  pkgs.gdb
                ];
              pathsToLink = [
                "/bin"
                "/lib"
                "/share"
              ];
            };

            config = {
              Cmd = cmd;
              # Cmd = ["${pkgs.bash}/bin/bash"];
              WorkingDir = "/";
              Labels = {
                "git-revision" = builtins.toString (self.rev or self.dirtyRev or "dirty");
                "build-date" = self.lastModifiedDate;
              } // labels;
              Env = (lib.optional debug "PS1=$(pwd) > ")
                ++ (lib.optional (!isNull rustLog) "RUST_LOG=${rustLog}")
                ++ [ "PATH=/bin:" ];
            };

          };
      in
      let
        walruspkgs = {
          walrus-full = mkCrate { };
          walrus-proxy = mkCrate {
            name = "walrus-proxy";
            bins = [ "walrus-proxy" ];
          };
          walrus-service = mkCrate {
            name = "walrus-service";
            wrapConfigPkg = configPkg;
            bins = ["walrus" "walrus-node" "walrus-deploy"];
          };
          walrus-backup = mkCrate {
            name = "walrus-backup";
            features = ["walrus-service/backup"];
            extraDeps = [ pkgs.postgresql ];
            bins = ["walrus-backup"];
          };
          walrus-stress = mkCrate {
            name = "walrus-stress";
            genesis = true;
            bins = ["walrus-stress"];
          };
          walrus = mkCrate {
            name = "walrus";
            wrapConfigPkg = configPkg;
            bins = ["walrus"];
          };
          # sui-light-client = mkCrate { name = "sui-light-client"; };
        };
      in
      let
        dockerImages = {
          docker-walrus-service = {
            name = "walrus-service";
            tpkg = walruspkgs.walrus-service;
            contracts = true;
            extraPackages = [ pkgs.curl pkgs.git ];
          };
          docker-walrus-backup = {
            name = "walrus-backup";
            tpkg = walruspkgs.walrus-backup;
            rustLog = "info,walrus_service::common::event_blob_downloader=warn";
            extraPackages = [ pkgs.curl pkgs.git ];
            contracts = true;
          };
          docker-walrus = {
            name = "walrus";
            tpkg = walruspkgs.walrus;
            cmd = [ "walrus" ];
          };
        };
      in
      {
        devShells.default = pkgs.mkShell.override { inherit stdenv; } {
          inherit nativeBuildInputs;
          RUST_SRC_PATH = "${fenix.packages.${system}.stable.rust-src}/bin/rust-lib/src";
          RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";
          # use sccache also for c
          shellHook = ''
            export CC="sccache $CC"
            export CXX="sccache $CXX"
            # make j an alias for just
            alias j=just
            # complete -F _just -o bashdefault -o default j
          '';
          RUST_BACKTRACE = 1;

          CFLAGS = "-O2";
          CXXFLAGS = "-O2";
          buildInputs =
            nativeBuildInputs
            ++ [ toolchain ]
            ++ (with pkgs; [
              (fenix.packages."${system}".stable.withComponents [
                "clippy"
                "rustfmt"
              ])
              # turbo
              cargo-deny
              cargo-nextest
              just
              mold
              nixfmt-rfc-style
              pnpm
              python3
              sccache
              git
              deno
            ]);
        };
        packages =
          walruspkgs
          // {
            walrus-contracts = contractsPkg;
            default = walruspkgs.walrus;
          }
          // (lib.attrsets.mapAttrs (name: spec: (mkDocker spec)) dockerImages)
          //
            # define debug versions of docker images
            (lib.attrsets.mapAttrs' (
              name: spec: (lib.nameValuePair (name + "-debug") (mkDocker (spec // { debug = true; })))
            ) dockerImages);
      }
    );
}
