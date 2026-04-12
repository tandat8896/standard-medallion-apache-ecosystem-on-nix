{
  description = "ETL Lab";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/107cba9eb4a8d8c9f8e9e61266d78d340867913a";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-unstable,
      rust-overlay,
      ...
    }:
    let
      system = "x86_64-linux";
      overlays = [ (import rust-overlay) ];

      pkgs = import nixpkgs {
        inherit system overlays;
        config.allowUnfree = true;
        config.cudaSupport = true;
      };

      pkgsUnstable = import nixpkgs-unstable {
        inherit system;
        config.allowUnfree = true;
      };

      codescene-cli = pkgs.stdenv.mkDerivation rec {
        pname = "codescene-cli";
        version = "latest";
        src = pkgs.fetchzip {
          url = "https://downloads.codescene.io/enterprise/cli/cs-linux-amd64-${version}.zip";
          sha256 = "sha256-zJiC4J0vejgpl0xWPUhMpFf1t4b8XbQaclDCPig0gdY=";
          stripRoot = false;
        };
        nativeBuildInputs = with pkgs; [
          autoPatchelfHook
          makeWrapper
        ];
        buildInputs = with pkgs; [
          stdenv.cc.cc.lib
          glibc
          zlib
        ];
        dontBuild = true;
        dontConfigure = true;
        installPhase = ''
          runHook preInstall
          mkdir -p $out/bin
          cp cs $out/bin/cs
          chmod +x $out/bin/cs
          runHook postInstall
        '';
      };

      rustToolchain = pkgs.rust-bin.stable.latest.default.override {
        extensions = [
          "rust-src"
          "rust-analyzer"
          "clippy"
        ];
      };

      pythonEnv = pkgs.python3.withPackages (ps: [
        ps.numpy
        ps.pytest
        ps.black
        ps.debugpy
        ps.pyspark
        ps.pandas
        ps.polars
        ps.psycopg2-binary
        ps.kafka-python-ng
      ]);

      runtimeLibs = with pkgs; [
        stdenv.cc.cc.lib
        zlib
        linuxPackages.nvidia_x11
        cudaPackages.cuda_cudart
        cudaPackages.libcublas
        cudaPackages.cudnn
        vscode-extensions.vadimcn.vscode-lldb.adapter
      ];

    in
    {
      devShells.${system}.default = pkgs.mkShell {
        strictDeps = true;
        NIX_LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath runtimeLibs;
        NIX_LD = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";

        packages = [
          pythonEnv
          pkgs.openjdk11
          pkgs.apacheKafka
          pkgsUnstable.apache-airflow #
          pkgs.zookeeper
          pkgs.postgresql_15
          pkgs.tree
          rustToolchain
          pkgs.lldb_19
          codescene-cli
          pkgs.just
          pkgs.jq
          pkgs.jqp
        ];

        shellHook = ''
          export LD_LIBRARY_PATH="/usr/lib/wsl/lib:${pkgs.lib.makeLibraryPath runtimeLibs}:$LD_LIBRARY_PATH"
          export RUST_SRC_PATH="${rustToolchain}/lib/rustlib/src/rust/library"

          # Config Java & Airflow
          export JAVA_HOME="${pkgs.openjdk11.home}"
          export AIRFLOW_HOME="$PWD/.airflow"
          mkdir -p $AIRFLOW_HOME
        '';
      };
    };
}
