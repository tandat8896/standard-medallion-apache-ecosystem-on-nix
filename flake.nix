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
      # Spark Kafka jars
      kafkaJars = [
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar";
          sha256 = "0k41as4bg1xxs41j6a2c6lh9ryiv9w3xlj87jq896bfk5sjsvqi6";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar";
          sha256 = "0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar";
          sha256 = "07gc6g2bppz7g3qwf4h2hzsw21qc97q2g7nv1nmz9zkas2iinps7";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar";
          sha256 = "10rzjswra1daf71ip9jiw84dixyrlp8y91p6q0d8pr8mfpp0a1ga";
        })
      ];
      # Flink Kafka connector jar
      flinkKafkaJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar";
        sha256 = "1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06";
      };
      # Flink với kafka connector baked in
      flink = pkgs.stdenv.mkDerivation rec {
        pname = "flink";
        version = "1.19.1";
        src = pkgs.fetchurl {
          url = "https://archive.apache.org/dist/flink/flink-${version}/flink-${version}-bin-scala_2.12.tgz";
          sha256 = "1kmxk7kkkmr6f88m0a7yxp0zfdxii6sr4gzh68cx8wfaxzrnjqar";
        };
        sourceRoot = ".";
        installPhase = ''
          mkdir -p $out
          cp -r flink-${version}/. $out/
          cp ${flinkKafkaJar} $out/lib/flink-connector-kafka-3.2.0-1.19.jar
        '';
      };
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
          pkgsUnstable.apache-airflow
          pkgs.zookeeper
          pkgs.postgresql_15
          pkgs.tree
          rustToolchain
          pkgs.lldb_19
          codescene-cli
          pkgs.just
          pkgs.jq
          pkgs.jqp
          flink
          pkgs.krb5 
        ];
        shellHook = ''
          export LD_LIBRARY_PATH="/usr/lib/wsl/lib:${pkgs.lib.makeLibraryPath runtimeLibs}:$LD_LIBRARY_PATH"
          export RUST_SRC_PATH="${rustToolchain}/lib/rustlib/src/rust/library"
          export JAVA_HOME="${pkgs.openjdk11.home}"

          # Tất cả data dồn vào ETL/infrastructure/<service>-data/
          export AIRFLOW_HOME="$PWD/ETL/infrastructure/airflow-data"
          export SPARK_KAFKA_JARS="${pkgs.lib.concatStringsSep "," kafkaJars}"
          export FLINK_HOME="${flink}"
          export PATH="$FLINK_HOME/bin:$PATH"
          export FLINK_CONF_DIR="$PWD/ETL/infrastructure/flink-data/conf"
          export FLINK_LOG_DIR="$PWD/ETL/infrastructure/flink-data/logs"

          mkdir -p $AIRFLOW_HOME
          mkdir -p $FLINK_LOG_DIR
          export ZOOCFGDIR="$PWD/ETL/infrastructure/zookeeper-data/conf"
          export ZOO_LOG_DIR="$PWD/ETL/infrastructure/zookeeper-data/logs"
          mkdir -p $ZOOCFGDIR
          mkdir -p $ZOO_LOG_DIR
          export KRB5_CONFIG="$PWD/ETL/infrastructure/kerberos-data/conf/krb5.conf"
          export KRB5_KDC_PROFILE="$PWD/ETL/infrastructure/kerberos-data/conf/kdc.conf"
        '';
      };
    };
}
